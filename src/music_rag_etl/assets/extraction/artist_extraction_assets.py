import polars as pl
from dagster import asset, AssetExecutionContext
from typing import List, Dict, Any, Optional

from music_rag_etl.settings import ARTIST_INDEX, ARTISTS_FILE, BATCH_SIZE
from music_rag_etl.utils.io_helpers import chunk_list, save_to_jsonl
from music_rag_etl.utils.concurrency_helpers import process_items_concurrently
from music_rag_etl.utils.lastfm_helpers import get_artist_info_with_fallback
from music_rag_etl.utils.wikidata_helpers import (
    fetch_wikidata_entities_batch_with_cache,
    resolve_qids_to_labels,
)


def _parse_artist_country(entity_data: Dict[str, Any]) -> Optional[str]:
    """Parses country of origin (P495) or citizenship (P27) QID from a Wikidata entity."""
    claims = entity_data.get("claims", {})
    # Property P495: country of origin
    if "P495" in claims:
        main_snak = claims["P495"][0].get("mainsnak", {})
        if main_snak.get("snaktype") == "value":
            country_id = main_snak.get("datavalue", {}).get("value", {}).get("id")
            return country_id
    # Property P27: country of citizenship
    if "P27" in claims:
        main_snak = claims["P27"][0].get("mainsnak", {})
        if main_snak.get("snaktype") == "value":
            country_id = main_snak.get("datavalue", {}).get("value", {}).get("id")
            return country_id
    return None


def _parse_artist_aliases(entity_data: Dict[str, Any]) -> List[str]:
    """Parses English aliases from a Wikidata entity."""
    aliases = entity_data.get("aliases", {}).get("en", [])
    return [alias["value"] for alias in aliases]


def _parse_artist_mbid(entity_data: Dict[str, Any]) -> Optional[str]:
    """Parses MusicBrainz ID (P434) from a Wikidata entity."""
    claims = entity_data.get("claims", {})
    # Property P434: MusicBrainz ID for artists
    if "P434" in claims:
        main_snak = claims["P434"][0].get("mainsnak", {})
        if main_snak.get("snaktype") == "value":
            mbid = main_snak.get("datavalue", {}).get("value")
            return mbid
    return None


def _enrich_artist_batch(
    artist_batch: List[Dict[str, Any]],
    context: AssetExecutionContext,
    api_key: str,
    api_url: str,
) -> List[Dict[str, Any]]:
    """Worker function to enrich a batch of artists with Wikidata and Last.fm data."""
    enriched_batch = []
    qids_in_batch = [artist["wikidata_id"] for artist in artist_batch]

    # 1. Fetch all Wikidata entities for the batch
    wikidata_entities = fetch_wikidata_entities_batch_with_cache(context, qids_in_batch)

    # 2. Collect all unique country QIDs from the current artist batch
    country_qids = set()
    for qid in qids_in_batch:
        artist_info = wikidata_entities.get(qid, {})
        country_qid = _parse_artist_country(artist_info)
        if country_qid:
            country_qids.add(country_qid)

    # 3. Resolve these country QIDs to labels in a single batch call
    country_labels_map = resolve_qids_to_labels(context, list(country_qids))

    # 4. Process each artist in the batch
    for artist in artist_batch:
        qid = artist["wikidata_id"]
        artist_name = artist["artist"]

        # Get Wikidata info
        wikidata_info = wikidata_entities.get(qid, {})
        country_qid = _parse_artist_country(wikidata_info)
        country_label = country_labels_map.get(country_qid)
        aliases = _parse_artist_aliases(wikidata_info)
        artist_mbid = _parse_artist_mbid(wikidata_info)

        # Get Last.fm data using fallback logic
        lastfm_data = get_artist_info_with_fallback(
            context, artist_name, aliases, artist_mbid, api_key, api_url
        )

        tags = []
        similar_artists = []
        if lastfm_data and lastfm_data.get("artist"):
            artist_data = lastfm_data["artist"]
            tags = [
                tag["name"]
                for tag in artist_data.get("tags", {}).get("tag", [])
                if "name" in tag
            ][:5]
            similar_artists = [
                sim["name"]
                for sim in artist_data.get("similar", {}).get("artist", [])
                if "name" in sim
            ][:5]

        final_record = {
            "id": qid,
            "name": artist_name,
            "aliases": aliases,
            "country": country_label,
            "genres": artist["genres"],
            "tags": tags,
            "similar_artists": similar_artists,
        }
        enriched_batch.append(final_record)

    return enriched_batch


@asset(
    name="artists_extraction_from_artist_index",
    deps=["artist_index_with_relevance"],
    description="Creates artist dataset with enriched details from Wikidata and Last.fm.",
    required_resource_keys={"api_config"},
)
def artists_extraction_from_artist_index(context: AssetExecutionContext):
    """
    Loads artists from the index, enriches them with data from Wikidata (country, aliases)
    and Last.fm (tags, similar artists) using concurrent, cached API calls, and saves
    the result to a new JSONL file.
    """
    context.log.info("Starting artist enrichment process.")
    api_key = context.resources.api_config["lastfm_api_key"].get_value()
    api_url = context.resources.api_config["lastfm_api_url"].get_value()

    # 1. Load upstream data
    artist_df = pl.read_ndjson(ARTIST_INDEX)
    artist_df = artist_df.head(50)
    
    artists_to_process = [
    artist for artist in artist_df.to_dicts() if artist.get("wikipedia_url")
    ]
    context.log.info(f"Loaded {len(artists_to_process)} artists to process.")

    # 2. Chunk data for concurrent processing
    artist_batches = list(chunk_list(artists_to_process, BATCH_SIZE))
    context.log.info(
        f"Split artists into {len(artist_batches)} batches of size {BATCH_SIZE}."
    )

    # 3. Define a partial function for the worker to pass extra args
    from functools import partial

    worker_fn = partial(
        _enrich_artist_batch, context=context, api_key=api_key, api_url=api_url
    )

    # 4. Process batches concurrently
    nested_results = process_items_concurrently(
        items=artist_batches, process_func=worker_fn, logger=context.log
    )

    # 5. Flatten, deduplicate, and save results
    all_results = [item for sublist in nested_results for item in sublist]
    context.log.info(f"Successfully enriched {len(all_results)} artists.")

    seen_ids = set()
    unique_results = []
    for record in all_results:
        if record["id"] not in seen_ids:
            unique_results.append(record)
            seen_ids.add(record["id"])

    context.log.info(
        f"Deduplicated results from {len(all_results)} to {len(unique_results)}."
    )

    save_to_jsonl(unique_results, ARTISTS_FILE)
    context.log.info(f"Successfully saved unique artist data to {ARTISTS_FILE}.")

    return str(ARTISTS_FILE)
