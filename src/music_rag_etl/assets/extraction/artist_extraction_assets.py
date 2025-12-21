import json
import asyncio
import aiohttp
import polars as pl
from dagster import asset, AssetExecutionContext
from typing import List, Dict, Any, Optional
from functools import partial

from music_rag_etl.settings import ARTIST_INDEX, ARTISTS_FILE, BATCH_SIZE, LASTFM_MAX_RPS
from music_rag_etl.utils.io_helpers import chunk_list, save_to_jsonl
from music_rag_etl.utils.concurrency_helpers import process_items_concurrently_async, process_items_incrementally_async, AsyncRateLimiter
from music_rag_etl.utils.lastfm_helpers import async_get_artist_info_with_fallback
from music_rag_etl.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch_with_cache,
    async_resolve_qids_to_labels,
)
from music_rag_etl.utils.request_utils import create_aiohttp_session


def _parse_artist_country(entity_data: Dict[str, Any]) -> Optional[str]:
    """Parses country of origin (P495) or citizenship (P27) QID from a Wikidata entity."""
    claims = entity_data.get("claims", {})
    # Property P495: country of origin
    if "P495" in claims:
        main_snak = claims["P495"][0].get("mainsnak", {})
        if main_snak.get("snaktype") == "value":
            country_id = main_snak.get("datavalue", {}).get("value", {}).get("id")
            if isinstance(country_id, dict):
                 print(f"DEBUG: Found dict as country_id: {country_id}")
                 return None
            return country_id
    # Property P27: country of citizenship
    if "P27" in claims:
        main_snak = claims["P27"][0].get("mainsnak", {})
        if main_snak.get("snaktype") == "value":
            country_id = main_snak.get("datavalue", {}).get("value", {}).get("id")
            if isinstance(country_id, dict):
                 print(f"DEBUG: Found dict as country_id: {country_id}")
                 return None
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


async def _async_enrich_artist_batch(
    artist_batch: List[Dict[str, Any]],
    context: AssetExecutionContext,
    api_key: str,
    api_url: str,
    session: Optional[aiohttp.ClientSession] = None,
    limiter: Optional[AsyncRateLimiter] = None,
) -> List[Dict[str, Any]]:
    """Async worker function to enrich a batch of artists with Wikidata and Last.fm data."""
    enriched_batch = []
    qids_in_batch = [artist["wikidata_id"] for artist in artist_batch]

    # Create a local session if one wasn't provided, though typically one should be passed.
    # process_items_concurrently_async doesn't easily allow passing a shared session to all
    # unless we bake it into the partial. We will assume a session is managed inside or
    # created per batch if not passed.
    # For best performance, we should ideally share a session.
    should_close_session = False
    if session is None:
        session = create_aiohttp_session()
        should_close_session = True

    try:
        # 1. Fetch all Wikidata entities for the batch
        # This respects the serial constraint via process_items_concurrently_async(limit=1) caller
        wikidata_entities = await async_fetch_wikidata_entities_batch_with_cache(
            context, qids_in_batch, session=session
        )

        # 2. Collect all unique country QIDs from the current artist batch
        country_qids = set()
        for qid in qids_in_batch:
            artist_info = wikidata_entities.get(qid, {})
            country_qid = _parse_artist_country(artist_info)
            if country_qid:
                country_qids.add(country_qid)

        # 3. Resolve these country QIDs to labels in a single batch call
        country_labels_map = await async_resolve_qids_to_labels(
            context, list(country_qids), session=session
        )

        # 4. Process each artist in the batch concurrently for LastFM
        async def process_single_artist(artist: Dict[str, Any]) -> Dict[str, Any]:
            qid = artist["wikidata_id"]
            artist_name = artist["artist"]

            # Get Wikidata info
            wikidata_info = wikidata_entities.get(qid, {})
            country_qid = _parse_artist_country(wikidata_info)
            country_label = country_labels_map.get(country_qid)
            aliases = _parse_artist_aliases(wikidata_info)
            artist_mbid = _parse_artist_mbid(wikidata_info)

            # Get Last.fm data using fallback logic
            lastfm_data = await async_get_artist_info_with_fallback(
                context, artist_name, aliases, artist_mbid, api_key, api_url, session=session, limiter=limiter
            )

            tags = []
            similar_artists = []
            if lastfm_data and lastfm_data.get("artist"):
                artist_data = lastfm_data["artist"]
                tags = [
                    tag["name"]
                    for tag in artist_data.get("tags", {}).get("tag", [])
                    if "name" in tag
                ]
                similar_artists = [
                    sim["name"]
                    for sim in artist_data.get("similar", {}).get("artist", [])
                    if "name" in sim
                ]

            return {
                "id": qid,
                "name": artist_name,
                "aliases": aliases,
                "country": country_label,
                "genres": artist["genres"],
                "tags": tags,
                "similar_artists": similar_artists,
            }

        # Run all LastFM fetches for this batch concurrently
        enriched_batch = await asyncio.gather(
            *[process_single_artist(artist) for artist in artist_batch]
        )

    finally:
        if should_close_session:
            await session.close()

    return list(enriched_batch)


@asset(
    name="artists_extraction_from_artist_index",
    deps=["genres_extraction_from_artist_index"],
    description="Creates artist dataset with enriched details from Wikidata and Last.fm.",
    required_resource_keys={"api_config"},
)
async def artists_extraction_from_artist_index(context: AssetExecutionContext):
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

    # artist_df = artist_df.head(50)

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
    # We create a shared session for better performance if possible, but passing it to 
    # process_items_concurrently_async via partial is tricky if it's not picklable 
    # (though asyncio stuff isn't pickled here, it's just async).
    # Since we are inside an async asset, we can create a session here.
    
    # Prepare output file (empty it)
    ARTISTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ARTISTS_FILE, "w", encoding="utf-8") as f:
        pass

    seen_ids = set()
    total_saved = 0

    # Initialize Global Rate Limiter for Last.fm
    lastfm_limiter = AsyncRateLimiter(max_rps=LASTFM_MAX_RPS)

    async with create_aiohttp_session() as session:
        worker_fn = partial(
            _async_enrich_artist_batch, 
            context=context, 
            api_key=api_key, 
            api_url=api_url, 
            session=session,
            limiter=lastfm_limiter
        )

        # 4. Process batches incrementally
        # max_concurrent_tasks=50 ensures we respect Wikidata serial etiquette for the batch requests.
        # But inside each batch, LastFM requests happen concurrently.
        iterator = process_items_incrementally_async(
            items=artist_batches, 
            process_func=worker_fn, 
            max_concurrent_tasks=50,
            logger=context.log
        )

        async for batch_result in iterator:
            unique_results = []
            for record in batch_result:
                if record["id"] not in seen_ids:
                    unique_results.append(record)
                    seen_ids.add(record["id"])
            
            if unique_results:
                with open(ARTISTS_FILE, "a", encoding="utf-8") as f:
                    for record in unique_results:
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                
                total_saved += len(unique_results)
                context.log.info(f"Saved {len(unique_results)} new artists. Total: {total_saved}")

    context.log.info(f"Successfully finished enrichment. Total artists saved: {total_saved}")

    return str(ARTISTS_FILE)
