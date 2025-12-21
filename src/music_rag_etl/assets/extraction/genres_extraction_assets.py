import asyncio
import aiohttp
from pathlib import Path
from typing import Dict, Any, List
from functools import partial

import polars as pl
from dagster import asset, AssetExecutionContext

from music_rag_etl.settings import ARTIST_INDEX, GENRES_FILE, CHUNK_SIZE
from music_rag_etl.utils.io_helpers import save_to_jsonl, chunk_list
from music_rag_etl.utils.transformation_helpers import (
    extract_unique_ids_from_column,
    clean_text_string,
)
from music_rag_etl.utils.concurrency_helpers import process_items_concurrently_async
from music_rag_etl.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch_with_cache,
)
from music_rag_etl.utils.request_utils import create_aiohttp_session


@asset(
    name="genres_extraction_from_artist_index",
    deps=["artist_index_preprocess"],
    description="Extraction of all genres (dict: QID/label/aliases) from the Artist index and saves them to a JSONL file.",
)
async def genres_extraction_from_artist_index(context: AssetExecutionContext) -> Path:
    """
    Extracts all unique music genre IDs from the artist index, fetches their
    English labels and aliases from Wikidata concurrently in batches (with caching),
    and saves the results to a JSONL file.
    """
    context.log.info("Starting genre extraction from artist index.")

    # 1. Read artist index and extract unique genre IDs
    df = pl.read_ndjson(ARTIST_INDEX)

    # Filter for artists with a valid Wikipedia URL
    df = df.filter(
        pl.col("wikipedia_url").is_not_null() & (pl.col("wikipedia_url") != "")
    )

    unique_genre_ids = extract_unique_ids_from_column(df, "genres")
    context.log.info(
        f"Found {len(unique_genre_ids)} unique genre IDs in the artist index."
    )

    context.log.info(f"Processing all {len(unique_genre_ids)} genres for this run.")

    # 2. Define a worker function for concurrent batch processing
    async def async_fetch_and_parse_genre_batch(
        id_chunk: List[str],
        session: aiohttp.ClientSession,
    ) -> List[Dict[str, Any]]:
        """
        Worker function to fetch a batch of Wikidata entities and parse their labels/aliases.
        """
        batch_results = []
        # Use cached fetcher. It returns a flat dict of {qid: entity_data}.
        entity_data_map = await async_fetch_wikidata_entities_batch_with_cache(
            context, id_chunk, session=session
        )
        if not entity_data_map:
            return []

        for genre_id in id_chunk:
            genre_entity = entity_data_map.get(genre_id)
            if not genre_entity:
                context.log.warning(f"No entity data found for genre ID {genre_id} in batch response.")
                continue

            # Parse Label directly from the entity
            raw_label = genre_entity.get("labels", {}).get("en", {}).get("value")
            if not raw_label:
                context.log.warning(f"No English label found for genre ID {genre_id}.")
                continue
            
            cleaned_label = clean_text_string(raw_label)

            # Parse Aliases directly from the entity
            aliases_list = genre_entity.get("aliases", {}).get("en", [])
            cleaned_aliases = []
            for alias in aliases_list:
                value = alias.get("value")
                if value:
                    cleaned = clean_text_string(value)
                    if cleaned:
                        cleaned_aliases.append(cleaned)
            
            sorted_aliases = sorted(list(set(cleaned_aliases)))

            batch_results.append({
                "wikidata_id": genre_id,
                "genre_label": cleaned_label,
                "aliases": sorted_aliases
            })
        return batch_results

    # 3. Chunk IDs and process concurrently
    id_chunks = list(chunk_list(unique_genre_ids, CHUNK_SIZE))
    context.log.info(
        f"Fetching {len(unique_genre_ids)} genre labels in {len(id_chunks)} chunks..."
    )

    async with create_aiohttp_session() as session:
        # Process chunks of IDs concurrently
        worker_fn = partial(async_fetch_and_parse_genre_batch, session=session)
        
        nested_results = await process_items_concurrently_async(
            items=id_chunks,
            process_func=worker_fn,
            max_concurrent_tasks=50,
            logger=context.log,
        )

    # Flatten the list of lists into a single list
    results = [item for sublist in nested_results for item in sublist]

    # 4. Save results to a JSONL file
    save_to_jsonl(results, GENRES_FILE)

    context.log.info(f"Successfully saved {len(results)} genres to {GENRES_FILE}")
    return GENRES_FILE
