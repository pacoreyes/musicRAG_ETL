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
from music_rag_etl.utils.concurrency_helpers import process_items_incrementally_async
from music_rag_etl.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch_with_cache,
)
from music_rag_etl.utils.request_utils import create_aiohttp_session


@asset(
    name="extract_genres",
    deps=["preprocess_artist_index"],
    description="Extract Genres dataset genres.jsonl from the Artist Index using the Wikidata API",
    group_name="extraction"
)
async def extract_genres(context: AssetExecutionContext) -> Path:
    """
    Extracts all unique music genre IDs from the artist index, fetches their
    English labels and aliases from Wikidata concurrently, and saves the results
    incrementally to a JSONL file.
    """
    context.log.info("Starting genre extraction from artist index.")

    # 1. Read artist index and extract unique genre IDs
    df = pl.read_ndjson(ARTIST_INDEX)
    df = df.filter(
        pl.col("wikipedia_url").is_not_null() & (pl.col("wikipedia_url") != "")
    )
    unique_genre_ids = extract_unique_ids_from_column(df, "genres")
    context.log.info(
        f"Found {len(unique_genre_ids)} unique genre IDs in the artist index."
    )

    # 2. Create the final output file upfront, ensuring it's empty
    GENRES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(GENRES_FILE, "w", encoding="utf-8") as f:
        pass
    context.log.info(f"Created empty output file at {GENRES_FILE}")

    # 3. Define a worker function for concurrent batch processing
    async def async_fetch_and_parse_genre_batch(
        id_chunk: List[str],
        session: aiohttp.ClientSession,
    ) -> List[Dict[str, Any]]:
        """
        Worker function to fetch a batch of Wikidata entities and parse their labels/aliases.
        """
        batch_results = []
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

            raw_label = genre_entity.get("labels", {}).get("en", {}).get("value")
            if not raw_label:
                context.log.warning(f"No English label found for genre ID {genre_id}.")
                continue
            
            cleaned_label = clean_text_string(raw_label)
            aliases_list = genre_entity.get("aliases", {}).get("en", [])
            cleaned_aliases = [
                clean_text_string(alias["value"])
                for alias in aliases_list
                if alias.get("value")
            ]
            sorted_aliases = sorted(list(set(cleaned_aliases)))

            batch_results.append({
                "id": genre_id,
                "genre_label": cleaned_label,
                "aliases": sorted_aliases
            })
        return batch_results

    # 4. Chunk IDs and process incrementally
    id_chunks = list(chunk_list(unique_genre_ids, CHUNK_SIZE))
    total_chunks = len(id_chunks)
    context.log.info(
        f"Fetching {len(unique_genre_ids)} genre labels in {total_chunks} chunks..."
    )

    processed_chunks = 0
    log_interval = 10  # Log progress every 10 chunks

    async with create_aiohttp_session() as session:
        worker_fn = partial(async_fetch_and_parse_genre_batch, session=session)
        
        results_iterator = process_items_incrementally_async(
            items=id_chunks,
            process_func=worker_fn,
            max_concurrent_tasks=50,
            logger=context.log,
        )

        async for batch_results in results_iterator:
            if batch_results:
                save_to_jsonl(batch_results, GENRES_FILE, mode="a")
            
            processed_chunks += 1
            if processed_chunks % log_interval == 0:
                context.log.info(f"Processed {processed_chunks} / {total_chunks} genre chunks...")

    context.log.info(f"Successfully saved {len(unique_genre_ids)} genres to {GENRES_FILE}")
    return GENRES_FILE
