from pathlib import Path
from typing import Dict, Any, Optional

import polars as pl
from dagster import asset, AssetExecutionContext

from music_rag_etl.settings import ARTIST_INDEX, GENRES_FILE
from music_rag_etl.utils.io_helpers import save_to_jsonl
from music_rag_etl.utils.transformation_helpers import extract_unique_ids_from_column
from music_rag_etl.utils.concurrency_helpers import process_items_concurrently
from music_rag_etl.utils.wikidata_helpers import (
    fetch_wikidata_entity,
    parse_wikidata_entity_label,
)


@asset(
    name="genres_extraction_from_artist_index",
    deps=["artist_index_with_relevance"],
    description="Extraction of all genres from the Artist index and saves them to a JSONL file."
)
def genres_extraction_from_artist_index(context: AssetExecutionContext) -> Path:
    """
    Extracts all unique music genre IDs from the artist index, fetches their
    English labels from Wikidata concurrently, and saves the results to a
    JSONL file.
    """
    context.log.info("Starting genre extraction from artist index.")

    # 1. Read artist index and extract unique genre IDs
    df = pl.read_ndjson(ARTIST_INDEX)
    unique_genre_ids = extract_unique_ids_from_column(df, "genres")
    context.log.info(f"Found {len(unique_genre_ids)} unique genre IDs in the artist index.")

    # Slicing for development/testing to avoid long runs
    unique_genre_ids = unique_genre_ids[:10]
    context.log.info(f"Processing a slice of {len(unique_genre_ids)} genres for this run.")

    # 2. Define a worker function for concurrent processing
    def fetch_and_parse_genre(genre_id: str) -> Optional[Dict[str, Any]]:
        """
        Worker function to fetch a Wikidata entity and parse its label.
        To be used with the concurrent processor.
        """
        entity_data = fetch_wikidata_entity(context, genre_id)
        if not entity_data:
            return None
        
        label = parse_wikidata_entity_label(entity_data, genre_id)
        if not label:
            context.log.warning(f"No English label found for genre ID {genre_id}.")
            return None
        
        return {"wikidata_id": genre_id, "genre_label": label}

    # 3. Fetch and process genres concurrently
    context.log.info("Fetching genre labels from Wikidata concurrently...")
    results = process_items_concurrently(
        items=unique_genre_ids,
        process_func=fetch_and_parse_genre,
        max_workers=5
    )

    # 4. Save results to a JSONL file
    save_to_jsonl(results, GENRES_FILE)
    
    context.log.info(f"Successfully saved {len(results)} genres to {GENRES_FILE}")
    return GENRES_FILE

