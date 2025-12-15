from pathlib import Path
from typing import Dict, Any, Optional, List

import polars as pl
from dagster import asset, AssetExecutionContext

from music_rag_etl.settings import ARTIST_INDEX, GENRES_FILE, CHUNK_SIZE
from music_rag_etl.utils.io_helpers import save_to_jsonl, chunk_list
from music_rag_etl.utils.transformation_helpers import extract_unique_ids_from_column
from music_rag_etl.utils.concurrency_helpers import process_items_concurrently
from music_rag_etl.utils.wikidata_helpers import (
    fetch_wikidata_entities_batch,
    parse_wikidata_entity_label,
)


@asset(
    name="genres_extraction_from_artist_index",
    deps=["artist_index_with_relevance"],
    description="Extraction of all genres (dict: QID/label) from the Artist index and saves them to a JSONL file."
)
def genres_extraction_from_artist_index(context: AssetExecutionContext) -> Path:
    """
    Extracts all unique music genre IDs from the artist index, fetches their
    English labels from Wikidata concurrently in batches, and saves the
    results to a JSONL file.
    """
    context.log.info("Starting genre extraction from artist index.")

    # 1. Read artist index and extract unique genre IDs
    df = pl.read_ndjson(ARTIST_INDEX)
    unique_genre_ids = extract_unique_ids_from_column(df, "genres")
    context.log.info(f"Found {len(unique_genre_ids)} unique genre IDs in the artist index.")

    context.log.info(f"Processing all {len(unique_genre_ids)} genres for this run.")

    # 2. Define a worker function for concurrent batch processing
    def fetch_and_parse_genre_batch(
        id_chunk: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Worker function to fetch a batch of Wikidata entities and parse their labels.
        """
        batch_results = []
        entity_data = fetch_wikidata_entities_batch(context, id_chunk)
        if not entity_data:
            return []

        for genre_id in id_chunk:
            label = parse_wikidata_entity_label(entity_data, genre_id)
            if not label:
                context.log.warning(f"No English label found for genre ID {genre_id}.")
                continue
            batch_results.append({"wikidata_id": genre_id, "genre_label": label})
        return batch_results

    # 3. Chunk IDs and process concurrently
    id_chunks = list(chunk_list(unique_genre_ids, CHUNK_SIZE))
    context.log.info(
        f"Fetching {len(unique_genre_ids)} genre labels in {len(id_chunks)} chunks..."
    )

    # Process chunks of IDs concurrently
    nested_results = process_items_concurrently(
        items=id_chunks,
        process_func=fetch_and_parse_genre_batch,
        max_workers=5,  # Max 5 simultaneous connections as requested
    )

    # Flatten the list of lists into a single list
    results = [item for sublist in nested_results for item in sublist]

    # 4. Save results to a JSONL file
    save_to_jsonl(results, GENRES_FILE)

    context.log.info(f"Successfully saved {len(results)} genres to {GENRES_FILE}")
    return GENRES_FILE

