import json
# from pathlib import Path
from typing import List
import urllib.request

from polars import pl
from concurrent.futures import ThreadPoolExecutor, as_completed

from dagster import asset, AssetExecutionContext
from music_rag_etl.settings import (
    PATH_TEMP,
    ARTIST_INDEX,
    DATA_DIR,
    WIKIPEDIA_CACHE_DIR,
    WIKIDATA_ENTITY_URL,
    USER_AGENT
)
from music_rag_etl.utils.extraction_helpers import fetch_wikidata_entity


@asset(
    name="genres_extraction_from_artists",
    deps=["artist_index_with_relevance"],
    description="Extraction of all genres from the Artist index."
)
def genres_extraction_from_artists(context: AssetExecutionContext) -> str:
    # output_filename = "artist_index.jsonl"
    # output_path = DATA_DIR / output_filename

    results = []

    # Read only the "genre_ids" column from the JSONL file
    df = pl.read_ndjson(ARTIST_INDEX, columns=["genres"])

    # Get unique values and convert to Python list
    unique_genre_ids = (
        df["genres"]
        # .explode()  # Flattens [[A, B], [A]] -> [A, B, A]
        # .drop_nulls()  # Removes any nulls/None
        .unique()  # Deduplicates [A, B, A] -> [A, B]
        .to_list()  # Converts to standard Python list
    )

    unique_genre_ids = unique_genre_ids[:10]  #split fir testing ---------------------------------->>>>>>>>>>>>

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_entity = {
            executor.submit(
                fetch_wikidata_entity(genre_id),
                context = context,
            ): genre_id for genre_id in unique_genre_ids
        }

        processed_count = 0

        for future in as_completed(future_to_entity):
            genre_data = future_to_entity[future]
            print(genre_data)

    return ""

"""            # Access the specific entity data
            entity = data.get("entities", {}).get(qid, {})



            try:
                # If text is empty
                if genre_data is {}:
                    continue
                else:
                    results.append(
                        {
                            "wikidata_id": genre_data["wikidata_id"],
                            "genre_label": genre_data["label"],

                        }
                    )
            except Exception as e:
                context.log.error(
                    f"Error processing artist {row['artist']} ({row['wikidata_id']}): {e}"
                    )

        return pl.DataFrame(results)"""
