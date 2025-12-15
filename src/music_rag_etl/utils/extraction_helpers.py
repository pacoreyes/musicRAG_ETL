import json
from pathlib import Path
from typing import Any, Callable, Dict

from dagster import AssetExecutionContext

from music_rag_etl.utils.transformation_helpers import get_best_label
from music_rag_etl.settings import (
    WIKIDATA_BATCH_SIZE,
    WIKIDATA_ENTITY_URL
)
from music_rag_etl.utils.wikidata_helpers import fetch_sparql_query


def fetch_wikidata_data(
    context: AssetExecutionContext,
    output_path: Path,
    get_query_function: Callable[[int, int, int, int], str],
    record_processor: Callable[[Dict[str, Any]], Dict[str, Any] | None],
    start_year: int,
    end_year: int,
    label: str,
) -> None:
    """
    Fetches data in batches using a SPARQL query and streams them to a JSONL file.

    Args:
        context: Dagster asset execution context.
        output_path: Destination JSONL file.
        get_query_function: Function that returns a formatted SPARQL query string.
        record_processor: Function to process each raw record from the API.
        start_year: The starting year for the data fetch.
        end_year: The ending year for the data fetch.
        label: A descriptive label for the process (e.g., "artists_60s").
    """
    offset = 0
    total_written = 0
    batch_num = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as outfile:
        while True:
            batch_num += 1
            try:
                query = get_query_function(
                    start_year=start_year,
                    end_year=end_year,
                    limit=WIKIDATA_BATCH_SIZE,
                    offset=offset
                )

                results = fetch_sparql_query(context, query)
                num_retrieved = len(results)
                context.log.info(f"Retrieved batch {batch_num} for {label}: {num_retrieved} records")

                if not results:
                    context.log.info(f"No more results from endpoint for {label}. Extraction finished.")
                    break

                batch_written_count = 0
                for item in results:
                    processed_record = record_processor(item)
                    if processed_record:
                        outfile.write(
                            json.dumps(processed_record, ensure_ascii=False) + "\n"
                        )
                        total_written += 1
                        batch_written_count += 1
                
                offset += num_retrieved

            except Exception as e:
                context.log.error(f"An unexpected critical error occurred in batch {batch_num} for {label}: {e}")
                return

    context.log.info(f"Total records stored in {output_path.name}: {total_written}")


def _get_value(data: Dict[str, Any], key: str) -> Any | None:
    """Safely extract the 'value' from a nested dictionary."""
    return data.get(key, {}).get("value")


def process_artist_record(item: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Processes a single artist record from the Wikidata SPARQL query result.

    Args:
        item: A dictionary representing a single record from the API response.

    Returns:
        A dictionary containing the cleaned and structured artist information,
        or None if the record is invalid.
    """
    artist_uri = _get_value(item, "artist")
    if not artist_uri:
        return None

    cleaned_label_text = get_best_label(item, "artistLabel")
    if not cleaned_label_text:
        return None
    
    # Simple string cleaning, as the clean_text from transformation_helpers expects a DataFrame
    cleaned_label = " ".join(cleaned_label_text.split())

    # Extract and clean genres and aliases
    genres_str = _get_value(item, "genres") or ""
    aliases_str = _get_value(item, "aliases") or ""

    return {
        "wikidata_id": artist_uri.replace(WIKIDATA_ENTITY_URL, ""),
        "artist": cleaned_label,
        "aliases": sorted(
            {alias.strip() for alias in aliases_str.split("|") if alias.strip()}
        ),
        "wikipedia_url": _get_value(item, "wikipedia_url") or "",
        "genres": sorted(
            {genre.strip() for genre in genres_str.split("|") if genre.strip()}
        ),
        "inception": _get_value(item, "date"),
        "linkcount": _get_value(item, "linkcount"),
    }
