from pathlib import Path

from music_rag_etl.settings import WIKIDATA_ENTITY_PREFIX
from music_rag_etl.utils.transformation_helpers import clean_text

import json
from pathlib import Path
from typing import Any, Callable, Dict, List

import requests
from tqdm import tqdm

from music_rag_etl.settings import (
    WIKIDATA_BATCH_SIZE,
    WIKIDATA_HEADERS,
    WIKIDATA_SPARQL_URL,
)
from music_rag_etl.utils.request_utils import make_request_with_retries


# Runner for executing and saving data from a SPARQL endpoint.
def run_extraction(
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

    print(f"Starting data extraction for: {label}\n")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as outfile:
        while True:
            batch_num += 1
            try:
                # 1. Build Query for the current batch
                query = get_query_function(
                    start_year=start_year,
                    end_year=end_year,
                    limit=WIKIDATA_BATCH_SIZE,
                    offset=offset
                )

                # 2. Fetch Data
                response = make_request_with_retries(
                    WIKIDATA_SPARQL_URL,
                    params={"query": query, "format": "json"},
                    headers=WIKIDATA_HEADERS,
                )
                data = response.json()
                results: List[Dict[str, Any]] = data.get("results", {}).get(
                    "bindings", []
                )

                num_retrieved = len(results)
                print(f"Retrieved batch {batch_num}: {num_retrieved} records")

                if not results:
                    print("\nNo more results from endpoint. Extraction finished.")
                    break

                # 3. Process and Save
                batch_written_count = 0
                for item in tqdm(
                    results,
                    desc=f"Processing batch {batch_num}",
                    unit="record",
                ):
                    processed_record = record_processor(item)
                    if processed_record:
                        outfile.write(
                            json.dumps(processed_record, ensure_ascii=False) + "\n"
                        )
                        total_written += 1
                        batch_written_count += 1

                tqdm.write(
                    f"Saved {batch_written_count} valid records from batch {batch_num}.\n"
                )

                offset += num_retrieved

            except requests.exceptions.RequestException as e:
                print(f"\nAn unrecoverable error occurred: {e}")
                print("Stopping the script. The data file may be incomplete.")
                return
            except json.JSONDecodeError as e:
                print(f"\nError decoding JSON response: {e}")
                print("Stopping the script. The data file may be incomplete.")
                return
            except Exception as e:
                print(f"\nAn unexpected critical error occurred: {e}")
                return

    print(f"\nTotal records stored in {output_path.name}: {total_written}")



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

    # Find the best available label, prioritizing languages
    label_candidates = [
        _get_value(item, "artistLabel_en"),
        _get_value(item, "artistLabel_es"),
        _get_value(item, "artistLabel_fr"),
        _get_value(item, "artistLabel_de"),
        _get_value(item, "artistLabel"),
    ]
    cleaned_label = next(
        (clean_text(c) for c in label_candidates if c and clean_text(c)),
        None,
    )

    if not cleaned_label:
        return None

    # Extract and clean genres and aliases
    genres_str = _get_value(item, "genres") or ""
    aliases_str = _get_value(item, "aliases") or ""

    return {
        "wikidata_id": artist_uri.replace(WIKIDATA_ENTITY_PREFIX, ""),
        "artist": cleaned_label,
        "aliases": sorted(
            {alias.strip() for alias in aliases_str.split("|") if alias.strip()}
        ),
        "wikipedia_url": _get_value(item, "wikipedia_url"),
        "genres": sorted(
            {genre.strip() for genre in genres_str.split("|") if genre.strip()}
        ),
        "inception": _get_value(item, "date"),
        "linkcount": _get_value(item, "linkcount"),
    }
