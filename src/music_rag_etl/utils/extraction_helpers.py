import json
from pathlib import Path
from typing import Any, Callable, Dict, List
import requests
import urllib.request


from dagster import AssetExecutionContext

from music_rag_etl.utils.transformation_helpers import clean_text
from music_rag_etl.settings import (
    WIKIDATA_BATCH_SIZE,
    WIKIDATA_HEADERS,
    WIKIDATA_SPARQL_URL,
    WIKIPEDIA_CACHE_DIR,
    USER_AGENT,
    WIKIDATA_ENTITY_URL
)
from music_rag_etl.utils.request_utils import make_request_with_retries


# Runner for executing and saving data from a SPARQL endpoint.
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
                # 1. Build Query for the current batch
                query = get_query_function(
                    start_year=start_year,
                    end_year=end_year,
                    limit=WIKIDATA_BATCH_SIZE,
                    offset=offset
                )

                # 2. Fetch Data
                response = make_request_with_retries(
                    context=context,
                    url=WIKIDATA_SPARQL_URL,
                    params={"query": query, "format": "json"},
                    headers=WIKIDATA_HEADERS,
                )
                data = response.json()
                results: List[Dict[str, Any]] = data.get("results", {}).get(
                    "bindings", []
                )

                num_retrieved = len(results)
                context.log.info(f"Retrieved batch {batch_num}: {num_retrieved} records")

                if not results:
                    context.log.info("No more results from endpoint. Extraction finished.")
                    break

                # 3. Process and Save
                batch_written_count = 0
                for item in results:
                    processed_record = record_processor(item)
                    if processed_record:
                        outfile.write(
                            json.dumps(processed_record, ensure_ascii=False) + "\n"
                        )
                        total_written += 1
                        batch_written_count += 1

                # context.log.info(
                #    f"Saved {batch_written_count} valid records from batch {batch_num}.\n"
                #)

                offset += num_retrieved

            except requests.exceptions.RequestException as e:
                context.log.error(f"An unrecoverable error occurred: {e}")
                context.log.info("Stopping the script. The data file may be incomplete.")
                return
            except json.JSONDecodeError as e:
                context.log.error(f"Error decoding JSON response: {e}")
                context.log.info("Stopping the script. The data file may be incomplete.")
                return
            except Exception as e:
                context.log.error(f"An unexpected critical error occurred: {e}")
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


def fetch_wikidata_entity(
    context: AssetExecutionContext,
    wikidata_id: str
) -> List[Dict[str, Any]]:
    """
    Fetches a Wikipedia page, either from local text cache or the API.
    """
    # Check if QID is well formed "Q###" # = numbers
    # 1. Check if not empty, 2. Check if starts with "Q" 3. Check if the rest (slicing from index 1) is digits
    if not wikidata_id and wikidata_id.startswith('Q') and wikidata_id[1:].isdigit():
        context.log.error(f"Malformed QID: {wikidata_id}")
        return []

    cache_file_path = WIKIPEDIA_CACHE_DIR / f"{wikidata_id}.jsonl"  # entity is stored in JSONL

    # Try cache
    if cache_file_path.exists():
        try:
            with open(cache_file_path, 'r', encoding='utf-8') as file:
                return [json.loads(line) for line in file if line.strip()]
        except Exception as e:
            context.log.error(f"Cache read failed for {wikidata_id}.json, falling back to API: {e}")
    try:
        # Make API request
        entity_wikidata_url = f"{WIKIDATA_ENTITY_URL}{wikidata_id}.json"
        request = urllib.request.Request(entity_wikidata_url, headers={"User-Agent": USER_AGENT})
        # Prepare payload in JSON format
        with open(urllib.request.urlopen(request, timeout=10)) as response:
            data = json.load(response)
            return [data]
    except Exception as e:
        print(f"Error: {e}")
        return []
