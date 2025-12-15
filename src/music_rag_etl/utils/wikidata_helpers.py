import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Generator

import requests
from dagster import AssetExecutionContext

from music_rag_etl import settings
from music_rag_etl.settings import (
    WIKIDATA_ENTITY_URL,
    WIKIPEDIA_CACHE_DIR,
    USER_AGENT,
    WIKIDATA_SPARQL_URL,
    WIKIDATA_HEADERS,
    WIKIDATA_BATCH_SIZE,
)
from music_rag_etl.utils.request_utils import make_request_with_retries


######################################################################
#                      1. HIGH-LEVEL ORCHESTRATORS
#    Main entry points for assets. These functions orchestrate a
#    complete extraction and processing pipeline from start to finish.
######################################################################


def execute_sparql_extraction(
    context: AssetExecutionContext,
    output_path: Path,
    get_query_function: Callable,
    record_processor: Callable,
    label: str,
    **query_params
) -> None:
    """
    Orchestrates fetching, processing, and saving data from a SPARQL endpoint.
    """
    total_written = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as outfile:
        for batch_num, results_batch in enumerate(
            paginate_sparql_query(context, get_query_function, **query_params), start=1
        ):
            context.log.info(f"Processing batch {batch_num} for {label}: {len(results_batch)} records")
            for item in results_batch:
                processed_record = record_processor(item)
                if processed_record:
                    outfile.write(json.dumps(processed_record, ensure_ascii=False) + "\n")
                    total_written += 1
    
    context.log.info(f"Total records stored in {output_path.name}: {total_written}")


######################################################################
#                  2. MID-LEVEL PIPELINE COMPONENTS
#  Reusable components that are composed by the high-level orchestrators.
#  This includes pagination logic and specific data record formatters.
######################################################################


def paginate_sparql_query(
    context: AssetExecutionContext,
    get_query_function: Callable[..., str],
    **query_params,
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    A generator that fetches data in batches from a SPARQL endpoint and yields each batch.
    Handles the pagination logic.
    """
    offset = 0
    while True:
        query = get_query_function(
            **query_params,
            limit=WIKIDATA_BATCH_SIZE,
            offset=offset
        )
        results = fetch_sparql_query(context, query)
        if not results:
            break
        yield results
        offset += len(results)


def get_best_label(
    record: Dict[str, any],
    base_key: str,
    lang_priority: List[str] = ['en', 'es', 'fr', 'de']
) -> Optional[str]:
    """
    Finds the best available label from a SPARQL record based on a language priority list.

    Args:
        record: The dictionary representing a single record from the SPARQL query result.
        base_key: The base name for the label key (e.g., "artistLabel").
        lang_priority: A list of language codes in order of preference (e.g., ['en', 'es']).

    Returns:
        The first non-empty label found, or None if no suitable label is available.
    """
    # First, try specific language labels based on priority
    for lang in lang_priority:
        label = record.get(f"{base_key}_{lang}", {}).get("value")
        if label:
            return label

    # If no prioritized language label is found, try the generic label
    generic_label = record.get(base_key, {}).get("value")
    if generic_label:
        return generic_label
    
    return None


def format_artist_record_from_sparql(item: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Processes a single artist record from the Wikidata SPARQL query result.

    Args:
        item: A dictionary representing a single record from the API response.

    Returns:
        A dictionary containing the cleaned and structured artist information,
        or None if the record is invalid.
    """
    artist_uri = get_sparql_binding_value(item, "artist")
    if not artist_uri:
        return None

    cleaned_label_text = get_best_label(item, "artistLabel")
    if not cleaned_label_text:
        return None
    
    cleaned_label = " ".join(cleaned_label_text.split())

    genres_str = get_sparql_binding_value(item, "genres") or ""
    aliases_str = get_sparql_binding_value(item, "aliases") or ""

    return {
        "wikidata_id": artist_uri.replace(WIKIDATA_ENTITY_URL, ""),
        "artist": cleaned_label,
        "aliases": sorted(
            {alias.strip() for alias in aliases_str.split("|") if alias.strip()}
        ),
        "wikipedia_url": get_sparql_binding_value(item, "wikipedia_url") or "",
        "genres": sorted(
            {genre.strip() for genre in genres_str.split("|") if genre.strip()}
        ),
        "inception": get_sparql_binding_value(item, "date"),
        "linkcount": get_sparql_binding_value(item, "linkcount"),
    }


######################################################################
#                   3. LOW-LEVEL API & PARSING HELPERS
#      Core functions that directly interact with Wikidata API
#   endpoints or parse the raw JSON structure of the API's responses.
######################################################################


def fetch_sparql_query(
    context: AssetExecutionContext,
    query: str
) -> List[Dict[str, Any]]:
    """
    Executes a SPARQL query against the Wikidata endpoint with retries.

    Args:
        context: Dagster asset execution context.
        query: The raw SPARQL query string.

    Returns:
        A list of result dictionaries from the SPARQL query.
    """
    try:

        response = make_request_with_retries(
            context=context,
            url=WIKIDATA_SPARQL_URL,
            method="POST",
            params={"query": query, "format": "json"},
            headers=WIKIDATA_HEADERS,
        )
        data = response.json()
        return data.get("results", {}).get("bindings", [])
    except requests.exceptions.RequestException as e:
        context.log.error(f"An unrecoverable error occurred during SPARQL query: {e}")
        return []
    except json.JSONDecodeError as e:
        context.log.error(f"Error decoding JSON response from SPARQL query: {e}")
        return []


def fetch_wikidata_entities_batch(
    context: AssetExecutionContext, qids: List[str]
) -> Dict[str, Any]:
    """
    Fetches a batch of Wikidata entities from the API.

    Args:
        context: Dagster asset execution context.
        qids: A list of Wikidata QIDs (e.g., ["Q123", "Q456"]).

    Returns:
        The JSON response from the API, typically containing an 'entities' dictionary.
    """
    if not qids:
        return {}

    # Per Wikidata API docs, up to 50 IDs can be joined with a '|'
    id_string = "|".join(qids)
    params = {
        "action": "wbgetentities",
        "ids": id_string,
        "props": "labels",
        "languages": "en",
        "format": "json",
    }

    try:
        response = make_request_with_retries(
            context=context,
            url=settings.WIKIDATA_API_URL,  # Use the new API endpoint URL
            method="GET",
            params=params,
            headers=settings.WIKIDATA_HEADERS,
        )
        return response.json()
    except requests.exceptions.RequestException as e:
        context.log.error(f"Unrecoverable error fetching entity batch: {e}")
        return {}
    except json.JSONDecodeError as e:
        context.log.error(f"Error decoding JSON for entity batch: {e}")
        return {}



def fetch_wikidata_entity(
    context: AssetExecutionContext,
    wikidata_id: str
) -> Dict[str, Any] | None:
    """
    Fetches a Wikidata entity, either from local text cache or the API.

    Uses a GET request with retries.
    """
    if not (wikidata_id and wikidata_id.startswith('Q') and wikidata_id[1:].isdigit()):
        context.log.error(f"Malformed QID: {wikidata_id}")
        return None

    cache_file_path = WIKIPEDIA_CACHE_DIR / f"{wikidata_id}.jsonl"

    if cache_file_path.exists():
        try:
            with open(cache_file_path, 'r', encoding='utf-8') as file:
                line = file.readline()
                if line:
                    return json.loads(line)
        except Exception as e:
            context.log.error(f"Cache read failed for {wikidata_id}, falling back to API: {e}")

    try:
        entity_url = f"{WIKIDATA_ENTITY_URL}{wikidata_id}.json"
        response = make_request_with_retries(
            context=context,
            url=entity_url,
            method="GET",
            headers={"User-Agent": USER_AGENT},
        )
        data = response.json()

        cache_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file_path, 'w', encoding='utf-8') as file:
            file.write(json.dumps(data, ensure_ascii=False) + "\n")
        return data

    except requests.exceptions.RequestException as e:
        context.log.error(f"Error fetching Wikidata entity {wikidata_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        context.log.error(f"Error decoding JSON for Wikidata entity {wikidata_id}: {e}")
        return None
    except Exception as e:
        context.log.error(f"An unexpected error occurred while fetching Wikidata entity {wikidata_id}: {e}")
        return None


def get_sparql_binding_value(data: Dict[str, Any], key: str) -> Any | None:
    """Safely extract the 'value' from a SPARQL binding."""
    return data.get(key, {}).get("value")


def parse_wikidata_entity_label(
    entity_data: Dict[str, Any],
    entity_id: str
) -> Optional[str]:
    """
    Parses the JSON response for a Wikidata entity to extract its English label.

    Args:
        entity_data: The raw dictionary from the Wikidata API response.
        entity_id: The ID of the entity to look for (e.g., "Q188450").

    Returns:
        The English label as a string, or None if not found.
    """
    entity = entity_data.get("entities", {}).get(entity_id, {})
    return entity.get("labels", {}).get("en", {}).get("value")
