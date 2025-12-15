import json
from typing import Any, Dict, List, Optional

import requests
from dagster import AssetExecutionContext

from music_rag_etl.settings import (
    WIKIDATA_ENTITY_URL,
    WIKIPEDIA_CACHE_DIR,
    USER_AGENT,
    WIKIDATA_SPARQL_URL,
    WIKIDATA_HEADERS,
)
from music_rag_etl.utils.request_utils import make_request_with_retries


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
