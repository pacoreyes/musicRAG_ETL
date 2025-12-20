import json
import hashlib
from typing import Dict, Any, Optional

import requests
from dagster import AssetExecutionContext

from music_rag_etl.settings import LASTFM_CACHE_DIR, LASTFM_REQUEST_TIMEOUT
from music_rag_etl.utils.request_utils import make_request_with_retries


def get_cache_key(text: str) -> str:
    """Creates a SHA256 hash of a string to use as a cache key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fetch_lastfm_data_with_cache(
    context: AssetExecutionContext,
    artist_name: str,
    api_key: str,
    api_url: str,
) -> Optional[Dict[str, Any]]:
    """
    Fetches artist data from the Last.fm API, using a local file cache to
    avoid redundant calls. It now checks for API errors in the response.

    Args:
        context: The Dagster asset execution context.
        artist_name: The name of the artist to query.
        api_key: The Last.fm API key.
        api_url: The Last.fm API base URL.

    Returns:
        A dictionary containing the API response, or None if an error occurs.
    """
    if not all([artist_name, api_key, api_url]):
        context.log.warning("Last.fm API key or URL not provided. Skipping fetch.")
        return None

    cache_key = get_cache_key(artist_name.lower())
    cache_file = LASTFM_CACHE_DIR / f"{cache_key}.json"

    LASTFM_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Also check cached data for errors
                if "error" in data:
                    return None
                context.log.info(f"Using cached Last.fm data for '{artist_name}'.")
                return data
        except (IOError, json.JSONDecodeError) as e:
            context.log.warning(
                f"Could not read Last.fm cache file for '{artist_name}'. Refetching. Error: {e}"
            )

    params = {
        "method": "artist.getInfo",
        "artist": artist_name,
        "api_key": api_key,
        "format": "json",
        "autocorrect": 1,
    }

    try:
        response = make_request_with_retries(
            context=context,
            url=api_url,
            method="GET",
            params=params,
            timeout=LASTFM_REQUEST_TIMEOUT,
        )
        response.raise_for_status()  # Will raise an exception for 4xx/5xx status
        data = response.json()

        # Check for Last.fm specific error payload
        if "error" in data:
            context.log.warning(
                f"Last.fm API error for '{artist_name}': {data.get('message', 'Unknown error')} "
                f"(Code: {data['error']})"
            )
            # Cache the error response to prevent refetching, but return None
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            return None

        # Save valid data to cache
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        return data

    except requests.exceptions.HTTPError as e:
        context.log.warning(
            f"Last.fm request failed for '{artist_name}' with HTTP status {e.response.status_code}."
        )
        return None
    except requests.exceptions.RequestException as e:
        context.log.warning(f"Last.fm request failed for '{artist_name}': {e}")
        return None
    except json.JSONDecodeError as e:
        context.log.warning(f"Last.fm returned invalid JSON for '{artist_name}': {e}")
        return None
