import json
import hashlib
from typing import Dict, Any, Optional, List

import requests
from dagster import AssetExecutionContext

from music_rag_etl.settings import LASTFM_CACHE_DIR, LASTFM_REQUEST_TIMEOUT
from music_rag_etl.utils.request_utils import make_request_with_retries


def get_cache_key(text: str) -> str:
    """Creates a SHA256 hash of a string to use as a cache key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _cache_lastfm_data(artist_name: str, data: Dict[str, Any]):
    """Helper function to cache Last.fm data."""
    cache_key = get_cache_key(artist_name.lower())
    cache_file = LASTFM_CACHE_DIR / f"{cache_key}.json"
    LASTFM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def get_artist_info_with_fallback(
    context: AssetExecutionContext,
    artist_name: str,
    aliases: List[str],
    artist_mbid: Optional[str],
    api_key: str,
    api_url: str,
) -> Optional[Dict[str, Any]]:
    """
    Fetches artist data from Last.fm, trying the primary name first, then
    falling back to aliases if the initial lookup fails. Validates against MBID.

    Args:
        context: The Dagster asset execution context.
        artist_name: The primary name of the artist.
        aliases: A list of alternative names for the artist.
        artist_mbid: The MusicBrainz ID of the artist for validation.
        api_key: The Last.fm API key.
        api_url: The Last.fm API base URL.

    Returns:
        A dictionary containing the API response, or None if not found.
    """
    # 1. Try the primary artist name
    main_artist_data = fetch_lastfm_data_with_cache(
        context, artist_name, api_key, api_url, artist_mbid
    )
    if main_artist_data:
        return main_artist_data

    # 2. If not found, try aliases
    if aliases:
        for alias in aliases:
            context.log.info(f"Trying alias '{alias}' for artist '{artist_name}'.")
            alias_data = fetch_lastfm_data_with_cache(
                context, alias, api_key, api_url, artist_mbid
            )
            if alias_data:
                context.log.info(
                    f"Found artist '{artist_name}' on Last.fm using alias '{alias}'."
                )
                # Cache the result under the primary artist name for future lookups
                _cache_lastfm_data(artist_name, alias_data)
                return alias_data

    context.log.warning(f"Could not find artist '{artist_name}' on Last.fm using primary name or aliases.")
    return None


def fetch_lastfm_data_with_cache(
    context: AssetExecutionContext,
    artist_name: str,
    api_key: str,
    api_url: str,
    artist_mbid: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetches artist data from the Last.fm API, using a local file cache.
    Validates against the MusicBrainz ID (MBID) if provided.

    Args:
        context: The Dagster asset execution context.
        artist_name: The name of the artist to query.
        api_key: The Last.fm API key.
        api_url: The Last.fm API base URL.
        artist_mbid: The MusicBrainz ID to validate against.

    Returns:
        A dictionary containing the API response, or None if an error occurs.
    """
    if not all([artist_name, api_key, api_url]):
        context.log.warning("Last.fm API key or URL not provided. Skipping fetch.")
        return None

    cache_key = get_cache_key(artist_name.lower())
    cache_file = LASTFM_CACHE_DIR / f"{cache_key}.json"

    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "error" in data:
                    return None  # Cached error, treat as not found
                context.log.info(f"Using cached Last.fm data for '{artist_name}'.")
                return data
        except (IOError, json.JSONDecodeError) as e:
            context.log.warning(f"Could not read cache for '{artist_name}'. Refetching. Error: {e}")

    params = {
        "method": "artist.getInfo",
        "artist": artist_name,
        "api_key": api_key,
        "format": "json",
        "autocorrect": 1,
    }

    try:
        response = make_request_with_retries(
            context=context, url=api_url, method="GET", params=params, timeout=LASTFM_REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            context.log.warning(
                f"Last.fm API error for '{artist_name}': {data.get('message', 'Unknown error')} "
                f"(Code: {data['error']})"
            )
            _cache_lastfm_data(artist_name, data)  # Cache the error
            return None

        # MBID Validation
        if artist_mbid:
            response_mbid = data.get("artist", {}).get("mbid")
            if response_mbid and response_mbid != artist_mbid:
                context.log.warning(
                    f"MBID mismatch for '{artist_name}'. Wikidata MBID: {artist_mbid}, "
                    f"Last.fm MBID: {response_mbid}. Skipping."
                )
                return None  # Mismatch, so we treat it as not found

        _cache_lastfm_data(artist_name, data)  # Cache valid data
        return data

    except requests.exceptions.RequestException as e:
        context.log.warning(f"Last.fm request failed for '{artist_name}': {e}")
        return None
    except json.JSONDecodeError as e:
        context.log.warning(f"Last.fm returned invalid JSON for '{artist_name}': {e}")
        return None