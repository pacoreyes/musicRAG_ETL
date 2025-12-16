from unittest.mock import patch, mock_open
import json
import hashlib
from pathlib import Path

from dagster import build_asset_context

from music_rag_etl.utils.lastfm_helpers import fetch_lastfm_data_with_cache


def get_cache_key(text: str) -> str:
    """Helper to compute cache key consistently."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@patch("music_rag_etl.utils.lastfm_helpers.make_request_with_retries")
def test_fetch_lastfm_data_cache_miss(mock_make_request):
    """
    Test that the function calls the API when the cache file does not exist
    and then creates the cache file.
    """
    artist_name = "test artist"
    api_key = "test_key"
    context = build_asset_context()
    mock_response_data = {"artist": {"name": artist_name, "tags": {}}}
    mock_cache_key = get_cache_key(artist_name.lower())

    # Mock the response from the API call
    mock_make_request.return_value.json.return_value = mock_response_data

    # Use mock_open to simulate file system interactions
    m = mock_open()
    with patch("builtins.open", m):
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False  # Simulate cache miss

            result = fetch_lastfm_data_with_cache(context, artist_name, api_key)

            # 1. Assert API was called
            mock_make_request.assert_called_once()

            # 2. Assert file was written to
            cache_file_path = (
                Path("music_rag_etl/settings.py").parent.parent
                / "data_volume"
                / ".cache"
                / "last_fm"
                / f"{mock_cache_key}.json"
            )
            m.assert_called_once_with(cache_file_path, "w", encoding="utf-8")
            handle = m()
            handle.write.assert_called_once_with(
                json.dumps(mock_response_data, ensure_ascii=False)
            )

            # 3. Assert correct data was returned
            assert result == mock_response_data


@patch("music_rag_etl.utils.lastfm_helpers.make_request_with_retries")
def test_fetch_lastfm_data_cache_hit(mock_make_request):
    """
    Test that the function reads from the cache file when it exists
    and does not call the API.
    """
    artist_name = "cached artist"
    api_key = "test_key"
    context = build_asset_context()
    mock_response_data = {"artist": {"name": artist_name, "tags": {}}}
    mock_cache_key = get_cache_key(artist_name.lower())
    cache_content = json.dumps(mock_response_data)

    # Mock the response from the API call (should not be called)
    mock_make_request.return_value.json.return_value = {"error": "API called"}

    # Use mock_open to simulate file system interactions
    m = mock_open(read_data=cache_content)
    with patch("builtins.open", m):
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True  # Simulate cache hit

            result = fetch_lastfm_data_with_cache(context, artist_name, api_key)

            # 1. Assert API was NOT called
            mock_make_request.assert_not_called()

            # 2. Assert file was read from
            cache_file_path = (
                Path("music_rag_etl/settings.py").parent.parent
                / "data_volume"
                / ".cache"
                / "last_fm"
                / f"{mock_cache_key}.json"
            )
            m.assert_called_once_with(cache_file_path, "r", encoding="utf-8")

            # 3. Assert correct data was returned
            assert result == mock_response_data
