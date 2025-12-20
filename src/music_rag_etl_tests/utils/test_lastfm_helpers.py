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
    mock_make_request.return_value.raise_for_status.return_value = None

    # Use mock_open to simulate file system interactions
    m = mock_open()
    with (
        patch("builtins.open", m),
        patch(
            "music_rag_etl.utils.lastfm_helpers.LASTFM_CACHE_DIR", Path("/tmp/cache")
        ),
    ):
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False  # Simulate cache miss

            result = fetch_lastfm_data_with_cache(
                context, artist_name, api_key, "dummy_url"
            )

            # 1. Assert API was called
            mock_make_request.assert_called_once()

            # 2. Assert file was written to
            cache_file_path = Path("/tmp/cache") / f"{mock_cache_key}.json"
            m.assert_called_once_with(cache_file_path, "w", encoding="utf-8")
            handle = m()
            written_data = "".join(call.args[0] for call in handle.write.call_args_list)
            assert json.loads(written_data) == mock_response_data

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
    cache_content = json.dumps(mock_response_data)

    # Use mock_open to simulate file system interactions
    m = mock_open(read_data=cache_content)
    with (
        patch("builtins.open", m),
        patch(
            "music_rag_etl.utils.lastfm_helpers.LASTFM_CACHE_DIR", Path("/tmp/cache")
        ),
    ):
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True  # Simulate cache hit

            result = fetch_lastfm_data_with_cache(
                context, artist_name, api_key, "dummy_url"
            )

            # 1. Assert API was NOT called
            mock_make_request.assert_not_called()

            # 2. Assert file was read from
            m.assert_called_once()
            assert m.call_args[0][0].name.endswith(".json")

            # 3. Assert correct data was returned
            assert result == mock_response_data


@patch("music_rag_etl.utils.lastfm_helpers.make_request_with_retries")
def test_fetch_lastfm_data_api_error(mock_make_request):
    """
    Test that the function handles a JSON response containing a Last.fm error.
    """
    artist_name = "error artist"
    api_key = "test_key"
    context = build_asset_context()
    mock_error_data = {"error": 6, "message": "Artist not found"}
    mock_cache_key = get_cache_key(artist_name.lower())

    # Mock the response from the API call
    mock_make_request.return_value.json.return_value = mock_error_data
    mock_make_request.return_value.raise_for_status.return_value = None

    # Use mock_open to simulate file system interactions
    m = mock_open()
    with (
        patch("builtins.open", m),
        patch(
            "music_rag_etl.utils.lastfm_helpers.LASTFM_CACHE_DIR", Path("/tmp/cache")
        ),
        patch.object(context.log, "warning") as mock_log,
    ):
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False  # Simulate cache miss

            result = fetch_lastfm_data_with_cache(
                context, artist_name, api_key, "dummy_url"
            )

            # 1. Assert correct data was returned (None)
            assert result is None

            # 2. Assert warning was logged
            mock_log.assert_called_once()
            assert "Last.fm API error" in mock_log.call_args[0][0]
            assert "Artist not found" in mock_log.call_args[0][0]

            # 3. Assert error response was cached
            cache_file_path = Path("/tmp/cache") / f"{mock_cache_key}.json"
            m.assert_called_once_with(cache_file_path, "w", encoding="utf-8")
            handle = m()
            written_data = "".join(call.args[0] for call in handle.write.call_args_list)
            assert json.loads(written_data) == mock_error_data
