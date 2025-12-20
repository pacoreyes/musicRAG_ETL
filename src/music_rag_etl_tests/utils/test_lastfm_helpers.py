from unittest.mock import patch, mock_open, call
import json
import hashlib
from pathlib import Path

from dagster import build_asset_context

from music_rag_etl.utils.lastfm_helpers import (
    fetch_lastfm_data_with_cache,
    get_artist_info_with_fallback,
    _cache_lastfm_data,
)


def get_cache_key(text: str) -> str:
    """Helper to compute cache key consistently."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --- Tests for fetch_lastfm_data_with_cache ---


@patch("music_rag_etl.utils.lastfm_helpers.make_request_with_retries")
@patch("music_rag_etl.utils.lastfm_helpers._cache_lastfm_data")
def test_fetch_lastfm_data_cache_miss(mock_cache, mock_make_request):
    artist_name = "test artist"
    context = build_asset_context()
    mock_response_data = {"artist": {"name": artist_name, "mbid": "123"}}

    mock_make_request.return_value.json.return_value = mock_response_data
    mock_make_request.return_value.raise_for_status.return_value = None

    with patch("pathlib.Path.exists", return_value=False):
        result = fetch_lastfm_data_with_cache(
            context, artist_name, "key", "url", artist_mbid="123"
        )
        assert result == mock_response_data
        mock_make_request.assert_called_once()
        mock_cache.assert_called_once_with(artist_name, mock_response_data)


@patch("music_rag_etl.utils.lastfm_helpers.make_request_with_retries")
def test_fetch_lastfm_data_cache_hit(mock_make_request):
    artist_name = "cached artist"
    context = build_asset_context()
    mock_response_data = {"artist": {"name": artist_name}}
    cache_content = json.dumps(mock_response_data)

    m = mock_open(read_data=cache_content)
    with patch("builtins.open", m), patch("pathlib.Path.exists", return_value=True):
        result = fetch_lastfm_data_with_cache(context, artist_name, "key", "url")
        assert result == mock_response_data
        mock_make_request.assert_not_called()


@patch("music_rag_etl.utils.lastfm_helpers.make_request_with_retries")
@patch("music_rag_etl.utils.lastfm_helpers._cache_lastfm_data")
def test_fetch_lastfm_data_mbid_mismatch(mock_cache, mock_make_request):
    artist_name = "mismatch artist"
    context = build_asset_context()
    mock_response_data = {"artist": {"name": artist_name, "mbid": "wrong-id"}}

    mock_make_request.return_value.json.return_value = mock_response_data
    mock_make_request.return_value.raise_for_status.return_value = None

    with patch("pathlib.Path.exists", return_value=False):
        with patch.object(context.log, "warning") as mock_log:
            result = fetch_lastfm_data_with_cache(
                context, artist_name, "key", "url", artist_mbid="correct-id"
            )
            assert result is None
            mock_log.assert_called_once()
            assert "MBID mismatch" in mock_log.call_args[0][0]
            mock_cache.assert_not_called()  # Should not cache a mismatch


# --- Tests for get_artist_info_with_fallback ---


@patch("music_rag_etl.utils.lastfm_helpers.fetch_lastfm_data_with_cache")
def test_fallback_primary_name_success(mock_fetch):
    """Test that the primary name is used successfully on the first try."""
    context = build_asset_context()
    artist_name = "Primary Artist"
    aliases = ["Alias 1", "Alias 2"]
    mock_data = {"artist": {"name": artist_name, "mbid": "123"}}
    mock_fetch.return_value = mock_data

    result = get_artist_info_with_fallback(
        context, artist_name, aliases, "123", "key", "url"
    )

    assert result == mock_data
    mock_fetch.assert_called_once_with(
        context, artist_name, "key", "url", "123"
    )


@patch("music_rag_etl.utils.lastfm_helpers._cache_lastfm_data")
@patch("music_rag_etl.utils.lastfm_helpers.fetch_lastfm_data_with_cache")
def test_fallback_to_alias_success(mock_fetch, mock_cache):
    """Test fallback to an alias when the primary name fails."""
    context = build_asset_context()
    artist_name = "Primary Artist"
    alias_name = "Alias 1"
    aliases = [alias_name, "Alias 2"]
    mbid = "123"
    mock_data = {"artist": {"name": alias_name, "mbid": mbid}}

    # Primary fails, alias succeeds
    mock_fetch.side_effect = [None, mock_data]

    result = get_artist_info_with_fallback(
        context, artist_name, aliases, mbid, "key", "url"
    )

    assert result == mock_data
    # Called for primary name, then for the successful alias
    assert mock_fetch.call_count == 2
    mock_fetch.assert_has_calls(
        [
            call(context, artist_name, "key", "url", mbid),
            call(context, alias_name, "key", "url", mbid),
        ]
    )
    # Important: Check that the successful data is cached under the PRIMARY name
    mock_cache.assert_called_once_with(artist_name, mock_data)


@patch("music_rag_etl.utils.lastfm_helpers.fetch_lastfm_data_with_cache")
def test_fallback_all_fail(mock_fetch):
    """Test that None is returned when primary and all aliases fail."""
    context = build_asset_context()
    artist_name = "Nowhere Artist"
    aliases = ["Alias 1", "Alias 2"]
    mbid = "123"

    # All lookups fail
    mock_fetch.return_value = None

    result = get_artist_info_with_fallback(
        context, artist_name, aliases, mbid, "key", "url"
    )

    assert result is None
    # Called for primary name and both aliases
    assert mock_fetch.call_count == 3


@patch("music_rag_etl.utils.lastfm_helpers._cache_lastfm_data")
def test__cache_lastfm_data():
    """Test the internal caching helper function."""
    artist_name = "Cache Test Artist"
    data = {"artist": "data"}
    mock_cache_key = get_cache_key(artist_name.lower())
    expected_path = Path("/tmp/cache") / f"{mock_cache_key}.json"

    m = mock_open()
    with patch("builtins.open", m), patch(
        "music_rag_etl.utils.lastfm_helpers.LASTFM_CACHE_DIR", Path("/tmp/cache")
    ):
        _cache_lastfm_data(artist_name, data)
        m.assert_called_once_with(expected_path, "w", encoding="utf-8")
        handle = m()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        assert json.loads(written_data) == data