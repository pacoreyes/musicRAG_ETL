from unittest.mock import patch, mock_open, call
import json
from pathlib import Path

from dagster import build_asset_context

from music_rag_etl.utils.wikidata_helpers import (
    fetch_wikidata_entities_batch_with_cache,
)
from music_rag_etl.settings import WIKIDATA_CACHE_DIR


@patch("music_rag_etl.utils.wikidata_helpers.fetch_wikidata_entities_batch")
def test_batch_cache_full_hit(mock_fetch_batch):
    """Tests that the API is not called if all QIDs are in the cache."""
    context = build_asset_context()
    qids = ["Q1", "Q2"]
    mock_cache_data = {
        "Q1": {"id": "Q1", "claims": {}},
        "Q2": {"id": "Q2", "claims": {}},
    }

    # Simulate that both files exist
    with patch("pathlib.Path.exists", return_value=True):
        # Simulate reading the two different cache files
        m = mock_open()
        m.side_effect = [
            mock_open(read_data=json.dumps(mock_cache_data["Q1"])).return_value,
            mock_open(read_data=json.dumps(mock_cache_data["Q2"])).return_value,
        ]
        with patch("builtins.open", m):
            results = fetch_wikidata_entities_batch_with_cache(context, qids)

            # Assert API was not called
            mock_fetch_batch.assert_not_called()
            # Assert results are correct
            assert results == mock_cache_data


@patch("music_rag_etl.utils.wikidata_helpers.fetch_wikidata_entities_batch")
def test_batch_cache_full_miss(mock_fetch_batch):
    """Tests that the API is called for all QIDs if none are in the cache."""
    context = build_asset_context()
    qids = ["Q1", "Q2"]
    mock_api_response = {
        "entities": {
            "Q1": {"id": "Q1", "claims": {}},
            "Q2": {"id": "Q2", "claims": {}},
        }
    }
    mock_fetch_batch.return_value = mock_api_response

    # Simulate that no files exist
    with patch("pathlib.Path.exists", return_value=False):
        m = mock_open()
        with patch("builtins.open", m):
            results = fetch_wikidata_entities_batch_with_cache(context, qids)

            # Assert API was called with all missing QIDs
            mock_fetch_batch.assert_called_once_with(context, ["Q1", "Q2"])

            # Assert cache files were written
            m.assert_any_call(WIKIDATA_CACHE_DIR / "Q1.json", "w", encoding="utf-8")
            m.assert_any_call(WIKIDATA_CACHE_DIR / "Q2.json", "w", encoding="utf-8")
            # Assert results are correct
            assert results == mock_api_response["entities"]


import tempfile
import shutil

# ... (existing imports)

@patch("music_rag_etl.utils.wikidata_helpers.fetch_wikidata_entities_batch")
def test_batch_cache_partial_hit(mock_fetch_batch):
    """Tests that the API is only called for the QID not in the cache, using a real file system cache."""
    context = build_asset_context()
    qids = ["Q1", "Q2"]  # Q1 is cached, Q2 is not

    mock_cached_q1 = {"id": "Q1", "claims": {}}
    mock_api_response_q2 = {"entities": {"Q2": {"id": "Q2", "claims": {}}}}
    mock_fetch_batch.return_value = mock_api_response_q2

    # Use a temporary directory for WIKIDATA_CACHE_DIR for this test
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_cache_dir = Path(tmpdir) / "wikidata"

        # Temporarily redirect WIKIDATA_CACHE_DIR to our temp directory
        with patch("music_rag_etl.utils.wikidata_helpers.WIKIDATA_CACHE_DIR", new=temp_cache_dir):
            temp_cache_dir.mkdir(parents=True, exist_ok=True)
            q1_cache_file = temp_cache_dir / "Q1.json"

            # Create the cached file for Q1
            with open(q1_cache_file, "w", encoding="utf-8") as f:
                json.dump(mock_cached_q1, f)

            results = fetch_wikidata_entities_batch_with_cache(context, qids)

            # Assert API was called only with the missing QID
            mock_fetch_batch.assert_called_once_with(context, ["Q2"])

            # Assert that the new cache file for Q2 was written
            q2_cache_file = temp_cache_dir / "Q2.json"
            assert q2_cache_file.exists()
            with open(q2_cache_file, "r", encoding="utf-8") as f:
                cached_q2_data = json.load(f)
            assert cached_q2_data == mock_api_response_q2["entities"]["Q2"]

            # Assert the final results contain both items
            assert "Q1" in results
            assert "Q2" in results
            assert results["Q1"] == mock_cached_q1
            assert results["Q2"] == mock_api_response_q2["entities"]["Q2"]

    # Cleanup is handled by TemporaryDirectory context manager

