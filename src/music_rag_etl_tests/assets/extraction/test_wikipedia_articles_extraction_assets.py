import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, call, AsyncMock

import polars as pl
from dagster import build_asset_context

from music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets import (
    create_wikipedia_articles_dataset,
)
from music_rag_etl.settings import PATH_TEMP


@pytest.mark.asyncio
@patch("pathlib.Path.glob")
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets._clean_cache_directory"
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.merge_jsonl_files"
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.save_to_jsonl"
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.async_fetch_artist_article_payload",
    new_callable=AsyncMock
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.pl.read_ndjson"
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.process_items_concurrently_async",
    new_callable=AsyncMock
)
async def test_create_wikipedia_articles_dataset_temp_file_workflow(
    mock_process_concurrently: AsyncMock,
    mock_read_ndjson: MagicMock,
    mock_fetch_payload: AsyncMock,
    mock_save_to_jsonl: MagicMock,
    mock_merge_jsonl: MagicMock,
    mock_clean_cache: MagicMock,
    mock_glob: MagicMock,
):
    """
    Tests the create_wikipedia_articles_dataset asset workflow using the temporary file strategy.
    """
    # --- Mocks Setup ---
    context = build_asset_context()

    # We mock process_items_concurrently_async to capture the worker function and items.
    # We will then execute the worker function manually to verify logic.
    mock_process_concurrently.return_value = [] # Return value ignored in asset logic, just needs to be awaitable list

    # Mock the glob call to simulate finding the temp files
    expected_temp_files = [
        PATH_TEMP / "0.jsonl",
        PATH_TEMP / "1.jsonl",
    ]
    mock_glob.return_value = expected_temp_files

    # Mock input dataFrames
    artist_df = pl.DataFrame(
        {
            "artist": ["Artist A", "Artist B"],
            "wikipedia_url": ["url_a", "url_b"],
            "genres": [["Q1"], ["Q2"]],  # Corrected length
            "inception_year": [2000, 2001],  # Corrected length
            "wikidata_entity": ["Q123", "Q456"],
            "references_score": [10, 20],  # Corrected length
        }
    )
    genres_df = pl.DataFrame({"wikidata_id": ["Q1"], "genre_label": ["Test Genre"]})
    mock_read_ndjson.side_effect = [artist_df, genres_df]

    # Mock different payloads for each artist
    mock_payload_a = {
        "artist": "Artist A",
        "page_text": "Article A text.",
        "genres": [],
        "inception_year": 2000,
        "wikipedia_url": "url_a",
        "wikidata_id": "Q123",
        "references_score": 10,
    }
    mock_payload_b = {
        "artist": "Artist B",
        "page_text": "Article B text.",
        "genres": [],
        "inception_year": 2000,
        "wikipedia_url": "url_b",
        "wikidata_id": "Q456",
        "references_score": 10,
    }
    mock_fetch_payload.side_effect = [mock_payload_a, mock_payload_b]

    # --- Run the Asset ---
    result_path = await create_wikipedia_articles_dataset(context)

    # --- Manually Execute Worker Logic to Test It ---
    # Retrieve the worker function passed to process_items_concurrently_async
    assert mock_process_concurrently.called
    call_kwargs = mock_process_concurrently.call_args.kwargs
    items = call_kwargs["items"]
    worker_fn = call_kwargs["process_func"] # This is a partial

    # Execute worker for each item
    # Note: worker_fn expects (item) because session is already partialled in?
    # No, in code: worker_fn = partial(async_process_artist_to_temp_file, session=session)
    # The signature of async_process_artist_to_temp_file is (item, session).
    # So calling worker_fn(item) works.
    for item in items:
        await worker_fn(item)

    # --- Assertions ---

    # 1. Assert cache is cleaned at the start and end
    assert mock_clean_cache.call_count == 2
    mock_clean_cache.assert_has_calls([call(PATH_TEMP), call(PATH_TEMP)])

    # 2. Assert save_to_jsonl was called for each artist with a temp file
    assert mock_save_to_jsonl.call_count == 2
    first_call = mock_save_to_jsonl.call_args_list[0]
    second_call = mock_save_to_jsonl.call_args_list[1]

    # Check first call (Artist A)
    # The chunk structure might depend on splitter, but metadata check is robust
    assert first_call.args[0][0]["metadata"]["artist_name"] == "Artist A"
    assert first_call.args[1] == PATH_TEMP / "0.jsonl"

    # Check second call (Artist B)
    assert second_call.args[0][0]["metadata"]["artist_name"] == "Artist B"
    assert second_call.args[1] == PATH_TEMP / "1.jsonl"

    # 3. Assert merge_jsonl_files was called correctly
    mock_merge_jsonl.assert_called_once()
    merge_call_args = mock_merge_jsonl.call_args[0]

    # The asset code uses sorted() on the glob result, so our mocked glob result should be passed directly
    passed_files_list = merge_call_args[0]
    assert passed_files_list == expected_temp_files

    # 4. Assert that the final path is returned
    # The return value comes from the function return, not the mock execution
    # Wait, create_wikipedia_articles_dataset returns WIKIPEDIA_ARTICLES_FILE from settings
    # We didn't patch WIKIPEDIA_ARTICLES_FILE, but imported it.
    # result_path should equal imported WIKIPEDIA_ARTICLES_FILE
    # But wait, result_path is returned by the asset.
    from music_rag_etl.settings import WIKIPEDIA_ARTICLES_FILE
    assert result_path == WIKIPEDIA_ARTICLES_FILE
