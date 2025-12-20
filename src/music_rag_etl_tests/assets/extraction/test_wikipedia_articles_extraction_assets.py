from pathlib import Path
from unittest.mock import patch, MagicMock, call
from concurrent.futures import ThreadPoolExecutor

import polars as pl
from dagster import build_asset_context

from music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets import (
    create_wikipedia_articles_dataset,
)
from music_rag_etl.settings import WIKIDATA_ENTITY_PREFIX_URL, PATH_TEMP


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
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.fetch_artist_article_payload"
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.pl.read_ndjson"
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.wikipediaapi.Wikipedia"
)
@patch(
    "music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.ThreadPoolExecutor"
)
def test_create_wikipedia_articles_dataset_temp_file_workflow(
    mock_executor: MagicMock,
    mock_wiki_api: MagicMock,
    mock_read_ndjson: MagicMock,
    mock_fetch_payload: MagicMock,
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

    # Mock ThreadPoolExecutor to run sequentially in tests
    mock_executor.return_value.__enter__.return_value.map = map

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
        "wikidata_entity": "Q123",
        "references_score": 10,
    }
    mock_payload_b = {
        "artist": "Artist B",
        "page_text": "Article B text.",
        "genres": [],
        "inception_year": 2000,
        "wikipedia_url": "url_b",
        "wikidata_entity": "Q456",
        "references_score": 10,
    }
    mock_fetch_payload.side_effect = [mock_payload_a, mock_payload_b]

    # --- Run the Asset ---
    result_path = create_wikipedia_articles_dataset(context)

    # --- Assertions ---

    # 1. Assert cache is cleaned at the start and end
    assert mock_clean_cache.call_count == 2
    mock_clean_cache.assert_has_calls([call(PATH_TEMP), call(PATH_TEMP)])

    # 2. Assert save_to_jsonl was called for each artist with a temp file
    assert mock_save_to_jsonl.call_count == 2
    first_call = mock_save_to_jsonl.call_args_list[0]
    second_call = mock_save_to_jsonl.call_args_list[1]

    # Check first call (Artist A)
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
    assert isinstance(result_path, Path)
