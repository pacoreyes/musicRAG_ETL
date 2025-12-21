import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path

import polars as pl
from dagster import build_asset_context

from music_rag_etl.assets.extraction.genres_extraction_assets import (
    genres_extraction_from_artist_index,
)
from music_rag_etl.settings import GENRES_FILE


@pytest.mark.asyncio
@patch("music_rag_etl.assets.extraction.genres_extraction_assets.pl.read_ndjson")
@patch(
    "music_rag_etl.assets.extraction.genres_extraction_assets.extract_unique_ids_from_column"
)
@patch("music_rag_etl.assets.extraction.genres_extraction_assets.save_to_jsonl")
@patch(
    "music_rag_etl.assets.extraction.genres_extraction_assets.async_fetch_wikidata_entities_batch_with_cache",
    new_callable=AsyncMock
)
@patch(
    "music_rag_etl.assets.extraction.genres_extraction_assets.clean_text_string"
)
async def test_genres_extraction_from_artist_index(
    mock_clean_text,
    mock_fetch_batch,
    mock_save_to_jsonl,
    mock_extract_unique_ids_from_column,
    mock_read_ndjson,
):
    # Arrange
    context = build_asset_context()
    mock_df = pl.DataFrame(
        {
            "genres": [["Q1", "Q2"], ["Q2", "Q3"]],
            "wikipedia_url": ["http://valid.url", "http://valid.url"],
        }
    )
    mock_read_ndjson.return_value = mock_df
    mock_extract_unique_ids_from_column.return_value = ["Q1", "Q2", "Q3"]

    # Mock clean_text to return identity for simplicity
    mock_clean_text.side_effect = lambda x: x

    # Mock batch response
    # The batch fetcher returns a flat dict of {qid: entity_data}
    async def fetch_side_effect(context, id_chunk, session=None):
        response = {}
        for qid in id_chunk:
            if qid == "Q1":
                response["Q1"] = {
                    "labels": {"en": {"value": "Genre 1"}},
                    "aliases": {"en": [{"value": "Alias A"}, {"value": "Alias B"}]}
                }
            elif qid == "Q2":
                response["Q2"] = {
                    "labels": {"en": {"value": "Genre 2"}},
                    "aliases": {}
                }
            elif qid == "Q3":
                # Missing label or entity
                pass
        return response
    
    mock_fetch_batch.side_effect = fetch_side_effect

    # Act
    result = await genres_extraction_from_artist_index(context)

    # Assert
    assert result == GENRES_FILE
    mock_read_ndjson.assert_called_once()
    mock_extract_unique_ids_from_column.assert_called_once()

    # We expect fetch_batch to be called.
    assert mock_fetch_batch.call_count >= 1

    expected_save_calls = [
        {
            "id": "Q1", 
            "genre_label": "Genre 1", 
            "aliases": ["Alias A", "Alias B"]
        },
        {
            "id": "Q2", 
            "genre_label": "Genre 2", 
            "aliases": []
        },
    ]

    # Get the actual data passed to save_to_jsonl
    actual_saved_data = mock_save_to_jsonl.call_args[0][0]

    # Helper to convert dict to tupleable structure (handling lists inside)
    def make_hashable(d):
        return tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in d.items()))

    assert set(make_hashable(d) for d in actual_saved_data) == set(make_hashable(d) for d in expected_save_calls)
