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
    "music_rag_etl.assets.extraction.genres_extraction_assets.async_fetch_wikidata_entities_batch",
    new_callable=AsyncMock
)
@patch(
    "music_rag_etl.assets.extraction.genres_extraction_assets.parse_wikidata_entity_label"
)
async def test_genres_extraction_from_artist_index(
    mock_parse,
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
        }
    )
    mock_read_ndjson.return_value = mock_df
    mock_extract_unique_ids_from_column.return_value = ["Q1", "Q2", "Q3"]

    # Mock batch response
    # The batch fetcher returns a dict of entities
    async def fetch_side_effect(context, id_chunk, session=None):
        response = {"entities": {}}
        for qid in id_chunk:
            if qid == "Q1":
                response["entities"]["Q1"] = {"labels": {"en": {"value": "Genre 1"}}}
            elif qid == "Q2":
                response["entities"]["Q2"] = {"labels": {"en": {"value": "Genre 2"}}}
            elif qid == "Q3":
                # Missing label or entity
                pass
        return response

    mock_fetch_batch.side_effect = fetch_side_effect

    # Mock parse (sync)
    def parse_side_effect(entity_data, genre_id):
        return entity_data.get("entities", {}).get(genre_id, {}).get("labels", {}).get("en", {}).get("value")

    mock_parse.side_effect = parse_side_effect

    # Act
    result = await genres_extraction_from_artist_index(context)

    # Assert
    assert result == GENRES_FILE
    mock_read_ndjson.assert_called_once()
    mock_extract_unique_ids_from_column.assert_called_once()

    # We expect fetch_batch to be called.
    # Since we have 3 IDs and chunk size is defined in settings, 
    # assuming CHUNK_SIZE is large enough, it calls once.
    assert mock_fetch_batch.call_count >= 1

    expected_save_calls = [
        {"wikidata_id": "Q1", "genre_label": "Genre 1"},
        {"wikidata_id": "Q2", "genre_label": "Genre 2"},
    ]

    # Get the actual data passed to save_to_jsonl
    actual_saved_data = mock_save_to_jsonl.call_args[0][0]

    # Convert lists of dicts to sets of tuples for order-independent comparison
    assert set(tuple(d.items()) for d in actual_saved_data) == set(tuple(d.items()) for d in expected_save_calls)
