
import unittest
from unittest.mock import patch, MagicMock, call
from pathlib import Path

import polars as pl
from dagster import build_asset_context

from music_rag_etl.assets.extraction.genres_extraction_assets import genres_extraction_from_artist_index
from music_rag_etl.settings import GENRES_FILE

class GenresExtractionFromArtistIndexTest(unittest.TestCase):

    @patch("music_rag_etl.assets.extraction.genres_extraction_assets.pl.read_ndjson")
    @patch("music_rag_etl.assets.extraction.genres_extraction_assets.extract_unique_ids_from_column")
    @patch("music_rag_etl.assets.extraction.genres_extraction_assets.save_to_jsonl")
    def test_genres_extraction_from_artist_index(
        self,
        mock_save_to_jsonl,
        mock_extract_unique_ids_from_column,
        mock_read_ndjson,
    ):
        # Arrange
        context = build_asset_context()
        mock_df = pl.DataFrame({
            "genres": [["Q1", "Q2"], ["Q2", "Q3"]],
        })
        mock_read_ndjson.return_value = mock_df
        mock_extract_unique_ids_from_column.return_value = ["Q1", "Q2", "Q3"]

        # Mocking the inner worker function's dependencies
        with patch("music_rag_etl.assets.extraction.genres_extraction_assets.fetch_wikidata_entity") as mock_fetch, \
             patch("music_rag_etl.assets.extraction.genres_extraction_assets.parse_wikidata_entity_label") as mock_parse:
            
            def fetch_side_effect(context, genre_id):
                if genre_id == "Q1":
                    return {"entities": {"Q1": {"labels": {"en": {"value": "Genre 1"}}}}}
                if genre_id == "Q2":
                    return {"entities": {"Q2": {"labels": {"en": {"value": "Genre 2"}}}}}
                if genre_id == "Q3":
                    return None
                return None
            mock_fetch.side_effect = fetch_side_effect
            
            def parse_side_effect(entity_data, genre_id):
                if entity_data:
                    return entity_data["entities"][genre_id]["labels"]["en"]["value"]
                return None
            mock_parse.side_effect = parse_side_effect

            # Act
            result = genres_extraction_from_artist_index(context)

            # Assert
            self.assertEqual(result, GENRES_FILE)
            mock_read_ndjson.assert_called_once()
            mock_extract_unique_ids_from_column.assert_called_once()

            self.assertEqual(mock_fetch.call_count, 3)
            mock_fetch.assert_any_call(context, "Q1")
            mock_fetch.assert_any_call(context, "Q2")
            mock_fetch.assert_any_call(context, "Q3")

            self.assertEqual(mock_parse.call_count, 2)

            expected_save_calls = [
                {'wikidata_id': 'Q1', 'genre_label': 'Genre 1'},
                {'wikidata_id': 'Q2', 'genre_label': 'Genre 2'},
            ]
            
            # Get the actual data passed to save_to_jsonl
            actual_saved_data = mock_save_to_jsonl.call_args[0][0]
            
            # Convert lists of dicts to sets of tuples for order-independent comparison
            self.assertEqual(
                set(tuple(d.items()) for d in actual_saved_data),
                set(tuple(d.items()) for d in expected_save_calls)
            )


if __name__ == "__main__":
    unittest.main()
