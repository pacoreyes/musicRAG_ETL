
import unittest
from unittest.mock import patch
import polars as pl
from pathlib import Path

from music_rag_etl.assets.transformation.update_relevance_score_asset import artist_index_with_relevance
from music_rag_etl.settings import ARTIST_INDEX, ARTIST_INDEX_CLEANED

class TestUpdateRelevanceScoreAsset(unittest.TestCase):

    @patch("music_rag_etl.assets.transformation.update_relevance_score_asset.pl.read_ndjson")
    def test_artist_index_with_relevance_normal_case(self, mock_read_ndjson):
        # Arrange
        data = {
            "artist": ["A", "B", "C"],
            "linkcount": [10, 60, 110],
        }
        mock_df = pl.DataFrame(data)
        mock_read_ndjson.return_value = mock_df

        captured_dfs = []
        def capture_self(self, *args, **kwargs):
            captured_dfs.append(self)

        with patch.object(pl.DataFrame, "write_ndjson", new=capture_self):
            # Act
            result_path = artist_index_with_relevance()

        # Assert
        self.assertEqual(result_path, ARTIST_INDEX_CLEANED)
        self.assertEqual(len(captured_dfs), 1)
        result_df = captured_dfs[0]
        
        expected_scores = pl.Series("relevance_score", [0.0, 0.5, 1.0])
        self.assertTrue(result_df["relevance_score"].equals(expected_scores))

    @patch("music_rag_etl.assets.transformation.update_relevance_score_asset.pl.read_ndjson")
    def test_artist_index_with_relevance_same_linkcount(self, mock_read_ndjson):
        # Arrange
        data = {
            "artist": ["A", "B", "C"],
            "linkcount": [50, 50, 50],
        }
        mock_df = pl.DataFrame(data)
        mock_read_ndjson.return_value = mock_df

        captured_dfs = []
        def capture_self(self, *args, **kwargs):
            captured_dfs.append(self)

        with patch.object(pl.DataFrame, "write_ndjson", new=capture_self):
            # Act
            result_path = artist_index_with_relevance()

        # Assert
        self.assertEqual(result_path, ARTIST_INDEX_CLEANED)
        self.assertEqual(len(captured_dfs), 1)
        result_df = captured_dfs[0]
        
        expected_scores = pl.Series("relevance_score", [0.5, 0.5, 0.5])
        self.assertTrue(result_df["relevance_score"].equals(expected_scores))

if __name__ == "__main__":
    unittest.main()
