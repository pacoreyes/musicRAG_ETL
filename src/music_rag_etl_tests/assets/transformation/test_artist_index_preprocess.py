import unittest
from unittest.mock import patch
import polars as pl
from dagster import build_asset_context

from music_rag_etl.assets.transformation.artist_index_preprocess import (
    artist_index_preprocess,
)


class TestArtistIndexPreprocess(unittest.TestCase):
    @patch("music_rag_etl.assets.transformation.artist_index_preprocess.pl.scan_ndjson")
    def test_artist_index_preprocess(self, mock_scan_ndjson):
        context = build_asset_context()
        
        # Data setup:
        # Q1: Duplicate. "Artist 1b" is older (1990) than "Artist 1a" (2000). Should keep 1b. Linkcount 10.
        # Q2: Duplicate. "Artist 2a" (1995) vs "Artist 2b" (1998). Keep 2a. Linkcount 20.
        # Q3: Unique. Linkcount 30.
        # Q4: Unique. Linkcount 10 (same as min).
        
        data = {
            "wikidata_id": ["Q1", "Q2", "Q1", "Q3", "Q2", "Q4"],
            "artist": ["Artist 1a", "Artist 2a", "Artist 1b", "Artist 3", "Artist 2b", "Artist 4"],
            "wikipedia_url": ["url1a", "url2a", "url1b", "url3", "url2b", "url4"],
            "inception": [
                "2000-01-01",
                "1995-01-01",
                "1990-01-01",
                "2005-01-01",
                "1998-01-01",
                "2010-01-01"
            ],
            "linkcount": ["10", "20", "10", "30", "20", "10"] # Strings as they come from JSON often
        }
        mock_lf = pl.LazyFrame(data)
        mock_scan_ndjson.return_value = mock_lf

        captured_dfs = []

        # Mock the write method on DataFrame (result of collect())
        def capture_self(self, *args, **kwargs):
            captured_dfs.append(self)

        with patch.object(pl.DataFrame, "write_ndjson", new=capture_self):
            artist_index_preprocess(context)

        self.assertEqual(len(captured_dfs), 1)
        result_df = captured_dfs[0]
        
        # Expected Logic:
        # 1. Deduplication:
        #    - Q1: Keep "Artist 1b" (1990). Linkcount 10.
        #    - Q2: Keep "Artist 2a" (1995). Linkcount 20.
        #    - Q3: Keep "Artist 3". Linkcount 30.
        #    - Q4: Keep "Artist 4". Linkcount 10.
        
        # 2. Relevance:
        #    - Min linkcount = 10 (Q1, Q4)
        #    - Max linkcount = 30 (Q3)
        #    - Range = 20
        #    - Q1 score: (10 - 10) / 20 = 0.0
        #    - Q2 score: (20 - 10) / 20 = 0.5
        #    - Q3 score: (30 - 10) / 20 = 1.0
        #    - Q4 score: (10 - 10) / 20 = 0.0
        
        # Let's verify row by row (sorting by wikidata_id to be deterministic)
        result_df = result_df.sort("wikidata_id")
        
        # Verify Q1
        row_q1 = result_df.filter(pl.col("wikidata_id") == "Q1")
        self.assertEqual(row_q1["artist"][0], "Artist 1b")
        self.assertEqual(row_q1["relevance_score"][0], 0.0)
        
        # Verify Q2
        row_q2 = result_df.filter(pl.col("wikidata_id") == "Q2")
        self.assertEqual(row_q2["artist"][0], "Artist 2a")
        self.assertEqual(row_q2["relevance_score"][0], 0.5)

        # Verify Q3
        row_q3 = result_df.filter(pl.col("wikidata_id") == "Q3")
        self.assertEqual(row_q3["relevance_score"][0], 1.0)
        
        # Verify Q4
        row_q4 = result_df.filter(pl.col("wikidata_id") == "Q4")
        self.assertEqual(row_q4["relevance_score"][0], 0.0)

    @patch("music_rag_etl.assets.transformation.artist_index_preprocess.pl.scan_ndjson")
    def test_artist_index_preprocess_single_value(self, mock_scan_ndjson):
        """Test case where all linkcounts are the same (avoid division by zero)."""
        context = build_asset_context()
        data = {
            "wikidata_id": ["Q1", "Q2"],
            "artist": ["A1", "A2"],
            "wikipedia_url": ["u1", "u2"],
            "inception": ["2000", "2000"],
            "linkcount": ["10", "10"]
        }
        mock_lf = pl.LazyFrame(data)
        mock_scan_ndjson.return_value = mock_lf

        captured_dfs = []
        def capture_self(self, *args, **kwargs):
            captured_dfs.append(self)

        with patch.object(pl.DataFrame, "write_ndjson", new=capture_self):
            artist_index_preprocess(context)
            
        result_df = captured_dfs[0]
        self.assertTrue(all(s == 0.5 for s in result_df["relevance_score"]))


if __name__ == "__main__":
    unittest.main()
