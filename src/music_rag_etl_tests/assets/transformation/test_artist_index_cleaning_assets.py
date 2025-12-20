import unittest
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context

from music_rag_etl.assets.transformation.artist_index_cleaning_assets import (
    deduplicate_artists,
)
from music_rag_etl.settings import DATA_DIR


class TestArtistIndexCleaningAssets(unittest.TestCase):
    @patch(
        "music_rag_etl.assets.transformation.artist_index_cleaning_assets.pl.scan_ndjson"
    )
    def test_deduplicate_artists(self, mock_scan_ndjson):
        context = build_asset_context()
        data = {
            "wikidata_id": ["Q1", "Q2", "Q1", "Q3", "Q2"],
            "artist": ["Artist 1a", "Artist 2a", "Artist 1b", "Artist 3", "Artist 2b"],
            "inception": [
                "2000-01-01",
                "1995-01-01",
                "1990-01-01",
                "2005-01-01",
                "1998-01-01",
            ],
        }
        mock_lf = pl.LazyFrame(data)
        mock_scan_ndjson.return_value = mock_lf

        captured_dfs = []

        def capture_self(self, *args, **kwargs):
            captured_dfs.append(self)

        with patch.object(pl.DataFrame, "write_ndjson", new=capture_self):
            deduplicate_artists(context)

        self.assertEqual(len(captured_dfs), 1)
        result_df = captured_dfs[0]

        expected_data = {
            "wikidata_id": ["Q1", "Q2", "Q3"],
            "artist": ["Artist 1b", "Artist 2a", "Artist 3"],
            "inception": ["1990-01-01", "1995-01-01", "2005-01-01"],
        }
        expected_df = pl.DataFrame(expected_data).sort("wikidata_id")

        self.assertTrue(result_df.sort("wikidata_id").equals(expected_df))


if __name__ == "__main__":
    unittest.main()
