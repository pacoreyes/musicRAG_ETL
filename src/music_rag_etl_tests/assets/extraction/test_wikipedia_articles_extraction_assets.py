
import unittest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import polars as pl
from dagster import build_asset_context
import wikipediaapi

from music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets import get_wikipedia_page, fetch_raw_wikipedia_articles
from music_rag_etl.settings import WIKIPEDIA_CACHE_DIR

class TestWikipediaArticlesExtractionAssets(unittest.TestCase):

    def setUp(self):
        self.context = build_asset_context()
        self.wiki_api = MagicMock()

    @patch("builtins.open", new_callable=mock_open, read_data="cached text")
    def test_get_wikipedia_page_from_cache(self, mock_file):
        WIKIPEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with patch.object(Path, 'exists') as mock_exists:
            mock_exists.return_value = True
            page_text = get_wikipedia_page(self.context, self.wiki_api, "http://en.wikipedia.org/wiki/Test_Page", "Q123")
            self.assertEqual(page_text, "cached text")
            mock_file.assert_called_with(WIKIPEDIA_CACHE_DIR / "Q123.txt", 'r', encoding='utf-8')

    @patch("builtins.open", new_callable=mock_open)
    @patch("music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.time.sleep", return_value=None)
    def test_get_wikipedia_page_from_api(self, mock_sleep, mock_file):
        with patch.object(Path, 'exists') as mock_exists:
            mock_exists.return_value = False
            
            mock_page = MagicMock()
            mock_page.exists.return_value = True
            mock_page.text = "api text"
            self.wiki_api.page.return_value = mock_page
            
            page_text = get_wikipedia_page(self.context, self.wiki_api, "http://en.wikipedia.org/wiki/Test_Page", "Q123")
            
            self.assertEqual(page_text, "api text")
            self.wiki_api.page.assert_called_with("Test_Page")
            mock_file.assert_called_with(WIKIPEDIA_CACHE_DIR / "Q123.txt", 'w', encoding='utf-8')
            mock_file().write.assert_called_with("api text")

    def test_get_wikipedia_page_invalid_url(self):
        page_text = get_wikipedia_page(self.context, self.wiki_api, "invalid_url", "Q123")
        self.assertIsNone(page_text)

    def test_get_wikipedia_page_nonexistent_page(self):
        with patch.object(Path, 'exists') as mock_exists:
            mock_exists.return_value = False
            mock_page = MagicMock()
            mock_page.exists.return_value = False
            self.wiki_api.page.return_value = mock_page
            page_text = get_wikipedia_page(self.context, self.wiki_api, "http://en.wikipedia.org/wiki/Test_Page", "Q123")
            self.assertIsNone(page_text)

    @patch("music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.pl.read_ndjson")
    @patch("music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.wikipediaapi.Wikipedia")
    @patch("music_rag_etl.assets.extraction.wikipedia_articles_extraction_assets.get_wikipedia_page")
    def test_fetch_raw_wikipedia_articles(self, mock_get_wikipedia_page, mock_wiki_api, mock_read_ndjson):
        # Arrange
        mock_artist_df = pl.DataFrame({
            "wikidata_id": ["Q1", "Q2", "Q3"],
            "artist": ["Artist 1", "Artist 2", "Artist 3"],
            "genres": [["g1"], ["g2"], ["g3"]],
            "inception": ["2001", "2002", "2003"],
            "wikipedia_url": ["url1", "url2", None],
            "relevance_score": [0.5, 0.6, 0.7],
        })
        mock_read_ndjson.return_value = mock_artist_df
        
        mock_get_wikipedia_page.side_effect = lambda ctx, api, url, wid: "text for " + wid if url else None

        # Act
        result_df = fetch_raw_wikipedia_articles(self.context)

        # Assert
        self.assertEqual(result_df.shape, (2, 7))
        self.assertIn("page_text", result_df.columns)
        self.assertEqual(result_df["page_text"][0], "text for Q1")
        self.assertEqual(result_df["page_text"][1], "text for Q2")
        mock_get_wikipedia_page.assert_any_call(self.context, mock_wiki_api.return_value, "url1", "Q1")
        mock_get_wikipedia_page.assert_any_call(self.context, mock_wiki_api.return_value, "url2", "Q2")
        # No call for Q3 because wikipedia_url is None
        self.assertEqual(mock_get_wikipedia_page.call_count, 2)

if __name__ == "__main__":
    unittest.main()
