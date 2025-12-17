import json
from unittest.mock import patch

import polars as pl
from dagster import materialize, build_asset_context, DagsterInstance

from music_rag_etl.assets.extraction.artist_extraction_assets import (
    artists_extraction_from_artist_index,
)
from music_rag_etl.settings import ARTIST_INDEX, ARTISTS_FILE


def test_artists_extraction_from_artist_index_asset(tmp_path):
    """
    Integration test for the artists_extraction_from_artist_index asset.
    """
    # 1. Prepare mock upstream data and paths
    mock_artist_index_path = tmp_path / "artist_index.jsonl"
    mock_artists_file_path = tmp_path / "artists.jsonl"

    mock_input_data = [
        {
            "wikidata_id": "Q1",
            "artist": "Artist One",
            "genres": ["genre-a", "genre-b"],
            "wikipedia_url": "https://example.com/artist-one",
        },
        {
            "wikidata_id": "Q2",
            "artist": "Artist Two",
            "genres": ["genre-c"],
            "wikipedia_url": "https://example.com/artist-two",
        },
        {
            "wikidata_id": "Q3",
            "artist": "Artist Three",
            "genres": ["genre-d"],
            "wikipedia_url": "",
        },
    ]
    with open(mock_artist_index_path, "w") as f:
        for item in mock_input_data:
            f.write(json.dumps(item) + "\n")

    # 2. Prepare mock API responses
    mock_wikidata_response = {
        "Q1": {
            "id": "Q1",
            "claims": {},  # Mocked, parsers will handle missing keys
            "aliases": {"en": [{"value": "alias1"}]},
        },
        "Q2": {
            "id": "Q2",
            "claims": {},
            "aliases": {"en": [{"value": "alias2"}]},
        },
    }
    mock_lastfm_response_q1 = {
        "artist": {
            "name": "Artist One",
            "tags": {"tag": [{"name": "tagA"}, {"name": "tagB"}]},
            "similar": {"artist": [{"name": "Similar Artist X"}]}
        }
    }
    mock_lastfm_response_q2 = {
        "artist": {
            "name": "Artist Two",
            "tags": {"tag": [{"name": "tagC"}]},
            "similar": {"artist": []},
        }
    }

    # 3. Patch external dependencies: settings and helper functions
    with patch(
        "music_rag_etl.assets.extraction.artist_extraction_assets.ARTIST_INDEX",
        mock_artist_index_path,
    ), patch(
        "music_rag_etl.assets.extraction.artist_extraction_assets.ARTISTS_FILE",
        mock_artists_file_path,
    ), patch(
        "music_rag_etl.assets.extraction.artist_extraction_assets.fetch_wikidata_entities_batch_with_cache"
    ) as mock_wikidata_fetch, patch(
        "music_rag_etl.assets.extraction.artist_extraction_assets.fetch_lastfm_data_with_cache"
    ) as mock_lastfm_fetch:
        # Configure mock return values for the patched helpers
        mock_wikidata_fetch.return_value = mock_wikidata_response

        def lastfm_side_effect(context, artist_name, api_key):
            if artist_name == "Artist One":
                return mock_lastfm_response_q1
            if artist_name == "Artist Two":
                return mock_lastfm_response_q2
            return None

        mock_lastfm_fetch.side_effect = lastfm_side_effect

        # 4. Materialize the asset
        instance = DagsterInstance.ephemeral()
        context = build_asset_context(
            instance=instance,
            resources={"api_config": {"lastfm_api_key": "dummy_key"}},
        )
        result = artists_extraction_from_artist_index(context)

        # 5. Verify the output
        assert result == str(mock_artists_file_path)
        assert mock_artists_file_path.exists()

        output_df = pl.read_ndjson(mock_artists_file_path)
        assert len(output_df) == 2
        
        output_dicts = output_df.to_dicts()
        artist_one_out = next(d for d in output_dicts if d["id"] == "Q1")
        artist_two_out = next(d for d in output_dicts if d["id"] == "Q2")
        assert all(d["id"] != "Q3" for d in output_dicts)

        # Verify Artist One
        assert artist_one_out["name"] == "Artist One"
        assert artist_one_out["aliases"] == ["alias1"]
        assert artist_one_out["genres"] == ["genre-a", "genre-b"]
        assert artist_one_out["tags"] == ["tagA", "tagB"]
        assert artist_one_out["similar_artists"] == ["Similar Artist X"]

        # Verify Artist Two
        assert artist_two_out["name"] == "Artist Two"
        assert artist_two_out["aliases"] == ["alias2"]
        assert artist_two_out["genres"] == ["genre-c"]
        assert artist_two_out["tags"] == ["tagC"]
        assert artist_two_out["similar_artists"] == []
