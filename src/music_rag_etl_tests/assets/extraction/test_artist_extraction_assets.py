import json
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

import polars as pl
import pytest
from dagster import materialize, build_asset_context, DagsterInstance

from music_rag_etl.assets.extraction.artist_extraction_assets import (
    _async_enrich_artist_batch,
    artists_extraction_from_artist_index,
)
from music_rag_etl.settings import ARTIST_INDEX, ARTISTS_FILE


@pytest.mark.asyncio
async def test_artists_extraction_from_artist_index_asset(tmp_path):
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
    
    mock_country_labels = {"Q_US": "United States"} # Dummy example

    mock_lastfm_response_q1 = {
        "artist": {
            "name": "Artist One",
            "tags": {"tag": [{"name": "tagA"}, {"name": "tagB"}]},
            "similar": {"artist": [{"name": "Similar Artist X"}]},
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
    with (
        patch(
            "music_rag_etl.assets.extraction.artist_extraction_assets.ARTIST_INDEX",
            mock_artist_index_path,
        ),
        patch(
            "music_rag_etl.assets.extraction.artist_extraction_assets.ARTISTS_FILE",
            mock_artists_file_path,
        ),
        patch(
            "music_rag_etl.assets.extraction.artist_extraction_assets.async_fetch_wikidata_entities_batch_with_cache",
            new_callable=AsyncMock
        ) as mock_wikidata_fetch,
        patch(
            "music_rag_etl.assets.extraction.artist_extraction_assets.async_resolve_qids_to_labels",
            new_callable=AsyncMock
        ) as mock_resolve_labels,
        patch(
            "music_rag_etl.assets.extraction.artist_extraction_assets.async_get_artist_info_with_fallback",
            new_callable=AsyncMock
        ) as mock_lastfm_fetch,
    ):
        # Configure mock return values for the patched helpers
        mock_wikidata_fetch.return_value = mock_wikidata_response
        mock_resolve_labels.return_value = mock_country_labels

        async def lastfm_side_effect(context, artist_name, aliases, mbid, api_key, api_url, session=None):
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
            resources={
                "api_config": {
                    "lastfm_api_key": MagicMock(),
                    "lastfm_api_url": MagicMock(),
                }
            },
        )
        
        # Call the async asset function
        result = await artists_extraction_from_artist_index(context)

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


@pytest.mark.parametrize(
    "lastfm_data, expected_tags",
    [
        (
            {"artist": {"tags": {"tag": [{"name": "pop"}, {"name": "rock"}]}}},
            ["pop", "rock"],
        ),
        ({"artist": {"tags": {"tag": []}}}, []),
        ({"artist": {"tags": {}}}, []),
        ({"artist": {}}, []),
        ({}, []),
        (None, []),
    ],
)
@patch(
    "music_rag_etl.assets.extraction.artist_extraction_assets.async_fetch_wikidata_entities_batch_with_cache",
    new_callable=AsyncMock
)
@patch(
    "music_rag_etl.assets.extraction.artist_extraction_assets.async_resolve_qids_to_labels",
    new_callable=AsyncMock
)
@patch(
    "music_rag_etl.assets.extraction.artist_extraction_assets.async_get_artist_info_with_fallback",
    new_callable=AsyncMock
)
@pytest.mark.asyncio
async def test_enrich_artist_batch_lastfm_tags(
    mock_lastfm_fetch,
    mock_resolve_labels,
    mock_wikidata_fetch,
    lastfm_data,
    expected_tags,
):
    """
    Unit test for _async_enrich_artist_batch focusing on Last.fm tag extraction.
    """
    # 1. Mock artist batch and context
    artist_batch = [
        {"wikidata_id": "Q1", "artist": "Test Artist", "genres": ["test-genre"]}
    ]
    mock_context = MagicMock()

    # 2. Mock API responses
    mock_wikidata_fetch.return_value = {"Q1": {"id": "Q1", "claims": {}, "aliases": {}}}
    mock_resolve_labels.return_value = {}
    mock_lastfm_fetch.return_value = lastfm_data

    # 3. Call the function
    enriched_data = await _async_enrich_artist_batch(
        artist_batch, mock_context, api_key="dummy_key", api_url="dummy_url"
    )

    # 4. Assertions
    assert len(enriched_data) == 1
    result = enriched_data[0]
    assert result["id"] == "Q1"
    assert result["name"] == "Test Artist"
    assert "tags" in result
    assert result["tags"] == expected_tags
    assert isinstance(result["tags"], list)
