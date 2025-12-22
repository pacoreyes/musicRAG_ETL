import json
from unittest.mock import MagicMock, patch
import pytest
from dagster import build_asset_context, ResourceDefinition
from music_rag_etl.assets.loading.load_graph_db import load_graph_db
from music_rag_etl.utils.memgraph_helpers import MemgraphConfig

@pytest.fixture
def mock_memgraph():
    with patch("music_rag_etl.assets.loading.load_graph_db.get_memgraph_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        yield mock_client

@pytest.fixture
def mock_data_dir(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    # Genres
    genres = [
        {"id": "G1", "genre_label": "Rock", "aliases": ["Classic Rock"]},
        {"id": "G2", "genre_label": "Jazz"} # aliases missing, should be []
    ]
    with open(data_dir / "genres.jsonl", "w") as f:
        for item in genres:
            f.write(json.dumps(item) + "\n")
            
    # Artists
    artists = [
        {
            "id": "A1", 
            "name": "Artist 1", 
            "country": "USA", 
            "genres": ["G1"], 
            "similar_artists": ["Artist 2"]
        },
        {
            "id": "A2",
            "name": "Artist 2",
            "country": "UK",
            # missing aliases/tags/genres/similar_artists should be []
        }
    ]
    with open(data_dir / "artists.jsonl", "w") as f:
        for item in artists:
            f.write(json.dumps(item) + "\n")
            
    # Albums
    albums = [
        {"id": "AL1", "title": "Album 1", "year": 2020, "artist_id": "A1"},
        {"id": "AL2", "title": "Album 2", "year": "2021", "artist_id": "A2"}
    ]
    with open(data_dir / "albums.jsonl", "w") as f:
        for item in albums:
            f.write(json.dumps(item) + "\n")
            
    # Tracks
    tracks = [
        {"id": "T1", "title": "Track 1", "album_id": "AL1"},
        {"id": "T2", "title": "Track 2", "album_id": "AL2"}
    ]
    with open(data_dir / "tracks.jsonl", "w") as f:
        for item in tracks:
            f.write(json.dumps(item) + "\n")
            
    return data_dir

def test_load_graph_db_success(mock_memgraph, mock_data_dir):
    config = MemgraphConfig(host="localhost", port=7687)
    
    with patch("music_rag_etl.assets.loading.load_graph_db.LOCAL_DATA_DIR", mock_data_dir), \
         patch("music_rag_etl.assets.loading.load_graph_db.clear_database") as mock_clear:
        
        context = build_asset_context()
        result = load_graph_db(context, config)
        
        # Verify clear_database was called
        mock_clear.assert_called_once()
        
        # Verify metadata
        assert result.metadata["nodes_loaded"]["genres"] == 2
        assert result.metadata["nodes_loaded"]["artists"] == 2
        assert result.metadata["nodes_loaded"]["albums"] == 2
        assert result.metadata["nodes_loaded"]["tracks"] == 2
        assert result.metadata["status"] == "success"
        
        # Verify memgraph.execute was called multiple times
        # We can check if CREATE commands were issued
        create_calls = [call for call in mock_memgraph.execute.call_args_list if "CREATE" in str(call)]
        assert len(create_calls) >= 8 # 2 genres + 2 artists + 2 albums + 2 tracks

def test_load_graph_db_with_invalid_data(mock_memgraph, mock_data_dir):
    # Overwrite albums with one invalid record (missing artist_id)
    with open(mock_data_dir / "albums.jsonl", "w") as f:
        f.write(json.dumps({"id": "AL_VALID", "title": "Valid", "artist_id": "A1"}) + "\n")
        f.write(json.dumps({"id": "AL_INVALID", "title": "Invalid"}) + "\n") # Missing artist_id
        
    config = MemgraphConfig(host="localhost", port=7687)
    
    with patch("music_rag_etl.assets.loading.load_graph_db.LOCAL_DATA_DIR", mock_data_dir), \
         patch("music_rag_etl.assets.loading.load_graph_db.clear_database"):
        
        context = build_asset_context()
        result = load_graph_db(context, config)
        
        # Valid album should be loaded, invalid one skipped
        assert result.metadata["nodes_loaded"]["albums"] == 1
