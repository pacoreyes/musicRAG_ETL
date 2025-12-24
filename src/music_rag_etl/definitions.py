from dagster import Definitions, load_assets_from_modules, EnvVar

from music_rag_etl import assets as root_assets
from music_rag_etl.assets.extraction import (
    build_artist_index,
    extract_wikipedia_articles,
    extract_genres,
    extract_artist,
    extract_albums,
    extract_tracks,
)
from music_rag_etl.assets.transformation import preprocess_artist_index
from music_rag_etl.assets.loading import load_graph_db


# Create a list of all asset modules
asset_modules = [
    root_assets,
    build_artist_index,
    preprocess_artist_index,
    extract_wikipedia_articles,
    extract_genres,
    extract_artist,
    extract_albums,
    extract_tracks,
    load_graph_db
]

# Load all assets from the specified modules
all_assets = load_assets_from_modules(asset_modules)

defs = Definitions(
    assets=all_assets,
    # Resources allow you to pass configuration (like API keys) to your assets
    resources={
        "api_config": {
            "lastfm_api_key": EnvVar("LASTFM_API_KEY"),
            "lastfm_api_url": EnvVar("LASTFM_API_URL"),
            "nomic_api_key": EnvVar("NOMIC_API_KEY"),
        }
    },
)
