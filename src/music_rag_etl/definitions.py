from dagster import Definitions, load_assets_from_modules, EnvVar

from music_rag_etl.assets.extraction import (
    artist_index_extraction_assets, wikipedia_articles_extraction_assets, genres_extraction_assets
)
from music_rag_etl.assets.transformation import artist_index_cleaning_assets, update_relevance_score_asset
from music_rag_etl import assets as root_assets

# Create a list of all asset modules
asset_modules = [
    root_assets,
    artist_index_extraction_assets,
    artist_index_cleaning_assets,
    update_relevance_score_asset,
    wikipedia_articles_extraction_assets,
    genres_extraction_assets
]

# Load all assets from the specified modules
all_assets = load_assets_from_modules(asset_modules)

# Original commented out section (preserved as requested)
"""all_assets_commented = load_assets_from_modules([root_assets])

defs_commented = Definitions(
    assets=all_assets_commented,
    # Resources allow you to pass configuration (like API keys) to your assets
    resources={
        "api_config": {
            "lastfm_api_key": EnvVar("LAST_FM_API_KEY"),
        }
    },
)"""

defs = Definitions(
    assets=all_assets,
    # Resources allow you to pass configuration (like API keys) to your assets
    resources={
        "api_config": {
            "lastfm_api_key": EnvVar("LAST_FM_API_KEY"),
        }
    },
)


