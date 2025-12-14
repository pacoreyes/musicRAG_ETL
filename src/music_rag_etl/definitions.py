from dagster import Definitions, load_assets_from_modules, EnvVar

from music_rag_etl import assets
from music_rag_etl.assets.extraction import create_artist_index

"""all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    # Resources allow you to pass configuration (like API keys) to your assets
    resources={
        "api_config": {
            "lastfm_api_key": EnvVar("LAST_FM_API_KEY"),
        }
    },
)"""

# 2. Extract the assets from that file
extraction_assets = load_assets_from_modules([create_artist_index])

# 3. Add them to the main Definitions object
defs = Definitions(
    assets=[*extraction_assets],  # The "*" unpacks the list
)