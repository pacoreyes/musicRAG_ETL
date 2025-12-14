# Example usage in an asset (for context)
import os
from dagster import asset
from music_rag_etl.settings import DATA_DIR


@asset
def my_asset():
    # Access constants
    print(f"Saving to {DATA_DIR}")

    # Access Secrets (Dagster loads .env automatically)
    api_key = os.getenv("LAST_FM_API_KEY")
