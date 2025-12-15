import polars as pl
from dagster import asset, AssetExecutionContext
from music_rag_etl.settings import PATH_TEMP, ARTIST_INDEX, DATA_DIR


@asset(
    name="artist_index_clean",
    deps=["artist_index"],  # Depends on the merge step
    description="Deduplicates artists based on QIDs but with conditions."
)
def deduplicate_artists(context: AssetExecutionContext) -> str:
    """
    Reads the raw merged artist index, deduplicates entries, and saves a clean version.
    Uses Polars for high-performance processing.
    """
    input_path = PATH_TEMP / ARTIST_INDEX
    output_filename = "artist_index.jsonl"
    output_path = DATA_DIR / output_filename

    # 1. Lazy Load: scan_ndjson is memory efficient (doesn't load all at once)
    lf = pl.scan_ndjson(input_path)

    # 2. Define Deduplication Logic
    # Scenario: We might have duplicates of "wikidata_id".
    # We want to keep the one that has the oldest "inception" date.
    # If both have same "wikidata_id" and "inception" date, choose the first.

    clean_lf = (
        lf
        .sort("inception", descending=False)  # Sort by inception date ascending (oldest first)
        .unique(subset=["wikidata_id"], keep="first")  # Keep the first unique wikidata_id
    )

    # 3. Collect & Save
    # collect() executes the lazy query
    # write_ndjson() streams the result to a new file
    clean_lf.collect().write_ndjson(output_path)

    context.log.info(f"Cleaned artist index saved to {output_path}")

    return str(output_path)
