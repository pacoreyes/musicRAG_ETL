import polars as pl
from dagster import asset, AssetExecutionContext
from music_rag_etl.settings import ARTIST_INDEX, ARTIST_INDEX_PRE_CLEAN


@asset(
    name="artist_index_clean",
    deps=["artist_index"],  # Depends on the merge step
    description="Deduplicates artists based on QIDs but with conditions.",
)
def deduplicate_artists(context: AssetExecutionContext) -> str:
    """
    Reads the raw merged artist index, deduplicates entries, and saves a clean version.
    Uses Polars for high-performance processing.
    """
    # input_path = PATH_DATASETS / ARTIST_INDEX_PRE_CLEAN
    # output_filename = "artist_index.jsonl"
    # output_path = DATA_DIR / output_filename

    # 1. Lazy Load: scan_ndjson is memory efficient (doesn't load all at once)
    lf = pl.scan_ndjson(ARTIST_INDEX_PRE_CLEAN)

    # 2. Define Deduplication Logic
    # Scenario: We might have duplicates of "wikidata_id".
    # We want to keep the one that has the oldest "inception" date.
    # If both have same "wikidata_id" and "inception" date, choose the first.

    clean_lf = (
        lf.sort(
            "inception", descending=False
        )  # Sort by inception date ascending (oldest first)
        .unique(
            subset=["wikidata_id"], keep="first"
        )  # Keep the first unique wikidata_id
        .unique(
            subset=["wikipedia_url"], keep="first"
        )  # Keep the first unique wikidata_id
        .unique(subset=["artist"], keep="first")  # Keep the first unique wikidata_id
    )

    # 3. Collect & Save
    # collect() executes the lazy query
    # write_ndjson() streams the result to a new file
    clean_lf.collect().write_ndjson(ARTIST_INDEX)

    context.log.info(f"Cleaned artist index saved to {ARTIST_INDEX}")

    return str(ARTIST_INDEX)
