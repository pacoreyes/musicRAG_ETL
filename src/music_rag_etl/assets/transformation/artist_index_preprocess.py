from pathlib import Path
import polars as pl
from dagster import asset, AssetExecutionContext
from music_rag_etl.settings import ARTIST_INDEX, ARTIST_INDEX_PRE_CLEAN


@asset(
    name="artist_index_preprocess",
    deps=["artist_index"],
    description="Deduplicates artists and computes relevance scores.",
)
def artist_index_preprocess(context: AssetExecutionContext) -> Path:
    """
    Reads the merged artist index, deduplicates entries, calculates
    a normalized relevance score based on 'linkcount', and saves the result.
    Uses Polars for high-performance processing.
    """
    # 1. Lazy Load
    lf = pl.scan_ndjson(ARTIST_INDEX_PRE_CLEAN)

    # 2. Deduplication Logic
    # Keep the entry with the oldest inception date for each wikidata_id
    clean_lf = (
        lf.sort("inception", descending=False)
        .unique(subset=["wikidata_id"], keep="first")
        .unique(subset=["wikipedia_url"], keep="first")
        .unique(subset=["artist"], keep="first")
    )

    # 3. Relevance Score Calculation
    # Cast 'linkcount' to integer
    clean_lf = clean_lf.with_columns(pl.col("linkcount").cast(pl.Int64))

    # Calculate min and max linkcount for normalization
    # We add them as columns to perform the calculation row-wise in the next step
    clean_lf = clean_lf.with_columns(
        [
            pl.col("linkcount").min().alias("min_refs"),
            pl.col("linkcount").max().alias("max_refs"),
        ]
    )

    # Compute normalized score
    # If max == min, assign neutral score 0.5
    clean_lf = clean_lf.with_columns(
        pl.when(pl.col("max_refs") == pl.col("min_refs"))
        .then(pl.lit(0.5))
        .otherwise(
            (pl.col("linkcount") - pl.col("min_refs"))
            / (pl.col("max_refs") - pl.col("min_refs"))
        )
        .alias("relevance_score")
    )

    # Drop temporary columns
    clean_lf = clean_lf.drop(["min_refs", "max_refs"])

    # 4. Collect & Save
    clean_lf.collect().write_ndjson(ARTIST_INDEX)

    context.log.info(f"Preprocessed artist index saved to {ARTIST_INDEX}")

    return ARTIST_INDEX
