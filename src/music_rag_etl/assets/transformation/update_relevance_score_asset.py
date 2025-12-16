from pathlib import Path
import polars as pl
from dagster import asset

from music_rag_etl.settings import ARTIST_INDEX_PRE_CLEAN, ARTIST_INDEX


@asset(
    name="artist_index_with_relevance",
    deps=["artist_index_clean"],
    description="Compute relevance score."
)
def artist_index_with_relevance() -> Path:
    """
    A Dagster asset that loads the artist index, calculates a normalized
    relevance score based on the 'linkcount' column, and overwrites the
    original file with the updated data.

    The relevance score is a float between 0 and 1, where a higher value
    indicates a higher link count.

    Returns:
        pathlib.Path: The path to the updated artist index file.
    """
    # Step 1: Load the artist index
    df = pl.read_ndjson(ARTIST_INDEX)

    # Cast 'linkcount' to integer type for calculations
    df = df.with_columns(pl.col("linkcount").cast(pl.Int64))

    min_refs = df["linkcount"].min()
    max_refs = df["linkcount"].max()

    # Avoid division by zero if all values are the same
    if max_refs == min_refs:
        # Assign a neutral score of 0.5 if all linkcounts are identical
        df = df.with_columns(pl.lit(0.5).alias("relevance_score"))
    else:
        # Step 2 & 3: Calculate normalized relevance score and add it as a new column
        df = df.with_columns(
            ((pl.col("linkcount") - min_refs) / (max_refs - min_refs)).alias(
                "relevance_score"
            )
        )

    # Overwrite the original file with the new data
    df.write_ndjson(ARTIST_INDEX)

    return ARTIST_INDEX
