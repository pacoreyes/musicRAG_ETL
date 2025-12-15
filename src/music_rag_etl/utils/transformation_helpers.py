import polars as pl
from typing import List, Optional, Dict


def clean_text(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
    return df.with_columns(
        pl.col(col_name)
        # 1. Manual replacements (escaped quotes & newlines)
        .str.replace_all(r'\\"', '"')  # Fix escaped quotes
        .str.replace_all(r"[\n\r]+", " ")  # Replace newlines with space

        # 2. Unicode Normalization (Optional: handles partial ftfy work)
        #.str.normalize("NFKC")

        # 3. Cleantext equivalent (extra_spaces=True)
        .str.replace_all(r"\s+", " ")  # Squash multiple spaces
        .str.strip_chars()  # Remove leading/trailing space
    )


def clean_text_string(text: str) -> str:
    """
    Applies text cleaning operations to a single string.
    Matches the logic of the `clean_text` DataFrame function.
    """
    if not isinstance(text, str):
        return text  # Or raise an error, depending on desired behavior

    # 1. Manual replacements (escaped quotes & newlines)
    text = text.replace('\\"', '"')  # Fix escaped quotes
    text = re.sub(r"[\n\r]+", " ", text)  # Replace newlines with space

    # 2. Cleantext equivalent (extra_spaces=True)
    text = re.sub(r"\s+", " ", text)  # Squash multiple spaces
    text = text.strip()  # Remove leading/trailing space
    return text


def extract_unique_ids_from_column(
    df: pl.DataFrame, column_name: str
) -> List[str]:
    """
    Extracts unique non-null values from a DataFrame column
    containing lists of IDs.

    Args:
        df: The input Polars DataFrame.
        column_name: The name of the column containing lists of IDs.

    Returns:
        A list of unique IDs.
    """
    list_of_lists = df[column_name].unique().to_list()
    
    unique_ids = set()
    for sublist in list_of_lists:
        if sublist:
            unique_ids.update(item for item in sublist if item)
            
    return sorted(list(unique_ids))  # Return sorted list for consistent output

import re

def normalize_relevance_score(
    raw_references: int, min_refs: int, max_refs: int
) -> float:
    """
    Normalizes a raw reference count to a score between 0 and 1.

    Args:
        raw_references: The raw number of references for an article.
        min_refs: The minimum raw reference count found across all articles.
        max_refs: The maximum raw reference count found across all articles.

    Returns:
        A normalized relevance score (float between 0 and 1).
    """
    if max_refs == min_refs:
        return 0.5  # Default to mid-score if no variance
    return (raw_references - min_refs) / (max_refs - min_refs)


def convert_year_from_iso(iso_date: str) -> Optional[str]:
    """
    Converts an ISO date string (e.g., "1999-12-31T00:00:00Z") to its year.
    If the date is just a year (e.g., "1999"), it returns the year.
    Returns None if the format is not recognized or input is not a string.
    """
    if not isinstance(iso_date, str):
        return None
    
    # Try to extract year from full ISO format (YYYY-MM-DD...)
    match = re.match(r"^(\d{4})-\d{2}-\d{2}", iso_date)
    if match:
        return match.group(1)
    
    # If it's just a year string
    if re.match(r"^\d{4}$", iso_date):
        return iso_date

    return None

