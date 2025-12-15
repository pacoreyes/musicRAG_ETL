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


def get_best_label(
    record: Dict[str, any],
    base_key: str,
    lang_priority: List[str] = ['en', 'es', 'fr', 'de']
) -> Optional[str]:
    """
    Finds the best available label from a SPARQL record based on a language priority list.

    Args:
        record: The dictionary representing a single record from the SPARQL query result.
        base_key: The base name for the label key (e.g., "artistLabel").
        lang_priority: A list of language codes in order of preference (e.g., ['en', 'es']).

    Returns:
        The first non-empty label found, or None if no suitable label is available.
    """
    # First, try specific language labels based on priority
    for lang in lang_priority:
        label = record.get(f"{base_key}_{lang}", {}).get("value")
        if label:
            return label

    # If no prioritized language label is found, try the generic label
    generic_label = record.get(base_key, {}).get("value")
    if generic_label:
        return generic_label
    
    return None

