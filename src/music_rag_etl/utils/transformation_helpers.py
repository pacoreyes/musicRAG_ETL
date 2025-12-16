import polars as pl
import re
from typing import List, Dict


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


def map_genre_ids_to_labels(
    genre_ids: list[str] | None, genre_lookup: dict[str, str]
) -> list[str]:
    """
    Maps genre QIDs to their labels using a lookup dictionary.
    """
    if not genre_ids:
        return []
    return [genre_lookup[g_id] for g_id in genre_ids if g_id in genre_lookup]


def map_genre_labels_to_ids(
    genre_labels: list[str] | None, label_to_id_lookup: dict[str, str]
) -> list[str]:
    """
    Maps genre labels to their QIDs using a lookup dictionary.
    """
    if not genre_labels:
        return []
    return [label_to_id_lookup[label] for label in genre_labels if label in label_to_id_lookup]
