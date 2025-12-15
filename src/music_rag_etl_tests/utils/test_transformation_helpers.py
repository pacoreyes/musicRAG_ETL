import polars as pl
from polars.testing import assert_frame_equal

from music_rag_etl.utils.transformation_helpers import (
    clean_text,
    extract_unique_ids_from_column,
    get_best_label,
)


# --- Tests for clean_text ---

def test_clean_text():
    """Tests the clean_text function for various cleaning operations."""
    # 1. Setup
    data = {
        "text": [
            '  hello   world  ',
            'line one\nline two',
            'has \"escaped quotes\"',
            ' extra  spaces ',
            None
        ]
    }
    df = pl.DataFrame(data)
    
    expected_data = {
        "text": [
            "hello world",
            "line one line two",
            'has "escaped quotes"',
            "extra spaces",
            None
        ]
    }
    expected_df = pl.DataFrame(expected_data)

    # 2. Action
    cleaned_df = clean_text(df, "text")

    # 3. Assertions
    assert_frame_equal(cleaned_df, expected_df)


# --- Tests for extract_unique_ids_from_column ---

def test_extract_unique_ids_from_column():
    """Tests extracting unique IDs from a column of lists."""
    # 1. Setup
    data = {
        "id": [1, 2, 3, 4, 5, 6],
        "genres": [
            ["Q1", "Q2"],
            ["Q2", "Q3"],
            ["Q1", "Q4"],
            None,
            ["Q5", None, "Q1"], # With a None in the list
            ["Q3", "Q2"],      # Duplicate list
        ],
    }
    df = pl.DataFrame(data)
    
    expected_ids = ["Q1", "Q2", "Q3", "Q4", "Q5"]

    # 2. Action
    unique_ids = extract_unique_ids_from_column(df, "genres")

    # 3. Assertions
    # The function returns a sorted list, so direct comparison is fine
    assert unique_ids == expected_ids


def test_extract_unique_ids_from_column_empty_and_nulls():
    """Tests behavior with empty lists, all nulls, and empty dataframe."""
    # Test with empty lists and nulls
    df1 = pl.DataFrame({"genres": [[], None, ["Q1"], []]})
    assert extract_unique_ids_from_column(df1, "genres") == ["Q1"]
    
    # Test with all nulls
    df2 = pl.DataFrame({"genres": [None, None]})
    assert extract_unique_ids_from_column(df2, "genres") == []

    # Test with an empty dataframe
    df3 = pl.DataFrame({"genres": pl.Series([], dtype=pl.List(pl.Utf8))})
    assert extract_unique_ids_from_column(df3, "genres") == []


# --- Tests for get_best_label ---

def test_get_best_label_priority_success():
    """Tests that get_best_label returns the highest priority available label."""
    record = {
        "artistLabel_es": {"value": "Artista Español"},
        "artistLabel_en": {"value": "English Artist"},
        "artistLabel": {"value": "Generic Artist"},
    }
    label = get_best_label(record, "artistLabel", ['en', 'es'])
    assert label == "English Artist"
    
    label_es_priority = get_best_label(record, "artistLabel", ['es', 'en'])
    assert label_es_priority == "Artista Español"


def test_get_best_label_fallback_to_generic():
    """Tests that get_best_label falls back to the generic label if no prioritized language label is found."""
    record = {
        "artistLabel_fr": {"value": "Artiste Français"},
        "artistLabel": {"value": "Generic Artist"},
    }
    label = get_best_label(record, "artistLabel", ['en', 'es'])
    assert label == "Generic Artist"


def test_get_best_label_no_label_found():
    """Tests that get_best_label returns None if no label is found."""
    record = {
        "otherField": {"value": "Something else"},
    }
    label = get_best_label(record, "artistLabel", ['en', 'es'])
    assert label is None


def test_get_best_label_empty_labels():
    """Tests handling of empty strings as labels."""
    record = {
        "artistLabel_en": {"value": ""},
        "artistLabel_es": {"value": "Artista Español"},
        "artistLabel": {"value": "Generic Artist"},
    }
    label = get_best_label(record, "artistLabel", ['en', 'es'])
    assert label == "Artista Español"
    
    record_no_valid = {
        "artistLabel_en": {"value": ""},
        "artistLabel": {"value": ""},
    }
    label_no_valid = get_best_label(record_no_valid, "artistLabel", ['en'])
    assert label_no_valid is None

