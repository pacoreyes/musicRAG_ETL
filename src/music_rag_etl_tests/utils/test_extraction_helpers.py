from unittest.mock import MagicMock, patch, mock_open

import pytest

from music_rag_etl.utils.extraction_helpers import (
    fetch_wikidata_data,
    process_artist_record,
    _get_value,
)

@pytest.fixture
def mock_context():
    """Fixture for a mock Dagster context."""
    context = MagicMock()
    context.log = MagicMock()
    return context


# --- Tests for fetch_wikidata_data ---

@patch("music_rag_etl.utils.wikidata_helpers.fetch_sparql_query")
def test_fetch_wikidata_data_success(mock_fetch_sparql, mock_context, tmp_path):
    """Tests the happy path where data is fetched, processed, and saved."""
    # 1. Setup
    output_path = tmp_path / "output.jsonl"
    
    # Mock the SPARQL query to return two batches of results
    mock_fetch_sparql.side_effect = [
        [{"artist": {"value": "Q1"}}, {"artist": {"value": "Q2"}}],
        [{"artist": {"value": "Q3"}}],
        [], # Empty list to terminate the loop
    ]
    
    # A simple processor function for the test
    def simple_processor(record):
        return {"processed": record["artist"]["value"]}
        
    # A mock query function
    get_query_func = MagicMock(return_value="SPARQL QUERY")

    m = mock_open()
    with patch("builtins.open", m):
        # 2. Action
        fetch_wikidata_data(
            context=mock_context,
            output_path=output_path,
            get_query_function=get_query_func,
            record_processor=simple_processor,
            start_year=2000,
            end_year=2001,
            label="test_run",
        )

    # 3. Assertions
    assert mock_fetch_sparql.call_count == 3
    # Check that the file was written to
    handle = m()
    assert handle.write.call_count == 3 # Q1, Q2, Q3
    handle.write.assert_any_call('{"processed": "Q1"}\n')
    handle.write.assert_any_call('{"processed": "Q2"}\n')
    handle.write.assert_any_call('{"processed": "Q3"}\n')
    
    # Check logs
    mock_context.log.info.assert_any_call("Retrieved batch 1 for test_run: 2 records")
    mock_context.log.info.assert_any_call("Retrieved batch 2 for test_run: 1 records")
    mock_context.log.info.assert_any_call("No more results from endpoint for test_run. Extraction finished.")
    mock_context.log.info.assert_any_call("Total records stored in output.jsonl: 3")


@patch("music_rag_etl.utils.wikidata_helpers.fetch_sparql_query", side_effect=Exception("Unexpected Error"))
def test_fetch_wikidata_data_critical_error(mock_fetch_sparql, mock_context, tmp_path):
    """Tests that the function exits gracefully on an unexpected exception."""
    # 1. Setup
    output_path = tmp_path / "output.jsonl"
    get_query_func = MagicMock()
    
    m = mock_open()
    with patch("builtins.open", m):
        # 2. Action
        fetch_wikidata_data(
            context=mock_context,
            output_path=output_path,
            get_query_function=get_query_func,
            record_processor=lambda r: r,
            start_year=2000,
            end_year=2001,
            label="test_error",
        )

    # 3. Assertions
    mock_context.log.error.assert_called_once_with(
        "An unexpected critical error occurred in batch 1 for test_error: Unexpected Error"
    )
    # Ensure no data was written
    handle = m()
    handle.write.assert_not_called()


# --- Tests for process_artist_record ---

def test_process_artist_record_full():
    """Tests processing a complete and valid artist record."""
    item = {
        "artist": {"value": "http://www.wikidata.org/entity/Q123"},
        "artistLabel_en": {"value": "The Band Name"},
        "genres": {"value": "Q2|Q1"},
        "aliases": {"value": "Band Name|The Band"},
        "wikipedia_url": {"value": "http://en.wikipedia.org/wiki/The_Band"},
        "date": {"value": "2001-01-01T00:00:00Z"},
        "linkcount": {"value": "42"},
    }
    processed = process_artist_record(item)
    assert processed == {
        "wikidata_id": "Q123",
        "artist": "The Band Name",
        "aliases": sorted(["Band Name", "The Band"]),
        "genres": sorted(["Q1", "Q2"]),
        "wikipedia_url": "http://en.wikipedia.org/wiki/The_Band",
        "inception": "2001-01-01T00:00:00Z",
        "linkcount": "42",
    }


def test_process_artist_record_missing_fields():
    """Tests a record with many missing (but not essential) fields."""
    item = {
        "artist": {"value": "http://www.wikidata.org/entity/Q456"},
        "artistLabel": {"value": "A Label"}, # Fallback label
    }
    processed = process_artist_record(item)
    assert processed == {
        "wikidata_id": "Q456",
        "artist": "A Label",
        "aliases": [],
        "genres": [],
        "wikipedia_url": "",
        "inception": None,
        "linkcount": None,
    }

def test_process_artist_record_no_artist_uri():
    """Tests that a record is skipped if it has no artist URI."""
    item = {"artistLabel": {"value": "No Artist URI"}}
    assert process_artist_record(item) is None


def test_process_artist_record_no_label():
    """Tests that a record is skipped if it has no usable label."""
    item = {"artist": {"value": "http://www.wikidata.org/entity/Q789"}}
    assert process_artist_record(item) is None


# --- Test for _get_value ---

def test_get_value():
    """Tests the internal _get_value helper."""
    assert _get_value({"key": {"value": "abc"}}, "key") == "abc"
    assert _get_value({"key": {"other": "abc"}}, "key") is None
    assert _get_value({}, "key") is None
