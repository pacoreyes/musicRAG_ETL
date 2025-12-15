import json
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import requests

from music_rag_etl.utils.wikidata_helpers import (
    fetch_wikidata_entity,
    parse_wikidata_entity_label,
    fetch_sparql_query,
    get_best_label,
)

# Mock Dagster context
@pytest.fixture
def mock_context():
    context = MagicMock()
    context.log = MagicMock()
    return context


# --- Tests for fetch_wikidata_entity ---

@patch("music_rag_etl.utils.wikidata_helpers.make_request_with_retries")
@patch("pathlib.Path.exists")
def test_fetch_wikidata_entity_from_api(mock_exists, mock_make_request, mock_context, tmp_path):
    """Tests fetching an entity from the API when it's not in cache."""
    # 1. Setup
    mock_exists.return_value = False
    wikidata_id = "Q123"
    api_response_data = {"entities": {wikidata_id: {"id": wikidata_id}}}
    
    mock_response = MagicMock()
    mock_response.json.return_value = api_response_data
    mock_make_request.return_value = mock_response
    
    # Mock open to capture what's written to the cache file
    m = mock_open()
    with patch("builtins.open", m):
        # 2. Action
        with patch("music_rag_etl.utils.wikidata_helpers.WIKIPEDIA_CACHE_DIR", tmp_path):
            result = fetch_wikidata_entity(mock_context, wikidata_id)

    # 3. Assertions
    assert result == api_response_data
    mock_make_request.assert_called_once()
    m.assert_called_once_with(tmp_path / f"{wikidata_id}.jsonl", "w", encoding="utf-8")
    handle = m()
    handle.write.assert_called_once_with(json.dumps(api_response_data, ensure_ascii=False) + "\n")
    mock_context.log.error.assert_not_called()


@patch("pathlib.Path.exists")
def test_fetch_wikidata_entity_from_cache(mock_exists, mock_context, tmp_path):
    """Tests fetching an entity from the cache."""
    # 1. Setup
    mock_exists.return_value = True
    wikidata_id = "Q456"
    cache_data = {"entities": {wikidata_id: {"id": wikidata_id, "from_cache": True}}}
    cache_content = json.dumps(cache_data) + "\n"
    
    m = mock_open(read_data=cache_content)
    with patch("builtins.open", m):
        # 2. Action
        with patch("music_rag_etl.utils.wikidata_helpers.WIKIPEDIA_CACHE_DIR", tmp_path):
            result = fetch_wikidata_entity(mock_context, wikidata_id)

    # 3. Assertions
    assert result == cache_data
    m.assert_called_once_with(tmp_path / f"{wikidata_id}.jsonl", "r", encoding="utf-8")
    mock_context.log.error.assert_not_called()

def test_fetch_wikidata_entity_malformed_id(mock_context):
    """Tests that a malformed QID is rejected."""
    result = fetch_wikidata_entity(mock_context, "INVALID_ID")
    assert result is None
    mock_context.log.error.assert_called_with("Malformed QID: INVALID_ID")


# --- Tests for parse_wikidata_entity_label ---

def test_parse_wikidata_entity_label_success():
    """Tests successful parsing of an English label."""
    entity_id = "Q188450"
    entity_data = {
        "entities": {
            entity_id: {
                "labels": {
                    "en": {"language": "en", "value": "Synth-pop"},
                    "fr": {"language": "fr", "value": "Synthpop"},
                }
            }
        }
    }
    label = parse_wikidata_entity_label(entity_data, entity_id)
    assert label == "Synth-pop"


def test_parse_wikidata_entity_label_no_english_label():
    """Tests when the English label is missing."""
    entity_id = "Q123"
    entity_data = {"entities": {entity_id: {"labels": {"fr": {"value": "Test"}}}}}
    label = parse_wikidata_entity_label(entity_data, entity_id)
    assert label is None

def test_parse_wikidata_entity_label_no_entity_or_labels():
    """Tests various states of missing data."""
    assert parse_wikidata_entity_label({}, "Q1") is None
    assert parse_wikidata_entity_label({"entities": {}}, "Q1") is None
    assert parse_wikidata_entity_label({"entities": {"Q1": {}}}, "Q1") is None
    assert parse_wikidata_entity_label({"entities": {"Q1": {"labels": {}}}}, "Q1") is None


# --- Tests for fetch_sparql_query ---

@patch("music_rag_etl.utils.wikidata_helpers.make_request_with_retries")
def test_fetch_sparql_query_success(mock_make_request, mock_context):
    """Tests a successful SPARQL query."""
    # 1. Setup
    query = "SELECT ?a WHERE { ?a wdt:P31 wd:Q5 . }"
    api_response_data = {
        "results": {"bindings": [{"a": "1"}, {"a": "2"}]}
    }
    mock_response = MagicMock()
    mock_response.json.return_value = api_response_data
    mock_make_request.return_value = mock_response

    # 2. Action
    results = fetch_sparql_query(mock_context, query)

    # 3. Assertions
    assert results == [{"a": "1"}, {"a": "2"}]
    mock_make_request.assert_called_once()
    mock_context.log.error.assert_not_called()


@patch("music_rag_etl.utils.wikidata_helpers.make_request_with_retries", side_effect=requests.exceptions.RequestException("API Error"))
def test_fetch_sparql_query_request_failure(mock_make_request, mock_context):
    """Tests when the API request fails."""
    results = fetch_sparql_query(mock_context, "query")
    assert results == []
    mock_context.log.error.assert_called_once()


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

