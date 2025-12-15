import pytest
from music_rag_etl.utils.concurrency_helpers import process_items_concurrently


# --- Tests for process_items_concurrently ---

def sample_worker_function(x):
    """A simple function for testing concurrency: square a number."""
    if x < 0:
        raise ValueError("Negative numbers not allowed")
    if x % 2 == 0:
        return x * x
    return None  # Simulate a task that yields no result


def test_process_items_concurrently_success():
    """Tests the concurrent processor with a simple worker function."""
    items = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] # Use slicing strategy
    
    # We only expect results for even numbers
    expected_results = [0, 4, 16, 36, 64]
    
    results = process_items_concurrently(items, sample_worker_function, max_workers=2)
    
    # Sort results because concurrency does not guarantee order
    assert sorted(results) == expected_results

def test_process_items_concurrently_with_exceptions(capsys):
    """Tests that exceptions in worker functions are caught and reported."""
    items = [1, 2, -3, 4]
    
    expected_results = [4, 16]
    
    results = process_items_concurrently(items, sample_worker_function, max_workers=2)
    
    captured = capsys.readouterr()
    
    assert sorted(results) == expected_results
    assert "Error processing item" in captured.err
    assert "Negative numbers not allowed" in captured.err
