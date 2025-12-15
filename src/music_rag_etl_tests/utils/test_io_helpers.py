import json
from pathlib import Path

from music_rag_etl.utils.io_helpers import save_to_jsonl


def test_save_to_jsonl(tmp_path: Path):
    """
    Tests that save_to_jsonl correctly writes a list of dicts to a JSONL file.
    """
    # 1. Setup
    # Create a temporary directory and define the output file path
    output_dir = tmp_path / "test_data"
    output_file = output_dir / "output.jsonl"

    # Sample data to be written
    sample_data = [
        {"id": 1, "name": "Alice", "tags": ["A", "B"]},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie", "extra": {"nested": True}},
        {"id": 4, "name": "Álvaro"},  # With unicode character
    ]

    # 2. Action
    save_to_jsonl(sample_data, output_file)

    # 3. Assertions
    # Check that the file was created
    assert output_file.exists()

    # Read the file and check its contents
    lines = output_file.read_text(encoding="utf-8").strip().split("\n")
    
    # Verify the number of lines
    assert len(lines) == len(sample_data)

    # Verify the content of each line
    for i, line in enumerate(lines):
        # Parse the JSON from the line and compare with the original data
        reloaded_record = json.loads(line)
        assert reloaded_record == sample_data[i]
    
    # Verify unicode character was written correctly
    reloaded_data = [json.loads(line) for line in lines]
    assert reloaded_data[3]["name"] == "Álvaro"

def test_save_to_jsonl_empty_list(tmp_path: Path):
    """
    Tests that save_to_jsonl correctly creates an empty file for an empty list.
    """
    # 1. Setup
    output_file = tmp_path / "empty.jsonl"
    sample_data = []

    # 2. Action
    save_to_jsonl(sample_data, output_file)

    # 3. Assertions
    assert output_file.exists()
    assert output_file.read_text(encoding="utf-8") == ""

def test_save_to_jsonl_creates_directory(tmp_path: Path):
    """
    Tests that save_to_jsonl creates the parent directory if it doesn't exist.
    """
    # 1. Setup
    # Define a path with a non-existent parent directory
    output_dir = tmp_path / "new_dir" / "sub_dir"
    output_file = output_dir / "output.jsonl"
    
    assert not output_dir.exists()

    sample_data = [{"id": 1, "message": "hello"}]

    # 2. Action
    save_to_jsonl(sample_data, output_file)

    # 3. Assertions
    assert output_dir.exists()
    assert output_file.exists()
    assert output_file.read_text(encoding="utf-8").strip() == '{"id": 1, "message": "hello"}'
