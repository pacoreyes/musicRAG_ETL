import json
import shutil
import threading
from pathlib import Path
from typing import List, Dict


def initialize_jsonl_file(file_path: Path):
    """
    Creates an empty file, overwriting it if it exists.
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        pass  # Just to create or truncate the file


def append_record_to_jsonl(record: Dict, file_path: Path, lock: threading.Lock):
    """
    Appends a single dictionary record to a JSONL file in a thread-safe manner.

    Args:
        record: The dictionary record to save.
        file_path: The Path object for the output file.
        lock: A threading.Lock object to ensure safe concurrent writes.
    """
    with lock:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def save_to_jsonl(data: List[Dict], file_path: Path, mode: str = "w"):
    """
    Saves a list of dictionaries to a file in JSONL format.

    Args:
        data: The list of dictionary records to save.
        file_path: The Path object for the output file.
        mode: The file open mode ('w' for write/overwrite, 'a' for append).
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, mode, encoding="utf-8") as f:
        for record in data:
            json_string = json.dumps(record, ensure_ascii=False)
            f.write(json_string + "\n")


def merge_jsonl_files(input_paths: List[Path], output_path: Path):
    """
    Merges multiple JSONL files into a single file efficiently.

    Args:
        input_paths: A list of Path objects for the files to merge.
        output_path: The Path object for the destination file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as outfile:
        for input_path in input_paths:
            if input_path.exists():
                with open(input_path, "rb") as infile:
                    shutil.copyfileobj(infile, outfile)


def chunk_list(items: List, size: int):
    """
    Yield successive n-sized chunks from a list.

    Args:
        items: The list to chunk.
        size: The size of each chunk.
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


from contextlib import contextmanager


@contextmanager
def jsonl_writer(file_path: Path):
    """A context manager to write to a JSONL file line by line."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:

        def writer(d: dict):
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

        yield writer
