import json
from pathlib import Path
from typing import List, Dict
import shutil


def save_to_jsonl(data: List[Dict], file_path: Path):
    """
    Saves a list of dictionaries to a file in JSONL format.

    Args:
        data: The list of dictionary records to save.
        file_path: The Path object for the output file.
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


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
