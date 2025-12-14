"""
This script provides utility functions for cleaning and processing text data in
JSON and JSONL files.

It includes a function to transform text using a combination of libraries, and a
function to process a file, applying the cleaning to a specified text field.
"""
import json
from pathlib import Path

import cleantext
import ftfy
from tqdm import tqdm


def clean_text(text: str) -> str:
    """
    Cleans a given text string using ftfy, unidecode, and cleantext.

    The order of operations is crucial:
    1. ftfy: Fixes broken Unicode, converting mojibake/escape sequences to proper Unicode characters.
    2. cleantext: Applies general cleanup (extra spaces, HTML removal, etc.).

    Args:
        text: The text to transform.

    Returns:
        The cleaned text.
    """
    text = text.replace('\"', '"').replace('\n', " ").replace('\n\n', "")
    text = ftfy.fix_text(text)
    text = cleantext.clean(
        text,
        clean_all=False,

        # General Cleaning
        extra_spaces=True,      # Remove extra whitespace
        
        # Keep features as requested in the commented block
        lowercase=False,        # Keep casing
        numbers=False,          # Keep numbers
        
        # Optional linguistic normalization (can be enabled if needed)
        stemming=False,
        stopwords=False,
    )
    return text


def process_file(file_path: Path, text_field: str) -> None:
    """
    Processes a JSON or JSONL file, cleans the text in the specified field,
    and saves the cleaned data to a new file.

    Args:
        file_path: The path to the input file.
        text_field: The name of the field containing the text to transform.
    """
    if not file_path.exists():
        print(f"Error: Input file not found at {file_path}")
        return

    output_path = file_path.with_name(f"{file_path.stem}_cleaned{file_path.suffix}")

    # Use 'r' and 'w' with utf-8 encoding for reliable Unicode handling
    try:
        with file_path.open("r", encoding="utf-8") as infile:
            if file_path.suffix == ".jsonl":
                # JSONL: Read line by line
                lines = infile.readlines()
                total_lines = len(lines)
                
                with output_path.open("w", encoding="utf-8") as outfile:
                    with tqdm(
                        lines,
                        desc="Cleaning JSONL file",
                        unit="line",
                        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} lines [{elapsed}<{remaining}, {rate_fmt}]"
                    ) as pbar:
                        for line in pbar:
                            try:
                                data = json.loads(line)
                                if text_field in data and isinstance(data[text_field], str):
                                    data[text_field] = clean_text(data[text_field])
                                outfile.write(json.dumps(data, ensure_ascii=False) + "\n")
                            except json.JSONDecodeError:
                                print(f"Skipping invalid JSON line: {line.strip()}")
            
            elif file_path.suffix == ".json":
                # JSON: Load the entire file structure
                infile.seek(0)
                data = json.load(infile)

                with output_path.open("w", encoding="utf-8") as outfile:
                    if isinstance(data, list):
                        # Handle list of objects
                        with tqdm(
                            data,
                            desc="Cleaning JSON list",
                            unit="item",
                            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} items [{elapsed}<{remaining}, {rate_fmt}]"
                        ) as pbar:
                            for item in pbar:
                                if text_field in item and isinstance(item[text_field], str):
                                    item[text_field] = clean_text(item[text_field])
                    elif isinstance(data, dict):
                        # Handle single object
                        if text_field in data and isinstance(data[text_field], str):
                            data[text_field] = clean_text(data[text_field])
                    
                    json.dump(data, outfile, indent=2, ensure_ascii=False)

    except json.JSONDecodeError:
        print(f"Skipping invalid JSON file: {file_path}. Please check file structure.")
        return
    except Exception as e:
        print(f"An unexpected error occurred during file processing: {e}")
        return

    print(f"Cleaned data saved to: {output_path}")
