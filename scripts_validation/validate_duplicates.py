import json
from pathlib import Path
import hashlib
from collections import defaultdict

# Define the path to the file to be validated, relative to the project root.
# This makes the script runnable from the project root directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
FILE_TO_VALIDATE = PROJECT_ROOT / "data_volume" / "wikipedia_articles.jsonl"


def hash_string(s: str) -> str:
    """Returns a SHA-256 hash of a string."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def validate_duplicates():
    """
    Reads the JSONL file and performs an exhaustive check for duplicates.
    """
    if not FILE_TO_VALIDATE.exists():
        print(f"Error: File not found at {FILE_TO_VALIDATE}")
        print("Please ensure the ETL pipeline has been run to generate the output file.")
        return

    print(f"--- Starting exhaustive duplicate validation for {FILE_TO_VALIDATE} ---\n")

    # --- Data structures for tracking seen items ---
    first_logical_occurrence = {}
    first_content_occurrence = {}

    # --- Data structures for storing duplicate details ---
    logical_duplicates = defaultdict(list)
    content_duplicates = defaultdict(list)
    full_row_duplicates = defaultdict(list)
    
    seen_full_row_hashes = set()
    total_rows = 0

    with open(FILE_TO_VALIDATE, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            total_rows += 1
            row_num = i + 1
            
            try:
                data = json.loads(line)
                metadata = data.get("metadata", {})
            except json.JSONDecodeError:
                print(f"Error decoding JSON on line {row_num}. Skipping.")
                continue

            # --- Prepare details for the current row ---
            artist_name = metadata.get("artist_name")
            chunk_index = metadata.get("chunk_index")
            article_content = data.get("article", "")

            occurrence_details = {
                "line": row_num,
                "artist": artist_name,
                "chunk": chunk_index,
                "wiki_url": metadata.get("wikipedia_url"),
                "qid_url": metadata.get("wikidata_entity_url"),
            }

            # --- Check 1: Logical Duplicates (Artist + Chunk Index) ---
            if artist_name and chunk_index is not None:
                logical_key = (artist_name, chunk_index)
                if logical_key in first_logical_occurrence:
                    logical_duplicates[logical_key].append(occurrence_details)
                else:
                    first_logical_occurrence[logical_key] = occurrence_details
            
            # --- Check 2: Content Duplicates (Article Text) ---
            content_hash = hash_string(article_content)
            if content_hash in first_content_occurrence:
                content_duplicates[content_hash].append(occurrence_details)
            else:
                first_content_occurrence[content_hash] = occurrence_details
                
            # --- Check 3: Full Row Duplicates (Entire Line) ---
            full_row_hash = hash_string(line.strip())
            if full_row_hash in seen_full_row_hashes:
                full_row_duplicates[full_row_hash].append(row_num)
            else:
                seen_full_row_hashes.add(full_row_hash)

    print("--- Analysis Complete ---")
    print(f"Total rows processed: {total_rows}\n")

    # --- Report Results ---
    any_duplicates_found = False

    # Logical
    if logical_duplicates:
        any_duplicates_found = True
        num_logical_dupes = sum(len(lines) for lines in logical_duplicates.values())
        print(f"🔴 Found {num_logical_dupes} LOGICAL duplicate record(s) (same artist name and chunk index):")
        for key, dupes in logical_duplicates.items():
            artist, chunk = key
            original = first_logical_occurrence[key]
            print(f"\n  - Artist: '{artist}', Chunk: {chunk}")
            print(f"    - First seen on line {original['line']}: (QID: {original['qid_url']})")
            print(f"    - Also seen on {len(dupes)} other line(s):")
            for dupe in dupes:
                print(f"      - Line {dupe['line']}: (QID: {dupe['qid_url']})")
    else:
        print("✅ No logical duplicates found (artist_name + chunk_index).")

    # Content
    if content_duplicates:
        any_duplicates_found = True
        num_content_dupes = sum(len(lines) for lines in content_duplicates.values())
        print(f"\n🔴 Found {num_content_dupes} CONTENT duplicate record(s) (identical article text):")
        for content_hash, dupes in content_duplicates.items():
            original = first_content_occurrence[content_hash]
            print(f"\n  - Content with hash {content_hash[:10]}...")
            print(f"    - First seen on line {original['line']} (Artist: '{original['artist']}', Chunk: {original['chunk']})")
            print(f"      - Wiki URL: {original['wiki_url']}")
            print(f"    - Also generated from {len(dupes)} other source(s):")
            for dupe in dupes:
                print(f"      - Line {dupe['line']} (Artist: '{dupe['artist']}', Chunk: {dupe['chunk']})")
                print(f"        - Wiki URL: {dupe['wiki_url']}")
    else:
        print("\n✅ No content duplicates found (identical article text).")
        
    # Full Row
    if full_row_duplicates:
        any_duplicates_found = True
        num_full_row_dupes = sum(len(lines) for lines in full_row_duplicates.values())
        print(f"\n🔴 Found {num_full_row_dupes} FULL ROW duplicate(s) (identical lines):")
        for row_hash, lines in full_row_duplicates.items():
            print(f"  - Row hash {row_hash[:10]}... appears on lines: {lines}")
    else:
        print("\n✅ No full row duplicates found (identical lines).")
        
    print("\n--- Final Conclusion ---")
    if not any_duplicates_found:
        print("🎉 Exhaustive analysis complete. No duplicates of any kind were found.")
    else:
        print("⚠️ Duplicates were found. Please review the detailed report above.")


if __name__ == "__main__":
    validate_duplicates()
