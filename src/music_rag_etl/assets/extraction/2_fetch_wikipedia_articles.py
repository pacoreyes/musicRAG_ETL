"""
This script fetches the full text of Wikipedia articles for a list of artists
provided in a JSONL file.

It applies a Sliding Window technique to split long articles into semantic chunks
and injects Metadata Context (Title, Genre) directly into the text payload
to ensure high-quality vector retrieval.

The output is a JSONL file where each line is a specific CHUNK of an article.
"""
import json
import urllib.parse

import wikipediaapi
from tqdm import tqdm
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

# NEW: Import for Sliding Window Chunking
from langchain_text_splitters import RecursiveCharacterTextSplitter

from scripts_etl.config import (
    ARTIST_INDEX,
    WIKIPEDIA_ARTICLES_FILE,
    WIKIPEDIA_ARTICLES_FILE_TEMP,
    WIKI_CACHE_DIR,  # Import WIKI_CACHE_DIR
    USER_AGENT,
)
from scripts_etl.utils.convert_year_from_iso import convert_year_from_iso
from scripts_etl.utils.calculate_references_from_wikipedia import get_references
from scripts_etl.utils.calculate_relevance_score import calculate_relevance_score
from scripts_etl.utils.clean_text_json import clean_text


def get_page_text_from_url(wiki_api: wikipediaapi.Wikipedia, url: str, cache_file_path: Path) -> str | None:
    """
    Fetches the text of a Wikipedia page from its URL, utilizing a cache.
    If the article is in the cache, it reads it from there. Otherwise, it fetches it
    from Wikipedia and saves it to the cache.
    """
    if not url or "/wiki/" not in url:
        return None

    # Check cache first
    if cache_file_path.exists():
        try:
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logging.warning(f"Error reading cached file {cache_file_path}: {e}. Attempting to fetch from Wikipedia.")

    try:
        # Extract the title from the URL
        raw_title = url.split("/wiki/")[-1]
        decoded_title = urllib.parse.unquote(raw_title)

        # Fetch the page
        page = wiki_api.page(decoded_title)
        if page.exists():
            page_text = page.text
            # Save to cache
            cache_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file_path, 'w', encoding='utf-8') as f:
                f.write(page_text)
            # Add a small random delay to avoid hitting API rate limits for each request
            time.sleep(random.uniform(0.5, 2.0))
            return page_text
        else:
            return None
    except Exception as e:
        logging.error(f"Error fetching URL {url} for cache {cache_file_path}: {e}")
        return None


def process_artist(artist_data: dict, wiki_api: wikipediaapi.Wikipedia,
                   text_splitter: RecursiveCharacterTextSplitter) -> list[str]:
    """
    Processes a single artist: fetches their Wikipedia article (with caching), cleans it,
    applies sliding window chunking, and returns a list of JSON-serialized chunks.
    """
    wiki_url = artist_data.get('wikipedia_url')
    wiki_artist_name = artist_data.get('artist')
    wiki_music_genre = artist_data.get('genre', )
    wikidata_entity = artist_data.get('wikidata_id')

    if not wiki_artist_name:
        logging.warning(f"Skipping entry with empty artist name: {wikidata_entity}")
        return []

    if not wikidata_entity:
        logging.warning(f"Skipping artist '{wiki_artist_name}' with no wikidata_id.")
        return []

    cache_file_path = WIKI_CACHE_DIR / f"{wikidata_entity}.txt"

    # Safe conversions for genre
    cleaned_genre_list = []
    if wiki_music_genre:
        if isinstance(wiki_music_genre, str):
            cleaned_genre_list = [clean_text(wiki_music_genre)]
        elif isinstance(wiki_music_genre, list):
            cleaned_genre_list = [clean_text(g) for g in wiki_music_genre]
        else:
            logging.warning(f"Unexpected genre type for {wiki_artist_name}: {type(wiki_music_genre)}")

    genre_str = ", ".join(cleaned_genre_list) if cleaned_genre_list else "Unknown Genre"

    # Safe conversion for inception year
    wiki_origin_date = None
    inception_raw = artist_data.get('inception')
    if inception_raw:
        try:
            wiki_origin_date = int(convert_year_from_iso(inception_raw))
        except (ValueError, TypeError) as e:
            logging.warning(f"Inception year conversion error for {wiki_artist_name} \
              ('{inception_raw}'): {e}. Setting to None.")

    article_chunks_json = []

    if wiki_url and wiki_url != 'N/A':
        page_text = get_page_text_from_url(wiki_api, wiki_url, cache_file_path)

        if page_text:
            cleaned_article = clean_text(page_text)
            references = get_references(wiki_url)
            # relevance_score = calculate_relevance_score(len(references))
            chunks = text_splitter.split_text(cleaned_article)

            for i, chunk_text in enumerate(chunks):
                enriched_text = (
                    f"Title: {wiki_artist_name}\n"
                    f"Genre: {genre_str}\n"
                    f"Content: {chunk_text}"
                )

                article_json = {
                    "metadata": {
                        "artist_name": wiki_artist_name,
                        "genre": cleaned_genre_list,
                        "inception_year": wiki_origin_date,
                        "wikipedia_url": wiki_url,
                        "wikidata_entity": wikidata_entity,
                        "relevance_score": len(references),
                        # "is_verified": "True",
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                        # "is_parent": len(chunks) == 1

                    },
                    "article": enriched_text
                }
                article_chunks_json.append(json.dumps(article_json, ensure_ascii=False))

    return article_chunks_json


def process_artists_file() -> None:
    """
    Reads a JSONL file of artists, fetches their Wikipedia articles,
    applies sliding window chunking, and saves enriched chunks to a new JSONL file.
    """
    # Ensure the input file exists
    if not ARTIST_INDEX.exists():
        logging.error(f"Error: Input file '{ARTIST_INDEX}' not found.")
        return

    # Create cache directory if it doesn't exist
    WIKI_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize the Wikipedia API
    wiki_api = wikipediaapi.Wikipedia(user_agent=USER_AGENT, language='en')

    # Initialize the Sliding Window Splitter
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )

    # Warm-up call to initialize cleantext resources in the main thread
    # This prevents a known race condition with the underlying nltk library.
    logging.info("Initializing text cleaning resources...")
    try:
        clean_text("warm-up")
        logging.info("Text cleaning resources initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize text cleaning resources: {e}. Aborting.")
        return

    # Read all artist data once
    all_artists_data = []
    with open(ARTIST_INDEX, 'r', encoding='utf-8') as infile:
        for line in infile:
            try:
                all_artists_data.append(json.loads(line))
            except json.JSONDecodeError:
                logging.error(f"Could not decode JSON from line in {ARTIST_INDEX}: {line.strip()}")

    total_artists = len(all_artists_data)
    logging.info(f"Loaded {total_artists} artists for processing.")

    # Clear the output file at the start
    with open(WIKIPEDIA_ARTICLES_FILE, 'w', encoding='utf-8') as outfile:
        pass # Truncate file

    # Process artists in parallel
    # Using max_workers=5 for now, can be made configurable

    # all_artists_data = all_artists_data[:100]  #split fir testing
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_artist, artist_data, wiki_api, text_splitter): artist_data for artist_data in all_artists_data}

        processed_count = 0
        with open(WIKIPEDIA_ARTICLES_FILE, 'a', encoding='utf-8') as outfile:
            for future in tqdm(as_completed(futures), total=total_artists, desc="Fetching & Chunking Articles"):
                artist_data = futures[future]
                artist_name = artist_data.get('artist', 'Unknown Artist')
                try:
                    chunks_json = future.result()
                    for chunk_json in chunks_json:
                        outfile.write(chunk_json + '\n')
                    processed_count += 1
                except Exception as e:
                    logging.error(f"Error processing artist {artist_name}: {e}")

    logging.info(f"Success! Processed {processed_count} artists. Data saved to '{WIKIPEDIA_ARTICLES_FILE}'")

    # Calculate relevance score iterating the content of WIKIPEDIA_ARTICLES_FILE
    # Generate a new file with the new relevance scores updated
    updated_lines = []
    # Open original JSON file
    with open(WIKIPEDIA_ARTICLES_FILE, "r", encoding="utf-8") as infile:
        lines = infile.readlines()
        for line in tqdm(lines, desc="Updating relevance scores"):
            try:
                # Convert JSON string to dict
                data = json.loads(line)
                # Update the relevance score
                references = data['metadata']["relevance_score"]
                data['metadata']["relevance_score"] = calculate_relevance_score(references)
                # Store updated dictionary
                updated_lines.append(data)
            except json.JSONDecodeError:
                logging.error(f"Could not decode JSON from line: {line.strip()}")

    # WRITE the stored dictionaries to the TEMP file
    with open(WIKIPEDIA_ARTICLES_FILE_TEMP, 'w', encoding='utf-8') as f:
        # Save rows with updated relevance scores in temp file
        for line in updated_lines:
            json_record = json.dumps(line, ensure_ascii=False)
            f.write(json_record + '\n')

    # ATOMIC SWAP: original replaced by temp
    try:
        WIKIPEDIA_ARTICLES_FILE.unlink(missing_ok=True)
        # Rename temp JSONL file WIKIPEDIA_ARTICLES_FILE_TEMP with WIKIPEDIA_ARTICLES_FILE
        WIKIPEDIA_ARTICLES_FILE_TEMP.rename(WIKIPEDIA_ARTICLES_FILE)
    except json.JSONDecodeError:
        logging.error(f"Failed to perform atomic file swap: {e}")

    logging.info(f"JSONL file {WIKIPEDIA_ARTICLES_FILE} correctly updated with calculated relevance scores")


if __name__ == "__main__":
    process_artists_file()
