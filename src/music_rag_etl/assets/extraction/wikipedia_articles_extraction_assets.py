import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Any, Tuple

from transformers import AutoTokenizer
import polars as pl
import wikipediaapi
from dagster import asset, AssetExecutionContext
from langchain_text_splitters import RecursiveCharacterTextSplitter

from music_rag_etl.settings import (
    USER_AGENT,
    WIKIPEDIA_ARTICLES_FILE,
    ARTIST_INDEX,
    GENRES_FILE,
    PATH_TEMP
)
from music_rag_etl.utils.io_helpers import save_to_jsonl, merge_jsonl_files
from music_rag_etl.utils.transformation_helpers import clean_text_string
from music_rag_etl.utils.wikipedia_helpers import fetch_artist_article_payload


def _clean_cache_directory(cache_dir: Path):
    """Ensures the cache directory is empty."""
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)


@asset(
    name="create_wikipedia_articles_dataset",
    deps=["genres_extraction_from_artist_index"],
    description="Fetch raw Wikipedia article text, split them in chunks and enrich them with metadata.",
)
def create_wikipedia_articles_dataset(
    context: AssetExecutionContext,
) -> Path:
    """
    Full pipeline: load artist index and genre labels, then for each artist,
    fetch and cache the raw article, chunk it, enrich metadata, and save
    incrementally to a JSONL file.
    """
    context.log.info("Loading artist index and genres lookup.")
    artist_df = pl.read_ndjson(ARTIST_INDEX)
    genres_df = pl.read_ndjson(GENRES_FILE)
    genre_lookup = dict(
        zip(genres_df["wikidata_id"].to_list(), genres_df["genre_label"].to_list())
    )

    wiki_api = wikipediaapi.Wikipedia(user_agent=USER_AGENT, language="en", timeout=30)
    
    _clean_cache_directory(PATH_TEMP)
    context.log.info(f"Cleaned and prepared cache directory at {PATH_TEMP}")

    rows_to_process = [row for row in artist_df.to_dicts() if row.get("wikipedia_url")]

    # De-duplicate rows based on the wikipedia_url to prevent processing the same article twice
    seen_urls = set()
    unique_rows_to_process = []
    for row in rows_to_process:
        if row["wikipedia_url"] not in seen_urls:
            unique_rows_to_process.append(row)
            seen_urls.add(row["wikipedia_url"])
    
    total_rows = len(unique_rows_to_process)
    context.log.info(
        f"Found {len(rows_to_process)} total rows, "
        f"de-duplicated to {total_rows} unique wikipedia articles."
    )
    
    # Prepare items with their designated temporary file path
    items_to_process = [
        (i, row, PATH_TEMP / f"{i}.jsonl")
        for i, row in enumerate(unique_rows_to_process)
    ]

    context.log.info(
        f"Starting concurrent fetching and processing of {total_rows} Wikipedia articles..."
    )

    tokenizer = AutoTokenizer.from_pretrained("nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True)

    text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer,
        chunk_size=2048,
        chunk_overlap=256,
        separators=["\n\n", "\n", ". ", " ", ""]
    )

    def process_artist_to_temp_file(item: Tuple[int, Dict[str, Any], Path]):
        """
        Worker function: fetches an article, processes it, and writes chunks to a temporary JSONL file.
        """
        _, artist_row, temp_file_path = item
        article_payload = fetch_artist_article_payload(
            context=context,
            wiki_api=wiki_api,
            artist_row=artist_row,
            genre_lookup=genre_lookup,
        )

        if not article_payload or not article_payload.get("page_text"):
            return

        cleaned_article = clean_text_string(article_payload["page_text"])
        chunks = text_splitter.split_text(cleaned_article)
        total_chunks = len(chunks)

        genres = article_payload.get("genres") or []
        # genre_header = genres[0] if genres else "N/A"

        artist_chunks = []
        for i, chunk_text in enumerate(chunks):
            title = article_payload["artist"]
            # Create enriched text to prepend to the chunk
            enriched_text = (
                f"search_document: {article_payload["artist"]} | {chunk_text}"
            )
            # Remove " . " pattern
            enriched_text = enriched_text.replace(" | . ", " | ")
            chunk_record = {
                "metadata": {
                    "title": title,
                    "artist_name": article_payload["artist"],
                    "genres": genres,
                    "inception_year": article_payload["inception_year"],
                    "wikipedia_url": article_payload["wikipedia_url"],
                    "wikidata_entity": article_payload["wikidata_id"],
                    "relevance_score": article_payload["references_score"],
                    "chunk_index": i + 1,
                    "total_chunks": total_chunks,
                },
                "article": enriched_text,
            }
            artist_chunks.append(chunk_record)
        
        if artist_chunks:
            save_to_jsonl(artist_chunks, temp_file_path)

    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(process_artist_to_temp_file, items_to_process))

    context.log.info("Concurrent processing finished. Merging temporary files...")

    # Merge temporary files in order
    temp_files_in_order = sorted(PATH_TEMP.glob("*.jsonl"), key=lambda f: int(f.stem))
    merge_jsonl_files(temp_files_in_order, WIKIPEDIA_ARTICLES_FILE)
    
    context.log.info(f"Successfully merged {len(temp_files_in_order)} files into {WIKIPEDIA_ARTICLES_FILE}.")

    # Clean up temporary files
    _clean_cache_directory(PATH_TEMP)
    context.log.info("Cleaned up temporary cache files.")

    return WIKIPEDIA_ARTICLES_FILE
