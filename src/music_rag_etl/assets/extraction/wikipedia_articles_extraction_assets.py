import json
import urllib.parse
import random
import time
from pathlib import Path
from typing import Optional, Union, Dict, Any

import polars as pl
import wikipediaapi
from dagster import asset, AssetExecutionContext, AssetIn
from langchain_text_splitters import RecursiveCharacterTextSplitter

from music_rag_etl.settings import (
    WIKIPEDIA_CACHE_DIR,
    USER_AGENT,
    WIKIPEDIA_ARTICLES_FILE,
    ARTIST_INDEX,
)
from music_rag_etl.utils.transformation_helpers import clean_text, clean_text_string, convert_year_from_iso, normalize_relevance_score
from music_rag_etl.utils.concurrency_helpers import process_items_concurrently
from music_rag_etl.utils.wikipedia_helpers import get_references
from music_rag_etl.utils.io_helpers import save_to_jsonl


def get_wikipedia_page(
    context: AssetExecutionContext,
    wiki_api: wikipediaapi.Wikipedia,
    url: str,
    wikidata_id: str
) -> str | None:
    """
    Fetches a Wikipedia page, either from local text cache or the API.
    """
    # Check if input is violated
    if not url or "/wiki/" not in url:
        return None
    # Define path of the cache file
    cache_file_path = WIKIPEDIA_CACHE_DIR / f"{wikidata_id}.txt"

    # Try cache
    if cache_file_path.exists():
        try:
            with open(cache_file_path, 'r', encoding='utf-8') as file:
                cached_text = file.read()
                return cached_text
        except Exception as e:
            context.log.error(f"Failed to read cache for {wikidata_id}, falling back to API: {e}")

    # Fetch from API
    try:
        raw_title = url.split("/wiki/")[-1]
        decoded_title = urllib.parse.unquote(raw_title)

        page = wiki_api.page(decoded_title)

        if page.exists():
            # Ensure Directory exists
            WIKIPEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

            # Save to cache
            with open(cache_file_path, 'w', encoding='utf-8') as file:
                file.write(page.text)
            # Gentle delay ;)
            time.sleep(random.uniform(0.1, 0.5))
            return page.text

    except Exception as e:
        context.log.error(f"Error fetching Wikipedia URL {url}: {e}")
    return None


def _fetch_and_process_artist_wikipedia(
    context: AssetExecutionContext,
    artist_row: Dict[str, Any],
    wiki_api: wikipediaapi.Wikipedia,
) -> Optional[Dict[str, Any]]:
    """
    Worker function to fetch Wikipedia page text and references for a single artist.
    """
    wikipedia_url = artist_row.get("wikipedia_url")
    wikidata_id = artist_row.get("wikidata_id")

    if not wikipedia_url:
        return None

    page_text = get_wikipedia_page(context, wiki_api, wikipedia_url, wikidata_id)
    if not page_text:
        return None

    references_count = get_references(wikipedia_url)

    processed_row = artist_row.copy()
    processed_row["page_text"] = page_text
    processed_row["references_count"] = references_count
    return processed_row


@asset(
    name="fetch_raw_wikipedia_articles",
    deps="artist_index_with_relevance2",
    description="Fetch raw Wikipedia article text and cache it from the Artist index.",
)
def fetch_raw_wikipedia_articles(context: AssetExecutionContext) -> pl.DataFrame:

    context.log.info(f"Loading artist index from Artist Index")
    artist_df = pl.read_ndjson(ARTIST_INDEX)

    wiki_api = wikipediaapi.Wikipedia(user_agent=USER_AGENT, language='en', timeout=30)

    # Ensure the cache directory exists
    WIKIPEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    rows_to_process = [
        row for row in artist_df.to_dicts() if row.get("wikipedia_url")
    ]

    context.log.info(f"Fetching {len(rows_to_process)} Wikipedia articles concurrently...")

    results = process_items_concurrently(
        items=rows_to_process,
        process_func=lambda row: _fetch_and_process_artist_wikipedia(
            context, row, wiki_api
        ),
        max_workers=5,
    )

    # Filter out None results from failed fetches
    successful_results = [res for res in results if res is not None]

    return pl.DataFrame(successful_results)


@asset(
    name="chunk_and_enrich_articles",
    deps="fetch_raw_wikipedia_articles",
    description="Chunks articles and enriches them with metadata.",
)
def chunk_and_enrich_articles(
    context: AssetExecutionContext, raw_wikipedia_articles: pl.DataFrame
) -> list:
    """
    Takes a DataFrame of raw articles, chunks them, and enriches each chunk with metadata.
    """
    context.log.info(f"Chunking and enriching Wikipedia articles...")

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    all_chunks = []
    for row in raw_wikipedia_articles.to_dicts():
        cleaned_article = clean_text_string(row["page_text"])
        chunks = text_splitter.split_text(cleaned_article)
        total_chunks = len(chunks)

        # Clean genres
        cleaned_genres = [clean_text_string(g) for g in row["genres"]] if row["genres"] else []
        genre_str = ", ".join(cleaned_genres) if cleaned_genres else "Unknown"

        # Safely get inception year
        inception_year = None
        if row["inception_year"]:
            try:
                converted_year = convert_year_from_iso(row["inception_year"])
                if converted_year:
                    inception_year = int(converted_year)
            except (ValueError, TypeError):
                context.log.warning(f"Could not convert inception year for {row['artist']} ({row['wikidata_id']}): {row['inception_year']}")


        for i, chunk_text in enumerate(chunks):
            enriched_text = (
                f"Title: {row['artist']}\n"
                f"Genre: {genre_str}\n"
                f"Content: {chunk_text}"
            )
            all_chunks.append(
                {
                    "metadata": {
                        "artist_name": row["artist"],
                        "genre": cleaned_genres,
                        "inception_year": inception_year,
                        "wikipedia_url": row["wikipedia_url"],
                        "wikidata_entity": row["wikidata_id"],
                        "references_count": row["references_count"],
                        "chunk_index": i,
                        "total_chunks": total_chunks,
                    },
                    "article": enriched_text,
                }
            )

    return all_chunks


@asset(
    name="wikipedia_articles_file",
    deps=["chunk_and_enrich_articles"],
    description="Calculates final relevance score and saves articles to a file.",
)
def wikipedia_articles_file(
    context: AssetExecutionContext, enriched_chunked_articles: list
) -> Path:
    """
    Takes the enriched chunks, calculates the final relevance score,
    and saves the result to a JSONL file.
    """
    if not enriched_chunked_articles:
        context.log.warning("No articles to process.")
        return WIKIPEDIA_ARTICLES_FILE
        
    # Extract all references_count to determine min/max for normalization
    references_counts = [d['metadata']['references_count'] for d in enriched_chunked_articles if 'references_count' in d['metadata']]
    min_refs = min(references_counts) if references_counts else 0
    max_refs = max(references_counts) if references_counts else 0
    
    final_data = []
    for record in enriched_chunked_articles:
        raw_references = record['metadata'].get('references_count', 0)
        relevance_score = normalize_relevance_score(raw_references, min_refs, max_refs)
        
        record['metadata']['relevance_score'] = relevance_score
        final_data.append(record)

    save_to_jsonl(final_data, WIKIPEDIA_ARTICLES_FILE)

    context.log.info(f"Successfully saved {len(final_data)} article chunks to {WIKIPEDIA_ARTICLES_FILE}")
    return WIKIPEDIA_ARTICLES_FILE