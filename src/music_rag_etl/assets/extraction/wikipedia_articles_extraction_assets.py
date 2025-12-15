import json
import urllib.parse
import random
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Union
from types import SimpleNamespace

import polars as pl
import wikipediaapi
from dagster import asset, AssetExecutionContext, AssetIn
from langchain_text_splitters import RecursiveCharacterTextSplitter

# from music_rag_etl.assets.transformation.update_relevance_score_asset import artist_index_with_relevance
from music_rag_etl.settings import (
    WIKIPEDIA_CACHE_DIR,
    USER_AGENT,
    WIKIPEDIA_ARTICLES_FILE,
    ARTIST_INDEX_CLEANED,

)
from music_rag_etl.utils.transformation_helpers import clean_text


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
            # WIKIPEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

            # Save to cache
            with open(cache_file_path, 'w', encoding='utf-8') as file:
                file.write(page.text)
            # Gentle delay ;)
            time.sleep(random.uniform(0.1, 0.5))
            return page.text

    except Exception as e:
        context.log.error(f"Error fetching Wikipedia URL {url}: {e}")
    return None


@asset(
    name="fetch_raw_wikipedia_articles",
    deps="artist_index_with_relevance2",
    description="Fetch raw Wikipedia article text and cache it from the Artist index.",
)
def fetch_raw_wikipedia_articles(context: AssetExecutionContext) -> pl.DataFrame:

    context.log.info(f"Loading artist index from Artist Index")
    artist_df = pl.read_ndjson(ARTIST_INDEX_CLEANED)

    wiki_api = wikipediaapi.Wikipedia(user_agent=USER_AGENT, language='en', timeout=30)

    # Ensure the cache directory exists
    WIKIPEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    rows = artist_df.to_dicts()

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_artist = {
            executor.submit(
                get_wikipedia_page,
                context,
                wiki_api,
                row["wikipedia_url"],
                row["wikidata_id"]
            ): row
            for row in rows
            if row.get("wikipedia_url")
        }

        for future in as_completed(future_to_artist):
            row = future_to_artist[future]
            try:
                # If text is empty
                page_text = future.result()
                if page_text is None:
                    continue

                if page_text:
                    results.append(
                        {
                            "wikidata_id": row["wikidata_id"],
                            "artist": row["artist"],
                            "genres": row["genres"],
                            "inception_year": row["inception"],
                            "wikipedia_url": row["wikipedia_url"],
                            "page_text": page_text,
                            "references_score": row["relevance_score"]
                        }
                    )
            except Exception as e:
                context.log.error(
                    f"Error processing artist {row['artist']} ({row['wikidata_id']}): {e}"
                )

    return pl.DataFrame(results)


"""@asset(
    name="chunk_and_enrich_articles",
    deps="raw_fetch_wikipedia_articles",
    description="Chunks articles and enriches them with metadata.",
)
def chunk_and_enrich_articles(
    context: AssetExecutionContext, raw_wikipedia_articles: pl.DataFrame
) -> list:
    
    # Takes a DataFrame of raw articles, chunks them, and enriches each chunk with metadata.
    
    context.log.info(f"Chunking and enriching Wikipedia articles...")

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    all_chunks = []
    for row in raw_wikipedia_articles.to_dicts():
        cleaned_article = clean_text(row["page_text"])
        chunks = text_splitter.split_text(cleaned_article)
        total_chunks = len(chunks)

        # Get genres names

        # Safely get inception year
        inception_year = None
        if row["inception_year"]:
            try:
                inception_year = int(row["inception_year"][:4])
            except (ValueError, TypeError):
                pass

        for i, chunk_text in enumerate(chunks):
            enriched_text = (
                f"Title: {row['artist']}\n"
                f"Genre: {', '.join(row["genres"])}\n"
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
    description="Calculates final relevance score and saves articles to a file.",
)
def wikipedia_articles_file(
    context: AssetExecutionContext, enriched_chunked_articles: list
) -> Path:
    
    # Takes the enriched chunks, calculates the final relevance score,
    # and saves the result to a JSONL file.
    
    if not enriched_chunked_articles:
        context.log.warning("No articles to process.")
        return WIKIPEDIA_ARTICLES_FILE
        
    df = pl.from_dicts(enriched_chunked_articles)

    # Since metadata is a dict, we need to unnest it to calculate relevance score
    # This is getting complex. For now, I'll iterate. A better way is to flatten earlier.
    
    references_counts = [d['metadata']['references_count'] for d in enriched_chunked_articles]
    min_refs = min(references_counts) if references_counts else 0
    max_refs = max(references_counts) if references_counts else 0
    
    final_data = []
    for record in enriched_chunked_articles:
        references = record['metadata']['references_count']
        if max_refs == min_refs:
            relevance_score = 0.5
        else:
            relevance_score = (references - min_refs) / (max_refs - min_refs)
        record['metadata']['relevance_score'] = relevance_score
        final_data.append(record)

    with open(WIKIPEDIA_ARTICLES_FILE, "w", encoding="utf-8") as f:
        for record in final_data:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    context.log.info(f"Successfully saved {len(final_data)} article chunks to {WIKIPEDIA_ARTICLES_FILE}")
    return WIKIPEDIA_ARTICLES_FILE
"""