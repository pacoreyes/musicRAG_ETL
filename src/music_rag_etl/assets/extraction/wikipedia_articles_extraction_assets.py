import asyncio
import aiohttp
from functools import partial
from pathlib import Path
from typing import Dict, Any, Tuple, List

from transformers import AutoTokenizer
import polars as pl
from dagster import asset, AssetExecutionContext
from langchain_text_splitters import RecursiveCharacterTextSplitter

from music_rag_etl.settings import (
    WIKIPEDIA_ARTICLES_FILE,
    ARTIST_INDEX,
    GENRES_FILE,
)
from music_rag_etl.utils.io_helpers import save_to_jsonl
from music_rag_etl.utils.transformation_helpers import clean_text_string
from music_rag_etl.utils.concurrency_helpers import process_items_incrementally_async
from music_rag_etl.utils.wikipedia_helpers import async_fetch_artist_article_payload
from music_rag_etl.utils.request_utils import create_aiohttp_session


@asset(
    name="create_wikipedia_articles_dataset",
    deps=["genres_extraction_from_artist_index"],
    description="Fetch raw Wikipedia article text, split them in chunks and enrich them with metadata.",
)
async def create_wikipedia_articles_dataset(
    context: AssetExecutionContext,
) -> Path:
    """
    Full pipeline: load artist index and genre labels, then for each artist,
    fetch and cache the raw article, chunk it, enrich metadata, and save
    incrementally to a single JSONL file.
    """
    context.log.info("Loading artist index and genres lookup.")
    artist_df = pl.read_ndjson(ARTIST_INDEX)
    genres_df = pl.read_ndjson(GENRES_FILE)
    genre_lookup = dict(
        zip(genres_df["id"].to_list(), genres_df["genre_label"].to_list())
    )

    # 1. Create the final output file upfront, ensuring it's empty
    WIKIPEDIA_ARTICLES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(WIKIPEDIA_ARTICLES_FILE, "w", encoding="utf-8") as f:
        pass  # Create an empty file
    context.log.info(f"Created empty output file at {WIKIPEDIA_ARTICLES_FILE}")

    rows_to_process = [row for row in artist_df.to_dicts() if row.get("wikipedia_url")]
    total_rows = len(rows_to_process)
    context.log.info(
        f"Starting concurrent fetching and processing of {total_rows} Wikipedia articles..."
    )

    tokenizer = AutoTokenizer.from_pretrained(
        "nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True
    )
    text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer,
        chunk_size=2048,
        chunk_overlap=256,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    async def async_process_artist(
        artist_row: Dict[str, Any],
        session: aiohttp.ClientSession
    ) -> List[Dict[str, Any]]:
        """
        Worker function: fetches an article, processes it, and returns a list of chunk records.
        """
        article_payload = await async_fetch_artist_article_payload(
            context=context,
            artist_row=artist_row,
            genre_lookup=genre_lookup,
            session=session
        )

        if not article_payload or not article_payload.get("page_text"):
            context.log.warning(f"Could not fetch article for '{artist_row.get('artist')}'. Skipping.")
            return []

        cleaned_article = clean_text_string(article_payload["page_text"])
        chunks = text_splitter.split_text(cleaned_article)
        total_chunks = len(chunks)

        genres = article_payload.get("genres") or []
        artist_chunks = []
        for i, chunk_text in enumerate(chunks):
            title = article_payload["artist"]
            enriched_text = f"search_document: {article_payload['artist']} | {chunk_text}"
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

        return artist_chunks

    processed_count = 0
    log_interval = 250  # Log progress every 250 articles

    async with create_aiohttp_session() as session:
        worker_fn = partial(async_process_artist, session=session)
        
        # Use incremental processor to get results as they are ready
        results_iterator = process_items_incrementally_async(
            items=rows_to_process,
            process_func=worker_fn,
            max_concurrent_tasks=10,
            logger=context.log
        )
        
        # Append results to the file as they come in
        async for artist_chunks in results_iterator:
            if artist_chunks:
                save_to_jsonl(artist_chunks, WIKIPEDIA_ARTICLES_FILE, mode="a")
            
            processed_count += 1
            if processed_count % log_interval == 0:
                context.log.info(f"Processed {processed_count} / {total_rows} articles...")

    context.log.info(
        f"Finished processing. Final dataset saved to {WIKIPEDIA_ARTICLES_FILE}."
    )
    return WIKIPEDIA_ARTICLES_FILE
