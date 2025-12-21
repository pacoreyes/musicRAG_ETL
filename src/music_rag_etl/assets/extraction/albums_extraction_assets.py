import asyncio
import aiohttp
from typing import Dict, Any, List, Set, Optional
from functools import partial

import polars as pl
from dagster import asset, AssetExecutionContext

from music_rag_etl.settings import (
    ARTIST_INDEX,
    ALBUMS_FILE,
    WIKIDATA_ENTITY_URL,
)
from music_rag_etl.utils.io_helpers import save_to_jsonl
from music_rag_etl.utils.transformation_helpers import extract_unique_ids_from_column
from music_rag_etl.utils.concurrency_helpers import process_items_incrementally_async
from music_rag_etl.utils.request_utils import create_aiohttp_session
from music_rag_etl.utils.sparql_queries import get_albums_by_artist_query
from music_rag_etl.utils.wikidata_helpers import async_fetch_sparql_with_cache


async def async_fetch_albums_for_artist(
    artist_qid: str,
    context: AssetExecutionContext,
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    """
    Worker function to fetch albums for a single artist via SPARQL (with caching).
    """
    query = get_albums_by_artist_query(artist_qid)
    
    response_data = await async_fetch_sparql_with_cache(
        context=context,
        artist_qid=artist_qid,
        query=query,
        session=session
    )

    results = response_data.get("results", {}).get("bindings", [])
    
    if not results:
        context.log.debug(f"No albums found in SPARQL response for artist {artist_qid}")

    albums = []
    for item in results:
        album_uri = item.get("album", {}).get("value")
        if not album_uri:
            continue
            
        qid = album_uri.replace(WIKIDATA_ENTITY_URL, "")
        title = item.get("albumLabel", {}).get("value")
        release_date = item.get("releaseDate", {}).get("value")
        
        context.log.debug(f"Processing item for artist {artist_qid}: qid='{qid}', title='{title}', release_date='{release_date}'")
        
        # Extract year: simple string slicing YYYY
        year = None
        if release_date and len(release_date) >= 4:
            year_str = release_date[:4]
            if year_str.isdigit():
                year = int(year_str)
        
        if year and title and qid:
            albums.append({
                "id": qid,
                "title": title,
                "year": year,
                "artist_id": artist_qid
            })
        else:
            context.log.debug(f"Skipping album '{title}' (QID: {qid}) due to missing year, title, or QID.")
            
    return albums


@asset(
    name="create_albums_asset",
    deps=["artists_extraction_from_artist_index"],
    description="Extracts albums for all artists from Wikidata",
)
async def create_albums_asset(context: AssetExecutionContext) -> str:
    """
    Materializes the albums dataset.
    """
    context.log.info("Starting album extraction.")

    # 1. Load Artists
    df = pl.read_ndjson(ARTIST_INDEX)
    artist_ids = df["wikidata_id"].unique().to_list()
    
    total_artists = len(artist_ids)
    context.log.info(f"Found {total_artists} artists to process.")

    # 2. Initialize Output File
    ALBUMS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ALBUMS_FILE, "w", encoding="utf-8") as f:
        pass
    context.log.info(f"Initialized empty output file at {ALBUMS_FILE}")

    seen_album_ids = set()
    processed_count = 0
    total_albums_saved = 0
    log_interval = 50

    # 3. Process Incrementally
    async with create_aiohttp_session() as session:
        worker_fn = partial(
            async_fetch_albums_for_artist, 
            context=context, 
            session=session
        )
        
        # Using concurrency of 5 for SPARQL endpoint to be safe. 
        # If the user insists on 50 for *Entity API*, that's fine, but for SPARQL it's risky.
        # I'll stick to a safe 10.
        results_iterator = process_items_incrementally_async(
            items=artist_ids,
            process_func=worker_fn,
            max_concurrent_tasks=10, 
            logger=context.log
        )

        async for batch_albums in results_iterator:
            new_unique_albums = []
            for album in batch_albums:
                if album["id"] not in seen_album_ids:
                    seen_album_ids.add(album["id"])
                    new_unique_albums.append(album)
            
            if new_unique_albums:
                save_to_jsonl(new_unique_albums, ALBUMS_FILE, mode="a")
                total_albums_saved += len(new_unique_albums)

            processed_count += 1
            if processed_count % log_interval == 0:
                context.log.info(f"Processed {processed_count}/{total_artists} artists. Saved {total_albums_saved} unique albums so far.")

    context.log.info(f"Finished. Total albums saved: {total_albums_saved}")
    return str(ALBUMS_FILE)
