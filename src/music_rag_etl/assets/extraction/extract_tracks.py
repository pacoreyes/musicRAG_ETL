import asyncio
import aiohttp
from typing import Dict, Any, List
from functools import partial

import polars as pl
from dagster import asset, AssetExecutionContext

from music_rag_etl.settings import (
    ALBUMS_FILE,
    TRACKS_FILE,
    WIKIDATA_ENTITY_URL,
)
from music_rag_etl.utils.io_helpers import save_to_jsonl
from music_rag_etl.utils.transformation_helpers import clean_text_string
from music_rag_etl.utils.concurrency_helpers import process_items_incrementally_async
from music_rag_etl.utils.request_utils import create_aiohttp_session
from music_rag_etl.utils.sparql_queries import get_tracks_by_album_query
from music_rag_etl.utils.wikidata_helpers import async_fetch_tracks_sparql_with_cache


async def async_fetch_tracks_for_album(
    album_qid: str,
    context: AssetExecutionContext,
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    """
    Worker function to fetch tracks for a single album via SPARQL (with caching).
    """
    query = get_tracks_by_album_query(album_qid)
    
    response_data = await async_fetch_tracks_sparql_with_cache(
        context=context,
        album_qid=album_qid,
        query=query,
        session=session
    )

    results = response_data.get("results", {}).get("bindings", [])
    if not results:
        context.log.debug(f"No tracks found in SPARQL response for album {album_qid}")

    tracks = []
    for item in results:
        track_uri = item.get("track", {}).get("value")
        if not track_uri:
            continue
            
        qid = track_uri.replace(WIKIDATA_ENTITY_URL, "")
        title = item.get("trackLabel", {}).get("value")
        
        context.log.debug(f"Processing item for album {album_qid}: qid='{qid}', title='{title}'")

        track_number_str = item.get("trackNumber", {}).get("value")
        track_number = int(track_number_str) if track_number_str and track_number_str.isdigit() else None
        
        if title and qid:
            tracks.append({
                "id": qid,
                "title": clean_text_string(title),
                "track_number": track_number,
                "album_id": album_qid
            })
        else:
            context.log.debug(f"Skipping track (QID: {qid}) due to missing title or QID.")
            
    return tracks


@asset(
    name="extract_tracks",
    deps=["extract_albums"],
    description="Extracts Tracks dataset tracks.jsonl from albums.jsonl using Wikidata API with SPARQL",
    group_name="extraction"
)
async def extract_tracks(context: AssetExecutionContext) -> str:
    """
    Materializes the tracks dataset by fetching tracks for each album.
    """
    context.log.info("Starting track extraction.")

    # 1. Load Albums
    df = pl.read_ndjson(ALBUMS_FILE)
    album_ids = df["id"].unique().to_list()
    
    total_albums = len(album_ids)
    context.log.info(f"Found {total_albums} albums to process.")

    # 2. Initialize Output File
    TRACKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TRACKS_FILE, "w", encoding="utf-8") as f:
        pass
    context.log.info(f"Initialized empty output file at {TRACKS_FILE}")

    seen_track_ids = set()
    processed_count = 0
    total_tracks_saved = 0
    log_interval = 250

    # 3. Process Incrementally
    async with create_aiohttp_session() as session:
        worker_fn = partial(
            async_fetch_tracks_for_album, 
            context=context, 
            session=session
        )
        
        results_iterator = process_items_incrementally_async(
            items=album_ids,
            process_func=worker_fn,
            max_concurrent_tasks=5,
            logger=context.log
        )

        async for batch_tracks in results_iterator:
            new_unique_tracks = []
            for track in batch_tracks:
                if track["id"] not in seen_track_ids:
                    seen_track_ids.add(track["id"])
                    new_unique_tracks.append(track)
            
            if new_unique_tracks:
                save_to_jsonl(new_unique_tracks, TRACKS_FILE, mode="a")
                total_tracks_saved += len(new_unique_tracks)

            processed_count += 1
            if processed_count % log_interval == 0:
                context.log.info(f"Processed {processed_count}/{total_albums} albums. Saved {total_tracks_saved} unique tracks so far.")

    context.log.info(f"Finished. Total tracks saved: {total_tracks_saved}")
    return str(TRACKS_FILE)
