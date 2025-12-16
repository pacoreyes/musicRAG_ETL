"""
Utility functions for interacting with Wikipedia.
"""

import logging
import random
import time
import urllib.parse
from typing import Optional, Dict, Any

import wikipediaapi
from dagster import AssetExecutionContext

from music_rag_etl.settings import WIKIPEDIA_CACHE_DIR, WIKIDATA_ENTITY_URL
from music_rag_etl.utils.transformation_helpers import map_genre_ids_to_labels

# Configure logging for this module
logger = logging.getLogger(__name__)


def get_wikipedia_page(
    context: AssetExecutionContext,
    wiki_api: wikipediaapi.Wikipedia,
    url: str,
    wikidata_id: str,
) -> str | None:
    """
    Fetches a Wikipedia page, either from local text cache or the API.
    """
    if not url or "/wiki/" not in url:
        return None

    cache_file_path = WIKIPEDIA_CACHE_DIR / f"{wikidata_id}.txt"

    if cache_file_path.exists():
        try:
            with open(cache_file_path, "r", encoding="utf-8") as file:
                cached_text = file.read()
                return cached_text
        except Exception as exc:  # noqa: BLE001
            context.log.error(
                "Failed to read cache for %s, falling back to API: %s",
                wikidata_id,
                exc,
            )

    try:
        raw_title = url.split("/wiki/")[-1]
        decoded_title = urllib.parse.unquote(raw_title)

        page = wiki_api.page(decoded_title)

        if page.exists():
            WIKIPEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

            with open(cache_file_path, "w", encoding="utf-8") as file:
                file.write(page.text)
            time.sleep(random.uniform(0.1, 0.5))
            return page.text

    except Exception as exc:  # noqa: BLE001
        context.log.error("Error fetching Wikipedia URL %s: %s", url, exc)
    return None


def fetch_artist_article_payload(
    context: AssetExecutionContext,
    wiki_api: wikipediaapi.Wikipedia,
    artist_row: Dict[str, Any],
    genre_lookup: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    """
    Fetches and prepares a single artist's Wikipedia article payload.
    """
    wikipedia_url = artist_row.get("wikipedia_url")
    wikidata_id = artist_row.get("wikidata_id")

    if not wikipedia_url:
        return None

    page_text = get_wikipedia_page(context, wiki_api, wikipedia_url, wikidata_id)
    if not page_text:
        return None

    genre_ids = artist_row.get("genres") or []
    mapped_genres = map_genre_ids_to_labels(genre_ids, genre_lookup)
    if genre_ids and not mapped_genres:
        context.log.warning(
            "No genre labels found for artist %s (%s).",
            artist_row.get("artist"),
            wikidata_id,
        )

    inception_raw = artist_row.get("inception")
    inception_year = None
    if inception_raw:
        year_str = str(inception_raw)[:4]
        inception_year = int(year_str) if year_str.isdigit() else "N/A"

    wikidata_entity = (
        f"{WIKIDATA_ENTITY_URL}{wikidata_id}" if wikidata_id else None
    )

    return {
        "artist": artist_row.get("artist"),
        "wikidata_id": wikidata_id,
        "wikipedia_url": wikipedia_url,
        "genres": mapped_genres,
        "inception_year": inception_year,
        "page_text": page_text,
        "references_score": artist_row.get("relevance_score"),
        "wikidata_entity": wikidata_entity,
    }
