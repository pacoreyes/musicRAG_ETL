"""Fuse artist metadata from Wikidata and Last.fm into a JSONL dataset."""

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util import Retry

from scripts_etl.utils.clean_text_json import clean_text
from scripts_etl.config import (
    ARTIST_INDEX,
    ARTISTS_FILE,
    CHUNK_SIZE,
    LASTFM_API_KEY,
    RATE_LIMIT_DELAY,
    USER_AGENT,
    WIKIDATA_SPARQL_URL,
)

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = CHUNK_SIZE
LASTFM_TIMEOUT = 10
WIKIDATA_TIMEOUT = 60


@dataclass
class ArtistEntry:
    """Container for an artist read from the index file."""

    qid: str
    name: str
    genres: List[str]


def build_retrying_session() -> Session:
    """
    Create a requests session with retry logic for transient failures.

    Returns:
        Session configured with retries and default headers.
    """
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def build_wikidata_query(qids: List[str]) -> str:
    """
    Build a SPARQL query to fetch country and genre QIDs for a batch of QIDs.

    Args:
        qids: Wikidata identifiers without the `wd:` prefix.

    Returns:
        SPARQL query string.
    """
    ids_string = " ".join([f"wd:{qid}" for qid in qids])
    return f"""
    SELECT DISTINCT ?item ?countryLabel ?genre ?altLabel
    WHERE {{
      VALUES ?item {{ {ids_string} }}
      
      # 1. Fetch Country Label (Try Origin P495, then Citizenship P27)
      OPTIONAL {{
        {{ ?item wdt:P495 ?country . }} UNION {{ ?item wdt:P27 ?country . }}
        ?country rdfs:label ?countryLabel .
        FILTER(LANG(?countryLabel) = "en")
      }}
      
      # 2. Fetch Genre QID (No labels or subgenres needed)
      OPTIONAL {{
        ?item wdt:P136 ?genre .
      }}

      # 3. Fetch Aliases
      OPTIONAL {{
        ?item skos:altLabel ?altLabel .
        FILTER(LANG(?altLabel) = "en")
      }}
    }}
    """


def extract_qid(value: str) -> Optional[str]:
    """
    Extract a Wikidata QID from a URI or raw identifier.

    Args:
        value: Input value that may contain a QID.

    Returns:
        QID string (e.g., "Q123") or None if not detected.
    """
    if not value:
        return None
    candidate = value.rsplit("/", 1)[-1]
    if candidate.startswith("Q") and candidate[1:].isdigit():
        return candidate
    return None


def fetch_wikidata_batch(session: Session, qids: List[str]) -> Dict[str, Dict]:
    """
    Query Wikidata for a batch of QIDs to obtain country and genre QIDs.

    Args:
        session: Requests session configured with retries.
        qids: Batch of Wikidata identifiers.

    Returns:
        Mapping of QID to metadata containing `country` and `genres`.
    """
    if not qids:
        return {}

    query = build_wikidata_query(qids)

    try:
        response = session.post(
            WIKIDATA_SPARQL_URL,
            data={"query": query, "format": "json"},
            timeout=WIKIDATA_TIMEOUT,
        )
        response.raise_for_status()
        bindings = response.json().get("results", {}).get("bindings", [])
    except requests.RequestException as exc:
        logger.error("Wikidata batch request failed: %s", exc)
        return {}
    except json.JSONDecodeError:
        logger.error("Invalid JSON returned from Wikidata.")
        return {}

    mapped_data: Dict[str, Dict[str, Optional[str] | List[str]]] = {}
    for binding in bindings:
        qid = binding["item"]["value"].split("/")[-1]
        if qid not in mapped_data:
            mapped_data[qid] = {"country": None, "genres": [], "aliases": []}
        country_label = binding.get("countryLabel", {}).get("value")
        if country_label:
            mapped_data[qid]["country"] = country_label

        genre_uri = binding.get("genre", {}).get("value")
        genre_qid = extract_qid(genre_uri) if genre_uri else None
        if genre_qid and genre_qid not in mapped_data[qid]["genres"]:
            mapped_data[qid]["genres"].append(genre_qid)

        alt_label = binding.get("altLabel", {}).get("value")
        if alt_label and alt_label not in mapped_data[qid]["aliases"]:
            mapped_data[qid]["aliases"].append(alt_label)

    return mapped_data


def get_lastfm_data(
    session: Session,
    artist_name: str,
    api_key: str,
) -> Dict[str, List[str]]:
    """
    Fetch social tags and similar artists from Last.fm.

    Args:
        session: Requests session configured with retries.
        artist_name: Name of the artist to query.
        api_key: Last.fm API key.

    Returns:
        Dictionary with `tags` and `similar_artists` lists.
    """
    if not artist_name or not api_key:
        return {"tags": [], "similar_artists": []}

    try:
        response = session.get(
            "http://ws.audioscrobbler.com/2.0/",
            params={
                "method": "artist.getInfo",
                "artist": artist_name,
                "api_key": api_key,
                "format": "json",
                "autocorrect": 1,
            },
            timeout=LASTFM_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.warning("Last.fm request failed for %s: %s", artist_name, exc)
        return {"tags": [], "similar_artists": []}
    except json.JSONDecodeError:
        logger.warning("Last.fm returned invalid JSON for %s", artist_name)
        return {"tags": [], "similar_artists": []}

    artist_data = payload.get("artist")
    if not artist_data:
        return {"tags": [], "similar_artists": []}

    tags = [
        tag["name"]
        for tag in artist_data.get("tags", {}).get("tag", [])
        if "name" in tag
    ]
    similar = [
        item["name"]
        for item in artist_data.get("similar", {}).get("artist", [])
        if "name" in item
    ]

    return {"tags": tags[:5], "similar_artists": similar[:5]}


def merge_genre_qids(source: Iterable[str], wikidata_genres: Iterable[str]) -> List[str]:
    """
    Combine genre QIDs from input (if valid) and Wikidata, removing duplicates.

    Args:
        source: Genres already present in the input file.
        wikidata_genres: Genre QIDs fetched from Wikidata.

    Returns:
        Sorted list of unique genre QIDs.
    """
    merged = {extract_qid(genre) for genre in source if extract_qid(genre)}
    merged.update(extract_qid(genre) for genre in wikidata_genres if extract_qid(genre))
    return sorted(merged)


def load_artists(input_path: Path, limit: Optional[int]) -> List[ArtistEntry]:
    """
    Load artists from the input JSONL file.

    Args:
        input_path: Path to the JSONL artist index.
        limit: Optional maximum number of rows to read for testing.

    Returns:
        List of ArtistEntry instances.
    """
    artists: List[ArtistEntry] = []
    seen_qids: set[str] = set()

    try:
        with open(input_path, "r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping invalid JSON line.")
                    continue

                qid_raw = (
                    row.get("wikidata_id")
                    or row.get("metadata", {}).get("wikidata_entity")
                )
                artist_name = row.get("artist") or row.get(
                    "metadata", {}
                ).get("artist_name")
                input_genres = row.get("genre") or row.get("genres") or []

                if not qid_raw or not artist_name:
                    logger.debug("Missing qid or artist name in row: %s", row)
                    continue

                cleaned_name = clean_text(artist_name).strip()
                if not cleaned_name:
                    logger.debug("Artist name empty after cleaning: %s", row)
                    continue

                qid = qid_raw.split("/")[-1]
                if qid in seen_qids:
                    continue
                seen_qids.add(qid)

                genres = input_genres if isinstance(input_genres, list) else []
                artists.append(ArtistEntry(qid=qid, name=cleaned_name, genres=genres))

                if limit and len(artists) >= limit:
                    break
    except FileNotFoundError:
        logger.error("Input file %s not found.", input_path)

    return artists


def enrich_artists(
    artists: List[ArtistEntry],
    output_path: Path,
    batch_size: int,
    skip_wikidata: bool,
    skip_lastfm: bool,
    api_key: str,
) -> None:
    """
    Enrich artists with Wikidata and Last.fm metadata and write to JSONL.

    Args:
        artists: Parsed artist entries from the index file.
        output_path: Destination JSONL file.
        batch_size: Number of artists to process per Wikidata batch.
        skip_wikidata: Whether to skip Wikidata calls (offline mode).
        skip_lastfm: Whether to skip Last.fm calls.
        api_key: Last.fm API key.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wikidata_session = build_retrying_session()
    lastfm_session = build_retrying_session()

    with open(output_path, "w", encoding="utf-8") as outfile:
        with tqdm(
            total=len(artists),
            desc="Enriching artists",
            unit="artist",
            dynamic_ncols=True,
        ) as progress:
            for start in range(0, len(artists), batch_size):
                batch = artists[start : start + batch_size]
                qids = [artist.qid for artist in batch]

                wikidata_data = (
                    {}
                    if skip_wikidata
                    else fetch_wikidata_batch(wikidata_session, qids)
                )

                for entry in batch:
                    cleaned_name = clean_text(entry.name).strip()
                    if not cleaned_name:
                        logger.debug("Skipping artist with empty cleaned name: %s", entry)
                        progress.update(1)
                        continue

                    wikidata_info = wikidata_data.get(
                        entry.qid, {"country": None, "genres": [], "aliases": []}
                    )
                    lastfm_info = (
                        {"tags": [], "similar_artists": []}
                        if skip_lastfm
                        else get_lastfm_data(lastfm_session, cleaned_name, api_key)
                    )
                    cleaned_tags = [
                        clean_text(tag).strip()
                        for tag in lastfm_info["tags"]
                        if tag
                    ]
                    cleaned_similar = [
                        clean_text(name).strip()
                        for name in lastfm_info["similar_artists"]
                        if name
                    ]
                    cleaned_aliases = [
                        clean_text(alias).strip()
                        for alias in wikidata_info.get("aliases", [])
                        if alias
                    ]

                    country = wikidata_info.get("country") or "Unknown"

                    final_row = {
                        "id": entry.qid,
                        "artist": cleaned_name,
                        "aliases": cleaned_aliases,
                        "country": country,
                        "genres": merge_genre_qids(
                            entry.genres, wikidata_info.get("genres", [])
                        ),
                        "tags": cleaned_tags,
                        "similar_artists": cleaned_similar,
                    }

                    outfile.write(json.dumps(final_row, ensure_ascii=False) + "\n")
                    progress.update(1)

                time.sleep(RATE_LIMIT_DELAY)


def main() -> None:
    """Entrypoint for enriching artists using repository defaults."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    artists = load_artists(ARTIST_INDEX, None)
    if not artists:
        logger.warning("No artists loaded from %s", ARTIST_INDEX)
        return

    skip_lastfm = not bool(LASTFM_API_KEY)
    if skip_lastfm:
        logger.warning(
            "LAST_FM_API_KEY missing. Proceeding without Last.fm enrichment."
        )

    enrich_artists(
        artists=artists,
        output_path=ARTISTS_FILE,
        batch_size=10,
        skip_wikidata=False,
        skip_lastfm=skip_lastfm,
        api_key=LASTFM_API_KEY,
    )

    logger.info("Finished writing enriched artists to %s", ARTISTS_FILE)


if __name__ == "__main__":
    main()
