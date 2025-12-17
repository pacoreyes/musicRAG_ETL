"""
Centralized configuration settings for the musicRAG ETL project.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ==============================================================================
#  CORE PATH DEFINITIONS
# ==============================================================================
# Defines the project's directory structure for robust path management.

# The 'src' directory, which is the root for Python imports.
SRC_ROOT = Path(__file__).resolve().parents[1]

# The absolute root of the project (one level up from 'src').
# Use this for accessing top-level project resources like 'data_volume'.
PROJECT_ROOT = SRC_ROOT.parent

# Load environment variables from .env file located at the project root
load_dotenv(PROJECT_ROOT / ".env")

# Top-level directory for all data, caches, and databases.
DATA_DIR = PROJECT_ROOT / "data_volume"

# ==============================================================================
#  CACHE & DATABASE PATHS
# ==============================================================================
# Paths for storing cached API responses and the vector database.

# --- Cache Directories ---
WIKIPEDIA_CACHE_DIR = DATA_DIR / ".cache" / "wikipedia_articles"
WIKIDATA_CACHE_DIR = DATA_DIR / ".cache" / "wikidata"
LASTFM_CACHE_DIR = DATA_DIR / ".cache" / "last_fm"

# --- Temporal Directory ---
# For intermediate files during ETL processes.
PATH_TEMP = DATA_DIR / ".temp"

# Datasets
PATH_DATASETS = DATA_DIR / "datasets"

# --- Vector DB ---
CHROMA_DB_PATH = DATA_DIR / "vector_db"

# ==============================================================================
#  EXPLICIT FILE PATHS
# ==============================================================================
# Direct paths to specific files. These are derived from the core paths above.

ARTIST_INDEX_PRE_CLEAN = DATA_DIR / "datasets" / "artist_index_pre_clean.jsonl"
ARTIST_INDEX = DATA_DIR / "datasets" / "artist_index.jsonl"

WIKIPEDIA_ARTICLES_FILE = DATA_DIR / "datasets" / "wikipedia_articles.jsonl"
WIKIPEDIA_ARTICLES_FILE_TEMP = DATA_DIR / ".temp" / "wikipedia_articles_temp.jsonl"

ARTISTS_FILE = DATA_DIR / "datasets" / "artists.jsonl"
GENRES_FILE = DATA_DIR / "datasets" / "genres.jsonl"
ALBUMS_FILE = DATA_DIR / "datasets" / "albums.jsonl"
TRACKS_FILE = DATA_DIR / "datasets" / "tracks.jsonl"

# ==============================================================================
#  API & SERVICE CONFIGURATION
# ==============================================================================
# Endpoints and credentials for external services.

# --- Wikidata ---
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_ENTITY_URL = "http://www.wikidata.org/entity/"
WIKIDATA_ENTITY_PREFIX_URL = "http://www.wikidata.org/wiki/"
USER_AGENT = "musicRAG_ETL (reyes@b-tu.de)"
WIKIDATA_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
}

# --- Last.fm ---
LASTFM_REQUEST_TIMEOUT = 10
LASTFM_MAX_RETRIES = 3
LASTFM_RETRY_DELAY = 1

# --- ChromaDB ---
DEFAULT_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
DEFAULT_COLLECTION_NAME = "musicrag_collection"

# ==============================================================================
#  ETL & PROCESSING PARAMETERS
# ==============================================================================
# Constants controlling ETL batch sizes, timeouts, and behavior.

BATCH_SIZE = 32
WIKIDATA_BATCH_SIZE = 400
CHUNK_SIZE = 50
REQUEST_TIMEOUT_SECONDS = 65
RATE_LIMIT_DELAY = 1

ENABLE_LOGGING = True

# --- Wikidata Extraction ---
DECADES_TO_EXTRACT = {
    "1960s": (1960, 1969),
    "1970s": (1970, 1979),
    "1980s": (1980, 1989),
    "1990s": (1990, 1999),
    "2000s": (2000, 2009),
    "2010s": (2010, 2019),
    "2020s": (2020, 2029),
}

