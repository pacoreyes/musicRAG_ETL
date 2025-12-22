import json
from pathlib import Path
from typing import Any, Dict, List

from dagster import AssetExecutionContext, Config, MaterializeResult, asset
from gqlalchemy import Memgraph
from pydantic import Field # Import Field for better type hinting and defaults
from tqdm import tqdm

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parents[2]
DATASETS_DIR = CURRENT_DIR.parents[1] / "data"


class MemgraphConfig(Config):
    """Configuration for Memgraph connection."""
    host: str = Field("127.0.0.1", description="Memgraph host address.")
    port: int = Field(7687, description="Memgraph port number.")


def get_memgraph_client(config: MemgraphConfig) -> Memgraph:
    """Initializes Memgraph client."""
    return Memgraph(host=config.host, port=config.port)


def clear_database(memgraph: Memgraph, context: AssetExecutionContext) -> None:
    """
    Clears all nodes, relationships, and indexes from the database.
    Mandatory step before ingestion.
    """
    context.log.info("Starting database cleanup...")

    # 1. Delete all nodes and relationships
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    context.log.info("Deleted all nodes and relationships.")

    # 2. Drop all indexes
    # Fetch existing indexes
    indexes = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))
    for idx in indexes:
        label = idx.get("label")
        property_name = idx.get("property")
        if label and property_name:
            query = f"DROP INDEX ON :{label}({property_name});"
            try:
                memgraph.execute(query)
                context.log.info(f"Dropped index: :{label}({property_name})")
            except Exception as e:
                context.log.warning(
                    f"Failed to drop index :{label}({property_name}). Error: {e}"
                )


def create_indexes(memgraph: Memgraph, context: AssetExecutionContext) -> None:
    """Creates necessary indexes for performance."""
    context.log.info("Creating indexes...")
    index_commands = [
        "CREATE INDEX ON :Artist(id);",
        "CREATE INDEX ON :Artist(name);",
        "CREATE INDEX ON :Album(id);",
        "CREATE INDEX ON :Album(artist_id);",
        "CREATE INDEX ON :Track(id);",
        "CREATE INDEX ON :Track(album_id);",
        "CREATE INDEX ON :Genre(id);",
    ]

    for cmd in index_commands:
        try:
            memgraph.execute(cmd)
            context.log.info(f"Executed: {cmd}")
        except Exception as e:
            context.log.error(f"Failed to execute '{cmd}': {e}")
            raise e


def load_jsonl(file_path: Path) -> List[Dict[str, Any]]:
    """Reads a JSONL file and returns a list of dictionaries."""
    data = []
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data


@asset(
    name="load_graph_db",
    deps=["create_tracks_asset"],
    description="Ingests Artists, Albums, Tracks, and Genres into Memgraph.",
    group_name="loading",
)
def load_graph_db(context: AssetExecutionContext, config: MemgraphConfig) -> MaterializeResult:
    memgraph = get_memgraph_client(config)
    # --- Step 1: Clear Database ---
    clear_database(memgraph, context)

    # --- Step 2: Node Ingestion ---
    context.log.info("Starting Stage 1: Node Ingestion")

    # 1. Genres
    genres_path = DATASETS_DIR / "genres.jsonl"
    genres_data = load_jsonl(genres_path)
    context.log.info(f"Loading {len(genres_data)} genres...")
    
    for genre in tqdm(genres_data, desc="Loading Genres"):
        # Map genre_label -> name
        query = """
        CREATE (:Genre {
            id: $id, 
            name: $name, 
            aliases: $aliases
        });
        """
        memgraph.execute(
            query, 
            {
                "id": genre.get("id"),
                "name": genre.get("genre_label"),
                "aliases": genre.get("aliases", [])
            }
        )

    # 2. Artists
    artists_path = DATASETS_DIR / "artists.jsonl"
    artists_data = load_jsonl(artists_path)
    context.log.info(f"Loading {len(artists_data)} artists...")
    
    for artist in tqdm(artists_data, desc="Loading Artists"):
        query = """
        CREATE (:Artist {
            id: $id, 
            name: $name, 
            country: $country, 
            aliases: $aliases,
            tags: $tags,
            genres: $genres,
            similar_artists: $similar_artists
        });
        """
        memgraph.execute(
            query,
            {
                "id": artist.get("id"),
                "name": artist.get("name"),
                "country": artist.get("country"),
                "aliases": artist.get("aliases", []),
                "tags": artist.get("tags", []),
                "genres": artist.get("genres", []),  # Temp prop
                "similar_artists": artist.get("similar_artists", [])  # Temp prop
            }
        )

    # 3. Albums
    albums_path = DATASETS_DIR / "albums.jsonl"
    albums_data = load_jsonl(albums_path)
    context.log.info(f"Loading {len(albums_data)} albums...")
    
    album_count = 0
    for album in tqdm(albums_data, desc="Loading Albums"):
        artist_id = album.get("artist_id")
        # Filter: Skip if artist_id is missing
        if not artist_id:
            continue
            
        query = """
        CREATE (:Album {
            id: $id, 
            title: $title, 
            year: $year,
            artist_id: $artist_id
        });
        """
        memgraph.execute(
            query,
            {
                "id": album.get("id"),
                "title": album.get("title"),
                "year": album.get("year"),
                "artist_id": artist_id # Temp prop
            }
        )
        album_count += 1
    context.log.info(f"Loaded {album_count} albums (filtered).")

    # 4. Tracks
    tracks_path = DATASETS_DIR / "tracks.jsonl"
    tracks_data = load_jsonl(tracks_path)
    context.log.info(f"Loading {len(tracks_data)} tracks...")
    
    track_count = 0
    for track in tqdm(tracks_data, desc="Loading Tracks"):
        album_id = track.get("album_id")
        # Filter: Skip if album_id is missing
        if not album_id:
            continue
            
        query = """
        CREATE (:Track {
            id: $id, 
            title: $title,
            album_id: $album_id
        });
        """
        memgraph.execute(
            query,
            {
                "id": track.get("id"),
                "title": track.get("title"),
                "album_id": album_id # Temp prop
            }
        )
        track_count += 1
    context.log.info(f"Loaded {track_count} tracks (filtered).")

    # --- Step 3: Index Creation ---
    create_indexes(memgraph, context)

    # --- Step 4: Relationship Ingestion ---
    context.log.info("Starting Stage 3: Relationship Ingestion")

    # 1. Artist -> Genre
    context.log.info("Creating (Artist)-[:HAS_GENRE]->(Genre)...")
    query_artist_genre = """
    MATCH (a:Artist)
    WHERE a.genres IS NOT NULL
    UNWIND a.genres AS genre_id
    MATCH (g:Genre {id: genre_id})
    MERGE (a)-[:HAS_GENRE]->(g);
    """
    memgraph.execute(query_artist_genre)

    # 2. Artist -> Artist (Similar)
    context.log.info("Creating (Artist)-[:SIMILAR_TO]->(Artist)...")
    query_artist_similar = """
    MATCH (a:Artist)
    WHERE a.similar_artists IS NOT NULL
    UNWIND a.similar_artists AS sim_name
    MATCH (target:Artist {name: sim_name})
    MERGE (a)-[:SIMILAR_TO]->(target);
    """
    memgraph.execute(query_artist_similar)

    # 3. Album -> Artist
    context.log.info("Creating (Album)-[:PERFORMED_BY]->(Artist)...")
    query_album_artist = """
    MATCH (alb:Album)
    WHERE alb.artist_id IS NOT NULL
    MATCH (art:Artist {id: alb.artist_id})
    MERGE (alb)-[:PERFORMED_BY]->(art);
    """
    memgraph.execute(query_album_artist)

    # 4. Album -> Track (CONTAINS_TRACK)
    # Note: Logic is Album -> Track based on track.album_id
    context.log.info("Creating (Album)-[:CONTAINS_TRACK]->(Track)...")
    query_album_track = """
    MATCH (t:Track)
    WHERE t.album_id IS NOT NULL
    MATCH (alb:Album {id: t.album_id})
    MERGE (alb)-[:CONTAINS_TRACK]->(t);
    """
    memgraph.execute(query_album_track)

    # --- Step 5: Cleanup ---
    context.log.info("Starting Stage 4: Cleanup (Removing temporary properties)")

    cleanup_queries = [
        "MATCH (n:Artist) REMOVE n.genres, n.similar_artists;",
        "MATCH (n:Album) REMOVE n.artist_id;",
        "MATCH (n:Track) REMOVE n.album_id;"
    ]
    
    for q in cleanup_queries:
        memgraph.execute(q)
        
    context.log.info("Graph population complete.")

    return MaterializeResult(
        metadata={
            "nodes_loaded": {
                "genres": len(genres_data),
                "artists": len(artists_data),
                "albums": album_count,
                "tracks": track_count
            },
            "status": "success"
        }
    )
