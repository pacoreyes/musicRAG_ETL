from dagster import AssetExecutionContext, MaterializeResult, asset
from gqlalchemy import Memgraph
from tqdm import tqdm
from pydantic import ValidationError

from music_rag_etl.settings import LOCAL_DATA_DIR
from music_rag_etl.utils.io_helpers import load_jsonl
from music_rag_etl.utils.memgraph_helpers import (
    MemgraphConfig,
    clear_database,
    get_memgraph_client,
)
from music_rag_etl.utils.models import (
    GenreNode,
    ArtistNode,
    AlbumNode,
    TrackNode,
)


def create_indexes(memgraph: Memgraph, context: AssetExecutionContext) -> None:
    """
    Creates necessary indexes in Memgraph to optimize query performance.

    Args:
        memgraph: The Memgraph client instance.
        context: The Dagster asset execution context for logging.

    Raises:
        Exception: If any index creation command fails.
    """
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


@asset(
    name="load_graph_db",
    deps=["extract_tracks"],
    description="Ingests Artists, Albums, Tracks, and Genres into Memgraph.",
    group_name="loading"
)
def load_graph_db(context: AssetExecutionContext, config: MemgraphConfig) -> MaterializeResult:
    """
    Dagster asset that ingests music data into the Memgraph database.
    
    This asset performs the following steps:
    1. Clears the existing database.
    2. Loads Genres, Artists, Albums, and Tracks nodes from JSONL files.
    3. Creates indexes for performance.
    4. Establishes relationships between nodes (e.g., Artist-Genre, Album-Artist).
    5. Cleans up temporary properties used for relationship creation.

    Args:
        context: The Dagster asset execution context.
        config: Configuration for the Memgraph connection.

    Returns:
        MaterializeResult: Metadata about the number of nodes loaded and status.
    """
    memgraph = get_memgraph_client(config)
    # --- Step 1: Clear Database ---
    clear_database(memgraph, context)

    # --- Step 2: Node Ingestion ---
    context.log.info("Starting Stage 1: Node Ingestion")

    # 1. Genres
    genres_path = LOCAL_DATA_DIR / "genres.jsonl"
    genres_data = load_jsonl(genres_path)
    context.log.info(f"Loading {len(genres_data)} genres...")
    
    genre_count = 0
    for raw_genre in tqdm(genres_data, desc="Loading Genres"):
        try:
            genre = GenreNode(**raw_genre)
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
                    "id": genre.id,
                    "name": genre.name,
                    "aliases": genre.aliases
                }
            )
            genre_count += 1
        except ValidationError as e:
            context.log.warning(f"Skipping invalid genre record: {e}")

    # 2. Artists
    artists_path = LOCAL_DATA_DIR / "artists.jsonl"
    artists_data = load_jsonl(artists_path)
    context.log.info(f"Loading {len(artists_data)} artists...")
    
    artist_count = 0
    for raw_artist in tqdm(artists_data, desc="Loading Artists"):
        try:
            artist = ArtistNode(**raw_artist)
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
                    "id": artist.id,
                    "name": artist.name,
                    "country": artist.country,
                    "aliases": artist.aliases,
                    "tags": artist.tags,
                    "genres": artist.genres,  # Temp prop
                    "similar_artists": artist.similar_artists  # Temp prop
                }
            )
            artist_count += 1
        except ValidationError as e:
            context.log.warning(f"Skipping invalid artist record: {e}")

    # 3. Albums
    albums_path = LOCAL_DATA_DIR / "albums.jsonl"
    albums_data = load_jsonl(albums_path)
    context.log.info(f"Loading {len(albums_data)} albums...")
    
    album_count = 0
    for raw_album in tqdm(albums_data, desc="Loading Albums"):
        try:
            album = AlbumNode(**raw_album)
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
                    "id": album.id,
                    "title": album.title,
                    "year": album.year,
                    "artist_id": album.artist_id # Temp prop
                }
            )
            album_count += 1
        except ValidationError as e:
            context.log.warning(f"Skipping invalid album record: {e}")
    context.log.info(f"Loaded {album_count} albums.")

    # 4. Tracks
    tracks_path = LOCAL_DATA_DIR / "tracks.jsonl"
    tracks_data = load_jsonl(tracks_path)
    context.log.info(f"Loading {len(tracks_data)} tracks...")
    
    track_count = 0
    for raw_track in tqdm(tracks_data, desc="Loading Tracks"):
        try:
            track = TrackNode(**raw_track)
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
                    "id": track.id,
                    "title": track.title,
                    "album_id": track.album_id # Temp prop
                }
            )
            track_count += 1
        except ValidationError as e:
            context.log.warning(f"Skipping invalid track record: {e}")
    context.log.info(f"Loaded {track_count} tracks.")

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