"""
A script to query a ChromaDB collection with a given text query.
"""
import argparse
import os
from pathlib import Path

# Disable Parallelism to prevent deadlocks with some model tokenizers
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import torch
import chromadb
from chromadb import Documents, EmbeddingFunction, Embeddings
from sentence_transformers import SentenceTransformer

from music_rag_etl.settings import DEFAULT_MODEL_NAME, CHROMA_DB_PATH, DEFAULT_COLLECTION_NAME


def get_device():
  """Returns the appropriate device available in the system: CUDA, MPS, or CPU"""
  if torch.backends.mps.is_available():
    return torch.device("mps")
  elif torch.cuda.is_available():
    return torch.device("cuda")
  else:
    return torch.device("cpu")


device = get_device


class NomicEmbeddingFunction(EmbeddingFunction):
    """
    Custom embedding function for the Nomic-v1.5 model.
    - Sets trust_remote_code=True.
    - Adds the required prefixes for queries.
    """
    def __init__(self, model_name: str, device: torch.device):
        print(f"Loading model '{model_name}' on device '{device}'...")
        # trust_remote_code is required for Nomic-v1.5
        self.model = SentenceTransformer(model_name, device=device, trust_remote_code=True)
        self.model.eval()

    def __call__(self, input_texts: Documents) -> Embeddings:
        # Nomic requires "search_document: " prefix for indexing
        processed_input = [
            f"search_document: {text}" if not text.startswith("search_document:") else text
            for text in input_texts
        ]

        # The SentenceTransformer model will automatically handle truncation for inputs
        # longer than the model's max sequence length (8192 for nomic-embed-text).
        embeddings = self.model.encode(
            processed_input,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return embeddings.tolist()

    def embed_query(self, query: str) -> list[float]:
        """Embeds a single query string, adding the Nomic-specific prefix."""
        # Nomic requires a "search_query: " prefix for effective searching
        prefixed_query = f"search_query: {query}"
        embedding = self.model.encode(
            [prefixed_query],
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        return embedding[0].tolist()


def main() -> None:
    """Main function to handle command-line arguments and perform the query."""
    parser = argparse.ArgumentParser(description="Query a ChromaDB collection.")
    parser.add_argument(
        "query_text",
        type=str,
        nargs='?', # Make query_text optional
        default=None,
        help="The text to query the database with. If not provided, you will be prompted."
    )
    parser.add_argument(
        "-n", "--n-results",
        type=int,
        default=8,
        help="The number of results to retrieve."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=CHROMA_DB_PATH,
        help=f"Path to the ChromaDB database directory. Defaults to '{CHROMA_DB_PATH}'."
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=DEFAULT_COLLECTION_NAME,
        help=f"Name of the collection to query. Defaults to '{DEFAULT_COLLECTION_NAME}'."
    )
    parser.add_argument(
        "--filter-genre",
        type=str,
        help="Filter results by a specific genre. Case-insensitive, partial match."
    )
    parser.add_argument(
        "--filter-min-year",
        type=int,
        help="Filter results by minimum inception year (e.g., 1990)."
    )
    parser.add_argument(
        "--filter-max-year",
        type=int,
        help="Filter results by maximum inception year (e.g., 1999)."
    )
    args = parser.parse_args()

    # If query_text is not provided as a command-line argument, prompt the user
    if args.query_text is None:
        args.query_text = input("Enter your query: ")
        if not args.query_text.strip():
            print("Error: Query text cannot be empty.")
            return

    if not args.db_path.exists():
        print(f"Error: Database path '{args.db_path}' does not exist.")
        print("Please run the ingestion script first.")
        return

    device = get_device()
    print(f"Using device: {device}")

    # Initialize the embedding function
    emb_fn = NomicEmbeddingFunction(model_name=DEFAULT_MODEL_NAME, device=device)

    # Connect to the persistent ChromaDB client
    client = chromadb.PersistentClient(path=str(args.db_path))

    # Get the collection (and handle potential errors if it doesn't exist)
    try:
        collection = client.get_collection(name=args.collection, embedding_function=emb_fn)
    except ValueError:
        print(f"Error: Collection '{args.collection}' not found in the database.")
        print("Please check the collection name or run the ingestion script.")
        return

    print(f"\nQuerying for: '{args.query_text}'")
    print("-" * 30)

    # Build the metadata filter
    where_filter = {}

    if args.filter_genre:
        # ChromaDB's "$contains" for list type metadata
        where_filter["genre"] = {"$contains": args.filter_genre}
    
    # Handle year filtering
    if args.filter_min_year is not None or args.filter_max_year is not None:
        year_filter = {}
        if args.filter_min_year is not None:
            year_filter["$gte"] = args.filter_min_year
        if args.filter_max_year is not None:
            year_filter["$lte"] = args.filter_max_year
        
        if year_filter:
            where_filter["inception_year"] = year_filter
            
    # Validate year range
    if args.filter_min_year is not None and args.filter_max_year is not None:
        if args.filter_min_year > args.filter_max_year:
            print("Error: --filter-min-year cannot be greater than --filter-max-year.")
            return

    # Generate the embedding for the user's query
    query_embedding = emb_fn.embed_query(args.query_text)

    # Perform the query with potential metadata filtering
    query_kwargs = {
        "query_embeddings": [query_embedding],
        "n_results": args.n_results,
    }
    if where_filter:
        query_kwargs["where"] = where_filter
        print(f"Applying filters: {where_filter}")

    results = collection.query(**query_kwargs)

    # Display the results, sorted by distance (lower is better)
    if not results or not results.get("ids")[0]:
        print("No results found.")
        return

    print("\nResults (sorted by distance - lower is better):")
    print("-" * 30)

    for i, doc_id in enumerate(results["ids"][0]):
        distance = results["distances"][0][i]
        metadata = results["metadatas"][0][i]
        document = results["documents"][0][i]

        print(f"\n--->{metadata} \n")

        print(f"Result {i + 1}:")
        print(f"  - ID:       {doc_id}")
        print(f"  - Title:    {metadata.get('artist_name', 'N/A')}")
        print(f"  - URL:      {metadata.get('wikipedia_url', 'N/A')}")
        print(f"  - Distance: {distance:.4f} (lower is better)")
        print(f"  - Relevance Score: {metadata.get('relevance_score', 'N/A')}")
        print(f"  - Chunk Index: {metadata.get('chunk_index', 'N/A')}")
        print(f"  - Total Chunks: {metadata.get('total_chunks', 'N/A')}")
        print(f"  - Document: {document[:600]}...") # Print a snippet
        print("-" * 30)


if __name__ == "__main__":
    main()
