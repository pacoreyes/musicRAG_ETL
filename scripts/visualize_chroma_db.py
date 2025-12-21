"""
Generates an interactive Nomic map for a ChromaDB collection's embeddings.
"""

from pathlib import Path
import os

from dotenv import load_dotenv
import chromadb
import numpy as np
import nomic
from nomic import atlas
from tqdm import tqdm

from music_rag_etl.settings import DEFAULT_COLLECTION_NAME, CHROMA_DB_PATH

# Load environment variables from .env file before other imports
load_dotenv()

# Get the API key from the environment
api_key = os.getenv("NOMIC_API_KEY")


def main():
    """Main function to generate the Nomic map."""
    # Authenticate with Nomic using the API key from .env
    if api_key:
        nomic.login(api_key)

    collection_name = DEFAULT_COLLECTION_NAME
    project_name = "music_rag_etl_visualization"
    max_documents = None

    if not CHROMA_DB_PATH.exists():
        print(f"Error: Database path '{CHROMA_DB_PATH}' does not exist.")
        return

    # Connect to ChromaDB
    print(f"Connecting to ChromaDB at '{CHROMA_DB_PATH}'...")
    client = chromadb.PersistentClient(path=str(CHROMA_DB_PATH))

    try:
        collection = client.get_collection(name=collection_name)
    except ValueError:
        print(f"Error: Collection '{collection_name}' not found.")
        return

    # Fetch all data from the collection
    total_docs = collection.count()
    limit = max_documents if max_documents else total_docs

    print(f"Fetching {limit} documents from collection '{collection_name}'...")

    # ChromaDB's get() can be slow for large datasets, so fetch in batches.
    batch_size = 500
    all_results = {"ids": [], "embeddings": [], "metadatas": []}

    with tqdm(total=limit, desc="Fetching documents") as pbar:
        for offset in range(0, limit, batch_size):
            batch = collection.get(
                limit=min(batch_size, limit - offset),
                offset=offset,
                include=["embeddings", "metadatas"],
            )
            all_results["ids"].extend(batch["ids"])
            all_results["embeddings"].extend(batch["embeddings"])
            all_results["metadatas"].extend(batch["metadatas"])
            pbar.update(len(batch["ids"]))

    if not all_results["embeddings"]:
        print("No documents found to visualize.")
        return

    print("Preparing data for Nomic Atlas...")
    embeddings = np.array(all_results["embeddings"])

    # Add the document ID to the metadata for Nomic to use
    metadata = all_results["metadatas"]
    for i, doc_id in enumerate(all_results["ids"]):
        metadata[i]["id"] = doc_id

    # Create the Nomic Atlas map
    print(f"Creating Nomic Atlas project '{project_name}'...")

    try:
        project = atlas.map_data(
            data=metadata,
            embeddings=embeddings,
            identifier=project_name,
            id_field="id",
            topic_model=True,
        )
        print("\nSuccessfully created Nomic map!")
        print(f"View your interactive map at: {project.maps[0].map_link}")
    except ValueError as e:
        if "You have not configured your Nomic API token" in str(e):
            print("\nError: Nomic API token not configured.")
            print("Please run 'nomic login' in your terminal to configure your token.")
        else:
            raise e


if __name__ == "__main__":
    main()

"""
DO NOT DELETE
python -m src.music_rag_etl.assets.loading.query_visualize_chroma_db
"""
