from pathlib import Path
import os
import chromadb
from music_rag_etl.settings import CHROMA_DB_PATH, DEFAULT_COLLECTION_NAME

# Disable Parallelism to prevent deadlocks with some model tokenizers
os.environ["TOKENIZERS_PARALLELISM"] = "false"

def view_chromadb_embeddings(db_path: Path, collection_name: str, limit: int = 5):
    """
    Connects to a ChromaDB collection and displays a sample of documents,
    their metadata, and their embedding vectors.

    Args:
        db_path (Path): The path to the ChromaDB database directory.
        collection_name (str): The name of the collection to query.
        limit (int): The number of sample documents to retrieve and display.
    """
    if not db_path.exists():
        print(f"Error: Database path '{db_path}' does not exist.")
        print("Please ensure the ingestion script has been run.")
        return

    print(f"Connecting to ChromaDB at: {db_path}")
    client = chromadb.PersistentClient(path=str(db_path))

    try:
        collection = client.get_collection(name=collection_name)
    except ValueError:
        print(f"Error: Collection '{collection_name}' not found in the database.")
        print("Please check the collection name or run the ingestion script.")
        return

    print(f"Fetching {limit} sample documents from collection '{collection_name}'...")
    try:
        # Fetch a sample of documents, including their embeddings and metadatas
        results = collection.get(
            limit=limit,
            include=['embeddings', 'documents', 'metadatas']
        )

        if not results['ids']:
            print("No documents found in the collection.")
            return

        for i, doc_id in enumerate(results['ids']):
            print(f"\n--- Document {i + 1} ---")
            print(f"ID: {doc_id}")
            print(f"Document (snippet): {results['documents'][i][:500]}...")
            print(f"Metadata: {results['metadatas'][i]}")
            
            # Display a snippet of the embedding vector
            embedding_snippet = results['embeddings'][i][:10]
            print(f"Embedding (first 10 elements): {embedding_snippet}...")
            print(f"Embedding length: {len(results['embeddings'][i])}")

    except Exception as e:
        print(f"An error occurred while fetching documents: {e}")

if __name__ == "__main__":
    view_chromadb_embeddings(
        db_path=CHROMA_DB_PATH,
        collection_name=DEFAULT_COLLECTION_NAME
    )
