"""
#####################################################
IMPORTANT: This code is meanto run in a Jupyter Notebook
#####################################################
"""

import json
import hashlib
import os
from pathlib import Path
from typing import List, Tuple

# 1. Disable Parallelism to prevent deadlocks
os.environ["TOKENIZERS_PARALLELISM"] = "false"
# 2. Configure PyTorch CUDA memory allocation to avoid fragmentation
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import numpy as np
import torch
import chromadb
from chromadb.api.models import Collection
from chromadb import Documents, EmbeddingFunction, Embeddings
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# --- CONFIGURATION for Colab ---
# Assumes the data file is uploaded to the root of the Colab environment
WIKIPEDIA_ARTICLES_FILE = Path("/content/wikipedia_articles.jsonl")
CHROMA_DB_PATH = Path("/content/vector_db")
DEFAULT_COLLECTION_NAME = "musicrag_collection"
DEFAULT_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"

# This batch size controls both the ChromaDB client and the embedding model.
# A T4 GPU on Colab can typically handle a batch size of 32, 64, 128 with this model.
# If you encounter out-of-memory errors, try reducing this value.
BATCH_SIZE = 64 # Further reduced batch size to accommodate memory constraints

TEST_QUERY_TEXT = "What is the discography of Depeche Mode?"
# ---------------------


def get_device():
  """Returns the appropriate device available in the system: CUDA or CPU"""
  if torch.cuda.is_available():
    return torch.device("cuda")
  else:
    return torch.device("cpu")


# --- CRITICAL: CUSTOM CLASS FOR NOMIC ---
class NomicEmbeddingFunction(EmbeddingFunction):
    """
    1. Sets trust_remote_code=True.
    2. Adds prefixes for Nomic model.
    3. Leverages batch processing for speed.
    """
    def __init__(self, model_name: str, device: str):
        print(f"Loading model '{model_name}' on device '{device}'...")
        self.model = SentenceTransformer(
            model_name,
            device=device,
            trust_remote_code=True
        )
        self.model.eval()

    def __call__(self, input: Documents) -> Embeddings:
        # Nomic requires "search_document: " prefix for indexing
        processed_input = []
        for text in input:
            if not text.startswith("search_document:"):
                processed_input.append(f"search_document: {text}")
            else:
                processed_input.append(text)

        # The SentenceTransformer model will automatically handle truncation for inputs
        # longer than the model's max sequence length (8192 for nomic-embed-text).
        embeddings = self.model.encode(
            processed_input,
            convert_to_numpy=True,
            show_progress_bar=False,  # Progress bar is handled by the main ingestion loop
            normalize_embeddings=True,
        )

        return embeddings.tolist()

    def embed_query(self, query: str) -> List[float]:
        # Nomic requires "search_query: " prefix for searching
        prefixed_query = f"search_query: {query}"
        embedding = self.model.encode(
            [prefixed_query],
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        return embedding[0].tolist()


def get_chroma_collection(
    db_path: Path,
    collection_name: str,
    device: str,
    model_name: str = DEFAULT_MODEL_NAME,
) -> Tuple[Collection, NomicEmbeddingFunction]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(db_path))

    emb_fn = NomicEmbeddingFunction(model_name=model_name, device=device)

    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=emb_fn,
    )
    return collection, emb_fn


def read_and_process_data(file_path: Path) -> Tuple[List[str], List[dict], List[str]]:
    documents: List[str] = []
    metadatas: List[dict] = []
    ids: List[str] = []
    print(f"Reading JSONL file from: {file_path}")

    if not file_path.exists():
        print(f"Error: The file '{file_path}' was not found.")
        print("Please make sure you have uploaded it to your Colab environment.")
        raise SystemExit(1)

    with open(file_path, "r", encoding="utf-8") as file:
        for index, line in enumerate(file):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                print(f"Warning: Skipping malformed JSON on line {index + 1}")
                print(entry)
                continue

            article_text = entry.get("article")
            metadata_obj = entry.get("metadata")

            # FIX 1: Ensure article, metadata, and artist_name all exist
            if article_text and metadata_obj and metadata_obj.get("title"):
                title = metadata_obj.get("title")

                # FIX 2: Create an enriched document with the title for better embedding
                # enriched_document = f"Artist: {title}\n\n{article_text}"
                documents.append(article_text)

                # FIX 3: Create a clean, predictable metadata dictionary for Chroma
                # This ensures the 'title' field is correctly populated.
                chroma_metadata = {
                    "title": title,
                    "artist_name": metadata_obj.get("artist_name"),
                    "genres": ", ".join(map(str, metadata_obj.get("genres"))),
                    "inception_year": metadata_obj.get("inception_year", 0),
                    "wikipedia_url": metadata_obj.get("wikipedia_url", "N/A"),
                    "wikidata_entity": metadata_obj.get("wikidata_entity", "N/A"),
                    "relevance_score": metadata_obj.get("relevance_score", 0.0),
                    "chunk_index": metadata_obj.get("chunk_index", "N/A"),
                    "total_chunks": metadata_obj.get("total_chunks", "N/A"),
                }
                metadatas.append(chroma_metadata)

                # Generate a stable and unique ID based on the content and its index
                doc_id = hashlib.md5(f"{article_text}-{index}".encode("utf-8")).hexdigest()
                ids.append(doc_id)

    return documents, metadatas, ids


def ingest_data(
    collection: Collection,
    documents: List[str],
    metadatas: List[dict],
    ids: List[str],
    batch_size: int = BATCH_SIZE,
) -> None:
    if not documents:
        print("No valid documents were found to ingest.")
        return

    print(f"Adding {len(documents)} documents to Chroma collection...")

    total_batches = (len(documents) + batch_size - 1) // batch_size
    for i in tqdm(
        range(0, len(documents), batch_size),
        total=total_batches,
        desc="Ingesting documents",
    ):
        batch_documents = documents[i: i + batch_size]
        batch_metadatas = metadatas[i: i + batch_size]
        batch_ids = ids[i: i + batch_size]

        collection.upsert(
            ids=batch_ids,
            documents=batch_documents,
            metadatas=batch_metadatas,
        )
    print("Success! Data ingested.")


def test_query(collection: Collection, emb_fn: NomicEmbeddingFunction) -> None:
    print("\n--- Testing Query ---")
    if collection.count() == 0:
        print("Collection is empty. Cannot perform a query.")
        return

    query_vec = emb_fn.embed_query(TEST_QUERY_TEXT)

    results = collection.query(
        query_embeddings=[query_vec],
        n_results=1,
    )
    if results and results.get("documents"):
        print(f"Query: {TEST_QUERY_TEXT}")
        print("Found document:", results["documents"][0][0][:200], "...")
        print("Metadata:", results["metadatas"][0][0])
    else:
        print("Query returned no results.")


def main() -> None:
    """Main function to run the ingestion process."""
    device = get_device()
    print(f"Using device: {device}")

    collection, emb_fn = get_chroma_collection(
        CHROMA_DB_PATH, DEFAULT_COLLECTION_NAME, device
    )
    documents, metadatas, ids = read_and_process_data(WIKIPEDIA_ARTICLES_FILE)
    ingest_data(collection, documents, metadatas, ids, BATCH_SIZE)
    test_query(collection, emb_fn)


if __name__ == "__main__":
    # In a notebook, you would typically call main() directly in a cell.
    # For example:
    # main()
    # This check prevents it from running automatically if imported.
    pass

main()
