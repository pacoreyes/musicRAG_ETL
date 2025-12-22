import os
from typing import List, Optional

import torch
from chromadb import Documents, EmbeddingFunction, Embeddings
from sentence_transformers import SentenceTransformer


# Disable Parallelism to prevent deadlocks with some model tokenizers
os.environ["TOKENIZERS_PARALLELISM"] = "false"


def get_device() -> torch.device:
    """
    Returns the most appropriate device available in the system.

    Returns:
        torch.device: The selected device (CUDA, MPS, or CPU).
    """
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


class NomicEmbeddingFunction(EmbeddingFunction):
    """
    Custom embedding function for the Nomic-v1.5 model.

    This class handles the specifics of using the Nomic embedding model,
    including adding required prefixes for documents and queries.

    Attributes:
        model: The loaded SentenceTransformer model.
    """

    _SEARCH_DOCUMENT_PREFIX = "search_document: "
    _SEARCH_QUERY_PREFIX = "search_query: "

    def __init__(self, model_name: str, device: torch.device):
        """
        Initializes the embedding function.

        Args:
            model_name: The name of the SentenceTransformer model to load.
            device: The device to run the model on.
        """
        print(f"Loading model '{model_name}' on device '{device}'...")
        self.model = SentenceTransformer(
            model_name, device=device, trust_remote_code=True
        )
        self.model.eval()

    def __call__(self, input_texts: Documents) -> Embeddings:
        """
        Embeds a batch of documents.

        Args:
            input_texts: A list of document texts to embed.

        Returns:
            Embeddings: A list of embeddings, one for each document.
        """
        processed_input = [
            f"{self._SEARCH_DOCUMENT_PREFIX}{text}"
            if not text.startswith(self._SEARCH_DOCUMENT_PREFIX)
            else text
            for text in input_texts
        ]
        embeddings = self.model.encode(
            processed_input,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return embeddings.tolist()

    def embed_query(self, query: str) -> List[float]:
        """
        Embeds a single query string.

        Args:
            query: The query text to embed.

        Returns:
            List[float]: The embedding for the query.
        """
        prefixed_query = f"{self._SEARCH_QUERY_PREFIX}{query}"
        embedding = self.model.encode(
            [prefixed_query], convert_to_numpy=True, normalize_embeddings=True
        )
        return embedding[0].tolist()
