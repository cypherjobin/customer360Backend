"""
VIBO Vector Store
=================
ChromaDB wrapper for semantic search across call transcripts and events.
Supports both Ollama (local) and Azure OpenAI embedding providers.

Usage:
    store = VectorStore()
    store.initialize()
    
    # Add documents
    store.upsert_transcripts([{"id": "T1", "text": "...", "metadata": {...}}, ...])
    
    # Search
    results = store.search_customer_history(customer_id="10900099", query="billing issue", top_k=5)
"""

import os
import logging
import time
from typing import Optional
import httpx
import chromadb
from chromadb.config import Settings

from vibo_config import (
    CHROMA_PERSIST_PATH, CHROMA_HOST, CHROMA_PORT,
    CHROMA_COLLECTION_TRANSCRIPTS, CHROMA_COLLECTION_EVENTS,
    EMBEDDING_PROVIDER, OLLAMA_BASE_URL, OLLAMA_EMBED_MODEL,
    AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_EMBED_DEPLOYMENT,
    AZURE_OPENAI_API_VERSION, EMBED_BATCH_SIZE, VECTOR_SEARCH_TOP_K,
)

logger = logging.getLogger("vibo.vector_store")


class EmbeddingProvider:
    """
    Abstraction over embedding providers.
    Supports Ollama (local) and Azure OpenAI.
    """
    
    def __init__(self, provider: str = None):
        self.provider = provider or EMBEDDING_PROVIDER
        self._dimension = None
        logger.info(f"Embedding provider: {self.provider}")
    
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for a list of texts."""
        if self.provider == "ollama":
            return self._embed_ollama(texts)
        elif self.provider == "azure_openai":
            return self._embed_azure(texts)
        else:
            raise ValueError(f"Unknown embedding provider: {self.provider}")
    
    def embed_single(self, text: str) -> list[float]:
        """Generate embedding for a single text."""
        results = self.embed([text])
        return results[0]
    
    @property
    def dimension(self) -> int:
        """Get embedding dimension (cached after first call)."""
        if self._dimension is None:
            test = self.embed_single("test")
            self._dimension = len(test)
        return self._dimension
    
    def _embed_ollama(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings via Ollama API."""
        embeddings = []
        for text in texts:
            try:
                resp = httpx.post(
                    f"{OLLAMA_BASE_URL}/api/embed",
                    json={"model": OLLAMA_EMBED_MODEL, "input": text},
                    timeout=60.0,
                )
                resp.raise_for_status()
                data = resp.json()
                # Ollama /api/embed returns {"embeddings": [[...]]}
                embeddings.append(data["embeddings"][0])
            except Exception as e:
                logger.error(f"Ollama embedding failed: {e}")
                raise
        return embeddings
    
    def _embed_azure(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings via Azure OpenAI API with retry on rate limit."""
        url = (
            f"{AZURE_OPENAI_ENDPOINT.rstrip('/')}/openai/deployments/"
            f"{AZURE_EMBED_DEPLOYMENT}/embeddings?api-version={AZURE_OPENAI_API_VERSION}"
        )
        headers = {
            "Content-Type": "application/json",
            "api-key": AZURE_OPENAI_API_KEY,
        }

        max_retries = 5
        base_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                resp = httpx.post(
                    url,
                    headers=headers,
                    json={"input": texts},
                    timeout=60.0,
                )

                # Handle rate limiting (429) with exponential backoff
                if resp.status_code == 429:
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"Rate limited (429), retrying in {delay}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                        continue
                    else:
                        raise Exception(f"Max retries exceeded for rate limit")

                resp.raise_for_status()
                data = resp.json()
                return [item["embedding"] for item in data["data"]]

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Azure embedding failed after {max_retries} retries: {e}")
                    raise
                # For other errors, also retry with backoff
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Azure embedding failed (retrying in {delay}s): {e}")
                time.sleep(delay)


class VectorStore:
    """
    ChromaDB-backed vector store for VIBO.

    Two collections:
    - vmi_llm_summaries: LLM-generated customer summaries (with sentiment, risk, etc.)
    - vmi_customer_events: Customer interaction event details
    """
    
    def __init__(self, embedding_provider: str = None):
        self.embedder = EmbeddingProvider(embedding_provider)
        self.client = None
        self.transcripts_collection = None
        self.events_collection = None
    
    def initialize(self):
        """Initialize ChromaDB client and collections."""
        if CHROMA_HOST:
            # Remote ChromaDB server
            logger.info(f"Connecting to ChromaDB at {CHROMA_HOST}:{CHROMA_PORT}")
            self.client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        else:
            # Local persistent storage
            os.makedirs(CHROMA_PERSIST_PATH, exist_ok=True)
            logger.info(f"Using local ChromaDB at {CHROMA_PERSIST_PATH}")
            self.client = chromadb.PersistentClient(
                path=CHROMA_PERSIST_PATH,
                settings=Settings(anonymized_telemetry=False),
            )
        
        # Get or create collections (no default embedding function - we manage our own)
        self.transcripts_collection = self.client.get_or_create_collection(
            name=CHROMA_COLLECTION_TRANSCRIPTS,
            metadata={"hnsw:space": "cosine"},
        )
        self.events_collection = self.client.get_or_create_collection(
            name=CHROMA_COLLECTION_EVENTS,
            metadata={"hnsw:space": "cosine"},
        )
        
        logger.info(
            f"Collections initialized: "
            f"transcripts={self.transcripts_collection.count()}, "
            f"events={self.events_collection.count()}"
        )
    
    # ─── UPSERT METHODS ───────────────────────────────────────────────────────
    
    def upsert_transcripts(self, documents: list[dict]) -> int:
        """
        Upsert call transcript embeddings.
        
        Each document: {
            "id": transcript_id,
            "text": call_summary text,
            "metadata": {
                "customer_id": str,
                "call_start": str (ISO),
                "call_segment": str,
                "call_product": str,
                "source": "CallTranscript"
            }
        }
        
        Returns: number of documents upserted.
        """
        return self._upsert_batch(self.transcripts_collection, documents)
    
    def upsert_events(self, documents: list[dict]) -> int:
        """
        Upsert customer event embeddings.
        
        Each document: {
            "id": natural_key,
            "text": extracted event text,
            "metadata": {
                "customer_id": str,
                "event_type": str,
                "source_system": str,
                "event_timestamp": str (ISO),
                "source": "Customer360_Events"
            }
        }
        
        Returns: number of documents upserted.
        """
        return self._upsert_batch(self.events_collection, documents)
    
    def _upsert_batch(self, collection, documents: list[dict]) -> int:
        """Batch upsert with embedding generation."""
        if not documents:
            return 0
        
        total = 0
        for i in range(0, len(documents), EMBED_BATCH_SIZE):
            batch = documents[i:i + EMBED_BATCH_SIZE]
            
            ids = [doc["id"] for doc in batch]
            texts = [doc["text"] for doc in batch]
            metadatas = [doc["metadata"] for doc in batch]
            
            # Generate embeddings
            embeddings = self.embedder.embed(texts)
            
            # Upsert to ChromaDB
            collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=texts,
                metadatas=metadatas,
            )
            
            total += len(batch)
            logger.debug(f"Upserted batch {i//EMBED_BATCH_SIZE + 1}: {len(batch)} docs")

            # Add delay between batches to avoid rate limiting (Azure TPM/RPM limits)
            if i + EMBED_BATCH_SIZE < len(documents):
                time.sleep(1)  # 1 second delay between batches

        return total
    
    # ─── SEARCH METHODS ───────────────────────────────────────────────────────
    
    def search_customer_history(
        self,
        customer_id: str,
        query: str,
        top_k: int = None,
        source: str = "all",
    ) -> dict:
        """
        Semantic search across a customer's transcripts and events.
        
        Args:
            customer_id: Customer to search within (mandatory filter)
            query: Natural language search query
            top_k: Number of results per collection (default from config)
            source: "all", "transcripts", or "events"
        
        Returns:
            dict with results from each searched collection
        """
        top_k = top_k or VECTOR_SEARCH_TOP_K
        query_embedding = self.embedder.embed_single(query)
        
        results = {
            "customer_id": customer_id,
            "query": query,
            "transcripts": [],
            "events": [],
        }
        
        # Search transcripts - first get customer IDs, then query within those
        if source in ("all", "transcripts") and self.transcripts_collection.count() > 0:
            try:
                # Get all IDs for this customer
                customer_data = self.transcripts_collection.get(
                    where={"customer_id": customer_id},
                    limit=1000
                )
                customer_ids = customer_data.get("ids", [])

                if customer_ids:
                    # Now query only within this customer's documents
                    tr = self.transcripts_collection.query(
                        query_embeddings=[query_embedding],
                        n_results=min(top_k, len(customer_ids)),
                        include=["documents", "metadatas", "distances"],
                        ids=customer_ids[:1000]  # ChromaDB limits
                    )
                    results["transcripts"] = self._format_search_results(tr)
            except Exception as e:
                logger.warning(f"Transcript search failed: {e}")

        # Search events - first get customer IDs, then query within those
        if source in ("all", "events") and self.events_collection.count() > 0:
            try:
                # Get all IDs for this customer
                customer_data = self.events_collection.get(
                    where={"customer_id": customer_id},
                    limit=1000
                )
                customer_ids = customer_data.get("ids", [])

                if customer_ids:
                    # Now query only within this customer's documents
                    ev = self.events_collection.query(
                        query_embeddings=[query_embedding],
                        n_results=min(top_k, len(customer_ids)),
                        include=["documents", "metadatas", "distances"],
                        ids=customer_ids[:1000]  # ChromaDB limits
                    )
                    results["events"] = self._format_search_results(ev)
            except Exception as e:
                logger.warning(f"Events search failed: {e}")
        
        results["total_results"] = len(results["transcripts"]) + len(results["events"])
        return results
    
    def _format_search_results(self, raw_results) -> list[dict]:
        """Format ChromaDB query results into a clean list."""
        if not raw_results or not raw_results.get("ids") or not raw_results["ids"][0]:
            return []
        
        formatted = []
        ids = raw_results["ids"][0]
        docs = raw_results["documents"][0] if raw_results.get("documents") else [None] * len(ids)
        metas = raw_results["metadatas"][0] if raw_results.get("metadatas") else [{}] * len(ids)
        dists = raw_results["distances"][0] if raw_results.get("distances") else [None] * len(ids)
        
        for doc_id, doc, meta, dist in zip(ids, docs, metas, dists):
            formatted.append({
                "id": doc_id,
                "text": doc,
                "metadata": meta,
                "similarity": round(1 - dist, 4) if dist is not None else None,  # cosine distance → similarity
            })
        
        return formatted
    
    # ─── MANAGEMENT METHODS ───────────────────────────────────────────────────
    
    def get_stats(self) -> dict:
        """Get collection statistics."""
        return {
            "transcripts_count": self.transcripts_collection.count() if self.transcripts_collection else 0,
            "events_count": self.events_collection.count() if self.events_collection else 0,
            "chroma_path": CHROMA_PERSIST_PATH,
            "embedding_provider": self.embedder.provider,
        }
    
    def delete_customer(self, customer_id: str) -> dict:
        """Delete all embeddings for a customer (GDPR compliance)."""
        deleted = {"transcripts": 0, "events": 0}
        
        for coll_name, collection in [
            ("transcripts", self.transcripts_collection),
            ("events", self.events_collection),
        ]:
            try:
                # Get IDs for this customer
                existing = collection.get(where={"customer_id": customer_id})
                if existing and existing["ids"]:
                    collection.delete(ids=existing["ids"])
                    deleted[coll_name] = len(existing["ids"])
            except Exception as e:
                logger.error(f"Failed to delete {coll_name} for {customer_id}: {e}")
        
        logger.info(f"Deleted embeddings for {customer_id}: {deleted}")
        return deleted
    
    def reset_collection(self, collection_name: str):
        """Delete and recreate a collection (for full rebuild)."""
        self.client.delete_collection(collection_name)
        if collection_name == CHROMA_COLLECTION_TRANSCRIPTS:
            self.transcripts_collection = self.client.create_collection(
                name=collection_name, metadata={"hnsw:space": "cosine"}
            )
        elif collection_name == CHROMA_COLLECTION_EVENTS:
            self.events_collection = self.client.create_collection(
                name=collection_name, metadata={"hnsw:space": "cosine"}
            )
        logger.info(f"Reset collection: {collection_name}")


if __name__ == "__main__":
    # Quick test
    print("Initializing VectorStore...")
    store = VectorStore()
    store.initialize()
    
    stats = store.get_stats()
    print(f"Stats: {stats}")
    
    # Test embedding
    print(f"\nTesting embedding provider ({store.embedder.provider})...")
    try:
        emb = store.embedder.embed_single("test embedding for VIBO")
        print(f"Embedding dimension: {len(emb)}")
        print(f"First 5 values: {emb[:5]}")
        print("✓ Embedding provider working")
    except Exception as e:
        print(f"✗ Embedding failed: {e}")
        print("  Make sure Ollama is running with nomic-embed-text pulled,")
        print("  or set VIBO_EMBEDDING_PROVIDER=azure_openai in .env")
