"""Semantic search capabilities for Aurum search platform.

Provides embedding model management, vector inference, hybrid scoring,
and integration with Elasticsearch for semantic search functionality.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from functools import lru_cache
import numpy as np
from dataclasses import dataclass
import hashlib

from sentence_transformers import SentenceTransformer
import torch

from aurum.core.settings import get_settings
from aurum.core import AurumSettings
from .elasticsearch_engine import SearchDocument, SearchResponse


logger = logging.getLogger(__name__)


@dataclass
class EmbeddingResult:
    """Result of embedding computation."""
    vector: List[float]
    model_name: str
    tokens: int
    processing_time_ms: float


@dataclass
class HybridScore:
    """Hybrid search score combining text and semantic components."""
    bm25_score: float
    semantic_score: float
    combined_score: float
    semantic_weight: float
    explanation: Optional[Dict[str, Any]] = None


class EmbeddingModelManager:
    """Manages sentence transformer models for semantic search."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize embedding model manager.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings or get_settings()
        self._model: Optional[SentenceTransformer] = None
        self._model_name = self.settings.search_embedding_model

    async def initialize(self) -> None:
        """Initialize embedding model."""
        try:
            # Load model in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            self._model = await loop.run_in_executor(
                None,
                self._load_model_sync
            )
            logger.info(f"Loaded embedding model: {self._model_name}")
        except Exception as e:
            logger.error(f"Failed to load embedding model {self._model_name}: {e}")
            raise

    def _load_model_sync(self) -> SentenceTransformer:
        """Load model synchronously (called in thread pool)."""
        return SentenceTransformer(self._model_name)

    def is_healthy(self) -> bool:
        """Check if model is loaded and ready."""
        return self._model is not None

    async def embed_texts(self, texts: List[str]) -> List[EmbeddingResult]:
        """Generate embeddings for text documents.

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding results
        """
        if not self._model:
            await self.initialize()

        if not self._model:
            raise RuntimeError("Embedding model not initialized")

        # Process in thread pool to avoid blocking
        loop = asyncio.get_event_loop()

        # Prepare texts
        clean_texts = [text.strip() for text in texts if text.strip()]

        if not clean_texts:
            return []

        # Generate embeddings
        start_time = asyncio.get_event_loop().time()
        embeddings = await loop.run_in_executor(
            None,
            self._generate_embeddings_sync,
            clean_texts
        )
        end_time = asyncio.get_event_loop().time()

        processing_time_ms = (end_time - start_time) * 1000

        # Create results
        results = []
        for i, (text, embedding) in enumerate(zip(clean_texts, embeddings)):
            # Estimate token count (rough approximation)
            tokens = len(text.split()) * 1.3  # Rough token estimation

            result = EmbeddingResult(
                vector=embedding.tolist(),
                model_name=self._model_name,
                tokens=int(tokens),
                processing_time_ms=processing_time_ms / len(clean_texts)
            )
            results.append(result)

        return results

    def _generate_embeddings_sync(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings synchronously."""
        # Encode texts with sentence transformer
        embeddings = self._model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,  # L2 normalize for cosine similarity
            show_progress_bar=False
        )
        return embeddings

    async def embed_query(self, query: str) -> Optional[List[float]]:
        """Generate embedding for search query.

        Args:
            query: Search query string

        Returns:
            Query embedding vector or None if empty
        """
        if not query.strip():
            return None

        results = await self.embed_texts([query])
        return results[0].vector if results else None

    async def embed_documents(self, documents: List[SearchDocument]) -> List[SearchDocument]:
        """Add embeddings to search documents with optimization.

        Args:
            documents: Documents to embed

        Returns:
            Documents with embeddings added
        """
        # Extract text content for embedding
        texts_to_embed = []
        doc_indices = []

        for i, doc in enumerate(documents):
            # Combine title, name, and content for embedding
            text_parts = []
            if doc.title:
                text_parts.append(doc.title)
            if doc.name:
                text_parts.append(doc.name)
            if doc.description:
                text_parts.append(doc.description)
            if doc.content_text:
                text_parts.append(doc.content_text)

            combined_text = " ".join(text_parts)
            if combined_text.strip():
                texts_to_embed.append(combined_text)
                doc_indices.append(i)

        if not texts_to_embed:
            return documents

        # Generate embeddings in batches for efficiency
        batch_size = 32  # Process in batches to avoid memory issues
        all_embeddings = []

        for i in range(0, len(texts_to_embed), batch_size):
            batch_texts = texts_to_embed[i:i + batch_size]
            batch_results = await self.embed_texts(batch_texts)
            all_embeddings.extend([result.vector for result in batch_results])

        # Add embeddings to documents
        for idx, embedding in zip(doc_indices, all_embeddings):
            documents[idx].embedding = embedding

        return documents


class HybridScorer:
    """Handles hybrid scoring combining BM25 and semantic search."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize hybrid scorer.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings or get_settings()

    def calculate_hybrid_score(
        self,
        bm25_score: float,
        semantic_score: float,
        semantic_weight: float = None
    ) -> HybridScore:
        """Calculate hybrid score combining BM25 and semantic components.

        Args:
            bm25_score: BM25 text relevance score
            semantic_score: Semantic similarity score (0-1)
            semantic_weight: Weight for semantic component (0-1)

        Returns:
            Hybrid score with explanation
        """
        if semantic_weight is None:
            semantic_weight = self.settings.search_semantic_weight

        # Normalize scores
        normalized_bm25 = min(bm25_score / 20.0, 1.0)  # Assume max BM25 around 20
        normalized_semantic = min(semantic_score, 1.0)

        # Linear combination
        combined_score = (
            (1 - semantic_weight) * normalized_bm25 +
            semantic_weight * normalized_semantic
        )

        return HybridScore(
            bm25_score=bm25_score,
            semantic_score=semantic_score,
            combined_score=combined_score,
            semantic_weight=semantic_weight,
            explanation={
                "bm25_normalized": normalized_bm25,
                "semantic_normalized": normalized_semantic,
                "formula": f"({1-semantic_weight}) * BM25 + {semantic_weight} * Semantic"
            }
        )

    def score_search_results(
        self,
        search_response: SearchResponse,
        query_embedding: Optional[List[float]] = None,
        semantic_weight: float = None
    ) -> SearchResponse:
        """Apply hybrid scoring to search results.

        Args:
            search_response: Original search response
            query_embedding: Query embedding for semantic scoring
            semantic_weight: Weight for semantic component

        Returns:
            Search response with hybrid scores
        """
        if not query_embedding or semantic_weight == 0:
            return search_response

        # Calculate hybrid scores for each result
        for result in search_response.results:
            # Get semantic score from document embedding
            doc_embedding = result.document.embedding
            semantic_score = 0.0

            if doc_embedding and query_embedding:
                # Calculate cosine similarity
                semantic_score = self._cosine_similarity(query_embedding, doc_embedding)

            # Calculate hybrid score
            hybrid_score = self.calculate_hybrid_score(
                bm25_score=result.score,
                semantic_score=semantic_score,
                semantic_weight=semantic_weight
            )

            # Update result score
            result.score = hybrid_score.combined_score

        # Re-sort by hybrid score
        search_response.results.sort(key=lambda r: r.score, reverse=True)

        # Update ranks
        for i, result in enumerate(search_response.results):
            result.rank = i + 1

        return search_response

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors.

        Args:
            vec1: First vector
            vec2: Second vector

        Returns:
            Cosine similarity score (0-1)
        """
        try:
            v1 = np.array(vec1)
            v2 = np.array(vec2)

            # Calculate cosine similarity
            dot_product = np.dot(v1, v2)
            norm1 = np.linalg.norm(v1)
            norm2 = np.linalg.norm(v2)

            if norm1 == 0 or norm2 == 0:
                return 0.0

            similarity = dot_product / (norm1 * norm2)
            return float(max(0.0, min(1.0, similarity)))  # Clamp to [0, 1]
        except Exception as e:
            logger.warning(f"Error calculating cosine similarity: {e}")
            return 0.0


class SemanticSearchService:
    """Main service for semantic search functionality."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize semantic search service.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings or get_settings()
        self.embedding_manager = EmbeddingModelManager(settings)
        self.hybrid_scorer = HybridScorer(settings)
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize semantic search service."""
        if self.settings.search_semantic_enabled:
            await self.embedding_manager.initialize()
            self._initialized = True
            logger.info("Semantic search service initialized")
        else:
            logger.info("Semantic search disabled by configuration")

    def is_enabled(self) -> bool:
        """Check if semantic search is enabled."""
        return self.settings.search_semantic_enabled and self._initialized

    async def embed_query(self, query: str) -> Optional[List[float]]:
        """Generate embedding for search query.

        Args:
            query: Search query string

        Returns:
            Query embedding or None if semantic search disabled
        """
        if not self.is_enabled():
            return None

        return await self.embedding_manager.embed_query(query)

    async def embed_documents(self, documents: List[SearchDocument]) -> List[SearchDocument]:
        """Add embeddings to search documents.

        Args:
            documents: Documents to embed

        Returns:
            Documents with embeddings added
        """
        if not self.is_enabled():
            return documents

        return await self.embedding_manager.embed_documents(documents)

    async def enhance_search_response(
        self,
        search_response: SearchResponse,
        query: str,
        semantic_weight: float = None
    ) -> SearchResponse:
        """Enhance search response with semantic scoring.

        Args:
            search_response: Original search response
            query: Original search query
            semantic_weight: Weight for semantic component

        Returns:
            Enhanced search response with hybrid scoring
        """
        if not self.is_enabled() or semantic_weight == 0:
            return search_response

        # Get query embedding
        query_embedding = await self.embed_query(query)
        if not query_embedding:
            return search_response

        # Apply hybrid scoring
        enhanced_response = self.hybrid_scorer.score_search_results(
            search_response,
            query_embedding=query_embedding,
            semantic_weight=semantic_weight
        )

        return enhanced_response

    async def get_semantic_suggestions(
        self,
        query: str,
        documents: List[SearchDocument],
        limit: int = 5
    ) -> List[Tuple[SearchDocument, float]]:
        """Get semantic suggestions based on query similarity.

        Args:
            query: Search query
            documents: Documents to compare against
            limit: Maximum number of suggestions

        Returns:
            List of (document, similarity_score) tuples
        """
        if not self.is_enabled():
            return []

        # Get query embedding
        query_embedding = await self.embed_query(query)
        if not query_embedding:
            return []

        # Calculate similarities
        suggestions = []
        for doc in documents:
            if doc.embedding:
                similarity = self.hybrid_scorer._cosine_similarity(query_embedding, doc.embedding)
                suggestions.append((doc, similarity))

        # Sort by similarity and return top results
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return suggestions[:limit]


# Global service instance
_semantic_search_service: Optional[SemanticSearchService] = None


def get_semantic_search_service(settings: Optional[AurumSettings] = None) -> SemanticSearchService:
    """Get or create global semantic search service instance.

    Args:
        settings: Application settings. If None, uses global settings.

    Returns:
        Semantic search service instance
    """
    global _semantic_search_service
    if _semantic_search_service is None:
        _semantic_search_service = SemanticSearchService(settings)
    return _semantic_search_service


async def initialize_semantic_search(settings: Optional[AurumSettings] = None) -> None:
    """Initialize semantic search service globally.

    Args:
        settings: Application settings. If None, uses global settings.
    """
    service = get_semantic_search_service(settings)
    await service.initialize()


def is_semantic_search_enabled(settings: Optional[AurumSettings] = None) -> bool:
    """Check if semantic search is enabled.

    Args:
        settings: Application settings. If None, uses global settings.

    Returns:
        True if semantic search is enabled and initialized
    """
    service = get_semantic_search_service(settings)
    return service.is_enabled()
