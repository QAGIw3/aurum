"""ANN (Approximate Nearest Neighbor) search optimization.

Provides optimization strategies for vector search including
index parameter tuning, query optimization, and performance monitoring.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from aurum.core.settings import get_settings
from aurum.core import AurumSettings
from .elasticsearch_engine import ElasticsearchEngine


logger = logging.getLogger(__name__)


@dataclass
class ANNIndexConfig:
    """Configuration for ANN index optimization."""
    m: int = 16  # Number of bi-directional links
    ef_construction: int = 200  # Size of dynamic candidate list during construction
    ef_search: int = 128  # Size of dynamic candidate list during search
    max_connections: int = 16  # Maximum number of connections per element
    similarity_threshold: float = 0.8


@dataclass
class ANNPerformanceMetrics:
    """Performance metrics for ANN search."""
    query_time_ms: float
    recall: float
    precision: float
    index_size_mb: float
    memory_usage_mb: float
    cpu_usage_percent: float


class ANNIndexTuner:
    """Tunes ANN index parameters for optimal performance."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize ANN index tuner.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.config = ANNIndexConfig()

    async def tune_index_parameters(
        self,
        engine: ElasticsearchEngine,
        test_queries: List[str],
        ground_truth: Dict[str, List[str]]
    ) -> ANNIndexConfig:
        """Tune ANN index parameters using test queries.

        Args:
            engine: Elasticsearch engine instance
            test_queries: List of test queries
            ground_truth: Ground truth relevance judgments

        Returns:
            Optimized ANN index configuration
        """
        best_config = self.config
        best_score = 0.0

        # Test different parameter combinations
        m_values = [8, 16, 32, 64]
        ef_construction_values = [100, 200, 400, 800]
        ef_search_values = [64, 128, 256, 512]

        for m in m_values:
            for ef_construction in ef_construction_values:
                for ef_search in ef_search_values:
                    # Skip invalid combinations
                    if ef_search > ef_construction:
                        continue

                    test_config = ANNIndexConfig(
                        m=m,
                        ef_construction=ef_construction,
                        ef_search=ef_search
                    )

                    # Test configuration
                    score = await self._evaluate_config(
                        engine, test_config, test_queries, ground_truth
                    )

                    if score > best_score:
                        best_score = score
                        best_config = test_config

        logger.info(f"Optimized ANN parameters - m: {best_config.m}, "
                   f"ef_construction: {best_config.ef_construction}, "
                   f"ef_search: {best_config.ef_search}")

        return best_config

    async def _evaluate_config(
        self,
        engine: ElasticsearchEngine,
        config: ANNIndexConfig,
        test_queries: List[str],
        ground_truth: Dict[str, List[str]]
    ) -> float:
        """Evaluate ANN configuration against test data."""
        total_score = 0.0

        for query in test_queries[:10]:  # Limit for performance
            try:
                # Run search with current config
                response = await engine.search(
                    query=query,
                    size=20,
                    semantic_weight=0.8  # High semantic weight for testing
                )

                # Calculate recall@10
                retrieved_docs = [r.document.id for r in response.results[:10]]
                relevant_docs = ground_truth.get(query, [])

                if relevant_docs:
                    recall = len(set(retrieved_docs) & set(relevant_docs)) / len(relevant_docs)
                    total_score += recall

            except Exception as e:
                logger.warning(f"Error evaluating config: {e}")
                continue

        return total_score / len(test_queries) if test_queries else 0.0


class ANNQueryOptimizer:
    """Optimizes ANN queries for better performance and accuracy."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize ANN query optimizer.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

    def optimize_query(
        self,
        query_vector: List[float],
        k: int,
        num_candidates: int,
        similarity_threshold: float = 0.0
    ) -> Dict[str, Any]:
        """Optimize ANN query parameters.

        Args:
            query_vector: Query embedding vector
            k: Number of nearest neighbors to return
            num_candidates: Number of candidates to consider
            similarity_threshold: Minimum similarity threshold

        Returns:
            Optimized query parameters
        """
        # Adaptive parameter selection based on query characteristics
        vector_magnitude = sum(x**2 for x in query_vector) ** 0.5

        # Adjust parameters based on vector characteristics
        if vector_magnitude < 0.5:  # Low magnitude vectors
            # Increase candidates for better recall
            optimized_candidates = min(num_candidates * 2, 10000)
            optimized_k = min(k * 2, 1000)
        elif vector_magnitude > 2.0:  # High magnitude vectors
            # Reduce candidates for better precision
            optimized_candidates = max(num_candidates // 2, 100)
            optimized_k = k
        else:
            optimized_candidates = num_candidates
            optimized_k = k

        # Build optimized query
        query = {
            "knn": {
                "field": "embedding",
                "query_vector": query_vector,
                "k": optimized_k,
                "num_candidates": optimized_candidates,
                "filter": [
                    {"exists": {"field": "embedding"}},
                    {"range": {"quality_score": {"gte": 0.1}}}  # Filter low quality docs
                ]
            }
        }

        # Add similarity threshold if specified
        if similarity_threshold > 0:
            query["knn"]["similarity_threshold"] = similarity_threshold

        return query

    def batch_optimize_queries(
        self,
        query_vectors: List[List[float]],
        k: int = 100,
        num_candidates: int = 1000
    ) -> List[Dict[str, Any]]:
        """Optimize multiple ANN queries for batch processing.

        Args:
            query_vectors: List of query embedding vectors
            k: Number of nearest neighbors to return
            num_candidates: Number of candidates to consider

        Returns:
            List of optimized query configurations
        """
        optimized_queries = []

        for vector in query_vectors:
            opt_query = self.optimize_query(vector, k, num_candidates)
            optimized_queries.append(opt_query)

        return optimized_queries


class ANNSearchService:
    """Service for optimized ANN search operations."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize ANN search service.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.tuner = ANNIndexTuner(settings)
        self.optimizer = ANNQueryOptimizer(settings)
        self._engine: Optional[ElasticsearchEngine] = None

    async def initialize(self, engine: ElasticsearchEngine):
        """Initialize with Elasticsearch engine.

        Args:
            engine: Elasticsearch engine instance
        """
        self._engine = engine

    async def hybrid_search_optimized(
        self,
        query: str,
        query_embedding: List[float],
        text_weight: float = 0.7,
        semantic_weight: float = 0.3,
        k: int = 100,
        tenant_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Perform optimized hybrid search with ANN.

        Args:
            query: Text query string
            query_embedding: Query embedding vector
            text_weight: Weight for text search component
            semantic_weight: Weight for semantic search component
            k: Number of ANN results to retrieve
            tenant_id: Tenant ID for filtering
            **kwargs: Additional search parameters

        Returns:
            Optimized hybrid search results
        """
        if not self._engine:
            raise RuntimeError("ANN search service not initialized")

        # Optimize ANN query parameters
        optimized_ann_query = self.optimizer.optimize_query(
            query_embedding, k, self.settings.search_knn_k
        )

        # Build hybrid query with optimized parameters
        query_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["title^5", "name^3", "description^2", "content_text^1", "tags^2"],
                                "type": "best_fields",
                                "boost": text_weight
                            }
                        },
                        {
                            **optimized_ann_query,
                            "knn": {
                                **optimized_ann_query["knn"],
                                "boost": semantic_weight
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": kwargs.get("size", 20)
        }

        # Add tenant filter if specified
        if tenant_id:
            query_body["query"]["bool"]["filter"] = [
                {"term": {"tenant_id": tenant_id}}
            ]

        # Execute optimized search
        client = await self._engine._get_client()
        response = await client.search(
            index=self._engine._index_name,
            body=query_body
        )

        return {
            "results": response["hits"]["hits"],
            "total": response["hits"]["total"]["value"],
            "took_ms": response["took"],
            "query_optimization": {
                "original_k": k,
                "optimized_k": optimized_ann_query["knn"]["k"],
                "original_candidates": self.settings.search_knn_k,
                "optimized_candidates": optimized_ann_query["knn"]["num_candidates"]
            }
        }

    async def tune_ann_parameters(
        self,
        test_queries: List[str],
        ground_truth: Dict[str, List[str]]
    ) -> ANNIndexConfig:
        """Tune ANN index parameters using test data.

        Args:
            test_queries: List of test queries
            ground_truth: Ground truth relevance judgments

        Returns:
            Optimized ANN index configuration
        """
        if not self._engine:
            raise RuntimeError("ANN search service not initialized")

        return await self.tuner.tune_index_parameters(
            self._engine, test_queries, ground_truth
        )


# Global service instance
_ann_search_service: Optional[ANNSearchService] = None


def get_ann_search_service(
    engine: ElasticsearchEngine,
    settings: Optional[AurumSettings] = None
) -> ANNSearchService:
    """Get or create global ANN search service.

    Args:
        engine: Elasticsearch engine instance
        settings: Application settings

    Returns:
        ANN search service instance
    """
    global _ann_search_service
    if _ann_search_service is None:
        _ann_search_service = ANNSearchService(settings)
    return _ann_search_service


async def initialize_ann_search(
    engine: ElasticsearchEngine,
    settings: Optional[AurumSettings] = None
) -> None:
    """Initialize ANN search service globally.

    Args:
        engine: Elasticsearch engine instance
        settings: Application settings
    """
    service = get_ann_search_service(engine, settings)
    await service.initialize(engine)
