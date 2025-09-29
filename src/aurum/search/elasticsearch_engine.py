"""Elasticsearch engine for Aurum search platform.

Provides connection management, index lifecycle, search operations,
suggestions, faceted search, and hybrid scoring capabilities.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
import hashlib

from elasticsearch import Elasticsearch, AsyncElasticsearch, exceptions
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio

from aurum.core.settings import get_settings
from aurum.core import AurumSettings


logger = logging.getLogger(__name__)


@dataclass
class SearchDocument:
    """Represents a document to be indexed in Elasticsearch."""
    id: str
    doc_type: str
    tenant_id: str
    title: str
    name: Optional[str] = None
    description: Optional[str] = None
    content_text: Optional[str] = None
    tags: List[str] = None
    domains: List[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    quality_score: Optional[float] = None
    popularity_score: Optional[float] = None
    embedding: Optional[List[float]] = None
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.domains is None:
            self.domains = []
        if self.metadata is None:
            self.metadata = {}


@dataclass
class SearchResult:
    """Represents a search result with metadata."""
    document: SearchDocument
    score: float
    highlights: Optional[Dict[str, List[str]]] = None
    rank: Optional[int] = None


@dataclass
class FacetResult:
    """Represents facet aggregation results."""
    field: str
    buckets: List[Dict[str, Any]]


@dataclass
class SearchResponse:
    """Complete search response with results and metadata."""
    results: List[SearchResult]
    total: int
    took_ms: int
    facets: Optional[List[FacetResult]] = None
    aggregations: Optional[Dict[str, Any]] = None
    cursor: Optional[str] = None


class ElasticsearchEngine:
    """Elasticsearch engine for search operations."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize Elasticsearch engine.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings or get_settings()
        self._client: Optional[AsyncElasticsearch] = None
        self._index_name = f"{self.settings.search_index_prefix}-search-v1"
        self._is_healthy = False

    async def initialize(self) -> None:
        """Initialize Elasticsearch connection and ensure index exists."""
        try:
            await self._get_client()
            await self._ensure_index()
            await self._ensure_mappings()
            await self._ensure_settings()
            self._is_healthy = True
            logger.info(f"Elasticsearch engine initialized with index: {self._index_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch engine: {e}")
            self._is_healthy = False
            raise

    async def close(self) -> None:
        """Close Elasticsearch connection."""
        if self._client:
            await self._client.close()
            self._client = None
            self._is_healthy = False
            logger.info("Elasticsearch engine closed")

    async def health_check(self) -> bool:
        """Check Elasticsearch health."""
        try:
            if not self._client or not self._is_healthy:
                return False

            health = await self._client.cluster.health()
            return health['status'] in ['green', 'yellow']
        except Exception as e:
            logger.warning(f"Elasticsearch health check failed: {e}")
            return False

    async def _get_client(self) -> AsyncElasticsearch:
        """Get or create Elasticsearch client."""
        if self._client is None:
            # Build connection parameters
            hosts = self.settings.search_hosts
            auth = None

            if self.settings.search_username and self.settings.search_password:
                auth = (self.settings.search_username, self.settings.search_password)
            elif self.settings.search_api_key:
                # API key authentication
                auth = self.settings.search_api_key

            # Create async client
            self._client = AsyncElasticsearch(
                hosts=hosts,
                basic_auth=auth if isinstance(auth, tuple) else None,
                api_key=auth if isinstance(auth, str) else None,
                verify_certs=False,  # TODO: Make configurable
                request_timeout=self.settings.search_query_timeout_ms / 1000,
                maxsize=20,  # Connection pool size
            )

            # Test connection
            await self._client.info()

        return self._client

    async def _ensure_index(self) -> None:
        """Ensure search index exists."""
        client = await self._get_client()

        if not await client.indices.exists(index=self._index_name):
            await client.indices.create(
                index=self._index_name,
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,  # Single node setup
                        "refresh_interval": "30s",
                        "analysis": {
                            "analyzer": {
                                "aurum_text": {
                                    "type": "standard",
                                    "stopwords": "_english_"
                                }
                            }
                        }
                    }
                }
            )
            logger.info(f"Created Elasticsearch index: {self._index_name}")

    async def _ensure_mappings(self) -> None:
        """Ensure index mappings are up to date."""
        client = await self._get_client()

        mappings = {
            "properties": {
                "id": {"type": "keyword"},
                "doc_type": {"type": "keyword"},
                "tenant_id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "aurum_text",
                    "boost": 5.0
                },
                "name": {
                    "type": "text",
                    "analyzer": "aurum_text",
                    "boost": 3.0
                },
                "description": {
                    "type": "text",
                    "analyzer": "aurum_text",
                    "boost": 2.0
                },
                "content_text": {
                    "type": "text",
                    "analyzer": "aurum_text",
                    "boost": 1.0
                },
                "tags": {
                    "type": "keyword",
                    "boost": 2.0
                },
                "domains": {
                    "type": "keyword"
                },
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
                "quality_score": {"type": "float"},
                "popularity_score": {"type": "float"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": 384,  # all-MiniLM-L6-v2 dimension
                    "index": True,
                    "similarity": "cosine",
                    "index_options": {
                        "type": "hnsw",
                        "m": 16,  # Number of bi-directional links created for every new element
                        "ef_construction": 200,  # Size of the dynamic candidate list
                        "ef_search": 128  # Size of the dynamic candidate list during search
                    }
                },
                "metadata": {
                    "type": "object",
                    "dynamic": True
                }
            }
        }

        await client.indices.put_mapping(
            index=self._index_name,
            body=mappings
        )

    async def _ensure_settings(self) -> None:
        """Ensure index settings for search optimization."""
        client = await self._get_client()

        settings = {
            "index": {
                "max_result_window": self.settings.search_max_result_window,
                "analysis": {
                    "analyzer": {
                        "edge_ngram_analyzer": {
                            "tokenizer": "edge_ngram_tokenizer",
                            "filter": ["lowercase"]
                        }
                    },
                    "tokenizer": {
                        "edge_ngram_tokenizer": {
                            "type": "edge_ngram",
                            "min_gram": 1,
                            "max_gram": 20,
                            "token_chars": ["letter", "digit"]
                        }
                    }
                }
            }
        }

        await client.indices.put_settings(
            index=self._index_name,
            body=settings
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((exceptions.ConnectionError, exceptions.Timeout))
    )
    async def bulk_index(self, documents: List[SearchDocument], refresh: bool = False) -> Dict[str, Any]:
        """Bulk index documents with retry logic.

        Args:
            documents: List of documents to index
            refresh: Whether to refresh index after bulk operation

        Returns:
            Bulk response from Elasticsearch
        """
        client = await self._get_client()

        actions = []
        for doc in documents:
            # Create deterministic ID for idempotency
            doc_id = hashlib.sha256(f"{doc.tenant_id}:{doc.doc_type}:{doc.id}".encode()).hexdigest()[:16]

            action = {
                "_index": self._index_name,
                "_id": doc_id,
                "_source": {
                    "id": doc.id,
                    "doc_type": doc.doc_type,
                    "tenant_id": doc.tenant_id,
                    "title": doc.title,
                    "name": doc.name,
                    "description": doc.description,
                    "content_text": doc.content_text,
                    "tags": doc.tags,
                    "domains": doc.domains,
                    "created_at": doc.created_at.isoformat() if doc.created_at else None,
                    "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
                    "quality_score": doc.quality_score,
                    "popularity_score": doc.popularity_score,
                    "embedding": doc.embedding,
                    "metadata": doc.metadata
                }
            }
            actions.append(action)

        response = await client.bulk(
            operations=actions,
            refresh=refresh
        )

        if response.get('errors', False):
            error_count = sum(1 for item in response['items'] if item.get('error'))
            logger.warning(f"Bulk index completed with {error_count} errors")

        return response

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((exceptions.ConnectionError, exceptions.Timeout))
    )
    async def delete_by_id(self, doc_id: str, tenant_id: str) -> Dict[str, Any]:
        """Delete document by ID with tenant context."""
        client = await self._get_client()

        # Use deterministic ID for consistency
        es_id = hashlib.sha256(f"{tenant_id}:{doc_id}".encode()).hexdigest()[:16]

        response = await client.delete(
            index=self._index_name,
            id=es_id,
            ignore=[404]  # Don't fail if document doesn't exist
        )

        return response

    async def search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        facets: Optional[List[str]] = None,
        facet_config: Optional[Dict[str, Any]] = None,
        size: int = 20,
        search_after: Optional[List[Any]] = None,
        semantic_weight: float = 0.0,
        tenant_id: Optional[str] = None,
        highlight_fields: Optional[List[str]] = None,
        sort_config: Optional[List[Dict[str, Any]]] = None,
        **kwargs
    ) -> SearchResponse:
        """Perform search with hybrid text + semantic capabilities.

        Args:
            query: Search query string
            filters: Additional filters to apply
            facets: Fields to aggregate facets for
            facet_config: Configuration for facet aggregations
            size: Number of results to return
            search_after: Cursor for pagination
            semantic_weight: Weight for semantic search (0-1)
            tenant_id: Tenant ID for filtering
            highlight_fields: Fields to highlight in results
            sort_config: Custom sort configuration
            **kwargs: Additional search parameters

        Returns:
            Complete search response
        """
        client = await self._get_client()

        # Build query body
        query_body = self._build_query_body(
            query=query,
            filters=filters,
            semantic_weight=semantic_weight,
            tenant_id=tenant_id,
            **kwargs
        )

        # Add highlighting if requested
        if highlight_fields:
            query_body['highlight'] = {
                'fields': {field: {} for field in highlight_fields},
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fragment_size': 150,
                'number_of_fragments': 3
            }

        # Add custom sorting
        if sort_config:
            query_body['sort'] = sort_config
        elif semantic_weight > 0:
            # For hybrid search, prioritize by combined score
            query_body['sort'] = [
                {"_score": {"order": "desc"}},
                {"quality_score": {"order": "desc", "missing": "_last"}},
                {"popularity_score": {"order": "desc", "missing": "_last"}},
                {"id": {"order": "asc"}}  # Deterministic tie-breaker
            ]
        else:
            # Standard sorting for text-only search
            query_body['sort'] = [
                {"_score": {"order": "desc"}},
                {"quality_score": {"order": "desc", "missing": "_last"}},
                {"popularity_score": {"order": "desc", "missing": "_last"}},
                {"created_at": {"order": "desc", "missing": "_last"}},
                {"id": {"order": "asc"}}
            ]

        # Add aggregations for facets
        if facets:
            query_body['aggs'] = self._build_facet_aggregations(facets, facet_config)

        # Add search_after for cursor pagination
        search_params = {
            'index': self._index_name,
            'body': query_body,
            'size': size,
            'track_total_hits': True
        }

        if search_after:
            search_params['search_after'] = search_after

        # Execute search
        response = await client.search(**search_params)

        # Process results
        results = []
        for i, hit in enumerate(response['hits']['hits']):
            source = hit['_source']
            document = SearchDocument(
                id=source['id'],
                doc_type=source['doc_type'],
                tenant_id=source['tenant_id'],
                title=source['title'],
                name=source.get('name'),
                description=source.get('description'),
                content_text=source.get('content_text'),
                tags=source.get('tags', []),
                domains=source.get('domains', []),
                created_at=datetime.fromisoformat(source['created_at']) if source.get('created_at') else None,
                updated_at=datetime.fromisoformat(source['updated_at']) if source.get('updated_at') else None,
                quality_score=source.get('quality_score'),
                popularity_score=source.get('popularity_score'),
                embedding=source.get('embedding'),
                metadata=source.get('metadata', {})
            )

            result = SearchResult(
                document=document,
                score=hit['_score'],
                highlights=hit.get('highlight'),
                rank=i + 1
            )
            results.append(result)

        # Build cursor for next page
        cursor = None
        if len(results) == size and response['hits']['hits']:
            last_hit = response['hits']['hits'][-1]['sort']
            cursor = json.dumps(last_hit)

        # Process facets
        facet_results = None
        if facets and 'aggregations' in response:
            facet_results = []
            for field in facets:
                agg_data = response['aggregations'].get(field, {})
                buckets = agg_data.get('buckets', [])
                facet_results.append(FacetResult(field=field, buckets=buckets))

        return SearchResponse(
            results=results,
            total=response['hits']['total']['value'],
            took_ms=response['took'],
            facets=facet_results,
            aggregations=response.get('aggregations'),
            cursor=cursor
        )

    def _build_query_body(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        semantic_weight: float = 0.0,
        tenant_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Build Elasticsearch query body."""
        # Base query structure
        query_body = {
            "query": {
                "bool": {
                    "must": [],
                    "should": [],
                    "filter": [],
                    "must_not": []
                }
            },
            "sort": [
                {"_score": {"order": "desc"}},
                {"id": {"order": "asc"}}  # Deterministic sort for cursor pagination
            ]
        }

        # Add tenant filter if specified
        if tenant_id:
            query_body["query"]["bool"]["filter"].append({
                "term": {"tenant_id": tenant_id}
            })

        # Add text query if provided
        if query and query.strip():
            # Multi-match query for full-text search
            text_query = {
                "multi_match": {
                    "query": query,
                    "fields": ["title^5", "name^3", "description^2", "content_text^1", "tags^2"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            }

            if semantic_weight > 0:
                # Enhanced hybrid search with ANN optimization
                query_vector_placeholder = []  # Will be populated by semantic search

                # ANN search using HNSW index
                ann_query = {
                    "knn": {
                        "field": "embedding",
                        "query_vector": query_vector_placeholder,
                        "k": min(self.settings.search_knn_k, 1000),  # Configurable k
                        "num_candidates": min(self.settings.search_knn_k * 10, 10000),  # 10x k for recall
                        "filter": [
                            {"exists": {"field": "embedding"}}  # Only search documents with embeddings
                        ]
                    }
                }

                # Combine text and semantic search
                query_body["query"]["bool"]["should"] = [
                    {"multi_match": text_query["multi_match"]},
                    ann_query
                ]

                # Add rank_features for hybrid scoring with proper normalization
                query_body["query"]["bool"]["should"].append({
                    "rank_features": {
                        "boost": semantic_weight,
                        "features": [
                            {"field": "quality_score", "linear": {"scale": 1.0}},
                            {"field": "popularity_score", "linear": {"scale": 1.0}},
                            {"field": "recency_score", "linear": {"scale": 1.0}}
                        ]
                    }
                })
                query_body["query"]["bool"]["minimum_should_match"] = 1
            else:
                query_body["query"]["bool"]["must"].append(text_query)

        # Add filters
        if filters:
            for field, value in filters.items():
                if isinstance(value, list):
                    query_body["query"]["bool"]["filter"].append({
                        "terms": {field: value}
                    })
                elif isinstance(value, dict):
                    # Range queries
                    if 'gte' in value or 'lte' in value or 'gt' in value or 'lt' in value:
                        query_body["query"]["bool"]["filter"].append({
                            "range": {field: value}
                        })
                    else:
                        query_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })
                else:
                    query_body["query"]["bool"]["filter"].append({
                        "term": {field: value}
                    })

        return query_body

    def _build_facet_aggregations(self, facets: List[str], facet_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Build facet aggregations with enhanced configuration.

        Args:
            facets: List of field names to aggregate
            facet_config: Optional configuration for each facet

        Returns:
            Aggregations configuration dictionary
        """
        aggs = {}

        for field in facets:
            config = facet_config.get(field, {}) if facet_config else {}

            # Base aggregation
            agg_config = {
                "terms": {
                    "field": field,
                    "size": config.get("size", 100),
                    "min_doc_count": config.get("min_doc_count", 1),
                    "order": config.get("order", {"_count": "desc"})
                }
            }

            # Add sub-aggregations if specified
            if config.get("sub_aggs"):
                agg_config["terms"]["aggs"] = config["sub_aggs"]

            # Handle special facet types
            if config.get("type") == "date_histogram":
                agg_config = {
                    "date_histogram": {
                        "field": field,
                        "calendar_interval": config.get("interval", "month"),
                        "format": config.get("format", "yyyy-MM-dd")
                    }
                }
                if config.get("sub_aggs"):
                    agg_config["date_histogram"]["aggs"] = config["sub_aggs"]

            elif config.get("type") == "range":
                agg_config = {
                    "range": {
                        "field": field,
                        "ranges": config.get("ranges", [])
                    }
                }

            elif config.get("type") == "histogram":
                agg_config = {
                    "histogram": {
                        "field": field,
                        "interval": config.get("interval", 1)
                    }
                }

            aggs[field] = agg_config

        return aggs

    async def suggest(self, prefix: str, size: int = 10) -> List[Dict[str, Any]]:
        """Get search suggestions for autocomplete.

        Args:
            prefix: Search prefix
            size: Number of suggestions to return

        Returns:
            List of suggestion objects
        """
        if not self.settings.search_suggest_enabled:
            return []

        client = await self._get_client()

        # Use completion suggester on title field
        response = await client.search(
            index=self._index_name,
            body={
                "suggest": {
                    "title_suggest": {
                        "prefix": prefix,
                        "completion": {
                            "field": "title",
                            "size": size,
                            "skip_duplicates": True
                        }
                    }
                },
                "size": 0
            }
        )

        suggestions = []
        if 'suggest' in response and 'title_suggest' in response['suggest']:
            for option in response['suggest']['title_suggest'][0]['options']:
                suggestions.append({
                    'text': option['text'],
                    'score': option['_score']
                })

        return suggestions

    async def refresh_index(self) -> None:
        """Refresh the search index."""
        client = await self._get_client()
        await client.indices.refresh(index=self._index_name)
        logger.debug("Elasticsearch index refreshed")

    async def get_facet_options(
        self,
        field: str,
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        size: int = 50,
        tenant_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get facet options for a specific field with current query context.

        Args:
            field: Field name to get facet options for
            query: Current search query
            filters: Applied filters
            size: Maximum number of options to return
            tenant_id: Tenant ID for filtering

        Returns:
            List of facet options with counts
        """
        client = await self._get_client()

        # Build base query with current filters
        query_body = self._build_query_body(
            query=query or "",
            filters=filters,
            tenant_id=tenant_id
        )

        # Add aggregation for the specific field
        query_body['aggs'] = {
            field: {
                'terms': {
                    'field': field,
                    'size': size,
                    'order': {'_count': 'desc'}
                }
            }
        }

        # Add size=0 to only get aggregations
        query_body['size'] = 0

        response = await client.search(
            index=self._index_name,
            body=query_body
        )

        # Extract facet options
        options = []
        if field in response.get('aggregations', {}):
            buckets = response['aggregations'][field].get('buckets', [])
            for bucket in buckets:
                options.append({
                    'value': bucket['key'],
                    'count': bucket['doc_count'],
                    'label': str(bucket['key'])  # Can be enhanced with label mappings
                })

        return options

    async def get_advanced_filters(
        self,
        query: str,
        current_filters: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get advanced filtering options based on current query and filters.

        Args:
            query: Current search query
            current_filters: Currently applied filters
            tenant_id: Tenant ID for filtering

        Returns:
            Dictionary of field names to available filter options
        """
        # Common facet fields that are useful for filtering
        facet_fields = [
            'doc_type', 'asset_class', 'iso', 'location', 'tags',
            'owner_team', 'source_system'
        ]

        filter_options = {}

        for field in facet_fields:
            try:
                options = await self.get_facet_options(
                    field=field,
                    query=query,
                    filters=current_filters,
                    size=20,
                    tenant_id=tenant_id
                )
                if options:
                    filter_options[field] = options
            except Exception as e:
                logger.warning(f"Failed to get facet options for {field}: {e}")
                continue

        return filter_options

    async def search_with_post_filter(
        self,
        query: str,
        post_filters: Optional[Dict[str, Any]] = None,
        facets: Optional[List[str]] = None,
        facet_config: Optional[Dict[str, Any]] = None,
        size: int = 20,
        search_after: Optional[List[Any]] = None,
        semantic_weight: float = 0.0,
        tenant_id: Optional[str] = None,
        **kwargs
    ) -> SearchResponse:
        """Search with post-filters (applied after main query but before facets).

        This is useful for filtering facets while maintaining query relevance.

        Args:
            query: Search query string
            post_filters: Filters to apply after main query
            facets: Fields to aggregate facets for
            facet_config: Configuration for facet aggregations
            size: Number of results to return
            search_after: Cursor for pagination
            semantic_weight: Weight for semantic search
            tenant_id: Tenant ID for filtering
            **kwargs: Additional search parameters

        Returns:
            Complete search response
        """
        client = await self._get_client()

        # Build main query body
        query_body = self._build_query_body(
            query=query,
            filters=None,  # Don't apply filters in main query
            semantic_weight=semantic_weight,
            tenant_id=tenant_id,
            **kwargs
        )

        # Add post-filters if provided
        if post_filters:
            if 'post_filter' not in query_body:
                query_body['post_filter'] = {'bool': {'filter': []}}

            for field, value in post_filters.items():
                if isinstance(value, list):
                    query_body['post_filter']['bool']['filter'].append({
                        'terms': {field: value}
                    })
                else:
                    query_body['post_filter']['bool']['filter'].append({
                        'term': {field: value}
                    })

        # Add aggregations (these run before post-filters)
        if facets:
            query_body['aggs'] = self._build_facet_aggregations(facets, facet_config)

        # Add search_after for cursor pagination
        search_params = {
            'index': self._index_name,
            'body': query_body,
            'size': size,
            'track_total_hits': True
        }

        if search_after:
            search_params['search_after'] = search_after

        response = await client.search(**search_params)

        # Process results (same as regular search)
        results = []
        for i, hit in enumerate(response['hits']['hits']):
            source = hit['_source']
            document = SearchDocument(
                id=source['id'],
                doc_type=source['doc_type'],
                tenant_id=source['tenant_id'],
                title=source['title'],
                name=source.get('name'),
                description=source.get('description'),
                content_text=source.get('content_text'),
                tags=source.get('tags', []),
                domains=source.get('domains', []),
                created_at=datetime.fromisoformat(source['created_at']) if source.get('created_at') else None,
                updated_at=datetime.fromisoformat(source['updated_at']) if source.get('updated_at') else None,
                quality_score=source.get('quality_score'),
                popularity_score=source.get('popularity_score'),
                embedding=source.get('embedding'),
                metadata=source.get('metadata', {})
            )

            result = SearchResult(
                document=document,
                score=hit['_score'],
                highlights=hit.get('highlight'),
                rank=i + 1
            )
            results.append(result)

        # Build cursor for next page
        cursor = None
        if len(results) == size and response['hits']['hits']:
            last_hit = response['hits']['hits'][-1]['sort']
            cursor = json.dumps(last_hit)

        # Process facets
        facet_results = None
        if facets and 'aggregations' in response:
            facet_results = []
            for field in facets:
                agg_data = response['aggregations'].get(field, {})
                buckets = agg_data.get('buckets', [])
                facet_results.append(FacetResult(field=field, buckets=buckets))

        return SearchResponse(
            results=results,
            total=response['hits']['total']['value'],
            took_ms=response['took'],
            facets=facet_results,
            aggregations=response.get('aggregations'),
            cursor=cursor
        )
