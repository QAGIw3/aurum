"""v2 Search API with advanced discovery capabilities.

This module provides the v2 implementation of the search API with:
- Full-text and semantic search
- Natural language query processing
- Faceted search and filtering
- Auto-complete and suggestions
- Search analytics and behavior tracking
- Hybrid BM25 + vector search
- Cursor-based pagination

Notes:
- Base path: `/v2/*` (see app wiring in src/aurum/api/app.py)
- Feature-flagged: requires SEARCH_ENABLED flag
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional, Any
import json

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends, Body
from pydantic import BaseModel, Field, validator

from ..http import respond_with_etag
from ..deps import get_settings, get_cache_manager
from libs.services.search_service import SearchService
from ..features import is_feature_enabled, require_feature
from aurum.core import AurumSettings
from ..cache.consolidated_manager import get_unified_cache_manager
from ..cache.enhanced_cache_manager import CacheNamespace
from ...telemetry.context import get_request_id
from .pagination import (
    build_next_cursor,
    build_pagination_envelope,
    resolve_pagination,
)

# Import search services
from ...search.elasticsearch_engine import ElasticsearchEngine, SearchResponse, SearchResult, FacetResult
from ...search.semantic_search import get_semantic_search_service, is_semantic_search_enabled
from ...search.query_processor import parse_query, build_search_dsl_from_query
from ...search.advanced_filtering import get_advanced_search_service, initialize_advanced_search
from ...search.ranking_engine import get_ranking_service, rank_search_results
from ...search.ann_optimizer import get_ann_search_service, initialize_ann_search
from ...search.analytics import get_search_analytics_service, record_search_analytics, SearchEventType
from ...search.index_lifecycle import get_search_index_manager, initialize_search_index_manager


# Pydantic models for request/response
class SearchQueryRequest(BaseModel):
    """Request body for advanced search queries."""
    q: str = Field(..., description="Search query string")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters to apply")
    facets: Optional[List[str]] = Field(None, description="Fields to aggregate facets for")
    semantic: Optional[Dict[str, Any]] = Field(None, description="Semantic search parameters")
    page: Optional[Dict[str, Any]] = Field(None, description="Pagination parameters")

    @validator('semantic')
    def validate_semantic(cls, v):
        if v is not None:
            if 'enabled' in v and not isinstance(v['enabled'], bool):
                raise ValueError('semantic.enabled must be boolean')
            if 'weight' in v:
                weight = v['weight']
                if not isinstance(weight, (int, float)) or not (0 <= weight <= 1):
                    raise ValueError('semantic.weight must be a number between 0 and 1')
        return v

    @validator('page')
    def validate_page(cls, v):
        if v is not None:
            if 'limit' in v:
                limit = v['limit']
                if not isinstance(limit, int) or not (1 <= limit <= 1000):
                    raise ValueError('page.limit must be an integer between 1 and 1000')
            if 'cursor' in v and v['cursor'] is not None:
                if not isinstance(v['cursor'], str):
                    raise ValueError('page.cursor must be a string')
        return v


class SearchQueryResponse(BaseModel):
    """Response for search queries."""
    results: List[Dict[str, Any]] = Field(..., description="Search results")
    total: int = Field(..., description="Total number of matching documents")
    took_ms: int = Field(..., description="Query execution time in milliseconds")
    facets: Optional[List[Dict[str, Any]]] = Field(None, description="Facet aggregations")
    aggregations: Optional[Dict[str, Any]] = Field(None, description="Additional aggregations")
    cursor: Optional[str] = Field(None, description="Cursor for next page")
    query_analysis: Optional[Dict[str, Any]] = Field(None, description="Query parsing analysis")
    semantic_used: bool = Field(False, description="Whether semantic search was used")
    _links: Optional[Dict[str, str]] = Field(None, description="Pagination links")


class SuggestResponse(BaseModel):
    """Response for search suggestions."""
    suggestions: List[Dict[str, Any]] = Field(..., description="Search suggestions")
    took_ms: int = Field(..., description="Suggestion generation time")


class ExplainResponse(BaseModel):
    """Response for query explanation."""
    query: str = Field(..., description="Original query")
    parsed_query: Dict[str, Any] = Field(..., description="Parsed query analysis")
    dsl: Dict[str, Any] = Field(..., description="Generated Elasticsearch DSL")
    explanation: List[str] = Field(..., description="Human-readable explanation")
    suggestions: List[str] = Field(..., description="Query improvement suggestions")


# Router setup
router = APIRouter(prefix="/search", tags=["search"])


async def get_search_engine() -> Any:
    """Provide search engine via SearchService."""
    settings = get_settings()
    return await SearchService().get_engine(settings)


async def get_search_service() -> ElasticsearchEngine:
    """Legacy alias for get_search_engine."""
    return await get_search_engine()


@router.get("/", response_model=SearchQueryResponse)
async def search(
    request: Request,
    response: Response,
    q: str = Query(..., description="Search query string"),
    filters: Optional[str] = Query(None, description="JSON-encoded filters"),
    facets: Optional[str] = Query(None, description="Comma-separated facet fields"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    semantic: bool = Query(False, description="Enable semantic search"),
    semantic_weight: float = Query(0.3, ge=0.0, le=1.0, description="Semantic search weight"),
    tenant_id: Optional[str] = Query(None, description="Tenant ID for filtering"),
    settings: AurumSettings = Depends(get_settings),
):
    """Perform search with optional semantic enhancement.

    Examples:
        GET /v2/search/?q=power%20demand%20texas&limit=10
        GET /v2/search/?q=wind%20forecast&semantic=true&semantic_weight=0.5
        GET /v2/search/?q=gas%20prices&filters={"iso": ["ERCOT"]}&facets=asset_class,iso
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    start_time = time.perf_counter()
    request_id = get_request_id() or "unknown"

    try:
        # Parse filters if provided
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid filters JSON")

        # Parse facets
        facet_fields = None
        if facets:
            facet_fields = [f.strip() for f in facets.split(",")]

        # Get tenant ID from request context if not provided
        if not tenant_id:
            tenant_id = getattr(request.state, "tenant_id", None)

        # Determine if semantic search should be used
        use_semantic = semantic and is_semantic_search_enabled(settings)

        # Parse query for additional insights
        parsed_query = parse_query(q, settings)

        # Perform search via service facade
        search_response = await SearchService().search(
            settings=settings,
            q=q,
            filters=parsed_filters,
            facets=facet_fields,
            size=limit,
            search_after=json.loads(cursor) if cursor else None,
            semantic_weight=semantic_weight if use_semantic else 0.0,
            tenant_id=tenant_id,
        )

        # Enhance with semantic search if enabled
        semantic_service = get_semantic_search_service(settings)
        if use_semantic:
            search_response = await semantic_service.enhance_search_response(
                search_response,
                q,
                semantic_weight
            )

        # Apply advanced ranking
        ranking_service = get_ranking_service(settings)
        query_terms = parsed_query.normalized_query.split() if parsed_query.normalized_query else []

        # Get semantic scores for ranking
        semantic_scores = {}
        if use_semantic and search_response.results:
            for result in search_response.results:
                if result.document.embedding:
                    semantic_score = await semantic_service.embed_query(q)
                    if semantic_score:
                        # Calculate semantic similarity
                        import numpy as np
                        doc_embedding = result.document.embedding
                        if doc_embedding:
                            similarity = np.dot(semantic_score, doc_embedding) / (
                                np.linalg.norm(semantic_score) * np.linalg.norm(doc_embedding)
                            )
                            semantic_scores[result.document.id] = similarity

        # Apply advanced ranking
        search_response = ranking_service.rank_search_results(
            search_response,
            query_terms,
            semantic_scores,
            use_ltr=False  # Can be enabled via configuration
        )

        # Convert to response format
        response_data = SearchQueryResponse(
            results=[{
                "id": result.document.id,
                "doc_type": result.document.doc_type,
                "title": result.document.title,
                "name": result.document.name,
                "description": result.document.description,
                "tags": result.document.tags,
                "domains": result.document.domains,
                "created_at": result.document.created_at.isoformat() if result.document.created_at else None,
                "quality_score": result.document.quality_score,
                "popularity_score": result.document.popularity_score,
                "score": result.score,
                "rank": result.rank,
                "highlights": result.highlights
            } for result in search_response.results],
            total=search_response.total,
            took_ms=search_response.took_ms,
            facets=[{
                "field": facet.field,
                "buckets": facet.buckets
            } for facet in (search_response.facets or [])],
            aggregations=search_response.aggregations,
            cursor=search_response.cursor,
            query_analysis={
                "original_query": parsed_query.original_query,
                "normalized_query": parsed_query.normalized_query,
                "entities": parsed_query.entities,
                "filters": parsed_query.filters,
                "intent": parsed_query.intent,
                "confidence": parsed_query.confidence,
                "explanation": parsed_query.explanation
            } if settings.debug else None,
            semantic_used=use_semantic
        )

        # Add pagination links if cursor available
        if search_response.cursor:
            response_data._links = {
                "next": f"/v2/search/?q={q}&limit={limit}&cursor={search_response.cursor}"
            }

        # Record analytics
        if settings.search_analytics_enabled:
            analytics_service = get_search_analytics_service(settings)
            analytics_service.record_search_performed(
                query=q,
                session_id=request_id,
                result_count=len(search_response.results),
                total_results=search_response.total,
                response_time_ms=search_response.took_ms,
                tenant_id=tenant_id,
                semantic_enabled=use_semantic,
                semantic_weight=semantic_weight,
                query_normalized=parsed_query.normalized_query,
                query_intent=parsed_query.intent,
                facets_used=facet_fields
            )

        return response_data

    except Exception as e:
        logger.exception(f"Search failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Search operation failed: {str(e)}"
        )


@router.post("/query", response_model=SearchQueryResponse)
async def advanced_search(
    request: Request,
    response: Response,
    search_request: SearchQueryRequest,
    settings: AurumSettings = Depends(get_settings),
):
    """Advanced search with full DSL support.

    Examples:
        POST /v2/search/query
        {
            "q": "power demand texas",
            "filters": {"iso": ["ERCOT"], "asset_class": ["power"]},
            "facets": ["asset_class", "iso"],
            "semantic": {"enabled": true, "weight": 0.4},
            "page": {"limit": 20}
        }
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    # Extract parameters from request body
    q = search_request.q
    filters = search_request.filters
    facets = search_request.facets
    semantic_config = search_request.semantic
    page_config = search_request.page

    # Extract pagination
    limit = page_config.get('limit', 20) if page_config else 20
    cursor = page_config.get('cursor') if page_config else None

    # Extract semantic settings
    semantic_enabled = semantic_config.get('enabled', False) if semantic_config else False
    semantic_weight = semantic_config.get('weight', 0.3) if semantic_config else 0.3

    # Get tenant ID
    tenant_id = getattr(request.state, "tenant_id", None)

    # Use GET endpoint logic
    return await search(
        request=request,
        response=response,
        q=q,
        filters=json.dumps(filters) if filters else None,
        facets=",".join(facets) if facets else None,
        limit=limit,
        cursor=cursor,
        semantic=semantic_enabled,
        semantic_weight=semantic_weight,
        tenant_id=tenant_id,
        settings=settings
    )


@router.get("/suggest", response_model=SuggestResponse)
async def suggest(
    request: Request,
    q: str = Query(..., description="Search prefix for suggestions"),
    limit: int = Query(10, ge=1, le=50, description="Maximum suggestions"),
    settings: AurumSettings = Depends(get_settings),
):
    """Get search suggestions for autocomplete.

    Examples:
        GET /v2/search/suggest?q=pow&limit=5
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_SUGGESTIONS_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search suggestions are currently disabled"
        )

    start_time = time.perf_counter()

    try:
        suggestions = await SearchService().suggest(settings=settings, q=q, limit=limit)

        return SuggestResponse(
            suggestions=suggestions,
            took_ms=int((time.perf_counter() - start_time) * 1000)
        )

    except Exception as e:
        logger.exception(f"Suggestion generation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Suggestion generation failed: {str(e)}"
        )


@router.get("/explain", response_model=ExplainResponse)
async def explain_query(
    request: Request,
    q: str = Query(..., description="Query to explain"),
    settings: AurumSettings = Depends(get_settings),
):
    """Explain how a query would be processed and executed.

    Examples:
        GET /v2/search/explain?q=power%20demand%20in%20texas
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        # Parse query
        parsed_query = parse_query(q, settings)

        # Build DSL
        dsl = build_search_dsl_from_query(q, settings)

        # Get suggestions
        processor = get_query_processor(settings)
        suggestions = processor.suggest_query_improvements(parsed_query)

        return ExplainResponse(
            query=q,
            parsed_query={
                "original_query": parsed_query.original_query,
                "normalized_query": parsed_query.normalized_query,
                "entities": parsed_query.entities,
                "filters": parsed_query.filters,
                "date_ranges": parsed_query.date_ranges,
                "operators": parsed_query.operators,
                "intent": parsed_query.intent,
                "confidence": parsed_query.confidence,
                "explanation": parsed_query.explanation
            },
            dsl=dsl,
            explanation=parsed_query.explanation,
            suggestions=suggestions
        )

    except Exception as e:
        logger.exception(f"Query explanation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Query explanation failed: {str(e)}"
        )


@router.get("/facets")
async def get_facet_options(
    request: Request,
    field: str = Query(..., description="Field to get facet options for"),
    q: Optional[str] = Query(None, description="Current search query"),
    filters: Optional[str] = Query(None, description="JSON-encoded current filters"),
    size: int = Query(50, ge=1, le=200, description="Maximum facet options"),
    tenant_id: Optional[str] = Query(None, description="Tenant ID for filtering"),
    settings: AurumSettings = Depends(get_settings),
):
    """Get facet options for a specific field with current query context.

    Examples:
        GET /v2/search/facets?field=asset_class&q=power&size=20
        GET /v2/search/facets?field=iso&filters={"doc_type": ["dataset"]}&size=10
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        # Parse filters if provided
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid filters JSON")

        # Get tenant ID from request context if not provided
        if not tenant_id:
            tenant_id = getattr(request.state, "tenant_id", None)

        # Get search engine
        options = await SearchService().facet_options(
            settings=settings,
            field=field,
            q=q,
            filters=parsed_filters,
            size=size,
            tenant_id=tenant_id,
        )

        return {
            "field": field,
            "options": options,
            "total_options": len(options)
        }

    except Exception as e:
        logger.exception(f"Facet options retrieval failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Facet options retrieval failed: {str(e)}"
        )


@router.get("/filters/suggest")
async def suggest_filters(
    request: Request,
    q: str = Query(..., description="Current search query"),
    filters: Optional[str] = Query(None, description="JSON-encoded current filters"),
    limit: int = Query(5, ge=1, le=20, description="Maximum suggestions"),
    tenant_id: Optional[str] = Query(None, description="Tenant ID for filtering"),
    settings: AurumSettings = Depends(get_settings),
):
    """Get intelligent filter suggestions based on query context.

    Examples:
        GET /v2/search/filters/suggest?q=power%20demand&limit=5
        GET /v2/search/filters/suggest?q=wind&filters={"iso": ["ERCOT"]}&limit=3
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        # Parse filters if provided
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid filters JSON")

        # Get tenant ID from request context if not provided
        if not tenant_id:
            tenant_id = getattr(request.state, "tenant_id", None)

        suggestions = await SearchService().suggest_filters(
            settings=settings,
            query=q,
            current_filters=parsed_filters,
            tenant_id=tenant_id,
            limit=limit,
        )

        return {
            "query": q,
            "suggestions": suggestions,
            "total_suggestions": len(suggestions)
        }

    except Exception as e:
        logger.exception(f"Filter suggestion failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Filter suggestion failed: {str(e)}"
        )


@router.get("/facets/hierarchical")
async def get_hierarchical_facets(
    request: Request,
    q: Optional[str] = Query(None, description="Base search query"),
    filters: Optional[str] = Query(None, description="JSON-encoded current filters"),
    hierarchy: Optional[str] = Query(None, description="JSON-encoded hierarchy config"),
    tenant_id: Optional[str] = Query(None, description="Tenant ID for filtering"),
    settings: AurumSettings = Depends(get_settings),
):
    """Get hierarchical facet structure for drill-down navigation.

    Examples:
        GET /v2/search/facets/hierarchical?q=energy
        GET /v2/search/facets/hierarchical?hierarchy={"doc_type": ["asset_class", "iso"]}
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        # Parse filters if provided
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid filters JSON")

        # Parse hierarchy config if provided
        hierarchy_config = None
        if hierarchy:
            try:
                hierarchy_config = json.loads(hierarchy)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid hierarchy JSON")

        # Get tenant ID from request context if not provided
        if not tenant_id:
            tenant_id = getattr(request.state, "tenant_id", None)

        facets = await SearchService().hierarchical_facets(
            settings=settings,
            query=q or "",
            filters=parsed_filters,
            hierarchy_config=hierarchy_config,
            tenant_id=tenant_id,
        )

        return {
            "query": q,
            "filters": parsed_filters,
            "facets": facets
        }

    except Exception as e:
        logger.exception(f"Hierarchical facets retrieval failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Hierarchical facets retrieval failed: {str(e)}"
        )


@router.post("/rank/tune")
async def tune_ranking(
    request: Request,
    evaluation_queries: List[Dict[str, Any]] = Body(..., description="Evaluation queries with expected results"),
    settings: AurumSettings = Depends(get_settings),
):
    """Tune ranking parameters using evaluation queries.

    Examples:
        POST /v2/search/rank/tune
        [
            {
                "query": "power demand texas",
                "relevant_docs": ["doc1", "doc2", "doc3"],
                "irrelevant_docs": ["doc4", "doc5"]
            }
        ]
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        # Get ranking service
        ranking_service = get_ranking_service(settings)

        # Tune parameters
        optimized_config = ranking_service.tune_ranking_parameters(evaluation_queries)

        return {
            "message": "Ranking parameters tuned successfully",
            "optimized_config": {
                "quality_weight": optimized_config.quality_weight,
                "popularity_weight": optimized_config.popularity_weight,
                "recency_weight": optimized_config.recency_weight,
                "semantic_weight": optimized_config.semantic_weight
            },
            "evaluation_queries_processed": len(evaluation_queries)
        }

    except Exception as e:
        logger.exception(f"Ranking tuning failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Ranking tuning failed: {str(e)}"
        )


@router.post("/rank/evaluate")
async def evaluate_ranking(
    request: Request,
    query: str = Body(..., description="Query to evaluate"),
    expected_docs: List[str] = Body(..., description="Expected relevant document IDs"),
    settings: AurumSettings = Depends(get_settings),
):
    """Evaluate ranking for a specific query.

    Examples:
        POST /v2/search/rank/evaluate
        {
            "query": "power demand in texas",
            "expected_docs": ["doc1", "doc2", "doc3"]
        }
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        # Get search engine
        engine = await get_search_engine()

        # Perform search
        search_response = await engine.search(query=query, size=50)

        # Calculate relevance breakdown for expected docs
        ranking_service = get_ranking_service(settings)
        query_terms = query.split()

        evaluation_results = []
        for result in search_response.results:
            is_expected = result.document.id in expected_docs
            relevance_score = ranking_service.calculate_relevance_breakdown(
                result.document, query_terms
            )

            evaluation_results.append({
                "document_id": result.document.id,
                "rank": result.rank,
                "score": result.score,
                "is_expected": is_expected,
                "relevance_breakdown": {
                    "total_score": relevance_score.total_score,
                    "components": [
                        {
                            "component": comp.component.value,
                            "score": comp.score,
                            "weight": comp.weight
                        }
                        for comp in relevance_score.components
                    ]
                }
            })

        # Calculate evaluation metrics
        expected_found = sum(1 for r in evaluation_results if r["is_expected"])
        precision_at_10 = sum(1 for r in evaluation_results[:10] if r["is_expected"]) / min(10, len(evaluation_results))
        recall = expected_found / len(expected_docs) if expected_docs else 0.0

        return {
            "query": query,
            "expected_docs": expected_docs,
            "results_evaluated": len(evaluation_results),
            "expected_found": expected_found,
            "precision_at_10": precision_at_10,
            "recall": recall,
            "results": evaluation_results
        }

    except Exception as e:
        logger.exception(f"Ranking evaluation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Ranking evaluation failed: {str(e)}"
        )


@router.post("/ann/tune")
async def tune_ann_parameters(
    request: Request,
    test_queries: List[str] = Body(..., description="Test queries for parameter tuning"),
    ground_truth: Dict[str, List[str]] = Body(..., description="Ground truth relevance judgments"),
    settings: AurumSettings = Depends(get_settings),
):
    """Tune ANN index parameters for optimal performance.

    Examples:
        POST /v2/search/ann/tune
        {
            "test_queries": ["power demand", "wind forecast", "gas prices"],
            "ground_truth": {
                "power demand": ["doc1", "doc2"],
                "wind forecast": ["doc3", "doc4"]
            }
        }
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_SEMANTIC_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Semantic search is currently disabled"
        )

    try:
        optimized_config = await SearchService().ann_tune_parameters(settings=settings, test_queries=test_queries, ground_truth=ground_truth)

        return {
            "message": "ANN parameters tuned successfully",
            "optimized_config": {
                "m": optimized_config.m,
                "ef_construction": optimized_config.ef_construction,
                "ef_search": optimized_config.ef_search,
                "max_connections": optimized_config.max_connections,
                "similarity_threshold": optimized_config.similarity_threshold
            },
            "test_queries_processed": len(test_queries)
        }

    except Exception as e:
        logger.exception(f"ANN parameter tuning failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"ANN parameter tuning failed: {str(e)}"
        )


@router.post("/ann/search")
async def ann_search(
    request: Request,
    query: str = Body(..., description="Search query"),
    query_embedding: List[float] = Body(..., description="Query embedding vector"),
    text_weight: float = Body(0.7, description="Weight for text search component"),
    semantic_weight: float = Body(0.3, description="Weight for semantic search component"),
    k: int = Body(100, description="Number of ANN results to retrieve"),
    tenant_id: Optional[str] = Body(None, description="Tenant ID for filtering"),
    settings: AurumSettings = Depends(get_settings),
):
    """Perform optimized ANN search with hybrid text + semantic.

    Examples:
        POST /v2/search/ann/search
        {
            "query": "power demand in texas",
            "query_embedding": [0.1, 0.2, 0.3, ...],
            "text_weight": 0.7,
            "semantic_weight": 0.3,
            "k": 100
        }
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_SEMANTIC_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Semantic search is currently disabled"
        )

    try:
        result = await SearchService().ann_hybrid_search(
            settings=settings,
            query=query,
            query_embedding=query_embedding,
            text_weight=text_weight,
            semantic_weight=semantic_weight,
            k=k,
            tenant_id=tenant_id,
        )

        return {
            "query": query,
            "results": [
                {
                    "id": hit["_source"]["id"],
                    "doc_type": hit["_source"]["doc_type"],
                    "title": hit["_source"]["title"],
                    "score": hit["_score"],
                    "rank": i + 1
                }
                for i, hit in enumerate(result["results"])
            ],
            "total": result["total"],
            "took_ms": result["took_ms"],
            "query_optimization": result["query_optimization"]
        }

    except Exception as e:
        logger.exception(f"ANN search failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"ANN search failed: {str(e)}"
        )


@router.get("/analytics")
async def get_search_analytics(
    request: Request,
    settings: AurumSettings = Depends(get_settings),
):
    """Get comprehensive search analytics summary.

    Examples:
        GET /v2/search/analytics
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ANALYTICS_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search analytics are currently disabled"
        )

    try:
        summary = await SearchService().analytics_summary(settings=settings)

        return {
            "analytics_enabled": True,
            "summary": summary,
            "last_updated": int(time.time() * 1000)
        }

    except Exception as e:
        logger.exception(f"Analytics retrieval failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Analytics retrieval failed: {str(e)}"
        )


@router.post("/analytics/click")
async def record_result_click(
    request: Request,
    query: str = Body(..., description="Search query"),
    result_id: str = Body(..., description="Clicked result ID"),
    result_rank: int = Body(..., description="Rank of clicked result"),
    settings: AurumSettings = Depends(get_settings),
):
    """Record that a search result was clicked.

    Examples:
        POST /v2/search/analytics/click
        {
            "query": "power demand texas",
            "result_id": "doc123",
            "result_rank": 1
        }
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ANALYTICS_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search analytics are currently disabled"
        )

    try:
        request_id = get_request_id() or "unknown"

        # Record click event
        SearchService().record_result_click(
            settings=settings,
            query=query,
            session_id=request_id,
            result_id=result_id,
            result_rank=result_rank,
        )

        return {"message": "Click recorded successfully"}

    except Exception as e:
        logger.exception(f"Click recording failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Click recording failed: {str(e)}"
        )


@router.post("/analytics/facet")
async def record_facet_applied(
    request: Request,
    query: str = Body(..., description="Search query"),
    facet_field: str = Body(..., description="Facet field that was applied"),
    facet_value: str = Body(..., description="Facet value that was applied"),
    settings: AurumSettings = Depends(get_settings),
):
    """Record that a facet filter was applied.

    Examples:
        POST /v2/search/analytics/facet
        {
            "query": "power demand texas",
            "facet_field": "asset_class",
            "facet_value": "power"
        }
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ANALYTICS_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search analytics are currently disabled"
        )

    try:
        request_id = get_request_id() or "unknown"

        # Record facet applied event
        SearchService().record_facet_applied(
            settings=settings,
            query=query,
            session_id=request_id,
            facet_field=facet_field,
            facet_value=facet_value,
        )

        return {"message": "Facet application recorded successfully"}

    except Exception as e:
        logger.exception(f"Facet recording failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Facet recording failed: {str(e)}"
        )


@router.get("/analytics/export")
async def export_analytics(
    request: Request,
    format: str = Query("json", description="Export format (json, csv)"),
    settings: AurumSettings = Depends(get_settings),
):
    """Export search analytics data.

    Examples:
        GET /v2/search/analytics/export?format=json
        GET /v2/search/analytics/export?format=csv
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ANALYTICS_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search analytics are currently disabled"
        )

    try:
        data = SearchService().export_analytics(settings=settings, fmt=format)

        if format.lower() == "json":
            return Response(
                content=data,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=search_analytics.json"}
            )
        else:
            return Response(
                content=data,
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=search_analytics.csv"}
            )

    except Exception as e:
        logger.exception(f"Analytics export failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Analytics export failed: {str(e)}"
        )


@router.post("/maintenance")
async def perform_index_maintenance(
    request: Request,
    settings: AurumSettings = Depends(get_settings),
):
    """Perform comprehensive index maintenance operations.

    Examples:
        POST /v2/search/maintenance
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        results = await SearchService().index_maintenance(settings=settings)

        return {
            "message": "Index maintenance completed",
            "results": results,
            "timestamp": int(time.time() * 1000)
        }

    except Exception as e:
        logger.exception(f"Index maintenance failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Index maintenance failed: {str(e)}"
        )


@router.post("/backup")
async def create_index_backup(
    request: Request,
    backup_name: Optional[str] = Body(None, description="Optional backup name"),
    settings: AurumSettings = Depends(get_settings),
):
    """Create a backup of all search indices.

    Examples:
        POST /v2/search/backup
        {
            "backup_name": "pre-upgrade-backup"
        }
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        success = await SearchService().create_backup(settings=settings, backup_name=backup_name)

        if success:
            return {
                "message": "Index backup created successfully",
                "backup_name": backup_name or f"search-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                "timestamp": int(time.time() * 1000)
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create backup")

    except Exception as e:
        logger.exception(f"Index backup failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Index backup failed: {str(e)}"
        )


@router.get("/health")
async def get_search_health(
    request: Request,
    settings: AurumSettings = Depends(get_settings),
):
    """Get comprehensive search health information.

    Examples:
        GET /v2/search/health
    """
    # Check feature flag
    if not is_feature_enabled(settings.FEATURE_FLAGS["SEARCH_ENABLED"]):
        raise HTTPException(
            status_code=503,
            detail="Search functionality is currently disabled"
        )

    try:
        summary = await SearchService().health_summary(settings=settings)
        return {
            "status": "healthy" if summary["elasticsearch_healthy"] else "unhealthy",
            "elasticsearch": {
                "healthy": summary["elasticsearch_healthy"],
                "index_name": summary.get("index_name"),
            },
            "indices": summary["index_health"],
            "circuit_breakers": summary["circuit_breakers"],
            "timestamp": int(time.time() * 1000)
        }

    except Exception as e:
        logger.exception(f"Health check failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )


# Import logger
import logging
logger = logging.getLogger(__name__)
