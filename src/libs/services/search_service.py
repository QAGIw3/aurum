from __future__ import annotations

from typing import Any, Dict, List, Optional


class SearchService:
    """Facade for search engine initialization and common operations.

    This wraps the existing search engine and initialization routines so API
    routes can depend on a stable service from `libs/services`.
    """

    def __init__(self) -> None:
        # Lazy imports to avoid heavy deps at import time
        from aurum.api.v2.search import (
            initialize_advanced_search,
            initialize_ann_search,
            initialize_search_index_manager,
        )  # type: ignore
        from aurum.api.search.elasticsearch_engine import ElasticsearchEngine  # type: ignore
        from aurum.api.search.analytics import get_search_analytics_service  # type: ignore
        from aurum.api.search.index_lifecycle import get_search_index_manager  # type: ignore
        from aurum.api.search.ann_optimizer import get_ann_search_service  # type: ignore
        from aurum.api.search.circuit_breaker import get_search_resilience_manager  # type: ignore
        from aurum.api.search.advanced_filtering import get_advanced_search_service  # type: ignore

        self._ElasticsearchEngine = ElasticsearchEngine
        self._initialize_advanced_search = initialize_advanced_search
        self._initialize_ann_search = initialize_ann_search
        self._initialize_search_index_manager = initialize_search_index_manager
        self._get_search_analytics_service = get_search_analytics_service
        self._get_search_index_manager = get_search_index_manager
        self._get_ann_search_service = get_ann_search_service
        self._get_search_resilience_manager = get_search_resilience_manager
        self._get_advanced_search_service = get_advanced_search_service

    async def get_engine(self, settings) -> Any:
        engine = self._ElasticsearchEngine(settings)
        await engine.initialize()
        await self._initialize_advanced_search(engine, settings)
        await self._initialize_ann_search(engine, settings)
        await self._initialize_search_index_manager(engine, settings)
        return engine

    async def search(
        self,
        *,
        settings,
        q: str,
        filters: Optional[Dict[str, Any]] = None,
        facets: Optional[List[str]] = None,
        size: int = 20,
        search_after: Optional[Any] = None,
        semantic_weight: float = 0.0,
        tenant_id: Optional[str] = None,
    ) -> Any:
        engine = await self.get_engine(settings)
        return await engine.search(
            query=q,
            filters=filters,
            facets=facets,
            size=size,
            search_after=search_after,
            semantic_weight=semantic_weight,
            tenant_id=tenant_id,
        )

    async def suggest(self, *, settings, q: str, limit: int = 10) -> Any:
        engine = await self.get_engine(settings)
        return await engine.suggest(q, limit)

    async def facet_options(
        self,
        *,
        settings,
        field: str,
        q: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        size: int = 50,
        tenant_id: Optional[str] = None,
    ) -> Any:
        engine = await self.get_engine(settings)
        return await engine.get_facet_options(
            field=field, query=q, filters=filters, size=size, tenant_id=tenant_id
        )

    async def analytics_summary(self, *, settings) -> Dict[str, Any]:
        svc = self._get_search_analytics_service(settings)
        return svc.get_analytics_summary()

    def export_analytics(self, *, settings, fmt: str) -> Any:
        svc = self._get_search_analytics_service(settings)
        return svc.export_analytics_data(fmt)

    def record_result_click(
        self,
        *,
        settings,
        query: str,
        session_id: str,
        result_id: str,
        result_rank: int,
    ) -> None:
        svc = self._get_search_analytics_service(settings)
        svc.record_result_clicked(
            query=query,
            session_id=session_id,
            result_id=result_id,
            result_rank=result_rank,
        )

    def record_facet_applied(
        self,
        *,
        settings,
        query: str,
        session_id: str,
        facet_field: str,
        facet_value: str,
    ) -> None:
        svc = self._get_search_analytics_service(settings)
        svc.record_facet_applied(
            query=query,
            session_id=session_id,
            facet_field=facet_field,
            facet_value=facet_value,
        )

    async def index_maintenance(self, *, settings) -> Any:
        engine = await self.get_engine(settings)
        manager = self._get_search_index_manager(engine, settings)
        return await manager.perform_maintenance()

    async def create_backup(self, *, settings, backup_name: Optional[str]) -> bool:
        engine = await self.get_engine(settings)
        manager = self._get_search_index_manager(engine, settings)
        return await manager.create_backup(backup_name)

    async def health_summary(self, *, settings) -> Dict[str, Any]:
        engine = await self.get_engine(settings)
        elasticsearch_healthy = await engine.health_check()
        index_manager = self._get_search_index_manager(engine, settings)
        index_health = await index_manager.get_index_health()
        resilience = self._get_search_resilience_manager(settings)
        cb_status = resilience.get_health_status()
        return {
            "elasticsearch_healthy": elasticsearch_healthy,
            "index_health": index_health,
            "circuit_breakers": cb_status,
            "index_name": getattr(engine, "_index_name", None),
        }

    async def ann_tune_parameters(self, *, settings, test_queries: List[str], ground_truth: Dict[str, List[str]]) -> Any:
        engine = await self.get_engine(settings)
        ann = self._get_ann_search_service(engine, settings)
        return await ann.tune_ann_parameters(test_queries, ground_truth)

    async def ann_hybrid_search(
        self,
        *,
        settings,
        query: str,
        query_embedding: List[float],
        text_weight: float,
        semantic_weight: float,
        k: int,
        tenant_id: Optional[str],
    ) -> Dict[str, Any]:
        engine = await self.get_engine(settings)
        ann = self._get_ann_search_service(engine, settings)
        return await ann.hybrid_search_optimized(
            query=query,
            query_embedding=query_embedding,
            text_weight=text_weight,
            semantic_weight=semantic_weight,
            k=k,
            tenant_id=tenant_id,
        )

    async def suggest_filters(
        self,
        *,
        settings,
        query: str,
        current_filters: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        limit: int = 5,
    ) -> Any:
        engine = await self.get_engine(settings)
        advanced_service = self._get_advanced_search_service(engine, settings)
        return await advanced_service.get_filter_suggestions(
            query=query,
            current_filters=current_filters,
            tenant_id=tenant_id,
            limit=limit,
        )

    async def hierarchical_facets(
        self,
        *,
        settings,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        hierarchy_config: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
    ) -> Any:
        engine = await self.get_engine(settings)
        advanced_service = self._get_advanced_search_service(engine, settings)
        return await advanced_service.get_hierarchical_facets(
            query=query or "",
            filters=filters,
            hierarchy_config=hierarchy_config,
            tenant_id=tenant_id,
        )


