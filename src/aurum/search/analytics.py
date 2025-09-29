"""Search analytics and user behavior tracking.

Provides event recording, aggregation, and insights for search behavior,
performance monitoring, and user experience optimization.
"""

import logging
import hashlib
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json

from aurum.core.settings import get_settings
from aurum.core import AurumSettings


logger = logging.getLogger(__name__)


class SearchEventType(Enum):
    """Types of search analytics events."""
    SEARCH_PERFORMED = "SEARCH_PERFORMED"
    RESULT_CLICKED = "RESULT_CLICKED"
    FACET_APPLIED = "FACET_APPLIED"
    SUGGESTION_USED = "SUGGESTION_USED"
    QUERY_REFORMULATED = "QUERY_REFORMULATED"
    ZERO_RESULTS = "ZERO_RESULTS"
    SLOW_QUERY = "SLOW_QUERY"
    ERROR_OCCURRED = "ERROR_OCCURRED"


class SearchSource(Enum):
    """Sources of search requests."""
    WEB_UI = "WEB_UI"
    API = "API"
    MOBILE_APP = "MOBILE_APP"
    INTEGRATION = "INTEGRATION"
    BATCH = "BATCH"


@dataclass
class SearchAnalyticsEvent:
    """Search analytics event data."""
    event_id: str
    timestamp: int
    event_type: SearchEventType
    session_id: str
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    query_hash: str
    query_text: str
    query_normalized: Optional[str] = None
    query_intent: Optional[str] = None
    query_enhanced: bool = False
    filters_applied: List[Dict[str, str]] = field(default_factory=list)
    facets_used: List[str] = field(default_factory=list)
    semantic_search_enabled: bool = False
    semantic_weight: Optional[float] = None
    result_count: int = 0
    total_results: int = 0
    response_time_ms: int = 0
    clicked_result_id: Optional[str] = None
    clicked_result_rank: Optional[int] = None
    facet_applied: Optional[Dict[str, str]] = None
    suggestion_used: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    request_id: Optional[str] = None
    search_source: SearchSource = SearchSource.API
    performance_metrics: Optional[Dict[str, int]] = None


class AnalyticsEventProducer:
    """Produces search analytics events for Kafka or storage."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize event producer.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

    def create_event(
        self,
        event_type: SearchEventType,
        query: str,
        session_id: str,
        **kwargs
    ) -> SearchAnalyticsEvent:
        """Create a search analytics event.

        Args:
            event_type: Type of event
            query: Search query text
            session_id: User session ID
            **kwargs: Additional event data

        Returns:
            Search analytics event
        """
        # Generate unique event ID
        event_id = f"{int(time.time() * 1000)}_{hashlib.md5(query.encode()).hexdigest()[:8]}"

        # Create query hash for deduplication
        query_hash = hashlib.sha256(query.encode()).hexdigest()

        return SearchAnalyticsEvent(
            event_id=event_id,
            timestamp=int(time.time() * 1000),
            event_type=event_type,
            session_id=session_id,
            query_hash=query_hash,
            query_text=query,
            **kwargs
        )

    def emit_event(self, event: SearchAnalyticsEvent) -> bool:
        """Emit analytics event to storage/Kafka.

        Args:
            event: Analytics event to emit

        Returns:
            True if successful
        """
        try:
            if self.settings.search_analytics_enabled:
                # In production, this would emit to Kafka or store in ClickHouse
                # For now, just log the event
                logger.info(f"Search analytics event: {event.event_type.value} - {event.query_text}")

                # TODO: Implement Kafka emission
                # kafka_producer.produce(
                #     topic='search.analytics.v1',
                #     key=event.event_id,
                #     value=event.to_dict()
                # )

                return True
            else:
                return False

        except Exception as e:
            logger.error(f"Failed to emit analytics event: {e}")
            return False

    def emit_search_performed(
        self,
        query: str,
        session_id: str,
        tenant_id: Optional[str] = None,
        result_count: int = 0,
        total_results: int = 0,
        response_time_ms: int = 0,
        semantic_enabled: bool = False,
        **kwargs
    ) -> bool:
        """Emit search performed event."""
        event = self.create_event(
            SearchEventType.SEARCH_PERFORMED,
            query=query,
            session_id=session_id,
            tenant_id=tenant_id,
            result_count=result_count,
            total_results=total_results,
            response_time_ms=response_time_ms,
            semantic_search_enabled=semantic_enabled,
            **kwargs
        )
        return self.emit_event(event)

    def emit_result_clicked(
        self,
        query: str,
        session_id: str,
        result_id: str,
        result_rank: int,
        **kwargs
    ) -> bool:
        """Emit result clicked event."""
        event = self.create_event(
            SearchEventType.RESULT_CLICKED,
            query=query,
            session_id=session_id,
            clicked_result_id=result_id,
            clicked_result_rank=result_rank,
            **kwargs
        )
        return self.emit_event(event)

    def emit_facet_applied(
        self,
        query: str,
        session_id: str,
        facet_field: str,
        facet_value: str,
        **kwargs
    ) -> bool:
        """Emit facet applied event."""
        event = self.create_event(
            SearchEventType.FACET_APPLIED,
            query=query,
            session_id=session_id,
            facet_applied={"field": facet_field, "value": facet_value},
            **kwargs
        )
        return self.emit_event(event)

    def emit_suggestion_used(
        self,
        query: str,
        session_id: str,
        suggestion: str,
        **kwargs
    ) -> bool:
        """Emit suggestion used event."""
        event = self.create_event(
            SearchEventType.SUGGESTION_USED,
            query=query,
            session_id=session_id,
            suggestion_used=suggestion,
            **kwargs
        )
        return self.emit_event(event)


class SearchAnalyticsAggregator:
    """Aggregates search analytics for insights and reporting."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize analytics aggregator.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.events: List[SearchAnalyticsEvent] = []

    def add_event(self, event: SearchAnalyticsEvent):
        """Add event to aggregation buffer.

        Args:
            event: Analytics event to add
        """
        self.events.append(event)

        # Keep only recent events (last 24 hours)
        cutoff_time = int(time.time() * 1000) - (24 * 60 * 60 * 1000)
        self.events = [e for e in self.events if e.timestamp >= cutoff_time]

    def get_query_popularity(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get most popular queries.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of popular queries with counts
        """
        query_counts = {}
        for event in self.events:
            if event.event_type == SearchEventType.SEARCH_PERFORMED:
                query_counts[event.query_text] = query_counts.get(event.query_text, 0) + 1

        # Sort by count and return top queries
        sorted_queries = sorted(query_counts.items(), key=lambda x: x[1], reverse=True)
        return [
            {"query": query, "count": count}
            for query, count in sorted_queries[:limit]
        ]

    def get_click_through_rate(self) -> Dict[str, float]:
        """Calculate click-through rate by query.

        Returns:
            Dictionary of query -> CTR mappings
        """
        query_clicks = {}
        query_searches = {}

        for event in self.events:
            if event.event_type == SearchEventType.SEARCH_PERFORMED:
                query_searches[event.query_text] = query_searches.get(event.query_text, 0) + 1
            elif event.event_type == SearchEventType.RESULT_CLICKED:
                query_clicks[event.query_text] = query_clicks.get(event.query_text, 0) + 1

        ctr_data = {}
        for query in set(query_searches.keys()) | set(query_clicks.keys()):
            searches = query_searches.get(query, 0)
            clicks = query_clicks.get(query, 0)
            ctr = clicks / searches if searches > 0 else 0.0
            ctr_data[query] = ctr

        return ctr_data

    def get_zero_result_queries(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get queries that returned zero results.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of zero-result queries with counts
        """
        zero_queries = {}
        for event in self.events:
            if (event.event_type == SearchEventType.SEARCH_PERFORMED and
                event.total_results == 0):
                zero_queries[event.query_text] = zero_queries.get(event.query_text, 0) + 1

        # Sort by count
        sorted_queries = sorted(zero_queries.items(), key=lambda x: x[1], reverse=True)
        return [
            {"query": query, "count": count}
            for query, count in sorted_queries[:limit]
        ]

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics summary.

        Returns:
            Dictionary with performance statistics
        """
        if not self.events:
            return {}

        # Calculate response time statistics
        response_times = [
            e.response_time_ms for e in self.events
            if e.event_type == SearchEventType.SEARCH_PERFORMED
        ]

        if not response_times:
            return {}

        avg_response_time = sum(response_times) / len(response_times)
        max_response_time = max(response_times)
        min_response_time = min(response_times)

        # Calculate percentiles
        sorted_times = sorted(response_times)
        p50 = sorted_times[int(len(sorted_times) * 0.5)]
        p95 = sorted_times[int(len(sorted_times) * 0.95)]
        p99 = sorted_times[int(len(sorted_times) * 0.99)]

        return {
            "total_searches": len([e for e in self.events if e.event_type == SearchEventType.SEARCH_PERFORMED]),
            "avg_response_time_ms": avg_response_time,
            "min_response_time_ms": min_response_time,
            "max_response_time_ms": max_response_time,
            "p50_response_time_ms": p50,
            "p95_response_time_ms": p95,
            "p99_response_time_ms": p99,
            "slow_queries": len([t for t in response_times if t > 1000]),  # > 1 second
            "error_count": len([e for e in self.events if e.event_type == SearchEventType.ERROR_OCCURRED])
        }

    def get_facet_usage(self) -> Dict[str, int]:
        """Get facet usage statistics.

        Returns:
            Dictionary of facet field -> usage count
        """
        facet_usage = {}
        for event in self.events:
            if event.event_type == SearchEventType.FACET_APPLIED and event.facet_applied:
                field = event.facet_applied["field"]
                facet_usage[field] = facet_usage.get(field, 0) + 1

        return facet_usage

    def get_semantic_search_usage(self) -> Dict[str, Any]:
        """Get semantic search usage statistics.

        Returns:
            Dictionary with semantic search statistics
        """
        semantic_events = [
            e for e in self.events
            if e.event_type == SearchEventType.SEARCH_PERFORMED and e.semantic_search_enabled
        ]

        if not semantic_events:
            return {"enabled": False}

        avg_weight = sum(e.semantic_weight or 0.3 for e in semantic_events) / len(semantic_events)

        return {
            "enabled": True,
            "total_searches": len(semantic_events),
            "avg_semantic_weight": avg_weight,
            "percentage_of_total": len(semantic_events) / len([
                e for e in self.events if e.event_type == SearchEventType.SEARCH_PERFORMED
            ]) * 100
        }


class SearchAnalyticsService:
    """Main service for search analytics and insights."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize analytics service.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.event_producer = AnalyticsEventProducer(settings)
        self.aggregator = SearchAnalyticsAggregator(settings)
        self._initialized = False

    def initialize(self):
        """Initialize analytics service."""
        self._initialized = True
        logger.info("Search analytics service initialized")

    def record_search_performed(
        self,
        query: str,
        session_id: str,
        result_count: int,
        total_results: int,
        response_time_ms: int,
        tenant_id: Optional[str] = None,
        semantic_enabled: bool = False,
        semantic_weight: Optional[float] = None,
        query_normalized: Optional[str] = None,
        query_intent: Optional[str] = None,
        filters_applied: Optional[List[Dict[str, str]]] = None,
        facets_used: Optional[List[str]] = None,
        **kwargs
    ) -> bool:
        """Record search performed event.

        Args:
            query: Search query text
            session_id: User session ID
            result_count: Number of results returned
            total_results: Total matching documents
            response_time_ms: Response time in milliseconds
            tenant_id: Tenant ID
            semantic_enabled: Whether semantic search was used
            semantic_weight: Semantic search weight
            query_normalized: Normalized query text
            query_intent: Detected query intent
            filters_applied: Applied filters
            facets_used: Used facet fields
            **kwargs: Additional event data

        Returns:
            True if event was recorded successfully
        """
        event = self.event_producer.create_event(
            SearchEventType.SEARCH_PERFORMED,
            query=query,
            session_id=session_id,
            tenant_id=tenant_id,
            result_count=result_count,
            total_results=total_results,
            response_time_ms=response_time_ms,
            semantic_search_enabled=semantic_enabled,
            semantic_weight=semantic_weight,
            query_normalized=query_normalized,
            query_intent=query_intent,
            filters_applied=filters_applied or [],
            facets_used=facets_used or [],
            **kwargs
        )

        # Add to aggregator
        self.aggregator.add_event(event)

        # Emit event
        return self.event_producer.emit_event(event)

    def record_result_clicked(
        self,
        query: str,
        session_id: str,
        result_id: str,
        result_rank: int,
        **kwargs
    ) -> bool:
        """Record result clicked event."""
        event = self.event_producer.create_event(
            SearchEventType.RESULT_CLICKED,
            query=query,
            session_id=session_id,
            clicked_result_id=result_id,
            clicked_result_rank=result_rank,
            **kwargs
        )

        self.aggregator.add_event(event)
        return self.event_producer.emit_event(event)

    def record_facet_applied(
        self,
        query: str,
        session_id: str,
        facet_field: str,
        facet_value: str,
        **kwargs
    ) -> bool:
        """Record facet applied event."""
        event = self.event_producer.create_event(
            SearchEventType.FACET_APPLIED,
            query=query,
            session_id=session_id,
            facet_applied={"field": facet_field, "value": facet_value},
            **kwargs
        )

        self.aggregator.add_event(event)
        return self.event_producer.emit_event(event)

    def record_suggestion_used(
        self,
        query: str,
        session_id: str,
        suggestion: str,
        **kwargs
    ) -> bool:
        """Record suggestion used event."""
        event = self.event_producer.create_event(
            SearchEventType.SUGGESTION_USED,
            query=query,
            session_id=session_id,
            suggestion_used=suggestion,
            **kwargs
        )

        self.aggregator.add_event(event)
        return self.event_producer.emit_event(event)

    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get comprehensive analytics summary.

        Returns:
            Dictionary with all analytics metrics
        """
        return {
            "query_popularity": self.aggregator.get_query_popularity(),
            "click_through_rate": self.aggregator.get_click_through_rate(),
            "zero_result_queries": self.aggregator.get_zero_result_queries(),
            "performance_metrics": self.aggregator.get_performance_metrics(),
            "facet_usage": self.aggregator.get_facet_usage(),
            "semantic_search_usage": self.aggregator.get_semantic_search_usage()
        }

    def export_analytics_data(self, format: str = "json") -> str:
        """Export analytics data for external analysis.

        Args:
            format: Export format (json, csv)

        Returns:
            Exported analytics data as string
        """
        if format.lower() == "json":
            return json.dumps({
                "events": [event.__dict__ for event in self.aggregator.events],
                "summary": self.get_analytics_summary()
            }, indent=2, default=str)
        else:
            # CSV export would be implemented here
            return "CSV export not yet implemented"


# Global service instance
_analytics_service: Optional[SearchAnalyticsService] = None


def get_search_analytics_service(settings: Optional[AurumSettings] = None) -> SearchAnalyticsService:
    """Get or create global search analytics service.

    Args:
        settings: Application settings

    Returns:
        Search analytics service instance
    """
    global _analytics_service
    if _analytics_service is None:
        _analytics_service = SearchAnalyticsService(settings)
        _analytics_service.initialize()
    return _analytics_service


def record_search_analytics(
    event_type: SearchEventType,
    query: str,
    session_id: str,
    settings: Optional[AurumSettings] = None,
    **kwargs
) -> bool:
    """Record search analytics event using global service.

    Args:
        event_type: Type of analytics event
        query: Search query text
        session_id: User session ID
        settings: Application settings
        **kwargs: Additional event data

    Returns:
        True if event was recorded successfully
    """
    service = get_search_analytics_service(settings)

    if event_type == SearchEventType.SEARCH_PERFORMED:
        return service.record_search_performed(query, session_id, **kwargs)
    elif event_type == SearchEventType.RESULT_CLICKED:
        return service.record_result_clicked(query, session_id, **kwargs)
    elif event_type == SearchEventType.FACET_APPLIED:
        return service.record_facet_applied(query, session_id, **kwargs)
    elif event_type == SearchEventType.SUGGESTION_USED:
        return service.record_suggestion_used(query, session_id, **kwargs)
    else:
        logger.warning(f"Unknown event type: {event_type}")
        return False
