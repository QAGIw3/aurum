"""Advanced filtering and faceting capabilities for search.

Provides sophisticated filtering logic including nested filters,
range queries, boolean combinations, and hierarchical faceting.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from aurum.core.settings import get_settings
from aurum.core import AurumSettings
from .elasticsearch_engine import ElasticsearchEngine


logger = logging.getLogger(__name__)


class FilterType(Enum):
    """Types of filters supported."""
    TERM = "term"
    TERMS = "terms"
    RANGE = "range"
    EXISTS = "exists"
    PREFIX = "prefix"
    WILDCARD = "wildcard"
    REGEXP = "regexp"
    FUZZY = "fuzzy"
    MATCH = "match"
    MATCH_PHRASE = "match_phrase"
    QUERY_STRING = "query_string"


class FilterOperator(Enum):
    """Boolean operators for combining filters."""
    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class FilterCondition:
    """Represents a single filter condition."""
    field: str
    operator: FilterOperator
    filter_type: FilterType
    value: Any
    boost: Optional[float] = None
    nested_path: Optional[str] = None


@dataclass
class FilterGroup:
    """Represents a group of filters with boolean logic."""
    operator: FilterOperator = FilterOperator.AND
    conditions: List[Union[FilterCondition, 'FilterGroup']] = field(default_factory=list)
    nested_path: Optional[str] = None


@dataclass
class FacetConfiguration:
    """Configuration for facet aggregations."""
    field: str
    type: str = "terms"  # terms, date_histogram, range, histogram
    size: int = 100
    min_doc_count: int = 1
    order: Dict[str, str] = field(default_factory=lambda: {"_count": "desc"})
    interval: Optional[str] = None
    ranges: Optional[List[Dict[str, Any]]] = None
    format: Optional[str] = None
    sub_aggs: Optional[Dict[str, Any]] = None


class AdvancedFilterBuilder:
    """Builds complex Elasticsearch filter queries."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize filter builder.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

    def build_filter_query(
        self,
        filter_group: FilterGroup,
        context_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build Elasticsearch filter query from filter group.

        Args:
            filter_group: Group of filters to build
            context_query: Optional query context for relevance

        Returns:
            Elasticsearch filter query
        """
        if not filter_group.conditions:
            return {}

        # Build boolean query structure
        bool_query = {"bool": {"filter": []}}

        for condition in filter_group.conditions:
            if isinstance(condition, FilterCondition):
                filter_clause = self._build_condition_filter(condition)
                if filter_clause:
                    bool_query["bool"]["filter"].append(filter_clause)
            elif isinstance(condition, FilterGroup):
                nested_query = self.build_filter_query(condition, context_query)
                if nested_query:
                    bool_query["bool"]["filter"].append(nested_query)

        # Apply operator logic
        if filter_group.operator == FilterOperator.OR:
            bool_query["bool"]["minimum_should_match"] = 1
            bool_query["bool"]["should"] = bool_query["bool"].pop("filter")
        elif filter_group.operator == FilterOperator.NOT:
            bool_query["bool"]["must_not"] = bool_query["bool"].pop("filter")

        return bool_query

    def _build_condition_filter(self, condition: FilterCondition) -> Dict[str, Any]:
        """Build filter clause for a single condition."""
        base_filter = {}

        # Handle nested queries
        if condition.nested_path:
            base_filter = {
                "nested": {
                    "path": condition.nested_path,
                    "query": {}
                }
            }

        # Build the actual filter based on type
        if condition.filter_type == FilterType.TERM:
            filter_clause = {"term": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.TERMS:
            filter_clause = {"terms": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.RANGE:
            filter_clause = {"range": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.EXISTS:
            filter_clause = {"exists": {"field": condition.field}}
        elif condition.filter_type == FilterType.PREFIX:
            filter_clause = {"prefix": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.WILDCARD:
            filter_clause = {"wildcard": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.REGEXP:
            filter_clause = {"regexp": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.FUZZY:
            filter_clause = {"fuzzy": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.MATCH:
            filter_clause = {"match": {condition.field: {"query": condition.value}}}
        elif condition.filter_type == FilterType.MATCH_PHRASE:
            filter_clause = {"match_phrase": {condition.field: condition.value}}
        elif condition.filter_type == FilterType.QUERY_STRING:
            filter_clause = {"query_string": {"query": condition.value, "default_field": condition.field}}
        else:
            logger.warning(f"Unknown filter type: {condition.filter_type}")
            return {}

        # Add boost if specified
        if condition.boost is not None:
            if condition.filter_type in [FilterType.MATCH, FilterType.MATCH_PHRASE]:
                filter_clause[condition.filter_type.value][condition.field]["boost"] = condition.boost
            else:
                logger.warning(f"Boost not supported for filter type: {condition.filter_type}")

        # Add to nested query if needed
        if condition.nested_path:
            base_filter["nested"]["query"] = filter_clause
            return base_filter
        else:
            return filter_clause

    def build_facet_config(
        self,
        facets: List[Union[str, Dict[str, Any]]]
    ) -> Tuple[List[str], Dict[str, Any]]:
        """Build facet field list and configuration from mixed input.

        Args:
            facets: List of facet specifications (strings or dict configs)

        Returns:
            Tuple of (facet_fields, facet_config)
        """
        facet_fields = []
        facet_config = {}

        for facet in facets:
            if isinstance(facet, str):
                # Simple field name
                facet_fields.append(facet)
                facet_config[facet] = FacetConfiguration(field=facet)
            elif isinstance(facet, dict):
                # Configuration object
                field = facet.get('field')
                if field:
                    facet_fields.append(field)
                    facet_config[field] = FacetConfiguration(
                        field=field,
                        type=facet.get('type', 'terms'),
                        size=facet.get('size', 100),
                        min_doc_count=facet.get('min_doc_count', 1),
                        order=facet.get('order', {"_count": "desc"}),
                        interval=facet.get('interval'),
                        ranges=facet.get('ranges'),
                        format=facet.get('format'),
                        sub_aggs=facet.get('sub_aggs')
                    )

        return facet_fields, facet_config

    def parse_filter_string(
        self,
        filter_str: str,
        default_operator: FilterOperator = FilterOperator.AND
    ) -> FilterGroup:
        """Parse filter string into structured filter group.

        Supports syntax like:
        - field:value
        - field:(value1 OR value2)
        - field:[min TO max]
        - (filter1 AND filter2) OR filter3

        Args:
            filter_str: Filter string to parse
            default_operator: Default operator for combining conditions

        Returns:
            Parsed filter group
        """
        # Simple parsing - can be enhanced with more sophisticated parser
        group = FilterGroup(operator=default_operator)

        # Split by top-level operators
        conditions = self._split_filter_string(filter_str)

        for condition in conditions:
            if condition.strip().startswith('(') and condition.strip().endswith(')'):
                # Nested group
                inner_str = condition.strip()[1:-1]
                inner_group = self.parse_filter_string(inner_str, default_operator)
                group.conditions.append(inner_group)
            else:
                # Individual condition
                filter_condition = self._parse_single_condition(condition.strip())
                if filter_condition:
                    group.conditions.append(filter_condition)

        return group

    def _split_filter_string(self, filter_str: str) -> List[str]:
        """Split filter string by operators while respecting parentheses."""
        conditions = []
        current = ""
        paren_depth = 0
        i = 0

        while i < len(filter_str):
            char = filter_str[i]

            if char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char in [' ', '\t', '\n'] and paren_depth == 0:
                # Check if this is an operator
                if current.strip().upper() in ['AND', 'OR']:
                    if conditions and current.strip():
                        conditions[-1] += f" {char} {current.strip()}"
                    current = ""
                else:
                    if current.strip():
                        conditions.append(current.strip())
                    current = ""
            else:
                current += char

            i += 1

        if current.strip():
            conditions.append(current.strip())

        return conditions

    def _parse_single_condition(self, condition: str) -> Optional[FilterCondition]:
        """Parse a single filter condition."""
        # Simple parsing - field:value or field:(values)
        if ':' not in condition:
            return None

        field, value_part = condition.split(':', 1)

        field = field.strip()
        value_part = value_part.strip()

        # Determine filter type and value
        if value_part.startswith('[') and value_part.endswith(']'):
            # Range query
            range_str = value_part[1:-1]
            if ' TO ' in range_str.upper():
                min_val, max_val = range_str.split(' TO ', 1)
                return FilterCondition(
                    field=field,
                    operator=FilterOperator.AND,
                    filter_type=FilterType.RANGE,
                    value={
                        'gte': min_val.strip(),
                        'lte': max_val.strip()
                    }
                )
        elif value_part.startswith('(') and value_part.endswith(')'):
            # Multiple values
            values = [v.strip().strip('"\'') for v in value_part[1:-1].split(',')]
            return FilterCondition(
                field=field,
                operator=FilterOperator.OR,
                filter_type=FilterType.TERMS,
                value=values
            )
        else:
            # Single value
            clean_value = value_part.strip().strip('"\'')
            return FilterCondition(
                field=field,
                operator=FilterOperator.AND,
                filter_type=FilterType.TERM,
                value=clean_value
            )

        return None


class HierarchicalFacetBuilder:
    """Builds hierarchical facet structures for drill-down navigation."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize hierarchical facet builder.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

    def build_hierarchical_facets(
        self,
        engine: ElasticsearchEngine,
        base_query: str = "",
        base_filters: Optional[Dict[str, Any]] = None,
        hierarchy_config: Optional[Dict[str, List[str]]] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build hierarchical facet structure.

        Args:
            engine: Elasticsearch engine instance
            base_query: Base search query
            base_filters: Base filters applied
            hierarchy_config: Configuration defining hierarchy levels
            tenant_id: Tenant ID for filtering

        Returns:
            Hierarchical facet structure
        """
        if hierarchy_config is None:
            # Default hierarchy: doc_type -> asset_class -> iso
            hierarchy_config = {
                'doc_type': ['asset_class', 'iso', 'location'],
                'asset_class': ['iso', 'location'],
                'iso': ['location']
            }

        result = {}

        # Build facets level by level
        for root_field, child_fields in hierarchy_config.items():
            result[root_field] = self._build_facet_level(
                engine, root_field, base_query, base_filters, tenant_id
            )

            # Build child facets for each root value
            for root_value in result[root_field].get('values', []):
                root_value['children'] = {}

                for child_field in child_fields:
                    child_facet = self._build_facet_level(
                        engine, child_field, base_query, base_filters,
                        tenant_id, {root_field: [root_value['value']]}
                    )
                    root_value['children'][child_field] = child_facet

        return result

    def _build_facet_level(
        self,
        engine: ElasticsearchEngine,
        field: str,
        query: str,
        filters: Optional[Dict[str, Any]],
        tenant_id: Optional[str],
        additional_filters: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Build a single level of facet hierarchy."""
        # Combine filters
        all_filters = filters.copy() if filters else {}
        if additional_filters:
            for key, values in additional_filters.items():
                all_filters[key] = values

        # Get facet options
        options = engine.get_facet_options(
            field=field,
            query=query,
            filters=all_filters,
            size=50,
            tenant_id=tenant_id
        )

        return {
            'field': field,
            'values': [
                {
                    'value': opt['value'],
                    'count': opt['count'],
                    'label': opt['label']
                }
                for opt in options[:20]  # Limit to top 20
            ]
        }


class FilterSuggestionEngine:
    """Provides intelligent filter suggestions based on query context."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize filter suggestion engine.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

    async def suggest_filters(
        self,
        engine: ElasticsearchEngine,
        query: str,
        current_filters: Optional[Dict[str, Any]] = None,
        max_suggestions: int = 5,
        tenant_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Suggest relevant filters based on query context.

        Args:
            engine: Elasticsearch engine
            query: Current search query
            current_filters: Currently applied filters
            max_suggestions: Maximum number of suggestions
            tenant_id: Tenant ID for filtering

        Returns:
            List of filter suggestions
        """
        suggestions = []

        # Get advanced filter options
        filter_options = await engine.get_advanced_filters(
            query=query,
            current_filters=current_filters,
            tenant_id=tenant_id
        )

        # Score and rank suggestions
        scored_suggestions = []

        for field, options in filter_options.items():
            for option in options:
                # Calculate relevance score
                score = self._calculate_filter_relevance(
                    field, option, query, current_filters
                )

                scored_suggestions.append({
                    'field': field,
                    'value': option['value'],
                    'label': option['label'],
                    'count': option['count'],
                    'score': score,
                    'type': 'filter'
                })

        # Sort by score and return top suggestions
        scored_suggestions.sort(key=lambda x: x['score'], reverse=True)

        return scored_suggestions[:max_suggestions]

    def _calculate_filter_relevance(
        self,
        field: str,
        option: Dict[str, Any],
        query: str,
        current_filters: Optional[Dict[str, Any]]
    ) -> float:
        """Calculate relevance score for a filter suggestion."""
        score = 0.0

        # Base score from document count
        count_score = min(option['count'] / 1000.0, 1.0)  # Normalize to 0-1
        score += count_score * 0.3

        # Query relevance - check if field or value appears in query
        query_lower = query.lower()
        field_lower = field.lower()
        value_lower = str(option['value']).lower()

        if field_lower in query_lower or value_lower in query_lower:
            score += 0.4

        # Avoid suggesting filters already applied
        if current_filters and field in current_filters:
            current_values = current_filters[field]
            if isinstance(current_values, list) and option['value'] in current_values:
                score -= 0.8  # Heavy penalty for already applied filters
            elif option['value'] == current_values:
                score -= 0.8

        # Field type relevance (some fields are more useful for filtering)
        field_weights = {
            'doc_type': 0.3,
            'asset_class': 0.25,
            'iso': 0.25,
            'location': 0.2,
            'tags': 0.15,
            'owner_team': 0.1
        }

        score += field_weights.get(field, 0.1) * 0.2

        return max(0.0, min(1.0, score))


class AdvancedSearchService:
    """Service for advanced filtering and faceting."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize advanced search service.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.filter_builder = AdvancedFilterBuilder(settings)
        self.facet_builder = HierarchicalFacetBuilder(settings)
        self.suggestion_engine = FilterSuggestionEngine(settings)
        self._engine: Optional[ElasticsearchEngine] = None

    async def initialize(self, engine: ElasticsearchEngine):
        """Initialize with Elasticsearch engine.

        Args:
            engine: Elasticsearch engine instance
        """
        self._engine = engine

    async def build_enhanced_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        facets: Optional[List[Union[str, Dict[str, Any]]]] = None,
        semantic_weight: float = 0.0,
        tenant_id: Optional[str] = None,
        use_post_filters: bool = False,
        **kwargs
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Build enhanced search with advanced filtering and facets.

        Args:
            query: Search query
            filters: Applied filters
            facets: Facet specifications
            semantic_weight: Semantic search weight
            tenant_id: Tenant ID
            use_post_filters: Whether to use post-filters for faceting
            **kwargs: Additional search parameters

        Returns:
            Tuple of (search_dsl, facet_config)
        """
        if not self._engine:
            raise RuntimeError("Advanced search service not initialized")

        # Parse facet specifications
        facet_fields, facet_config = self.filter_builder.build_facet_config(facets or [])

        # Build base search query
        if use_post_filters and filters:
            # Split filters into main query filters and post-filters
            post_filters = {}
            query_filters = {}

            # Define which filters should be post-filters
            post_filter_fields = {'doc_type', 'tags', 'owner_team'}

            for field, value in filters.items():
                if field in post_filter_fields:
                    post_filters[field] = value
                else:
                    query_filters[field] = value

            # Use post-filter search method
            response = await self._engine.search_with_post_filter(
                query=query,
                post_filters=post_filters,
                facets=facet_fields,
                facet_config=facet_config,
                semantic_weight=semantic_weight,
                tenant_id=tenant_id,
                **kwargs
            )
        else:
            # Standard search
            response = await self._engine.search(
                query=query,
                filters=filters,
                facets=facet_fields,
                facet_config=facet_config,
                semantic_weight=semantic_weight,
                tenant_id=tenant_id,
                **kwargs
            )

        return response

    async def get_filter_suggestions(
        self,
        query: str,
        current_filters: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Get intelligent filter suggestions.

        Args:
            query: Current search query
            current_filters: Currently applied filters
            tenant_id: Tenant ID
            limit: Maximum suggestions to return

        Returns:
            List of filter suggestions
        """
        if not self._engine:
            return []

        return await self.suggestion_engine.suggest_filters(
            engine=self._engine,
            query=query,
            current_filters=current_filters,
            max_suggestions=limit,
            tenant_id=tenant_id
        )

    async def get_hierarchical_facets(
        self,
        query: str = "",
        filters: Optional[Dict[str, Any]] = None,
        hierarchy_config: Optional[Dict[str, List[str]]] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get hierarchical facet structure.

        Args:
            query: Base search query
            filters: Applied filters
            hierarchy_config: Custom hierarchy configuration
            tenant_id: Tenant ID

        Returns:
            Hierarchical facet structure
        """
        if not self._engine:
            return {}

        return self.facet_builder.build_hierarchical_facets(
            engine=self._engine,
            base_query=query,
            base_filters=filters,
            hierarchy_config=hierarchy_config,
            tenant_id=tenant_id
        )


# Global service instance
_advanced_search_service: Optional[AdvancedSearchService] = None


def get_advanced_search_service(
    engine: ElasticsearchEngine,
    settings: Optional[AurumSettings] = None
) -> AdvancedSearchService:
    """Get or create global advanced search service.

    Args:
        engine: Elasticsearch engine instance
        settings: Application settings

    Returns:
        Advanced search service
    """
    global _advanced_search_service
    if _advanced_search_service is None:
        _advanced_search_service = AdvancedSearchService(settings)
    return _advanced_search_service


async def initialize_advanced_search(
    engine: ElasticsearchEngine,
    settings: Optional[AurumSettings] = None
) -> None:
    """Initialize advanced search service globally.

    Args:
        engine: Elasticsearch engine instance
        settings: Application settings
    """
    service = get_advanced_search_service(engine, settings)
    await service.initialize(engine)
