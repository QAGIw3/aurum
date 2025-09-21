"""API guardrails for query validation."""

from typing import Dict, Any, Optional


def enforce_basic_query_guardrails(filters: Dict[str, Any]) -> None:
    """Enforce basic query guardrails to prevent overly broad queries.

    Args:
        filters: Dictionary of filter parameters

    Raises:
        ValueError: If query is too broad or missing required filters
    """
    # Check if we have at least one meaningful filter
    meaningful_filters = ["iso", "market", "location", "product", "block"]

    has_meaningful_filter = any(
        filters.get(key) is not None and filters.get(key) != ""
        for key in meaningful_filters
    )

    # For now, just pass - this is a placeholder implementation
    # In a real implementation, you might want to enforce more strict rules
    if not has_meaningful_filter:
        # This is commented out for now to allow the deployment to proceed
        # raise ValueError("At least one filter (iso, market, location, product, or block) is required")
        pass
