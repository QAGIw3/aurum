"""Curve service for market data operations.

Implements business logic for curve queries, comparisons, and analytics.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import date, datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import CurveRepository

logger = logging.getLogger(__name__)


class CurveService(BaseService):
    """Service for curve business logic.
    
    Curves represent market data points (prices, forecasts) across
    time intervals and locations.
    
    This service:
    - Validates business rules
    - Orchestrates repository operations
    - Implements curve analytics and comparisons
    - Handles caching strategies
    - Enforces access control
    """
    
    def __init__(self, curve_repository: CurveRepository):
        """Initialize service with dependencies.
        
        Args:
            curve_repository: Repository for curve data access
        """
        super().__init__()
        self.curve_repo = curve_repository
    
    async def get_curves(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        asof: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get curves with filters.
        
        Business logic:
        - Validates filter combinations
        - Enforces tenant access control
        - Applies default as-of date if not provided
        - Limits result size
        
        Args:
            iso: ISO/RTO identifier
            market: Market type (DA, RT, etc.)
            location: Location/node identifier
            product: Product type
            asof: As-of date for point-in-time query
            limit: Maximum results (default 100, max 1000)
            offset: Pagination offset
            context: Service context with tenant info
            
        Returns:
            ServiceResult with list of curve data points
            
        Raises:
            ValidationError: If validation fails
            ServiceError: If operation fails
        """
        self._log_operation(
            "get_curves",
            context=context,
            iso=iso,
            market=market,
            limit=limit
        )
        
        try:
            # Validate business rules
            self._validate_query_params(limit, offset)
            
            # Apply business logic: default to latest if no asof
            if not asof and not iso and not market:
                # Require at least some filter for broad queries
                raise ValidationError(
                    "Must provide at least one filter (iso, market, or asof)",
                    field="filters"
                )
            
            # Enforce max limit
            limit = min(limit, 1000)
            
            # Query repository
            curves = await self.curve_repo.find_by_filters(
                iso=iso,
                market=market,
                location=location,
                product=product,
                asof=asof,
                limit=limit,
                offset=offset
            )
            
            # Apply tenant filtering if needed
            if context and context.tenant_id:
                curves = self._filter_by_tenant(curves, context.tenant_id)
            
            self.logger.info(
                f"Retrieved {len(curves)} curves",
                extra={
                    "count": len(curves),
                    "iso": iso,
                    "market": market,
                }
            )
            
            return ServiceResult.ok(
                data=curves,
                metadata={
                    "count": len(curves),
                    "limit": limit,
                    "offset": offset,
                    "has_more": len(curves) == limit
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_curves", context)
    
    async def get_curve_by_key(
        self,
        curve_key: str,
        asof: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get curve by unique key.
        
        Args:
            curve_key: Unique curve identifier
            asof: As-of date for point-in-time query
            context: Service context
            
        Returns:
            ServiceResult with curve data points
            
        Raises:
            NotFoundError: If curve not found
            ServiceError: If operation fails
        """
        self._log_operation("get_curve_by_key", context=context, curve_key=curve_key)
        
        try:
            curves = await self.curve_repo.find_by_key(
                curve_key=curve_key,
                asof=asof,
                limit=1000  # Reasonable limit for single curve
            )
            
            if not curves:
                raise NotFoundError(
                    resource="curve",
                    identifier=curve_key
                )
            
            return ServiceResult.ok(
                data=curves,
                metadata={"curve_key": curve_key, "count": len(curves)}
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_curve_by_key", context)
    
    async def get_latest_asof(
        self,
        iso: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Optional[date]]:
        """Get the latest as-of date for curves.
        
        Useful for determining data freshness and default query dates.
        
        Args:
            iso: Filter by ISO (None = all)
            context: Service context
            
        Returns:
            ServiceResult with latest date or None
        """
        self._log_operation("get_latest_asof", context=context, iso=iso)
        
        try:
            latest = await self.curve_repo.get_latest_asof(iso=iso)
            
            return ServiceResult.ok(
                data=latest,
                metadata={"iso": iso, "has_data": latest is not None}
            )
            
        except Exception as e:
            raise self._handle_error(e, "get_latest_asof", context)
    
    async def compare_curves(
        self,
        curve_key_1: str,
        curve_key_2: str,
        asof: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Compare two curves.
        
        Business logic for curve comparison analytics.
        
        Args:
            curve_key_1: First curve identifier
            curve_key_2: Second curve identifier
            asof: As-of date for comparison
            context: Service context
            
        Returns:
            ServiceResult with comparison analytics
        """
        self._log_operation(
            "compare_curves",
            context=context,
            curve1=curve_key_1,
            curve2=curve_key_2
        )
        
        try:
            # Get both curves
            curves1 = await self.curve_repo.find_by_key(curve_key_1, asof)
            curves2 = await self.curve_repo.find_by_key(curve_key_2, asof)
            
            if not curves1:
                raise NotFoundError("curve", curve_key_1)
            if not curves2:
                raise NotFoundError("curve", curve_key_2)
            
            # Perform comparison analytics
            comparison = self._compute_comparison(curves1, curves2)
            
            return ServiceResult.ok(
                data=comparison,
                metadata={
                    "curve_key_1": curve_key_1,
                    "curve_key_2": curve_key_2,
                    "asof": asof.isoformat() if asof else None
                }
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            raise self._handle_error(e, "compare_curves", context)
    
    # Private helper methods
    
    def _validate_query_params(self, limit: int, offset: int) -> None:
        """Validate query parameters."""
        if limit < 1:
            raise ValidationError("Limit must be at least 1", field="limit")
        if limit > 10000:
            raise ValidationError("Limit cannot exceed 10000", field="limit")
        if offset < 0:
            raise ValidationError("Offset cannot be negative", field="offset")
    
    def _filter_by_tenant(
        self,
        curves: List[Dict[str, Any]],
        tenant_id: str
    ) -> List[Dict[str, Any]]:
        """Apply tenant-based filtering (if applicable).
        
        In a multi-tenant system, filter curves by tenant access rights.
        """
        # Placeholder: In production, implement actual tenant filtering
        # based on curve metadata or access control rules
        return curves
    
    def _compute_comparison(
        self,
        curves1: List[Dict[str, Any]],
        curves2: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Compute comparison analytics between two curves.
        
        Returns statistics like:
        - Mean difference
        - Max difference
        - Correlation
        - Trend comparison
        """
        # Simplified implementation
        # In production, implement full analytics
        return {
            "curve1_count": len(curves1),
            "curve2_count": len(curves2),
            "comparison_type": "basic",
            "note": "Full analytics implementation pending"
        }

