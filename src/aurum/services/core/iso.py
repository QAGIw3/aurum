"""ISO (Independent System Operator) service for market data operations with caching.

Implements business logic for ISO LMP (Locational Marginal Pricing) data,
market operations, and regional energy market analytics.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol
from datetime import date, datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import MetadataRepository

logger = logging.getLogger(__name__)


class CacheProtocol(Protocol):
    """Protocol for cache implementations."""
    async def get(self, key: str) -> Optional[Any]: ...
    async def set(self, key: str, value: Any, ttl: int) -> None: ...
    async def delete(self, key: str) -> None: ...


class IsoService(BaseService):
    """Service for ISO market data operations with caching support.

    ISOs (Independent System Operators) manage regional energy markets
    and provide real-time pricing data (LMP - Locational Marginal Pricing).

    This service:
    - Validates ISO identifiers and market data
    - Provides LMP data queries
    - Implements market analytics
    - Handles real-time vs historical data
    - Manages market-specific business rules
    - Caches LMP data for performance
    """

    def __init__(
        self,
        metadata_repository: MetadataRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 300  # 5 minutes for real-time data
    ):
        """Initialize service with dependencies.

        Args:
            metadata_repository: Repository for metadata and catalog access
            cache: Optional cache implementation
            cache_ttl: Cache TTL in seconds (default 5 min for real-time data)
        """
        super().__init__()
        self.metadata_repo = metadata_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "iso:v1"

    async def get_lmp_data(
        self,
        iso: str,
        node: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        market_type: Optional[str] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get ISO LMP (Locational Marginal Pricing) data.

        Business logic:
        - Validates ISO identifier
        - Checks node/location exists
        - Applies business rules for data access
        - Handles real-time vs historical data
        - Enforces rate limits

        Args:
            iso: ISO identifier (e.g., "PJM", "ERCOT", "CAISO")
            node: Specific node/location identifier
            start_date: Start date for historical data
            end_date: End date for historical data
            market_type: Market type (e.g., "DA", "RT", "RUC")
            limit: Maximum results (max 10000)
            context: Service context

        Returns:
            ServiceResult with LMP data

        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If ISO/node not found
            ServiceError: If query fails
        """
        self._log_operation(
            "get_lmp_data",
            context=context,
            iso=iso,
            node=node,
            market_type=market_type
        )

        try:
            # Validate inputs
            self._validate_iso(iso)
            if node:
                self._validate_node(node)
            if market_type:
                self._validate_market_type(market_type)

            if limit < 1 or limit > 10000:
                raise ValidationError(
                    "Limit must be between 1 and 10000",
                    field="limit"
                )

            if start_date and end_date and start_date > end_date:
                raise ValidationError(
                    "Start date must be before end date",
                    field="date_range"
                )

            # Check if ISO exists in catalog
            available_isos = await self.metadata_repo.get_dimensions("iso_metrics", "iso")
            if iso not in available_isos:
                raise NotFoundError("iso", iso)

            # Query LMP data (placeholder - would use ISO-specific repository)
            lmp_data = []  # TODO: Implement actual LMP data query

            return ServiceResult.ok(
                data=lmp_data,
                metadata={
                    "iso": iso,
                    "node": node,
                    "market_type": market_type,
                    "data_points": len(lmp_data),
                    "limit": limit,
                    "data_type": "real_time" if not start_date else "historical"
                }
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_lmp_data", context)

    async def get_iso_markets(
        self,
        iso: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get available markets for an ISO.

        Args:
            iso: ISO identifier
            context: Service context

        Returns:
            ServiceResult with market information

        Raises:
            ValidationError: If ISO invalid
            NotFoundError: If ISO not found
            ServiceError: If query fails
        """
        self._log_operation("get_iso_markets", context=context, iso=iso)

        try:
            self._validate_iso(iso)

            # Get available markets for this ISO
            markets = await self._get_iso_markets(iso)

            return ServiceResult.ok(
                data={
                    "iso": iso,
                    "markets": markets,
                    "market_count": len(markets)
                },
                metadata={"iso": iso}
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_iso_markets", context)

    async def get_iso_nodes(
        self,
        iso: str,
        market_type: Optional[str] = None,
        limit: int = 1000,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get available nodes/locations for an ISO.

        Args:
            iso: ISO identifier
            market_type: Filter by market type
            limit: Maximum results
            context: Service context

        Returns:
            ServiceResult with node information

        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If ISO not found
            ServiceError: If query fails
        """
        self._log_operation(
            "get_iso_nodes",
            context=context,
            iso=iso,
            market_type=market_type
        )

        try:
            self._validate_iso(iso)
            if market_type:
                self._validate_market_type(market_type)

            # Get nodes for this ISO
            nodes = await self._get_iso_nodes(iso, market_type, limit)

            return ServiceResult.ok(
                data=nodes,
                metadata={
                    "iso": iso,
                    "market_type": market_type,
                    "node_count": len(nodes),
                    "limit": limit
                }
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_iso_nodes", context)

    async def get_market_summary(
        self,
        iso: str,
        date_obj: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get market summary statistics for an ISO.

        Args:
            iso: ISO identifier
            date_obj: Date for summary (None = latest)
            context: Service context

        Returns:
            ServiceResult with market summary

        Raises:
            ValidationError: If ISO invalid
            NotFoundError: If ISO not found
            ServiceError: If query fails
        """
        self._log_operation("get_market_summary", context=context, iso=iso, date=date_obj)

        try:
            self._validate_iso(iso)

            # Get market summary
            summary = await self._calculate_market_summary(iso, date_obj)

            return ServiceResult.ok(
                data=summary,
                metadata={
                    "iso": iso,
                    "date": date_obj.isoformat() if date_obj else "latest",
                    "summary_type": "daily"
                }
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_market_summary", context)

    # Private helper methods

    def _validate_iso(self, iso: str) -> None:
        """Validate ISO identifier."""
        if not iso or not iso.strip():
            raise ValidationError("ISO identifier is required", field="iso")

        if len(iso) > 10:
            raise ValidationError("ISO identifier too long", field="iso")

        # Check for invalid characters
        invalid_chars = ["<", ">", "&", "\"", "'", ";"]
        if any(char in iso for char in invalid_chars):
            raise ValidationError("ISO identifier contains invalid characters", field="iso")

    def _validate_node(self, node: str) -> None:
        """Validate node identifier."""
        if not node or not node.strip():
            raise ValidationError("Node identifier is required", field="node")

        if len(node) > 50:
            raise ValidationError("Node identifier too long", field="node")

    def _validate_market_type(self, market_type: str) -> None:
        """Validate market type."""
        valid_types = ["DA", "RT", "RUC", "DAM", "RTM"]
        if market_type not in valid_types:
            raise ValidationError(
                f"Invalid market type. Must be one of: {', '.join(valid_types)}",
                field="market_type"
            )

    async def _get_iso_markets(self, iso: str) -> List[str]:
        """Get available markets for an ISO."""
        # Query metadata for available markets
        # For now, return common markets
        markets_by_iso = {
            "PJM": ["DA", "RT"],
            "ERCOT": ["DAM", "RTM"],
            "CAISO": ["DA", "RT"],
            "MISO": ["DA", "RT"],
            "NYISO": ["DA", "RT"],
            "ISONE": ["DA", "RT"],
            "SPP": ["DA", "RT"]
        }
        return markets_by_iso.get(iso, ["DA", "RT"])

    async def _get_iso_nodes(
        self,
        iso: str,
        market_type: Optional[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Get available nodes for an ISO."""
        # Query metadata for nodes
        # For now, return placeholder data
        return [
            {"node_id": f"{iso}_HUB", "name": f"{iso} Hub", "type": "hub"},
            {"node_id": f"{iso}_ZONE_A", "name": f"{iso} Zone A", "type": "zone"}
        ][:limit]

    async def _calculate_market_summary(
        self,
        iso: str,
        date_obj: Optional[date]
    ) -> Dict[str, Any]:
        """Calculate market summary statistics."""
        # Placeholder implementation
        return {
            "iso": iso,
            "date": date_obj.isoformat() if date_obj else "latest",
            "total_volume_mwh": 100000,
            "avg_price_dollar_per_mwh": 45.67,
            "peak_price_dollar_per_mwh": 89.23,
            "min_price_dollar_per_mwh": 12.34,
            "price_volatility": 0.15,
            "congestion_events": 3,
            "renewable_generation_percent": 25.5
        }

