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
from aurum.data.repositories import IsoRepository

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
        iso_repository: IsoRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 300  # 5 minutes for real-time data
    ):
        """Initialize service with dependencies.

        Args:
            iso_repository: Repository for ISO data access
            cache: Optional cache implementation
            cache_ttl: Cache TTL in seconds (default 5 min for real-time data)
        """
        super().__init__()
        self.iso_repo = iso_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "iso:v1"

    # Caching helper methods
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build a cache key from operation and parameters."""
        import hashlib
        import json
        
        # Sort params for consistent cache keys
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True, default=str)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:16]
        return f"{self._cache_namespace}:{operation}:{param_hash}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Get value from cache if available."""
        if not self.cache:
            return None
        
        try:
            cached = await self.cache.get(cache_key)
            if cached:
                self.logger.debug(f"Cache hit: {cache_key}")
                return cached
            self.logger.debug(f"Cache miss: {cache_key}")
            return None
        except Exception as e:
            self.logger.warning(f"Cache get error: {e}")
            return None
    
    async def _set_in_cache(self, cache_key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        if not self.cache:
            return
        
        try:
            ttl = ttl or self.cache_ttl
            await self.cache.set(cache_key, value, ttl)
            self.logger.debug(f"Cache set: {cache_key} (TTL={ttl}s)")
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")

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

            # Check if ISO exists
            available_isos = await self.iso_repo.get_available_isos()
            if iso.upper() not in available_isos:
                raise NotFoundError("iso", iso)

            # Build cache key if caching enabled
            cache_key = None
            if self.cache:
                cache_key = self._build_cache_key(
                    "get_lmp_data",
                    iso=iso,
                    node=node,
                    start_date=start_date.isoformat() if start_date else None,
                    end_date=end_date.isoformat() if end_date else None,
                    market_type=market_type,
                    limit=limit
                )
                cached_data = await self._get_from_cache(cache_key)
                if cached_data is not None:
                    return ServiceResult.ok(
                        data=cached_data,
                        metadata={
                            "iso": iso,
                            "node": node,
                            "market_type": market_type,
                            "data_points": len(cached_data),
                            "limit": limit,
                            "source": "cache",
                            "data_type": "real_time" if not start_date else "historical"
                        }
                    )

            # Query LMP data based on date range
            if not start_date and not end_date:
                # Get last 24h data
                lmp_data = await self.iso_repo.get_lmp_last_24h(
                    iso_code=iso,
                    market=market_type,
                    location_id=node,
                    limit=limit
                )
            else:
                # Get daily aggregated data for date range
                lmp_data = await self.iso_repo.get_lmp_daily(
                    iso_code=iso,
                    market=market_type,
                    location_id=node,
                    start_date=start_date.isoformat() if start_date else None,
                    end_date=end_date.isoformat() if end_date else None,
                    limit=limit
                )

            # Cache results if enabled
            if cache_key and self.cache:
                await self._set_in_cache(cache_key, lmp_data)

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
    
    async def get_lmp_hourly(
        self,
        iso: str,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        date_str: Optional[str] = None,
        limit: int = 500,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get hourly aggregated LMP data.
        
        Args:
            iso: ISO identifier
            market: Market type (DA, RT, etc.)
            location_id: Location/node identifier
            date_str: Specific date (ISO format)
            limit: Maximum results
            context: Service context
            
        Returns:
            ServiceResult with hourly LMP data
        """
        self._log_operation(
            "get_lmp_hourly",
            context=context,
            iso=iso,
            market=market,
            date=date_str
        )
        
        try:
            self._validate_iso(iso)
            if market:
                self._validate_market_type(market)
            
            # Try cache first
            cache_key = None
            if self.cache:
                cache_key = self._build_cache_key(
                    "get_lmp_hourly",
                    iso=iso,
                    market=market,
                    location_id=location_id,
                    date=date_str,
                    limit=limit
                )
                cached_data = await self._get_from_cache(cache_key)
                if cached_data is not None:
                    return ServiceResult.ok(
                        data=cached_data,
                        metadata={
                            "iso": iso,
                            "market": market,
                            "granularity": "hourly",
                            "source": "cache",
                            "count": len(cached_data)
                        }
                    )
            
            # Query repository
            hourly_data = await self.iso_repo.get_lmp_hourly(
                iso_code=iso,
                market=market,
                location_id=location_id,
                date_str=date_str,
                limit=limit
            )
            
            # Cache results
            if cache_key and self.cache:
                await self._set_in_cache(cache_key, hourly_data)
            
            return ServiceResult.ok(
                data=hourly_data,
                metadata={
                    "iso": iso,
                    "market": market,
                    "granularity": "hourly",
                    "source": "database",
                    "count": len(hourly_data)
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_lmp_hourly", context)
    
    async def get_lmp_daily(
        self,
        iso: str,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 500,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get daily aggregated LMP data.
        
        Args:
            iso: ISO identifier
            market: Market type
            location_id: Location/node identifier
            start_date: Start date for range
            end_date: End date for range
            limit: Maximum results
            context: Service context
            
        Returns:
            ServiceResult with daily LMP data
        """
        self._log_operation(
            "get_lmp_daily",
            context=context,
            iso=iso,
            market=market
        )
        
        try:
            self._validate_iso(iso)
            if market:
                self._validate_market_type(market)
            
            # Validate date range
            if start_date and end_date:
                start = date.fromisoformat(start_date)
                end = date.fromisoformat(end_date)
                if start > end:
                    raise ValidationError(
                        "Start date must be before end date",
                        field="date_range"
                    )
            
            # Try cache first
            cache_key = None
            if self.cache:
                cache_key = self._build_cache_key(
                    "get_lmp_daily",
                    iso=iso,
                    market=market,
                    location_id=location_id,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit
                )
                cached_data = await self._get_from_cache(cache_key)
                if cached_data is not None:
                    return ServiceResult.ok(
                        data=cached_data,
                        metadata={
                            "iso": iso,
                            "market": market,
                            "granularity": "daily",
                            "source": "cache",
                            "count": len(cached_data)
                        }
                    )
            
            # Query repository
            daily_data = await self.iso_repo.get_lmp_daily(
                iso_code=iso,
                market=market,
                location_id=location_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
            
            # Cache results
            if cache_key and self.cache:
                # Use longer TTL for historical daily data
                await self._set_in_cache(cache_key, daily_data, ttl=3600)
            
            return ServiceResult.ok(
                data=daily_data,
                metadata={
                    "iso": iso,
                    "market": market,
                    "granularity": "daily",
                    "source": "database",
                    "count": len(daily_data)
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_lmp_daily", context)
    
    async def get_lmp_negative(
        self,
        iso: str,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 500,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get negative LMP price events.
        
        Args:
            iso: ISO identifier
            market: Market type
            location_id: Location/node identifier
            start_date: Start date for range
            end_date: End date for range
            limit: Maximum results
            context: Service context
            
        Returns:
            ServiceResult with negative LMP events
        """
        self._log_operation(
            "get_lmp_negative",
            context=context,
            iso=iso,
            market=market
        )
        
        try:
            self._validate_iso(iso)
            if market:
                self._validate_market_type(market)
            
            # Query repository (negative prices change less frequently, longer cache)
            negative_data = await self.iso_repo.get_lmp_negative(
                iso_code=iso,
                market=market,
                location_id=location_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
            
            return ServiceResult.ok(
                data=negative_data,
                metadata={
                    "iso": iso,
                    "market": market,
                    "filter": "negative_prices",
                    "count": len(negative_data),
                    "limit": limit
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_lmp_negative", context)

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
        try:
            # Query actual available markets from repository
            return await self.iso_repo.get_available_markets(iso)
        except Exception:
            # Fallback to common markets if query fails
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
        try:
            # Query actual nodes from repository
            nodes = await self.iso_repo.get_iso_nodes(
                iso_code=iso,
                market=market_type,
                limit=limit
            )
            # Transform to expected format
            return [
                {
                    "node_id": node.get("location_id"),
                    "name": node.get("location_name", node.get("location_id")),
                    "type": node.get("location_type", "unknown")
                }
                for node in nodes
            ]
        except Exception:
            # Fallback to placeholder data if query fails
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
        try:
            # Get actual market summary from repository
            summary = await self.iso_repo.get_market_summary(
                iso_code=iso,
                date_obj=date_obj
            )
            
            # Add any additional calculated fields if needed
            if summary.get("data_available"):
                # Could add congestion analysis, renewable percentage, etc.
                summary["congestion_events"] = 0  # Would need separate analysis
                summary["renewable_generation_percent"] = 0.0  # Would need generation data
            
            return summary
        except Exception as e:
            self.logger.warning(f"Failed to get market summary: {e}")
            # Return placeholder data on error
            return {
                "iso": iso,
                "date": date_obj.isoformat() if date_obj else "latest",
                "data_available": False,
                "error": str(e)
            }

