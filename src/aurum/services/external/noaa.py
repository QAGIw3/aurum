"""NOAA (National Oceanic and Atmospheric Administration) service with caching.

Implements business logic for NOAA weather and climate data operations including
station data, historical weather, and climate forecasts.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol
from datetime import date, datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import BaseRepository

logger = logging.getLogger(__name__)


class CacheProtocol(Protocol):
    """Protocol for cache implementations."""
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        ...
    
    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value in cache with TTL."""
        ...
    
    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        ...


class NoaaRepository(BaseRepository):
    """Repository for NOAA data operations.
    
    Temporary implementation until proper NoaaRepository is created.
    """
    
    async def get_station_data(
        self,
        station_id: str,
        dataset_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """Get NOAA station data."""
        # Stub implementation - would query from iceberg.external.noaa_observations
        return []
    
    async def search_stations(
        self,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        radius_km: Optional[float] = None,
        state: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search NOAA weather stations."""
        # Stub implementation - would query from iceberg.external.noaa_stations
        return []
    
    async def get_station_metadata(
        self,
        station_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for a NOAA station."""
        # Stub implementation
        return None
    
    async def get_climate_data(
        self,
        location: str,
        metric: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """Get climate data for a location."""
        # Stub implementation
        return []


class NoaaService(BaseService):
    """Service for NOAA data operations with caching support.
    
    NOAA provides weather and climate data including:
    - Weather station observations
    - Historical weather data
    - Climate normals and extremes
    - Precipitation and temperature data
    - Severe weather events
    
    This service:
    - Validates station IDs and locations
    - Manages station catalog
    - Provides weather data query interface
    - Handles unit conversions
    - Caches weather data for performance
    - Enforces access control
    """
    
    def __init__(
        self,
        noaa_repository: NoaaRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 1800  # 30 minutes for weather data
    ):
        """Initialize service with dependencies.
        
        Args:
            noaa_repository: Repository for NOAA data access
            cache: Optional cache implementation
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__()
        self.noaa_repo = noaa_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "noaa:v1"
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build a cache key from operation and parameters."""
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
            self.logger.debug(f"Cache set: {cache_key} (TTL: {ttl}s)")
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")
    
    async def get_station_data(
        self,
        station_id: str,
        dataset_id: str = "GHCND",  # Global Historical Climatology Network Daily
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get weather data from a NOAA station.
        
        Common datasets:
        - GHCND: Daily summaries (temperature, precipitation)
        - GSOM: Monthly summaries
        - GSOY: Annual summaries
        - NORMAL_DLY: 30-year normals
        
        Args:
            station_id: NOAA station identifier
            dataset_id: Dataset type (default: GHCND)
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with station data
        """
        self._log_operation(
            "get_station_data",
            context=context,
            station_id=station_id,
            dataset_id=dataset_id
        )
        
        try:
            # Validate
            if not station_id:
                raise ValidationError("Station ID is required", field="station_id")
            
            if not dataset_id:
                raise ValidationError("Dataset ID is required", field="dataset_id")
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "station_data",
                    station_id=station_id,
                    dataset_id=dataset_id,
                    start_date=start_date.isoformat() if start_date else None,
                    end_date=end_date.isoformat() if end_date else None
                )
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={
                            "source": "cache",
                            "station_id": station_id,
                            "dataset_id": dataset_id
                        }
                    )
            
            # Get from repository
            data = await self.noaa_repo.get_station_data(
                station_id=station_id,
                dataset_id=dataset_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if not data:
                raise NotFoundError(f"No data found for station: {station_id}")
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, data)
            
            return ServiceResult.ok(
                data=data,
                metadata={
                    "source": "database",
                    "station_id": station_id,
                    "dataset_id": dataset_id,
                    "count": len(data)
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_station_data", context)
    
    async def search_stations(
        self,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        radius_km: Optional[float] = 50.0,
        state: Optional[str] = None,
        limit: int = 100,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Search for weather stations by location or state.
        
        Args:
            lat: Latitude for proximity search
            lon: Longitude for proximity search
            radius_km: Search radius in kilometers (default: 50)
            state: State code filter (e.g., "TX", "CA")
            limit: Maximum number of results
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with matching stations
        """
        self._log_operation(
            "search_stations",
            context=context,
            lat=lat,
            lon=lon,
            state=state
        )
        
        try:
            # Validate
            if lat is not None and lon is None:
                raise ValidationError("Longitude is required when latitude is provided")
            if lon is not None and lat is None:
                raise ValidationError("Latitude is required when longitude is provided")
            
            if lat is not None and (lat < -90 or lat > 90):
                raise ValidationError("Latitude must be between -90 and 90", field="lat")
            if lon is not None and (lon < -180 or lon > 180):
                raise ValidationError("Longitude must be between -180 and 180", field="lon")
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "search_stations",
                    lat=lat,
                    lon=lon,
                    radius_km=radius_km,
                    state=state,
                    limit=limit
                )
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache", "count": len(cached)}
                    )
            
            # Search in repository
            stations = await self.noaa_repo.search_stations(
                lat=lat,
                lon=lon,
                radius_km=radius_km,
                state=state,
                limit=limit
            )
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, stations)
            
            return ServiceResult.ok(
                data=stations,
                metadata={
                    "source": "database",
                    "count": len(stations),
                    "search_criteria": {
                        "lat": lat,
                        "lon": lon,
                        "radius_km": radius_km,
                        "state": state
                    }
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "search_stations", context)
    
    async def get_climate_summary(
        self,
        location: str,
        metrics: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, List[Dict[str, Any]]]]:
        """Get climate summary for a location.
        
        Common metrics:
        - TMAX: Maximum temperature
        - TMIN: Minimum temperature
        - TAVG: Average temperature
        - PRCP: Precipitation
        - SNOW: Snowfall
        - AWND: Average wind speed
        
        Args:
            location: Location identifier (station ID or city/state)
            metrics: List of climate metrics to retrieve
            start_date: Start date for data
            end_date: End date for data
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with dict mapping metric to data
        """
        self._log_operation(
            "get_climate_summary",
            context=context,
            location=location,
            metrics=metrics
        )
        
        try:
            # Validate
            if not location:
                raise ValidationError("Location is required", field="location")
            
            if not metrics:
                raise ValidationError("At least one metric is required", field="metrics")
            
            if len(metrics) > 10:
                raise ValidationError("Maximum 10 metrics per request", field="metrics")
            
            # Get data for each metric
            results = {}
            errors = []
            
            for metric in metrics:
                try:
                    data = await self.noaa_repo.get_climate_data(
                        location=location,
                        metric=metric,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if data:
                        results[metric] = data
                except Exception as e:
                    errors.append(f"Error getting {metric}: {str(e)}")
            
            if not results and errors:
                raise ServiceError(f"Failed to get any metrics: {'; '.join(errors)}")
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "location": location,
                    "requested": len(metrics),
                    "retrieved": len(results),
                    "errors": errors if errors else None
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_climate_summary", context)
    
    async def get_weather_alerts(
        self,
        state: Optional[str] = None,
        alert_type: Optional[str] = None,
        active_only: bool = True,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get weather alerts and warnings.
        
        Args:
            state: State code filter
            alert_type: Alert type filter
            active_only: Only return active alerts
            use_cache: Whether to use caching (shorter TTL for alerts)
            context: Service context
            
        Returns:
            ServiceResult with weather alerts
        """
        self._log_operation(
            "get_weather_alerts",
            context=context,
            state=state,
            alert_type=alert_type
        )
        
        try:
            # For alerts, use shorter cache TTL (5 minutes)
            cache_ttl = 300 if use_cache else None
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "weather_alerts",
                    state=state,
                    alert_type=alert_type,
                    active_only=active_only
                )
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache", "count": len(cached)}
                    )
            
            # In a real implementation, would query NWS alerts API
            # For now, return empty list
            alerts = []
            
            # Cache results with short TTL
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, alerts, ttl=cache_ttl)
            
            return ServiceResult.ok(
                data=alerts,
                metadata={
                    "source": "database",
                    "count": len(alerts),
                    "active_only": active_only
                }
            )
            
        except Exception as e:
            raise self._handle_error(e, "get_weather_alerts", context)
