"""World Bank service with caching.

Implements business logic for World Bank data operations including
economic indicators, development metrics, and country statistics.
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


class WorldBankRepository(BaseRepository):
    """Repository for World Bank data operations.
    
    Temporary implementation until proper WorldBankRepository is created.
    """
    
    async def get_indicator_data(
        self,
        indicator_id: str,
        country_codes: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get World Bank indicator data."""
        # Stub implementation - would query from iceberg.external.worldbank_indicators
        return []
    
    async def search_indicators(
        self,
        search_text: str,
        topic: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search World Bank indicators."""
        # Stub implementation - would query from iceberg.external.worldbank_catalog
        return []
    
    async def get_country_info(
        self,
        country_code: str
    ) -> Optional[Dict[str, Any]]:
        """Get country information."""
        # Stub implementation
        return None
    
    async def get_countries_by_region(
        self,
        region: str
    ) -> List[Dict[str, Any]]:
        """Get countries in a region."""
        # Stub implementation
        return []


class WorldBankService(BaseService):
    """Service for World Bank data operations with caching support.
    
    World Bank provides global development data including:
    - Economic indicators (GDP, inflation, trade)
    - Social indicators (education, health, poverty)
    - Environmental indicators (emissions, energy, resources)
    - Infrastructure metrics
    - Financial sector data
    
    This service:
    - Validates indicator and country codes
    - Manages indicator catalog
    - Provides data query interface
    - Handles data aggregations
    - Caches indicator data for performance
    - Enforces access control
    """
    
    def __init__(
        self,
        worldbank_repository: WorldBankRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 7200  # 2 hours for World Bank data
    ):
        """Initialize service with dependencies.
        
        Args:
            worldbank_repository: Repository for World Bank data access
            cache: Optional cache implementation
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__()
        self.wb_repo = worldbank_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "worldbank:v1"
    
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
    
    async def get_indicator(
        self,
        indicator_id: str,
        country_codes: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get World Bank indicator data.
        
        Common indicators:
        - NY.GDP.MKTP.CD: GDP (current US$)
        - NY.GDP.PCAP.CD: GDP per capita (current US$)
        - FP.CPI.TOTL.ZG: Inflation, consumer prices (annual %)
        - SL.UEM.TOTL.ZS: Unemployment, total (% of total labor force)
        - EG.USE.PCAP.KG.OE: Energy use (kg of oil equivalent per capita)
        
        Args:
            indicator_id: World Bank indicator code
            country_codes: List of country ISO codes (e.g., ["USA", "CHN", "IND"])
            start_year: Start year for data
            end_year: End year for data
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with indicator data
        """
        self._log_operation(
            "get_indicator",
            context=context,
            indicator_id=indicator_id,
            countries=len(country_codes) if country_codes else "all"
        )
        
        try:
            # Validate
            if not indicator_id:
                raise ValidationError("Indicator ID is required", field="indicator_id")
            
            # Validate year range
            current_year = datetime.now().year
            if start_year and (start_year < 1960 or start_year > current_year):
                raise ValidationError(
                    f"Start year must be between 1960 and {current_year}",
                    field="start_year"
                )
            if end_year and (end_year < 1960 or end_year > current_year):
                raise ValidationError(
                    f"End year must be between 1960 and {current_year}",
                    field="end_year"
                )
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "indicator",
                    indicator_id=indicator_id,
                    country_codes=sorted(country_codes) if country_codes else None,
                    start_year=start_year,
                    end_year=end_year
                )
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={
                            "source": "cache",
                            "indicator_id": indicator_id
                        }
                    )
            
            # Get from repository
            data = await self.wb_repo.get_indicator_data(
                indicator_id=indicator_id,
                country_codes=country_codes,
                start_year=start_year,
                end_year=end_year
            )
            
            if not data:
                raise NotFoundError(f"No data found for indicator: {indicator_id}")
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, data)
            
            return ServiceResult.ok(
                data=data,
                metadata={
                    "source": "database",
                    "indicator_id": indicator_id,
                    "count": len(data),
                    "countries": len(country_codes) if country_codes else "all"
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_indicator", context)
    
    async def search_indicators(
        self,
        search_text: str,
        topic: Optional[str] = None,
        limit: int = 100,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Search World Bank indicators.
        
        Topics include:
        - Economy & Growth
        - Education
        - Energy & Mining
        - Environment
        - Financial Sector
        - Health
        - Infrastructure
        - Poverty
        - Social Development
        
        Args:
            search_text: Text to search in indicator names/descriptions
            topic: Topic filter
            limit: Maximum number of results
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with matching indicators
        """
        self._log_operation(
            "search_indicators",
            context=context,
            search_text=search_text,
            topic=topic
        )
        
        try:
            # Validate
            if not search_text:
                raise ValidationError("Search text is required", field="search_text")
            
            if limit < 1 or limit > 1000:
                raise ValidationError("Limit must be between 1 and 1000", field="limit")
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "search",
                    search_text=search_text.lower(),
                    topic=topic,
                    limit=limit
                )
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache", "search_text": search_text}
                    )
            
            # Search in repository
            results = await self.wb_repo.search_indicators(
                search_text=search_text,
                topic=topic,
                limit=limit
            )
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, results)
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "source": "database",
                    "search_text": search_text,
                    "topic": topic,
                    "count": len(results)
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "search_indicators", context)
    
    async def get_country_indicators(
        self,
        country_code: str,
        indicators: List[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, List[Dict[str, Any]]]]:
        """Get multiple indicators for a single country.
        
        Args:
            country_code: ISO country code (e.g., "USA", "CHN")
            indicators: List of indicator IDs
            start_year: Start year for data
            end_year: End year for data
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with dict mapping indicator_id to data
        """
        self._log_operation(
            "get_country_indicators",
            context=context,
            country_code=country_code,
            indicators=len(indicators)
        )
        
        try:
            # Validate
            if not country_code:
                raise ValidationError("Country code is required", field="country_code")
            
            if not indicators:
                raise ValidationError("At least one indicator is required", field="indicators")
            
            if len(indicators) > 20:
                raise ValidationError("Maximum 20 indicators per request", field="indicators")
            
            # Get each indicator
            results = {}
            errors = []
            
            for indicator_id in indicators:
                try:
                    result = await self.get_indicator(
                        indicator_id=indicator_id,
                        country_codes=[country_code],
                        start_year=start_year,
                        end_year=end_year,
                        use_cache=use_cache,
                        context=context
                    )
                    if result.success:
                        results[indicator_id] = result.data
                except NotFoundError:
                    errors.append(f"Indicator not found: {indicator_id}")
                except Exception as e:
                    errors.append(f"Error getting {indicator_id}: {str(e)}")
            
            if not results and errors:
                raise ServiceError(f"Failed to get any indicators: {'; '.join(errors)}")
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "country_code": country_code,
                    "requested": len(indicators),
                    "retrieved": len(results),
                    "errors": errors if errors else None
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_country_indicators", context)
    
    async def get_regional_comparison(
        self,
        indicator_id: str,
        region: str,
        year: Optional[int] = None,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get indicator data for all countries in a region.
        
        Regions:
        - EAS: East Asia & Pacific
        - ECS: Europe & Central Asia
        - LCN: Latin America & Caribbean
        - MEA: Middle East & North Africa
        - NAC: North America
        - SAS: South Asia
        - SSF: Sub-Saharan Africa
        
        Args:
            indicator_id: World Bank indicator code
            region: Region code
            year: Year for comparison (default: latest available)
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with regional comparison data
        """
        self._log_operation(
            "get_regional_comparison",
            context=context,
            indicator_id=indicator_id,
            region=region
        )
        
        try:
            # Validate
            if not indicator_id:
                raise ValidationError("Indicator ID is required", field="indicator_id")
            
            if not region:
                raise ValidationError("Region is required", field="region")
            
            # Get countries in region
            countries = await self.wb_repo.get_countries_by_region(region)
            if not countries:
                raise NotFoundError(f"No countries found for region: {region}")
            
            country_codes = [c["code"] for c in countries]
            
            # Get indicator data for all countries
            result = await self.get_indicator(
                indicator_id=indicator_id,
                country_codes=country_codes,
                start_year=year,
                end_year=year,
                use_cache=use_cache,
                context=context
            )
            
            if result.success:
                # Sort by value for easy comparison
                data = sorted(
                    result.data,
                    key=lambda x: x.get("value", 0) or 0,
                    reverse=True
                )
                
                return ServiceResult.ok(
                    data=data,
                    metadata={
                        "indicator_id": indicator_id,
                        "region": region,
                        "year": year,
                        "countries": len(country_codes)
                    }
                )
            else:
                return result
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_regional_comparison", context)
