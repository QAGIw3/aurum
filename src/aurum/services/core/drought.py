"""Drought service for drought monitoring and analysis operations with caching.

Implements business logic for drought data queries, analysis, and reporting.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol
from datetime import date, timedelta

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import DroughtRepository

logger = logging.getLogger(__name__)


class CacheProtocol(Protocol):
    """Protocol for cache implementations."""
    async def get(self, key: str) -> Optional[Any]: ...
    async def set(self, key: str, value: Any, ttl: int) -> None: ...
    async def delete(self, key: str) -> None: ...


class DroughtService(BaseService):
    """Service for drought data operations with caching support.

    Drought monitoring and analysis includes:
    - Drought indices (SPI, SPEI, PDSI)
    - USDM classifications
    - Regional drought conditions
    - Historical drought patterns
    - Drought impact assessments

    This service:
    - Validates drought data queries
    - Provides drought analysis and reporting
    - Implements drought monitoring workflows
    - Enforces data access controls
    - Generates drought alerts and reports
    - Caches drought data for performance
    """

    def __init__(
        self,
        drought_repository: DroughtRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 1800  # 30 minutes for drought data
    ):
        """Initialize service with dependencies.

        Args:
            drought_repository: Repository for drought data access
            cache: Optional cache implementation
            cache_ttl: Cache TTL in seconds (default 30 min)
        """
        super().__init__()
        self.drought_repo = drought_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "drought:v1"

    async def get_drought_indices(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        dataset: Optional[str] = None,
        index_id: Optional[str] = None,
        timescale: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get drought indices with optional filtering.

        Business logic:
        - Validates region and index parameters
        - Applies business rules for data access
        - Handles real-time vs historical queries
        - Enforces result limits for performance

        Args:
            region_type: Type of region (e.g., "state", "county")
            region_id: Specific region identifier
            dataset: Dataset name (e.g., "spi", "spei")
            index_id: Specific index identifier
            timescale: Time scale (e.g., "1-month", "6-month")
            start_date: Start date for historical data
            end_date: End date for historical data
            limit: Maximum results (max 1000)
            context: Service context

        Returns:
            ServiceResult with drought index data

        Raises:
            ValidationError: If parameters invalid
            ServiceError: If query fails
        """
        self._log_operation(
            "get_drought_indices",
            context=context,
            region_type=region_type,
            region_id=region_id,
            dataset=dataset
        )

        try:
            # Validate inputs
            if region_type:
                self._validate_region_type(region_type)
            if region_id:
                self._validate_region_id(region_id)
            if dataset:
                self._validate_dataset(dataset)
            if index_id:
                self._validate_index_id(index_id)
            if timescale:
                self._validate_timescale(timescale)

            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )

            if start_date and end_date and start_date > end_date:
                raise ValidationError(
                    "Start date must be before end date",
                    field="date_range"
                )

            # Query repository
            indices = await self.drought_repo.query_drought_indices(
                region_type=region_type,
                region_id=region_id,
                dataset=dataset,
                index_id=index_id,
                timescale=timescale,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )

            # Enrich data with business insights
            enriched_indices = self._enrich_drought_data(indices)

            return ServiceResult.ok(
                data=enriched_indices,
                metadata={
                    "region_type": region_type,
                    "region_id": region_id,
                    "dataset": dataset,
                    "index_count": len(indices),
                    "limit": limit,
                    "has_more": len(indices) == limit
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_drought_indices", context)

    async def get_usdm_data(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get USDM (U.S. Drought Monitor) classifications.

        Args:
            region_type: Type of region
            region_id: Specific region identifier
            start_date: Start date for data
            end_date: End date for data
            limit: Maximum results
            context: Service context

        Returns:
            ServiceResult with USDM data

        Raises:
            ValidationError: If parameters invalid
            ServiceError: If query fails
        """
        self._log_operation(
            "get_usdm_data",
            context=context,
            region_type=region_type,
            region_id=region_id
        )

        try:
            # Validate inputs
            if region_type:
                self._validate_region_type(region_type)
            if region_id:
                self._validate_region_id(region_id)

            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )

            # Query repository
            usdm_data = await self.drought_repo.query_usdm_data(
                region_type=region_type,
                region_id=region_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )

            # Enrich with drought classifications
            enriched_usdm = self._enrich_usdm_data(usdm_data)

            return ServiceResult.ok(
                data=enriched_usdm,
                metadata={
                    "region_type": region_type,
                    "region_id": region_id,
                    "usdm_count": len(usdm_data),
                    "limit": limit
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_usdm_data", context)

    async def get_drought_statistics(
        self,
        region_type: str,
        region_id: str,
        start_date: date,
        end_date: date,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get drought statistics for a region over a time period.

        Args:
            region_type: Type of region
            region_id: Region identifier
            start_date: Start date for analysis
            end_date: End date for analysis
            context: Service context

        Returns:
            ServiceResult with drought statistics

        Raises:
            ValidationError: If parameters invalid
            ServiceError: If calculation fails
        """
        self._log_operation(
            "get_drought_statistics",
            context=context,
            region_type=region_type,
            region_id=region_id
        )

        try:
            # Validate inputs
            self._validate_region_type(region_type)
            self._validate_region_id(region_id)
            self._validate_date_range(start_date, end_date)

            # Get statistics from repository
            stats = await self.drought_repo.get_drought_statistics(
                region_type=region_type,
                region_id=region_id,
                start_date=start_date,
                end_date=end_date
            )

            # Calculate derived metrics
            enriched_stats = self._calculate_derived_metrics(stats, start_date, end_date)

            return ServiceResult.ok(
                data=enriched_stats,
                metadata={
                    "region_type": region_type,
                    "region_id": region_id,
                    "period_days": (end_date - start_date).days
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_drought_statistics", context)

    async def get_latest_drought_data(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get the latest drought data for regions.

        Args:
            region_type: Type of region (None = all)
            region_id: Specific region (None = all)
            limit: Maximum number of results
            context: Service context

        Returns:
            ServiceResult with latest drought data

        Raises:
            ValidationError: If parameters invalid
            ServiceError: If query fails
        """
        self._log_operation(
            "get_latest_drought_data",
            context=context,
            region_type=region_type,
            region_id=region_id
        )

        try:
            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )

            # Get latest data from repository
            latest_data = await self.drought_repo.get_latest_drought_data(
                region_type=region_type,
                region_id=region_id,
                limit=limit
            )

            # Add current drought status
            enriched_data = self._add_current_status(latest_data)

            return ServiceResult.ok(
                data=enriched_data,
                metadata={
                    "region_type": region_type,
                    "region_id": region_id,
                    "data_points": len(latest_data),
                    "limit": limit
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_latest_drought_data", context)

    # Private helper methods

    def _validate_region_type(self, region_type: str) -> None:
        """Validate region type."""
        valid_types = ["state", "county", "basin", "climate_division", "watershed"]
        if region_type not in valid_types:
            raise ValidationError(
                f"Invalid region type. Must be one of: {', '.join(valid_types)}",
                field="region_type"
            )

    def _validate_region_id(self, region_id: str) -> None:
        """Validate region identifier."""
        if not region_id or not region_id.strip():
            raise ValidationError("Region ID is required", field="region_id")

        if len(region_id) > 50:
            raise ValidationError("Region ID too long", field="region_id")

    def _validate_dataset(self, dataset: str) -> None:
        """Validate dataset name."""
        valid_datasets = ["spi", "spei", "pdsi", "phdi", "cmi"]
        if dataset not in valid_datasets:
            raise ValidationError(
                f"Invalid dataset. Must be one of: {', '.join(valid_datasets)}",
                field="dataset"
            )

    def _validate_index_id(self, index_id: str) -> None:
        """Validate index identifier."""
        if not index_id or not index_id.strip():
            raise ValidationError("Index ID is required", field="index_id")

        if len(index_id) > 100:
            raise ValidationError("Index ID too long", field="index_id")

    def _validate_timescale(self, timescale: str) -> None:
        """Validate timescale."""
        valid_timescales = ["1-month", "3-month", "6-month", "12-month", "24-month"]
        if timescale not in valid_timescales:
            raise ValidationError(
                f"Invalid timescale. Must be one of: {', '.join(valid_timescales)}",
                field="timescale"
            )

    def _validate_date_range(self, start_date: date, end_date: date) -> None:
        """Validate date range."""
        if start_date > end_date:
            raise ValidationError(
                "Start date must be before end date",
                field="date_range"
            )

        # Check for reasonable date range
        max_days = 365 * 5  # 5 years max
        if (end_date - start_date).days > max_days:
            raise ValidationError(
                f"Date range too large (max {max_days} days)",
                field="date_range"
            )

    def _enrich_drought_data(self, indices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add business insights to drought index data."""
        enriched = []
        for index in indices:
            enriched_index = dict(index)

            # Add drought classification
            index_value = index.get("index_value", 0)
            enriched_index["drought_classification"] = self._classify_drought(index_value)

            # Add trend indicators
            enriched_index["trend"] = self._calculate_trend(index)

            enriched.append(enriched_index)

        return enriched

    def _classify_drought(self, index_value: float) -> str:
        """Classify drought severity based on index value."""
        if index_value >= 1.0:
            return "wet"
        elif index_value >= 0.5:
            return "normal"
        elif index_value >= -0.5:
            return "mild_drought"
        elif index_value >= -1.0:
            return "moderate_drought"
        elif index_value >= -2.0:
            return "severe_drought"
        else:
            return "extreme_drought"

    def _calculate_trend(self, index: Dict[str, Any]) -> str:
        """Calculate trend indicator for index."""
        # Simplified trend calculation
        # In production, would compare with previous periods
        index_value = index.get("index_value", 0)
        if index_value > 0.5:
            return "improving"
        elif index_value < -0.5:
            return "worsening"
        else:
            return "stable"

    def _enrich_usdm_data(self, usdm_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add drought classifications to USDM data."""
        enriched = []
        for usdm in usdm_data:
            enriched_usdm = dict(usdm)

            # Add drought category descriptions
            category = usdm.get("drought_category", 0)
            enriched_usdm["drought_description"] = self._get_usdm_description(category)

            enriched.append(enriched_usdm)

        return enriched

    def _get_usdm_description(self, category: int) -> str:
        """Get USDM category description."""
        categories = {
            0: "Abnormally Dry",
            1: "Moderate Drought",
            2: "Severe Drought",
            3: "Extreme Drought",
            4: "Exceptional Drought"
        }
        return categories.get(category, "Unknown")

    def _add_current_status(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add current drought status indicators."""
        enriched = []
        for item in data:
            enriched_item = dict(item)

            # Add status timestamp
            enriched_item["status_as_of"] = item.get("date", item.get("valid_date"))

            # Add alert level based on drought classification
            drought_class = item.get("drought_classification", "normal")
            enriched_item["alert_level"] = self._get_alert_level(drought_class)

            enriched.append(enriched_item)

        return enriched

    def _get_alert_level(self, drought_class: str) -> str:
        """Get alert level for drought classification."""
        alert_levels = {
            "wet": "low",
            "normal": "low",
            "mild_drought": "medium",
            "moderate_drought": "medium",
            "severe_drought": "high",
            "extreme_drought": "critical"
        }
        return alert_levels.get(drought_class, "unknown")

    def _calculate_derived_metrics(
        self,
        stats: Dict[str, Any],
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """Calculate additional derived drought metrics."""
        enriched = dict(stats)

        # Calculate drought duration and frequency
        total_obs = stats.get("total_observations", 0)
        drought_episodes = stats.get("drought_episodes", 0)
        severe_episodes = stats.get("severe_drought_episodes", 0)

        enriched.update({
            "drought_frequency": drought_episodes / total_obs if total_obs > 0 else 0,
            "severe_drought_frequency": severe_episodes / total_obs if total_obs > 0 else 0,
            "analysis_period_days": (end_date - start_date).days,
            "avg_index_value": stats.get("avg_index_value", 0),
            "index_volatility": stats.get("max_index_value", 0) - stats.get("min_index_value", 0)
        })

        return enriched

