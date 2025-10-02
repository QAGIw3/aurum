"""Unified external data collector framework.

This module provides enhanced abstractions for external data collectors,
building on the existing framework to reduce code duplication and provide
common patterns for rate limiting, error handling, and data transformation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Protocol, TypeVar, Generic, Union
from urllib.parse import urljoin

from .base import (
    ExternalCollector,
    CollectorConfig,
    CollectorContext,
    RateLimiter,
    RetryConfig,
    HttpRequest,
    HttpResponse,
)

logger = logging.getLogger(__name__)

T = TypeVar('T')  # Generic type for data records


@dataclass
class ProviderConfig:
    """Configuration for a specific data provider."""

    name: str
    base_url: str
    api_key: Optional[str] = None
    rate_limit_requests_per_minute: int = 60
    rate_limit_burst_size: int = 10
    timeout_seconds: float = 30.0
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    user_agent: str = "Aurum/1.0"


@dataclass
class DatasetConfig:
    """Configuration for a specific dataset within a provider."""

    dataset_id: str
    endpoint_path: str
    data_format: str = "json"  # json, csv, xml, etc.
    pagination: bool = False
    pagination_param: str = "page"
    pagination_size: int = 1000
    date_field: Optional[str] = None
    id_field: str = "id"
    checkpoint_field: Optional[str] = None


class DataTransformer(Protocol):
    """Protocol for transforming raw API data into canonical format."""

    def transform_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single raw record into canonical format."""
        ...

    def transform_response(self, response_data: Any) -> List[Dict[str, Any]]:
        """Transform raw API response into list of canonical records."""
        ...


class BaseProviderCollector(ExternalCollector, Generic[T]):
    """Enhanced base collector with common patterns for external providers."""

    def __init__(
        self,
        provider_config: ProviderConfig,
        dataset_configs: List[DatasetConfig],
        data_transformer: DataTransformer,
        collector_config: Optional[CollectorConfig] = None,
        context: Optional[CollectorContext] = None,
    ):
        super().__init__(collector_config, context)
        self.provider_config = provider_config
        self.dataset_configs = {cfg.dataset_id: cfg for cfg in dataset_configs}
        self.data_transformer = data_transformer

        # Create rate limiter for this provider
        from .base import RateLimitConfig
        rate_config = RateLimitConfig(
            requests_per_minute=provider_config.rate_limit_requests_per_minute,
            burst_size=provider_config.rate_limit_burst_size,
        )
        self.rate_limiter = RateLimiter(rate_config)

        # Create retry config for this provider
        self.retry_config = RetryConfig(
            max_retries=provider_config.max_retries,
            initial_delay=provider_config.retry_backoff_seconds,
        )

    async def collect_catalog(self) -> None:
        """Collect dataset catalog information."""
        for dataset_id, dataset_config in self.dataset_configs.items():
            try:
                logger.info(f"Collecting catalog for {self.provider_config.name}:{dataset_id}")

                # Build catalog request
                url = urljoin(self.provider_config.base_url, dataset_config.endpoint_path)
                headers = self._build_headers()

                # Make request with rate limiting and retries
                async with self.rate_limiter:
                    response = await self._make_request(
                        HttpRequest(
                            method="GET",
                            url=url,
                            headers=headers,
                            timeout=self.provider_config.timeout_seconds,
                        )
                    )

                # Transform and emit catalog records
                catalog_records = self.data_transformer.transform_response(response.data)
                for record in catalog_records:
                    record.update({
                        "provider": self.provider_config.name,
                        "dataset_id": dataset_id,
                        "collected_at": datetime.utcnow().isoformat(),
                    })

                    await self.emit_catalog_record(record)

            except Exception as e:
                logger.error(f"Failed to collect catalog for {dataset_id}: {e}")
                await self.emit_error(f"catalog_collection_failed", dataset_id, str(e))

    async def collect_observations(self, dataset_id: Optional[str] = None) -> None:
        """Collect observation data for datasets."""
        datasets_to_collect = (
            [self.dataset_configs[dataset_id]] if dataset_id
            else list(self.dataset_configs.values())
        )

        for dataset_config in datasets_to_collect:
            try:
                logger.info(f"Collecting observations for {self.provider_config.name}:{dataset_config.dataset_id}")

                # Get last checkpoint for this dataset
                last_checkpoint = await self.get_checkpoint(dataset_config.dataset_id)

                # Build observation request
                url = urljoin(self.provider_config.base_url, dataset_config.endpoint_path)
                params = {}

                if dataset_config.pagination:
                    params[dataset_config.pagination_param] = 1
                    params["limit"] = dataset_config.pagination_size

                # Add date filtering if supported
                if dataset_config.date_field and last_checkpoint:
                    # Calculate date range from checkpoint
                    start_date = last_checkpoint.get("last_date")
                    if start_date:
                        params["start_date"] = start_date

                headers = self._build_headers()

                # Collect all pages if pagination is enabled
                page = 1
                while True:
                    if dataset_config.pagination:
                        params[dataset_config.pagination_param] = page

                    async with self.rate_limiter:
                        response = await self._make_request(
                            HttpRequest(
                                method="GET",
                                url=url,
                                params=params,
                                headers=headers,
                                timeout=self.provider_config.timeout_seconds,
                            )
                        )

                    # Transform and emit observation records
                    observation_records = self.data_transformer.transform_response(response.data)
                    records_emitted = 0

                    for record in observation_records:
                        # Transform record
                        canonical_record = self.data_transformer.transform_record(record)
                        canonical_record.update({
                            "provider": self.provider_config.name,
                            "dataset_id": dataset_config.dataset_id,
                            "collected_at": datetime.utcnow().isoformat(),
                        })

                        await self.emit_observation_record(canonical_record)
                        records_emitted += 1

                    logger.info(f"Emitted {records_emitted} records for {dataset_config.dataset_id} page {page}")

                    # Check if we got a full page (indicates more pages available)
                    if dataset_config.pagination and len(observation_records) < dataset_config.pagination_size:
                        break

                    page += 1

                    # Update checkpoint after each page
                    if records_emitted > 0:
                        checkpoint_data = {
                            "last_date": max(r.get(dataset_config.date_field) for r in observation_records if r.get(dataset_config.date_field)),
                            "last_page": page,
                        }
                        await self.update_checkpoint(dataset_config.dataset_id, checkpoint_data)

            except Exception as e:
                logger.error(f"Failed to collect observations for {dataset_config.dataset_id}: {e}")
                await self.emit_error("observation_collection_failed", dataset_config.dataset_id, str(e))

    def _build_headers(self) -> Dict[str, str]:
        """Build common headers for API requests."""
        headers = {
            "User-Agent": self.provider_config.user_agent,
            "Accept": "application/json",
        }

        if self.provider_config.api_key:
            headers["Authorization"] = f"Bearer {self.provider_config.api_key}"

        return headers

    async def _make_request(self, request: HttpRequest) -> HttpResponse:
        """Make HTTP request with retry logic and error handling."""
        last_exception = None

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                if attempt > 0:
                    # Apply exponential backoff
                    delay = self.retry_config.initial_delay * (2 ** (attempt - 1))
                    logger.debug(f"Retrying request in {delay}s (attempt {attempt})")
                    await asyncio.sleep(delay)

                # Make the actual request
                response = await self._execute_http_request(request)

                # Check for HTTP errors
                if response.status_code >= 400:
                    if response.status_code == 429:  # Rate limited
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            await asyncio.sleep(int(retry_after))
                        continue
                    elif response.status_code >= 500:  # Server error
                        continue
                    else:
                        # Client error - don't retry
                        break

                return response

            except Exception as e:
                last_exception = e
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")

        # All retries exhausted
        raise last_exception

    @abstractmethod
    async def _execute_http_request(self, request: HttpRequest) -> HttpResponse:
        """Execute the actual HTTP request. Must be implemented by subclasses."""
        pass


class JSONDataTransformer:
    """Default JSON data transformer for API responses."""

    def __init__(
        self,
        data_path: str = "data",
        record_path: Optional[str] = None,
        field_mappings: Optional[Dict[str, str]] = None,
    ):
        self.data_path = data_path
        self.record_path = record_path
        self.field_mappings = field_mappings or {}

    def transform_response(self, response_data: Any) -> List[Dict[str, Any]]:
        """Transform JSON response into list of records."""
        if not isinstance(response_data, dict):
            return []

        # Navigate to data array
        data = response_data
        if self.data_path and self.data_path != ".":
            for key in self.data_path.split("."):
                if isinstance(data, dict) and key in data:
                    data = data[key]
                else:
                    return []

        # Extract records
        if isinstance(data, list):
            records = data
        elif self.record_path and isinstance(data, dict) and self.record_path in data:
            records = data[self.record_path]
            if not isinstance(records, list):
                records = [records]
        else:
            records = []

        return records

    def transform_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record, applying field mappings."""
        transformed = {}

        for field, value in raw_record.items():
            # Apply field mapping if defined
            mapped_field = self.field_mappings.get(field, field)
            transformed[mapped_field] = value

        return transformed


def create_provider_collector(
    provider_name: str,
    provider_config: ProviderConfig,
    dataset_configs: List[DatasetConfig],
    data_transformer: Optional[DataTransformer] = None,
) -> BaseProviderCollector:
    """Factory function to create a provider collector with sensible defaults."""

    if data_transformer is None:
        data_transformer = JSONDataTransformer()

    # Create collector configuration
    collector_config = CollectorConfig(
        provider=provider_name,
        kafka_bootstrap_servers=["localhost:9092"],  # Should be configurable
        catalog_topic="aurum.ext.series_catalog.upsert.v1",
        observation_topic="aurum.ext.timeseries.obs.v1",
    )

    return BaseProviderCollector(
        provider_config=provider_config,
        dataset_configs=dataset_configs,
        data_transformer=data_transformer,
        collector_config=collector_config,
    )
