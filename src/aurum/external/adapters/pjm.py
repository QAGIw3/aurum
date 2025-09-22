"""PJM Interconnection adapter for data ingestion.

PJM provides electricity to 65 million people across 13 states and the District of Columbia.
This adapter handles PJM's API endpoints for LMP, load, generation, and other market data.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence, Tuple, List

from .base import IsoAdapter, IsoAdapterConfig, IsoRequestChunk
from ..collect import HttpRequest
from ...common.circuit_breaker import CircuitBreaker
from ...observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


DEFAULT_BASE = os.getenv("AURUM_PJM_BASE", "https://api.pjm.com/api/v1")
DEFAULT_API_KEY = os.getenv("AURUM_PJM_API_KEY")


@dataclass(frozen=True)
class PjmConfig:
    """Configuration for PJM data extraction."""

    api_key: Optional[str] = DEFAULT_API_KEY
    max_retries: int = int(os.getenv("AURUM_PJM_MAX_RETRIES", "5"))
    base_backoff_seconds: float = float(os.getenv("AURUM_PJM_BASE_BACKOFF", "1.5"))
    max_backoff_seconds: float = float(os.getenv("AURUM_PJM_MAX_BACKOFF", "90.0"))
    circuit_breaker_threshold: int = int(os.getenv("AURUM_PJM_CB_THRESHOLD", "5"))
    circuit_breaker_timeout: int = int(os.getenv("AURUM_PJM_CB_TIMEOUT", "300"))
    timeout_seconds: int = int(os.getenv("AURUM_PJM_TIMEOUT", "60"))
    max_page_size: int = int(os.getenv("AURUM_PJM_MAX_PAGE_SIZE", "5000"))

    # PJM-specific settings
    market_types: List[str] = None  # ["DA", "RT"]
    location_types: List[str] = None  # ["HUB", "NODE", "ZONE", "EHV", "AGGREGATE"]

    def __post_init__(self):
        if self.market_types is None:
            object.__setattr__(self, 'market_types', ["DA", "RT"])
        if self.location_types is None:
            object.__setattr__(self, 'location_types', ["HUB", "NODE", "ZONE", "EHV", "AGGREGATE"])


class PjmAdapter(IsoAdapter):
    """Adapter for PJM Interconnection data APIs.

    Supports LMP (Locational Marginal Pricing), load, generation mix,
    and other PJM market data endpoints.
    """

    def __init__(self, *, series_id: str, kafka_topic: str, schema_registry_url: Optional[str] = None) -> None:
        config = IsoAdapterConfig(
            provider="iso.pjm",
            base_url=DEFAULT_BASE,
            kafka_topic=kafka_topic,
            schema_registry_url=schema_registry_url,
            default_headers={
                "Ocp-Apim-Subscription-Key": DEFAULT_API_KEY or "",
                "Accept": "application/json"
            } if DEFAULT_API_KEY else {"Accept": "application/json"},
        )
        super().__init__(config, series_id=series_id)
        self._pjm = PjmConfig()

        # Enhanced resilience features
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self._pjm.circuit_breaker_threshold,
            recovery_timeout=self._pjm.circuit_breaker_timeout
        )
        self.metrics = get_metrics_client()

        # Performance tracking
        self._request_count = 0
        self._error_count = 0
        self._last_request_time = 0.0
        self._total_records_processed = 0

    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:
        """Build request with PJM-specific parameters."""
        # PJM uses different parameter names than other ISOs
        params = {
            "startDate": chunk.start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": chunk.end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        # Add PJM-specific parameters from chunk metadata
        if hasattr(chunk, 'params') and chunk.params:
            params.update(chunk.params)

        # Validate request parameters
        self._validate_request_params(params)

        return HttpRequest(
            method="GET",
            path=self._get_endpoint_for_data_type(chunk.params.get('data_type', 'lmp')),
            params=params,
            timeout=self._pjm.timeout_seconds
        )

    def _get_endpoint_for_data_type(self, data_type: str) -> str:
        """Get the appropriate PJM API endpoint for the data type."""
        endpoint_map = {
            'lmp': '/da-lmp-hourly',
            'load': '/load-forecast',
            'generation': '/generation',
            'ancillary': '/ancillary-services',
            'rt_lmp': '/rt-lmp-five-minute'
        }
        return endpoint_map.get(data_type, '/da-lmp-hourly')

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
        """Parse PJM response with enhanced error handling."""
        try:
            if not payload:
                return ([], None)

            # PJM responses vary by endpoint
            data = self._extract_data_from_response(payload)

            if not isinstance(data, list):
                logger.warning("Expected list of items in response, got: %s", type(data))
                return ([], None)

            out: List[Mapping[str, Any]] = []

            for i, item in enumerate(data):
                try:
                    rec = self._normalize_pjm_record(item)

                    # Validate record structure
                    if self._validate_record(rec):
                        out.append(rec)
                        self._total_records_processed += 1
                    else:
                        logger.warning("Invalid record at index %d: %s", i, rec)
                        self.metrics.increment_counter("pjm.record_validation_errors")

                except Exception as e:
                    logger.error("Error processing record at index %d: %s", i, e)
                    self.metrics.increment_counter("pjm.record_processing_errors")

            # Check for reasonable page size limits
            if len(out) > self._pjm.max_page_size:
                logger.warning("Page size %d exceeds limit %d", len(out), self._pjm.max_page_size)

            # PJM doesn't typically use cursor-based pagination
            return (out, None)

        except Exception as e:
            logger.error("Error parsing response: %s", e)
            self.metrics.increment_counter("pjm.response_parse_errors")
            raise

    def _extract_data_from_response(self, payload: Mapping[str, Any]) -> List[Mapping[str, Any]]:
        """Extract data from PJM response, handling different response structures."""
        # PJM responses can have different structures based on the endpoint
        if "items" in payload:
            return payload["items"]
        elif "data" in payload:
            return payload["data"]
        elif "results" in payload:
            return payload["results"]
        elif isinstance(payload, list):
            return payload
        else:
            # Try to find array fields
            for key, value in payload.items():
                if isinstance(value, list) and len(value) > 0:
                    return value
            return []

    def _normalize_pjm_record(self, item: Mapping[str, Any]) -> dict:
        """Normalize PJM record to standard format."""
        rec = dict(item)

        # Ensure required fields exist
        rec.setdefault("ingest_ts", int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000))
        rec.setdefault("iso_code", "PJM")

        # Normalize field names to match schema
        field_mappings = {
            "datetime_beginning_ept": "interval_start",
            "datetime_beginning_utc": "interval_start_utc",
            "pnode_id": "location_id",
            "pnode_name": "location_name",
            "total_lmp_da": "price_total",
            "total_lmp_rt": "price_total_rt",
            "congestion_price_da": "price_congestion",
            "congestion_price_rt": "price_congestion_rt",
            "marginal_loss_price_da": "price_loss",
            "marginal_loss_price_rt": "price_loss_rt",
            "system_energy_price_da": "price_energy",
            "system_energy_price_rt": "price_energy_rt",
            "load_mw": "load_mw",
            "forecast_load_mw": "forecast_load_mw"
        }

        for pjm_field, standard_field in field_mappings.items():
            if pjm_field in item and standard_field not in rec:
                rec[standard_field] = item[pjm_field]

        # Set currency and units
        rec.setdefault("currency", "USD")
        rec.setdefault("uom", "MWh")

        return rec

    def _validate_request_params(self, params: dict) -> None:
        """Validate request parameters for PJM API."""
        start_date = params.get("startDate")
        end_date = params.get("endDate")

        if not all([start_date, end_date]):
            raise ValueError("Missing required parameters: startDate and endDate")

        # Basic format validation
        try:
            datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        except ValueError as e:
            raise ValueError(f"Invalid datetime format in request: {e}")

    def _validate_record(self, record: dict) -> bool:
        """Validate individual PJM record structure."""
        # Required fields depend on data type
        required_fields = ["location_id", "interval_start"]

        for field in required_fields:
            if field not in record:
                logger.warning("Missing required field '%s' in record", field)
                return False

        # Type validation
        if not isinstance(record.get("location_id"), str):
            logger.warning("Invalid location_id type in record")
            return False

        if not isinstance(record.get("interval_start"), str):
            logger.warning("Invalid interval_start type in record")
            return False

        # Value validation (should be numeric for prices/load)
        numeric_fields = ["price_total", "price_congestion", "price_loss", "load_mw"]
        for field in numeric_fields:
            if field in record:
                value = record[field]
                if value is not None and not isinstance(value, (int, float)):
                    logger.warning("Invalid %s type in record: %s", field, type(value))
                    return False

        return True

    async def make_resilient_request(self, request: HttpRequest) -> dict:
        """Make HTTP request with enhanced resilience patterns."""
        if self.circuit_breaker and self.circuit_breaker.is_open():
            logger.warning("Circuit breaker is open, rejecting request")
            self.metrics.increment_counter("pjm.circuit_breaker_rejections")
            raise RuntimeError("Circuit breaker is open")

        self._request_count += 1
        current_time = time.time()

        # Rate limiting - PJM has API limits
        time_since_last = current_time - self._last_request_time
        min_interval = 0.5  # Minimum 0.5 seconds between requests
        if time_since_last < min_interval:
            await asyncio.sleep(min_interval - time_since_last)

        self._last_request_time = time.time()

        try:
            # Make request with retries
            response = await self._retry_with_backoff(request)

            # Record success
            if self.circuit_breaker:
                self.circuit_breaker.record_success()
            self.metrics.increment_counter("pjm.requests_success")

            return response.json() if response.content else {}

        except Exception as e:
            # Record failure
            self._error_count += 1
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()
            self.metrics.increment_counter("pjm.requests_failed")
            logger.error("Request failed after retries: %s", e)
            raise

    async def _retry_with_backoff(self, request: HttpRequest) -> Any:
        """Execute request with exponential backoff retry logic."""
        last_exception = None

        for attempt in range(self._pjm.max_retries + 1):
            try:
                # Create fresh HTTP request for each attempt
                http_request = HttpRequest(
                    method=request.method,
                    path=request.path,
                    params=request.params,
                    timeout=self._pjm.timeout_seconds
                )

                response = self.collector.request(http_request)

                # Check for HTTP errors
                if response.status >= 400:
                    error_content = response.content.decode('utf-8', errors='ignore') if response.content else 'No content'
                    raise RuntimeError(f"HTTP {response.status}: {error_content}")

                return response

            except Exception as e:
                last_exception = e
                if attempt == self._pjm.max_retries:
                    break

                # Calculate backoff time
                backoff_time = min(
                    self._pjm.base_backoff_seconds * (2 ** attempt),
                    self._pjm.max_backoff_seconds
                )

                # Add jitter
                jitter = backoff_time * 0.1 * (0.5 - (hash(str(request.path)) % 100) / 100.0)
                backoff_time += jitter

                logger.warning(
                    "Request attempt %d failed, retrying in %.2f seconds: %s",
                    attempt + 1, backoff_time, e
                )

                await asyncio.sleep(backoff_time)

        # All retries failed
        raise last_exception or RuntimeError("All retry attempts failed")

    def get_health_status(self) -> dict:
        """Get health status of the PJM adapter."""
        circuit_breaker_status = "closed" if not self.circuit_breaker or not self.circuit_breaker.is_open() else "open"

        return {
            "adapter": "pjm",
            "circuit_breaker_status": circuit_breaker_status,
            "total_requests": self._request_count,
            "error_count": self._error_count,
            "error_rate": self._error_count / max(self._request_count, 1),
            "total_records_processed": self._total_records_processed,
            "last_request_time": self._last_request_time,
            "config": {
                "max_retries": self._pjm.max_retries,
                "circuit_breaker_threshold": self._pjm.circuit_breaker_threshold,
                "circuit_breaker_timeout": self._pjm.circuit_breaker_timeout
            }
        }
