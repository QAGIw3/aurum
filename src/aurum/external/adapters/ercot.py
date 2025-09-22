"""ERCOT adapter for Texas electricity market data.

ERCOT manages the flow of electric power to more than 26 million Texas customers,
representing about 90 percent of the state's electric load.
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
from ...data.iso_catalog import canonicalize_iso_observation_record

logger = logging.getLogger(__name__)


DEFAULT_BASE = os.getenv("AURUM_ERCOT_BASE", "https://api.ercot.com/api/v1")
DEFAULT_API_KEY = os.getenv("AURUM_ERCOT_API_KEY")


@dataclass(frozen=True)
class ErcotConfig:
    """Configuration for ERCOT data extraction."""

    api_key: Optional[str] = DEFAULT_API_KEY
    max_retries: int = int(os.getenv("AURUM_ERCOT_MAX_RETRIES", "5"))
    base_backoff_seconds: float = float(os.getenv("AURUM_ERCOT_BASE_BACKOFF", "2.0"))
    max_backoff_seconds: float = float(os.getenv("AURUM_ERCOT_MAX_BACKOFF", "120.0"))
    circuit_breaker_threshold: int = int(os.getenv("AURUM_ERCOT_CB_THRESHOLD", "5"))
    circuit_breaker_timeout: int = int(os.getenv("AURUM_ERCOT_CB_TIMEOUT", "300"))
    timeout_seconds: int = int(os.getenv("AURUM_ERCOT_TIMEOUT", "90"))
    max_page_size: int = int(os.getenv("AURUM_ERCOT_MAX_PAGE_SIZE", "10000"))

    # ERCOT-specific settings
    settlement_point_types: List[str] = None  # ["HUB", "LZ", "DC", "SC"]
    market_types: List[str] = None  # ["DAM", "RTM", "SCED"]

    def __post_init__(self):
        if self.settlement_point_types is None:
            object.__setattr__(self, 'settlement_point_types', ["HUB", "LZ", "DC", "SC"])
        if self.market_types is None:
            object.__setattr__(self, 'market_types', ["DAM", "RTM", "SCED"])


class ErcotAdapter(IsoAdapter):
    """Adapter for ERCOT market data APIs.

    Supports SPP (Settlement Point Prices), load, generation,
    and other ERCOT market data endpoints.
    """

    def __init__(self, *, series_id: str, kafka_topic: str, schema_registry_url: Optional[str] = None) -> None:
        config = IsoAdapterConfig(
            provider="iso.ercot",
            base_url=DEFAULT_BASE,
            kafka_topic=kafka_topic,
            schema_registry_url=schema_registry_url,
            default_headers={
                "Authorization": f"Bearer {DEFAULT_API_KEY}" if DEFAULT_API_KEY else "",
                "Accept": "application/json"
            } if DEFAULT_API_KEY else {"Accept": "application/json"},
        )
        super().__init__(config, series_id=series_id)
        self._ercot = ErcotConfig()

        # Enhanced resilience features
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self._ercot.circuit_breaker_threshold,
            recovery_timeout=self._ercot.circuit_breaker_timeout
        )
        self.metrics = get_metrics_client()

        # Performance tracking
        self._request_count = 0
        self._error_count = 0
        self._last_request_time = 0.0
        self._total_records_processed = 0

    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:
        """Build request with ERCOT-specific parameters."""
        # ERCOT uses different parameter names
        params = {
            "startTime": chunk.start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": chunk.end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        # Add ERCOT-specific parameters from chunk metadata
        if hasattr(chunk, 'params') and chunk.params:
            params.update(chunk.params)

        # Validate request parameters
        self._validate_request_params(params)

        return HttpRequest(
            method="GET",
            path=self._get_endpoint_for_data_type(chunk.params.get('data_type', 'spp')),
            params=params,
            timeout=self._ercot.timeout_seconds
        )

    def _get_endpoint_for_data_type(self, data_type: str) -> str:
        """Get the appropriate ERCOT API endpoint for the data type."""
        endpoint_map = {
            'spp': '/spp/current',
            'dam_spp': '/spp/dam',
            'load': '/load',
            'wind_generation': '/wind',
            'solar_generation': '/solar',
            'ancillary_services': '/as'
        }
        return endpoint_map.get(data_type, '/spp/current')

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
        """Parse ERCOT response with enhanced error handling."""
        try:
            if not payload:
                return ([], None)

            # ERCOT responses vary by endpoint
            data = self._extract_data_from_response(payload)

            if not isinstance(data, list):
                logger.warning("Expected list of items in response, got: %s", type(data))
                return ([], None)

            out: List[Mapping[str, Any]] = []

            for i, item in enumerate(data):
                try:
                    rec = self._normalize_ercot_record(item)

                    # Validate record structure
                    if self._validate_record(rec):
                        out.append(rec)
                        self._total_records_processed += 1
                    else:
                        logger.warning("Invalid record at index %d: %s", i, rec)
                        self.metrics.increment_counter("ercot.record_validation_errors")

                except Exception as e:
                    logger.error("Error processing record at index %d: %s", i, e)
                    self.metrics.increment_counter("ercot.record_processing_errors")

            # Check for reasonable page size limits
            if len(out) > self._ercot.max_page_size:
                logger.warning("Page size %d exceeds limit %d", len(out), self._ercot.max_page_size)

            # ERCOT doesn't typically use cursor-based pagination
            return (out, None)

        except Exception as e:
            logger.error("Error parsing response: %s", e)
            self.metrics.increment_counter("ercot.response_parse_errors")
            raise

    def _extract_data_from_response(self, payload: Mapping[str, Any]) -> List[Mapping[str, Any]]:
        """Extract data from ERCOT response, handling different response structures."""
        # ERCOT API responses can have different structures
        if "data" in payload:
            return payload["data"]
        elif "items" in payload:
            return payload["items"]
        elif "response" in payload and isinstance(payload["response"], list):
            return payload["response"]
        elif isinstance(payload, list):
            return payload
        else:
            # Try to find array fields
            for key, value in payload.items():
                if isinstance(value, list) and len(value) > 0:
                    return value
            return []

    def _normalize_ercot_record(self, item: Mapping[str, Any]) -> dict:
        """Normalize ERCOT record to standard format."""
        rec = dict(item)

        # Ensure required fields exist
        rec.setdefault("ingest_ts", int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000))
        rec.setdefault("iso_code", "ERCOT")

        # Normalize field names to match schema
        field_mappings = {
            "settlement_point": "location_id",
            "settlement_point_name": "location_name",
            "spp": "price_total",
            "mcpc": "price_energy",
            "mcl": "price_loss",
            "mcc": "price_congestion",
            "load": "load_mw",
            "generation": "generation_mw",
            "wind_generation": "wind_generation_mw",
            "solar_generation": "solar_generation_mw",
            "timestamp": "interval_start",
            "time": "interval_start"
        }

        for ercot_field, standard_field in field_mappings.items():
            if ercot_field in item and standard_field not in rec:
                rec[standard_field] = item[ercot_field]

        # Set currency and units
        rec.setdefault("currency", "USD")
        rec.setdefault("uom", "MWh")

        # ERCOT uses Central Time
        rec.setdefault("timezone", "America/Chicago")

        metadata = dict(rec.get("metadata") or {})
        metadata.setdefault(
            "market",
            rec.get("market")
            or rec.get("market_type")
            or metadata.get("market")
            or "RTM",
        )
        metadata.setdefault("product", metadata.get("product") or rec.get("data_type") or "LMP")
        metadata.setdefault(
            "location_id",
            rec.get("location_id")
            or rec.get("settlement_point")
            or metadata.get("location_id"),
        )
        metadata.setdefault(
            "location_type",
            rec.get("settlement_point_type")
            or rec.get("location_type")
            or metadata.get("location_type")
            or "NODE",
        )
        metadata.setdefault("unit", rec.get("uom") or metadata.get("unit") or "USD/MWh")
        metadata.setdefault("interval_minutes", metadata.get("interval_minutes") or 15)
        rec["metadata"] = metadata
        if self.series_id:
            rec.setdefault("series_id", self.series_id)
        rec = canonicalize_iso_observation_record("iso.ercot", rec)

        return rec

    def _validate_request_params(self, params: dict) -> None:
        """Validate request parameters for ERCOT API."""
        start_time = params.get("startTime")
        end_time = params.get("endTime")

        if not all([start_time, end_time]):
            raise ValueError("Missing required parameters: startTime and endTime")

        # Basic format validation
        try:
            datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        except ValueError as e:
            raise ValueError(f"Invalid datetime format in request: {e}")

    def _validate_record(self, record: dict) -> bool:
        """Validate individual ERCOT record structure."""
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

        # Value validation (should be numeric for prices/load/generation)
        numeric_fields = ["price_total", "price_congestion", "price_loss", "price_energy",
                         "load_mw", "generation_mw", "wind_generation_mw", "solar_generation_mw"]
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
            self.metrics.increment_counter("ercot.circuit_breaker_rejections")
            raise RuntimeError("Circuit breaker is open")

        self._request_count += 1
        current_time = time.time()

        # Rate limiting - ERCOT has API limits
        time_since_last = current_time - self._last_request_time
        min_interval = 1.0  # Minimum 1 second between requests
        if time_since_last < min_interval:
            await asyncio.sleep(min_interval - time_since_last)

        self._last_request_time = time.time()

        try:
            # Make request with retries
            response = await self._retry_with_backoff(request)

            # Record success
            if self.circuit_breaker:
                self.circuit_breaker.record_success()
            self.metrics.increment_counter("ercot.requests_success")

            return response.json() if response.content else {}

        except Exception as e:
            # Record failure
            self._error_count += 1
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()
            self.metrics.increment_counter("ercot.requests_failed")
            logger.error("Request failed after retries: %s", e)
            raise

    async def _retry_with_backoff(self, request: HttpRequest) -> Any:
        """Execute request with exponential backoff retry logic."""
        last_exception = None

        for attempt in range(self._ercot.max_retries + 1):
            try:
                # Create fresh HTTP request for each attempt
                http_request = HttpRequest(
                    method=request.method,
                    path=request.path,
                    params=request.params,
                    timeout=self._ercot.timeout_seconds
                )

                response = self.collector.request(http_request)

                # Check for HTTP errors
                if response.status >= 400:
                    error_content = response.content.decode('utf-8', errors='ignore') if response.content else 'No content'
                    raise RuntimeError(f"HTTP {response.status}: {error_content}")

                return response

            except Exception as e:
                last_exception = e
                if attempt == self._ercot.max_retries:
                    break

                # Calculate backoff time with longer delays for ERCOT
                backoff_time = min(
                    self._ercot.base_backoff_seconds * (2 ** attempt),
                    self._ercot.max_backoff_seconds
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
        """Get health status of the ERCOT adapter."""
        circuit_breaker_status = "closed" if not self.circuit_breaker or not self.circuit_breaker.is_open() else "open"

        return {
            "adapter": "ercot",
            "circuit_breaker_status": circuit_breaker_status,
            "total_requests": self._request_count,
            "error_count": self._error_count,
            "error_rate": self._error_count / max(self._request_count, 1),
            "total_records_processed": self._total_records_processed,
            "last_request_time": self._last_request_time,
            "config": {
                "max_retries": self._ercot.max_retries,
                "circuit_breaker_threshold": self._ercot.circuit_breaker_threshold,
                "circuit_breaker_timeout": self._ercot.circuit_breaker_timeout
            }
        }
