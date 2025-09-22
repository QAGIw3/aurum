"""ISO-NE adapter for Web Services endpoints (JSON mode).

For ingestion we stick to JSON responses (not XML) and normalize to Avro.
Enhanced with circuit breaker patterns, exponential backoff, and comprehensive error handling.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Tuple, List

from .base import IsoAdapter, IsoAdapterConfig, IsoRequestChunk
from ..collect import HttpRequest
from ...common.circuit_breaker import CircuitBreaker
from ...observability.metrics import get_metrics_client
from ...data.iso_catalog import canonicalize_iso_observation_record

logger = logging.getLogger(__name__)


DEFAULT_BASE = os.getenv("AURUM_ISONE_BASE", "https://webservices.iso-ne.com/api/v1.1")


@dataclass(frozen=True)
class IsoneConfig:
    endpoint: str = os.getenv("AURUM_ISONE_LMP_ENDPOINT", "/markets/real-time/hourly-lmp")
    max_retries: int = int(os.getenv("AURUM_ISONE_MAX_RETRIES", "5"))
    base_backoff_seconds: float = float(os.getenv("AURUM_ISONE_BASE_BACKOFF", "1.0"))
    max_backoff_seconds: float = float(os.getenv("AURUM_ISONE_MAX_BACKOFF", "60.0"))
    circuit_breaker_threshold: int = int(os.getenv("AURUM_ISONE_CB_THRESHOLD", "5"))
    circuit_breaker_timeout: int = int(os.getenv("AURUM_ISONE_CB_TIMEOUT", "300"))
    timeout_seconds: int = int(os.getenv("AURUM_ISONE_TIMEOUT", "30"))


class IsoneAdapter(IsoAdapter):
    def __init__(self, *, series_id: str, kafka_topic: str, schema_registry_url: Optional[str] = None) -> None:
        config = IsoAdapterConfig(
            provider="iso.isone",
            base_url=DEFAULT_BASE,
            kafka_topic=kafka_topic,
            schema_registry_url=schema_registry_url,
            default_headers={"Accept": "application/json"},
        )
        super().__init__(config, series_id=series_id)
        self._cfg = IsoneConfig()
        self._market_hint = self._infer_market_hint(series_id)

        # Enhanced resilience features
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self._cfg.circuit_breaker_threshold,
            recovery_timeout=self._cfg.circuit_breaker_timeout
        )
        self.metrics = get_metrics_client()

        # Performance tracking
        self._request_count = 0
        self._error_count = 0
        self._last_request_time = 0.0

    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:
        """Build request with enhanced error handling and validation."""
        params = {
            "start": chunk.start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": chunk.end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        # Validate request parameters
        self._validate_request_params(params)

        return HttpRequest(
            method="GET",
            path=self._cfg.endpoint,
            params=params,
            timeout=self._cfg.timeout_seconds
        )

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
        """Parse response with enhanced error handling and data validation."""
        try:
            items = payload.get("items") or payload.get("data") or []

            if not isinstance(items, list):
                logger.warning("Expected list of items in response, got: %s", type(items))
                items = []

            out: List[Mapping[str, Any]] = []

            for i, it in enumerate(items):
                try:
                    rec: dict[str, Any] = dict(it)
                    rec.setdefault("ingest_ts", int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000))
                    if self.series_id:
                        rec.setdefault("series_id", self.series_id)
                    metadata = dict(rec.get("metadata") or {})
                    metadata.setdefault("market", rec.get("market") or self._market_hint)
                    metadata.setdefault("product", metadata.get("product") or "LMP")
                    metadata.setdefault(
                        "location_id",
                        rec.get("location_id")
                        or rec.get("LocationID")
                        or rec.get("locationId")
                        or metadata.get("location_id"),
                    )
                    metadata.setdefault(
                        "location_type",
                        rec.get("location_type")
                        or rec.get("locationType")
                        or rec.get("LocationType")
                        or metadata.get("location_type")
                        or "NODE",
                    )
                    metadata.setdefault("unit", rec.get("uom") or metadata.get("unit") or "USD/MWh")
                    metadata.setdefault(
                        "interval_minutes",
                        metadata.get("interval_minutes") or 60,
                    )
                    rec["metadata"] = metadata
                    rec = canonicalize_iso_observation_record("iso.isone", rec)

                    # Validate record structure
                    if self._validate_record(rec):
                        out.append(rec)
                    else:
                        logger.warning("Invalid record at index %d: %s", i, rec)

                except Exception as e:
                    logger.error("Error processing record at index %d: %s", i, e)
                    self.metrics.increment_counter("isone.record_processing_errors")

            return (out, None)

        except Exception as e:
            logger.error("Error parsing response: %s", e)
            self.metrics.increment_counter("isone.response_parse_errors")
            raise

    def _infer_market_hint(self, series_id: Optional[str]) -> str:
        candidates = [series_id or "", self._cfg.endpoint]
        for value in candidates:
            lowered = value.lower()
            if any(token in lowered for token in ("rt", "real-time", "real_time")):
                return "RTM"
            if any(token in lowered for token in ("da", "day-ahead", "day_ahead")):
                return "DAM"
        return "UNKNOWN"

    def _validate_request_params(self, params: dict) -> None:
        """Validate request parameters."""
        start_time = params.get("start")
        end_time = params.get("end")

        if not start_time or not end_time:
            raise ValueError("Missing start or end time in request parameters")

        # Basic format validation
        try:
            datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        except ValueError as e:
            raise ValueError(f"Invalid datetime format in request: {e}")

    def _validate_record(self, record: dict) -> bool:
        """Validate individual record structure."""
        # Basic validation - can be extended based on data type
        required_fields = ["location_id", "value", "timestamp"]

        for field in required_fields:
            if field not in record:
                logger.warning("Missing required field '%s' in record", field)
                return False

        # Type validation
        if not isinstance(record.get("location_id"), str):
            logger.warning("Invalid location_id type in record")
            return False

        if not isinstance(record.get("value"), (int, float)):
            logger.warning("Invalid value type in record")
            return False

        return True

    async def make_resilient_request(self, request: HttpRequest) -> dict:
        """Make HTTP request with enhanced resilience patterns."""
        if self.circuit_breaker and self.circuit_breaker.is_open():
            logger.warning("Circuit breaker is open, rejecting request")
            self.metrics.increment_counter("isone.circuit_breaker_rejections")
            raise RuntimeError("Circuit breaker is open")

        self._request_count += 1
        current_time = time.time()

        # Rate limiting
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
            self.metrics.increment_counter("isone.requests_success")

            return response.json() if response.content else {}

        except Exception as e:
            # Record failure
            self._error_count += 1
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()
            self.metrics.increment_counter("isone.requests_failed")
            logger.error("Request failed after retries: %s", e)
            raise

    async def _retry_with_backoff(self, request: HttpRequest) -> Any:
        """Execute request with exponential backoff retry logic."""
        last_exception = None

        for attempt in range(self._cfg.max_retries + 1):
            try:
                # Create fresh HTTP request for each attempt
                http_request = HttpRequest(
                    method=request.method,
                    path=request.path,
                    params=request.params,
                    timeout=self._cfg.timeout_seconds
                )

                response = self.collector.request(http_request)

                # Check for HTTP errors
                if response.status >= 400:
                    raise RuntimeError(f"HTTP {response.status}: {response.content}")

                return response

            except Exception as e:
                last_exception = e
                if attempt == self._cfg.max_retries:
                    break

                # Calculate backoff time
                backoff_time = min(
                    self._cfg.base_backoff_seconds * (2 ** attempt),
                    self._cfg.max_backoff_seconds
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
        """Get health status of the adapter."""
        circuit_breaker_status = "closed" if not self.circuit_breaker or not self.circuit_breaker.is_open() else "open"

        return {
            "adapter": "isone",
            "circuit_breaker_status": circuit_breaker_status,
            "total_requests": self._request_count,
            "error_count": self._error_count,
            "error_rate": self._error_count / max(self._request_count, 1),
            "last_request_time": self._last_request_time,
            "config": {
                "endpoint": self._cfg.endpoint,
                "max_retries": self._cfg.max_retries,
                "circuit_breaker_threshold": self._cfg.circuit_breaker_threshold,
                "circuit_breaker_timeout": self._cfg.circuit_breaker_timeout
            }
        }
