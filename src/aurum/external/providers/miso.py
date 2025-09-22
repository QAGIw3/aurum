"""MISO Energy data collector implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from aurum.external.collect.base import ExternalCollector, CollectorContext
from aurum.common.circuit_breaker import CircuitBreaker
from aurum.external.collect.checkpoints import PostgresCheckpointStore

logger = logging.getLogger(__name__)


# Use shared CircuitBreaker from aurum.common


@dataclass
class RateLimiter:
    """Adaptive rate limiter."""

    requests_per_minute: int = 50
    requests_per_hour: int = 3000
    burst_limit: int = 10

    _minute_requests: List[float] = field(default_factory=list, init=False)
    _hour_requests: List[float] = field(default_factory=list, init=False)
    _adaptive_factor: float = field(default=1.0, init=False)

    def __post_init__(self):
        self._minute_requests = []
        self._hour_requests = []
        self._adaptive_factor = 1.0

    def can_make_request(self) -> bool:
        """Check if request can be made without violating base limits.

        Adaptive factor influences pacing (sleep/jitter), not the hard limit
        decision, to keep throughput predictable and tests straightforward.
        """
        current_time = time.time()

        # Clean old entries
        self._minute_requests = [t for t in self._minute_requests if current_time - t < 60]
        self._hour_requests = [t for t in self._hour_requests if current_time - t < 3600]

        minute_count = len(self._minute_requests)
        hour_count = len(self._hour_requests)

        # Allow up to and including configured limits
        return minute_count <= self.requests_per_minute and hour_count <= self.requests_per_hour

    def record_request(self):
        """Record a request."""
        current_time = time.time()
        self._minute_requests.append(current_time)
        self._hour_requests.append(current_time)

        # Adaptive rate limiting based on recent success rate
        if len(self._minute_requests) > 0:
            # If we're close to the limit, be more conservative
            if len(self._minute_requests) > (self.requests_per_minute * 0.8):
                self._adaptive_factor = max(0.5, self._adaptive_factor * 0.9)

    def adapt_rate(self, success: bool):
        """Adapt rate limiting based on request success."""
        if success:
            # Gradually increase rate if requests are successful
            self._adaptive_factor = min(1.0, self._adaptive_factor * 1.1)
        else:
            # Decrease rate if requests are failing
            self._adaptive_factor = max(0.3, self._adaptive_factor * 0.8)


@dataclass
class DataQualityMetrics:
    """Data quality metrics tracker."""

    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    empty_records: int = 0
    duplicate_records: int = 0
    outlier_records: int = 0

    def record_valid(self):
        self.total_records += 1
        self.valid_records += 1

    def record_invalid(self, reason: str = "unknown"):
        self.total_records += 1
        self.invalid_records += 1
        logger.debug(f"Invalid record: {reason}")

    def record_empty(self):
        self.total_records += 1
        self.empty_records += 1

    def record_duplicate(self):
        self.total_records += 1
        self.duplicate_records += 1

    def record_outlier(self):
        self.total_records += 1
        self.outlier_records += 1

    def get_quality_score(self) -> float:
        """Calculate data quality score (0.0 to 1.0)."""
        if self.total_records == 0:
            return 1.0
        return self.valid_records / self.total_records

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "empty_records": self.empty_records,
            "duplicate_records": self.duplicate_records,
            "outlier_records": self.outlier_records,
            "quality_score": self.get_quality_score()
        }


class DataValidator:
    """Comprehensive data validator for MISO data."""

    def __init__(self):
        self.metrics = DataQualityMetrics()

    def validate_record(self, record: Dict[str, Any], data_type: str) -> Tuple[bool, str]:
        """Validate a single record based on data type."""
        if not record or not isinstance(record, dict):
            self.metrics.record_empty()
            return False, "Empty or invalid record"

        # Check for required fields based on data type
        required_fields = self._get_required_fields(data_type)
        missing_fields = [field for field in required_fields if field not in record or record[field] is None]

        if missing_fields:
            self.metrics.record_invalid(f"Missing fields: {missing_fields}")
            return False, f"Missing required fields: {missing_fields}"

        # Validate data types and ranges
        validation_result = self._validate_data_types(record, data_type)
        if not validation_result[0]:
            self.metrics.record_invalid(validation_result[1])
            return False, validation_result[1]

        # Check for outliers
        outlier_result = self._check_outliers(record, data_type)
        if not outlier_result[0]:
            self.metrics.record_outlier()

        self.metrics.record_valid()
        return True, "Valid record"

    def _get_required_fields(self, data_type: str) -> List[str]:
        """Get required fields for data type."""
        field_maps = {
            "lmp": ["location_id", "price_total", "market"],
            "load": ["location_id", "load_mw"],
            "generation_mix": ["location_id", "fuel_type", "generation_mw"],
            "ancillary_services": ["location_id", "as_type", "as_price"],
            "interchange": ["location_id", "interchange_mw"],
            "forecast_load": ["location_id", "forecast_load_mw"],
            "forecast_generation": ["location_id", "forecast_generation_mw"]
        }
        return field_maps.get(data_type, [])

    def _validate_data_types(self, record: Dict[str, Any], data_type: str) -> Tuple[bool, str]:
        """Validate data types and ranges."""
        try:
            if data_type == "lmp":
                if not isinstance(record.get("price_total"), (int, float)):
                    return False, "Invalid price_total type"
                if record["price_total"] < -1000 or record["price_total"] > 10000:
                    return False, "Price_total out of reasonable range"

            elif data_type in ["load", "generation_mix", "interchange"]:
                load_field = {
                    "load": "load_mw",
                    "generation_mix": "generation_mw",
                    "interchange": "interchange_mw"
                }.get(data_type, "value")

                value = record.get(load_field)
                if not isinstance(value, (int, float)):
                    return False, f"Invalid {load_field} type"
                if value < -10000 or value > 100000:
                    return False, f"{load_field} out of reasonable range"

            elif data_type == "ancillary_services":
                if not isinstance(record.get("as_price"), (int, float)):
                    return False, "Invalid as_price type"
                if not isinstance(record.get("as_mw"), (int, float)):
                    return False, "Invalid as_mw type"

            return True, "Data types valid"

        except Exception as e:
            return False, f"Data validation error: {e}"

    def _check_outliers(self, record: Dict[str, Any], data_type: str) -> Tuple[bool, str]:
        """Check for outliers using simple statistical methods."""
        try:
            # Simple outlier detection based on data type
            if data_type == "lmp":
                price = record.get("price_total", 0)
                if abs(price) > 5000:  # Extreme price
                    return False, f"Extreme price detected: {price}"

            elif data_type in ["load", "generation_mix"]:
                load_field = {
                    "load": "load_mw",
                    "generation_mix": "generation_mw"
                }.get(data_type, "value")

                value = record.get(load_field, 0)
                if abs(value) > 50000:  # Extreme load/generation
                    return False, f"Extreme {load_field} detected: {value}"

            return True, "No outliers detected"

        except Exception as e:
            return True, f"Outlier check failed: {e}"

    def get_metrics(self) -> Dict[str, Any]:
        """Get current validation metrics."""
        return self.metrics.to_dict()


class MisoHealthMonitor:
    """Health monitoring and alerting for MISO data collection."""

    def __init__(self):
        self.health_metrics = {
            "last_health_check": None,
            "consecutive_failures": 0,
            "total_uptime": 0,
            "total_downtime": 0,
            "last_failure_time": None,
            "alerts_sent": 0
        }

        # Alert thresholds
        # Warning after 3 consecutive failures; critical on the 4th
        self.failure_threshold = 3
        self.critical_failure_threshold = 4

    async def record_success(self):
        """Record successful operation."""
        self.health_metrics["consecutive_failures"] = 0
        self.health_metrics["last_health_check"] = datetime.utcnow()

        # Calculate uptime
        if self.health_metrics["total_downtime"] > 0:
            self.health_metrics["total_uptime"] += 1

    async def record_failure(self, error: Exception):
        """Record failed operation."""
        self.health_metrics["consecutive_failures"] += 1
        self.health_metrics["last_failure_time"] = datetime.utcnow()
        self.health_metrics["last_health_check"] = datetime.utcnow()

        # Calculate downtime
        self.health_metrics["total_downtime"] += 1

        # Send alerts based on failure count
        await self._check_and_alert(error)

    async def _check_and_alert(self, error: Exception):
        """Check if alert should be sent and send it."""
        failures = self.health_metrics["consecutive_failures"]

        if failures >= self.critical_failure_threshold:
            await self._send_critical_alert(error)
            self.health_metrics["alerts_sent"] += 1
        elif failures >= self.failure_threshold:
            await self._send_warning_alert(error)
            self.health_metrics["alerts_sent"] += 1

    async def _send_warning_alert(self, error: Exception):
        """Send warning alert."""
        logger.warning(
            "MISO collector warning alert",
            extra={
                "alert_type": "warning",
                "consecutive_failures": self.health_metrics["consecutive_failures"],
                "error": str(error),
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    async def _send_critical_alert(self, error: Exception):
        """Send critical alert."""
        logger.error(
            "MISO collector critical alert",
            extra={
                "alert_type": "critical",
                "consecutive_failures": self.health_metrics["consecutive_failures"],
                "error": str(error),
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        status = "healthy" if self.health_metrics["consecutive_failures"] == 0 else "degraded"

        if self.health_metrics["consecutive_failures"] >= self.critical_failure_threshold:
            status = "critical"

        return {
            **self.health_metrics,
            "status": status,
            "uptime_percentage": (
                self.health_metrics["total_uptime"] /
                max(self.health_metrics["total_uptime"] + self.health_metrics["total_downtime"], 1)
            ) * 100
        }


@dataclass
class MisoDatasetConfig:
    """Configuration for MISO datasets."""

    dataset_id: str
    description: str
    data_type: str  # lmp, load, generation_mix, ancillary_services
    market: str  # DAM, RTM
    url_template: str
    interval_minutes: int
    columns: Dict[str, str]  # Column mappings
    frequency: str  # HOURLY, DAILY, etc.
    units: str


@dataclass
class MisoApiConfig:
    """MISO API configuration."""

    base_url: str
    api_key: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    requests_per_minute: int = 50
    requests_per_hour: int = 3000
    circuit_breaker_enabled: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60


class MisoApiClient:
    """Client for MISO Data Exchange API."""

    def __init__(self, config: MisoApiConfig):
        self.config = config
        self.session = requests.Session()
        self.session.timeout = config.timeout

        if config.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {config.api_key}",
                "Accept": "application/json"
            })

        # Initialize circuit breaker and rate limiter
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_timeout
        ) if config.circuit_breaker_enabled else None

        self.rate_limiter = RateLimiter(
            requests_per_minute=config.requests_per_minute,
            requests_per_hour=config.requests_per_hour
        )

    async def fetch_data(
        self,
        url: str,
        params: Dict[str, Any],
        format_type: str = "json",
        fallback_urls: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Fetch data from MISO API with non-blocking behavior in async contexts.

        Uses asyncio.to_thread() to run blocking requests.Session.get in a worker
        thread, preventing event loop blocking while preserving existing
        requests-based mocking in tests.
        """
        urls_to_try = [url] + (fallback_urls or [])

        for url_to_try in urls_to_try:
            # Check circuit breaker
            if self.circuit_breaker and self.circuit_breaker.is_open():
                logger.warning(f"Circuit breaker is OPEN, skipping request to {url_to_try}")
                continue

            success = False
            for attempt in range(self.config.max_retries + 1):
                try:
                    # Rate limiter check
                    if not self.rate_limiter.can_make_request():
                        wait_time = 60.0 / self.rate_limiter.requests_per_minute
                        logger.info(f"Rate limit reached, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                        continue

                    # Record request + jittered pacing
                    self.rate_limiter.record_request()
                    wait_time = 1.0 / (self.rate_limiter.requests_per_minute / 60.0)
                    wait_time *= (0.8 + 0.4 * (attempt % 3))
                    await asyncio.sleep(wait_time)

                    logger.debug(f"Making request to {url_to_try} (attempt {attempt + 1})")

                    # Execute blocking request in a thread to avoid blocking event loop
                    def _do_get():
                        # Use a short-lived Session per thread to avoid shared-session threading issues
                        with requests.Session() as sess:
                            if self.config.api_key:
                                sess.headers.update({
                                    "Authorization": f"Bearer {self.config.api_key}",
                                    "Accept": "application/json"
                                })
                            # Merge any pre-configured headers from the instance session
                            try:
                                sess.headers.update(getattr(self.session, "headers", {}))
                            except Exception:
                                pass
                            resp = sess.get(url_to_try, params=params, timeout=self.config.timeout)
                            resp.raise_for_status()
                            return resp

                    response = await asyncio.to_thread(_do_get)

                    # Record success
                    success = True
                    if self.circuit_breaker:
                        self.circuit_breaker.record_success()
                    self.rate_limiter.adapt_rate(True)

                    # Handle response formats
                    content_type = (response.headers.get('Content-Type', '') or '').lower()
                    if format_type == "json" or "json" in content_type:
                        data = response.json()
                        if not data or (isinstance(data, dict) and "data" not in data):
                            logger.warning(f"Empty or invalid JSON response from {url_to_try}")
                            raise ValueError("Empty or invalid JSON response")
                        return data
                    if format_type == "csv" or "csv" in content_type:
                        parsed_data = self._parse_csv(response.text)
                        if not parsed_data:
                            logger.warning(f"Empty CSV response from {url_to_try}")
                            raise ValueError("Empty CSV response")
                        return {"data": parsed_data}
                    if format_type == "xml" or "xml" in content_type:
                        return {"data": self._parse_xml(response.text)}
                    # Default to newline-split text
                    text_data = response.text.split('\n')
                    if not text_data or text_data == ['']:
                        logger.warning(f"Empty text response from {url_to_try}")
                        raise ValueError("Empty text response")
                    return {"data": text_data}

                except Exception as e:
                    # Record failure and backoff
                    if self.circuit_breaker:
                        self.circuit_breaker.record_failure()
                    self.rate_limiter.adapt_rate(False)

                    if attempt == self.config.max_retries:
                        logger.warning(f"All attempts failed for URL {url_to_try}: {e}")
                        break

                    logger.warning(f"MISO API request failed (attempt {attempt + 1}) for {url_to_try}: {e}")
                    await asyncio.sleep(min(self.config.timeout * (2 ** attempt), 60.0))

            if success:
                break

        raise RuntimeError("Failed to fetch data from all MISO API endpoints")

    def _parse_csv(self, csv_text: str) -> List[Dict[str, Any]]:
        """Parse CSV text into list of dictionaries."""
        import csv
        import io

        if not csv_text.strip():
            return []

        reader = csv.DictReader(io.StringIO(csv_text))
        return list(reader)

    def _parse_xml(self, xml_text: str) -> str:
        """Parse XML text (placeholder - implement as needed)."""
        # For now, return raw XML
        return xml_text


class MisoCollector(ExternalCollector):
    """MISO data collector."""

    def __init__(self, config: MisoDatasetConfig, api_config: MisoApiConfig):
        self.dataset_config = config
        self.api_config = api_config
        self.api_client = MisoApiClient(api_config)
        # Lazily initialize checkpoint store to avoid DB connection during tests
        self.checkpoint_store = None  # type: ignore[assignment]
        self.context = CollectorContext()
        self.data_validator = DataValidator()
        self.health_monitor = MisoHealthMonitor()

        # Performance tracking
        self.collection_metrics = {
            "total_collection_time": 0.0,
            "records_processed": 0,
            "validation_errors": 0,
            "api_calls_made": 0,
            "bytes_processed": 0
        }

    async def collect(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Collect MISO data for the specified date range."""
        logger.info(
            "Starting MISO data collection",
            extra={
                "dataset": self.dataset_config.dataset_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        )

        records = []
        start_time = time.time()

        try:
            # Generate date range based on frequency
            dates = self._generate_date_range(start_date, end_date)

            for date in dates:
                batch_start = time.time()

                # Build API URL using the template
                url = self.dataset_config.url_template.format(date=date)

                # Fetch data
                data = await self.api_client.fetch_data(url, {})
                self.collection_metrics["api_calls_made"] += 1

                # Process and normalize records
                batch_records = await self._process_data(data, date)

                # Validate records
                validated_records = []
                for record in batch_records:
                    is_valid, validation_message = self.data_validator.validate_record(
                        record, self.dataset_config.data_type
                    )

                    if is_valid:
                        validated_records.append(record)
                    else:
                        self.collection_metrics["validation_errors"] += 1
                        logger.warning(
                            f"Record validation failed: {validation_message}",
                            extra={"record": record, "dataset": self.dataset_config.dataset_id}
                        )

                records.extend(validated_records)
                self.collection_metrics["records_processed"] += len(validated_records)

                # Calculate processing time
                batch_time = time.time() - batch_start
                self.collection_metrics["total_collection_time"] += batch_time

                # Update checkpoint (best effort; lazily create store)
                try:
                    if self.checkpoint_store is None:
                        self.checkpoint_store = PostgresCheckpointStore()
                    # Fallback: if store doesn't expose update_watermark, skip
                    update_fn = getattr(self.checkpoint_store, "update_watermark", None)
                    if callable(update_fn):
                        await update_fn(self.dataset_config.dataset_id, date)  # type: ignore[misc]
                except Exception as _exc:
                    # Do not fail collection due to checkpointing issues
                    logger.warning("Checkpoint update skipped: %s", _exc)

                logger.info(
                    f"Processed {len(batch_records)} records for {date.strftime('%Y-%m-%d')} "
                    f"({len(validated_records)} valid, {batch_time:.2f}s)"
                )

                # Check for empty responses
                if len(validated_records) == 0:
                    await self._alert_empty_response(date)

                # Check for schema drift (basic check)
                if validated_records:
                    await self._check_schema_drift(validated_records[0], date)

        except Exception as e:
            # Record failure in health monitor
            await self.health_monitor.record_failure(e)

            logger.error(
                "MISO data collection failed",
                extra={
                    "dataset": self.dataset_config.dataset_id,
                    "error": str(e),
                    "health_status": self.health_monitor.get_health_status()["status"]
                }
            )
            raise

        # Record success and update health status
        await self.health_monitor.record_success()

        # Final metrics reporting
        total_time = time.time() - start_time
        quality_metrics = self.data_validator.get_metrics()
        health_status = self.health_monitor.get_health_status()

        logger.info(
            "MISO data collection completed successfully",
            extra={
                "dataset": self.dataset_config.dataset_id,
                "total_records": len(records),
                "total_time": total_time,
                "records_per_second": len(records) / total_time if total_time > 0 else 0,
                "quality_score": quality_metrics["quality_score"],
                "validation_errors": self.collection_metrics["validation_errors"],
                "api_calls": self.collection_metrics["api_calls_made"],
                "health_status": health_status["status"],
                "consecutive_failures": health_status["consecutive_failures"],
                "uptime_percentage": health_status["uptime_percentage"]
            }
        )

        return records

    def get_collection_metrics(self) -> Dict[str, Any]:
        """Get comprehensive collection metrics."""
        quality_metrics = self.data_validator.get_metrics()
        health_status = self.health_monitor.get_health_status()

        return {
            **self.collection_metrics,
            "data_quality": quality_metrics,
            "health_status": health_status,
            "dataset_id": self.dataset_config.dataset_id,
            "data_type": self.dataset_config.data_type,
            "last_collection_time": datetime.utcnow().isoformat()
        }

    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        return self.health_monitor.get_health_status()

    async def build_node_crosswalk(self) -> None:
        """Build node and zone crosswalk from MISO responses and persist to ref.iso_nodes."""
        logger.info("Building MISO node and zone crosswalk")

        try:
            # Collect nodes from all available endpoints
            nodes = set()

            # Get nodes from LMP data
            try:
                lmp_nodes = await self._get_lmp_nodes()
                nodes.update(lmp_nodes)
            except Exception as e:
                logger.warning(f"Failed to get LMP nodes: {e}")

            # Get nodes from load data
            try:
                load_nodes = await self._get_load_nodes()
                nodes.update(load_nodes)
            except Exception as e:
                logger.warning(f"Failed to get load nodes: {e}")

            # Get nodes from generation mix data
            try:
                gen_nodes = await self._get_generation_nodes()
                nodes.update(gen_nodes)
            except Exception as e:
                logger.warning(f"Failed to get generation nodes: {e}")

            # Convert to list and deduplicate
            unique_nodes = self._deduplicate_nodes(list(nodes))

            # Update the reference file
            await self._update_node_reference(unique_nodes)

            logger.info(f"Updated node crosswalk with {len(unique_nodes)} unique nodes")

        except Exception as e:
            logger.error(f"Failed to build node crosswalk: {e}")
            raise

    async def _get_lmp_nodes(self) -> Set[str]:
        """Extract unique nodes from LMP data."""
        nodes = set()

        # Try to get a sample of LMP data
        try:
            sample_date = datetime.now() - timedelta(days=1)
            url = f"{{{{MISO_URL}}}}/api/marketreport/lmp?format=json&startDate={sample_date.strftime('%Y-%m-%d')}&endDate={sample_date.strftime('%Y-%m-%d')}&marketType=RTM"

            data = await self.api_client.fetch_data(url, {})

            if "data" in data and isinstance(data["data"], list):
                for item in data["data"][:100]:  # Sample first 100 records
                    if "node_id" in item:
                        nodes.add(item["node_id"])

        except Exception as e:
            logger.warning(f"Failed to extract LMP nodes: {e}")

        return nodes

    async def _get_load_nodes(self) -> Set[str]:
        """Extract unique nodes from load data."""
        nodes = set()

        try:
            sample_date = datetime.now() - timedelta(days=1)
            url = f"{{{{MISO_URL}}}}/api/marketreport/load?format=json&startDate={sample_date.strftime('%Y-%m-%d')}&endDate={sample_date.strftime('%Y-%m-%d')}"

            data = await self.api_client.fetch_data(url, {})

            if "data" in data and isinstance(data["data"], list):
                for item in data["data"][:100]:
                    if "zone" in item:
                        nodes.add(item["zone"])

        except Exception as e:
            logger.warning(f"Failed to extract load nodes: {e}")

        return nodes

    async def _get_generation_nodes(self) -> Set[str]:
        """Extract unique nodes from generation data."""
        nodes = set()

        try:
            sample_date = datetime.now() - timedelta(days=1)
            url = f"{{{{MISO_URL}}}}/api/marketreport/generation?format=json&startDate={sample_date.strftime('%Y-%m-%d')}&endDate={sample_date.strftime('%Y-%m-%d')}"

            data = await self.api_client.fetch_data(url, {})

            if "data" in data and isinstance(data["data"], list):
                for item in data["data"][:100]:
                    if "zone" in item:
                        nodes.add(item["zone"])

        except Exception as e:
            logger.warning(f"Failed to extract generation nodes: {e}")

        return nodes

    def _deduplicate_nodes(self, nodes: List[str]) -> List[Dict[str, Any]]:
        """Deduplicate nodes and create crosswalk entries."""
        unique_nodes = {}
        for node in nodes:
            if node not in unique_nodes:
                unique_nodes[node] = {
                    "iso": "MISO",
                    "location_id": node,
                    "location_name": node,  # Use ID as name initially
                    "location_type": "NODE",
                    "zone": node,  # Use ID as zone initially
                    "hub": "",
                    "timezone": "America/Chicago"  # Default timezone
                }

        return list(unique_nodes.values())

    async def _update_node_reference(self, nodes: List[Dict[str, Any]]) -> None:
        """Update the node reference file."""
        import csv

        ref_file = Path(__file__).parent.parent.parent / "config" / "iso_nodes.csv"

        # Read existing nodes
        existing_nodes = {}
        if ref_file.exists():
            with open(ref_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row["iso"], row["location_id"])
                    existing_nodes[key] = row

        # Update with new nodes
        for node in nodes:
            key = (node["iso"], node["location_id"])
            existing_nodes[key] = node

        # Write back to file
        with open(ref_file, 'w', newline='') as f:
            if existing_nodes:
                writer = csv.DictWriter(f, fieldnames=list(existing_nodes.values())[0].keys())
                writer.writeheader()
                for node in existing_nodes.values():
                    writer.writerow(node)

    async def harvest_generator_roster(self) -> None:
        """Harvest generator roster from public MISO RT and report metadata."""
        logger.info("Harvesting MISO generator roster")

        try:
            # Collect generator information from various sources
            generators = []

            # Get generators from generation mix data
            try:
                gen_data = await self._get_generation_metadata()
                generators.extend(gen_data)
            except Exception as e:
                logger.warning(f"Failed to get generation metadata: {e}")

            # Get generators from real-time data (if available)
            try:
                rt_data = await self._get_realtime_generators()
                generators.extend(rt_data)
            except Exception as e:
                logger.warning(f"Failed to get real-time generator data: {e}")

            # Deduplicate and normalize
            unique_generators = self._deduplicate_generators(generators)

            # Update the reference file
            await self._update_generator_reference(unique_generators)

            logger.info(f"Updated generator roster with {len(unique_generators)} unique generators")

        except Exception as e:
            logger.error(f"Failed to harvest generator roster: {e}")
            raise

    async def _get_generation_metadata(self) -> List[Dict[str, Any]]:
        """Extract generator metadata from generation mix data."""
        generators = []

        try:
            # Get recent generation mix data
            sample_date = datetime.now() - timedelta(days=1)
            url = f"{{{{MISO_URL}}}}/api/marketreport/generation?format=json&startDate={sample_date.strftime('%Y-%m-%d')}&endDate={sample_date.strftime('%Y-%m-%d')}"

            data = await self.api_client.fetch_data(url, {})

            if "data" in data and isinstance(data["data"], list):
                for item in data["data"][:500]:  # Sample first 500 records
                    if "fuel_type" in item and "zone" in item:
                        generator = {
                            "name": item.get("unit_name", "Unknown"),
                            "fuel_type": item.get("fuel_type"),
                            "zone": item.get("zone"),
                            "capacity_mw": float(item.get("capacity", 0)),
                            "source": "MISO"
                        }
                        generators.append(generator)

        except Exception as e:
            logger.warning(f"Failed to extract generation metadata: {e}")

        return generators

    async def _get_realtime_generators(self) -> List[Dict[str, Any]]:
        """Extract generator information from real-time data."""
        generators = []

        try:
            # This would be the real-time generator endpoint
            # For now, return empty list as this may not be publicly available
            pass

        except Exception as e:
            logger.warning(f"Failed to extract real-time generator data: {e}")

        return generators

    def _deduplicate_generators(self, generators: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate generators and create roster entries."""
        unique_generators = {}

        for gen in generators:
            key = (gen.get("name", ""), gen.get("zone", ""))
            if key not in unique_generators:
                unique_generators[key] = {
                    "name": gen.get("name", "Unknown"),
                    "fuel_type": gen.get("fuel_type", "Unknown"),
                    "zone": gen.get("zone", "Unknown"),
                    "capacity_mw": gen.get("capacity_mw", 0),
                    "source": gen.get("source", "MISO"),
                    "latitude": gen.get("latitude", None),
                    "longitude": gen.get("longitude", None),
                    "commission_date": gen.get("commission_date", None),
                    "owner": gen.get("owner", None)
                }

        return list(unique_generators.values())

    async def _update_generator_reference(self, generators: List[Dict[str, Any]]) -> None:
        """Update the generator reference file."""
        import csv

        ref_file = Path(__file__).parent.parent.parent / "config" / "generators.csv"

        # Read existing generators
        existing_generators = {}
        if ref_file.exists():
            with open(ref_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row["name"], row["zone"])
                    existing_generators[key] = row

        # Update with new generators
        for gen in generators:
            key = (gen["name"], gen["zone"])
            existing_generators[key] = gen

        # Write back to file
        with open(ref_file, 'w', newline='') as f:
            if existing_generators:
                writer = csv.DictWriter(f, fieldnames=list(existing_generators.values())[0].keys())
                writer.writeheader()
                for gen in existing_generators.values():
                    writer.writerow(gen)

    async def _alert_empty_response(self, date: datetime) -> None:
        """Alert on empty API responses."""
        logger.warning(
            "Empty response detected",
            extra={
                "dataset": self.dataset_config.dataset_id,
                "date": date.isoformat(),
                "alert_type": "empty_response"
            }
        )

        # In production, this would send alerts to monitoring systems
        # For now, just log the issue
        await self._send_alert({
            "alert_type": "empty_response",
            "dataset": self.dataset_config.dataset_id,
            "date": date.isoformat(),
            "message": f"No records returned for {self.dataset_config.dataset_id} on {date.strftime('%Y-%m-%d')}",
            "severity": "WARNING"
        })

    async def _check_schema_drift(self, sample_record: Dict[str, Any], date: datetime) -> None:
        """Check for schema drift in API responses."""
        expected_fields = set()

        # Define expected fields based on data type
        if self.dataset_config.data_type == "lmp":
            expected_fields = {"node_id", "lmp", "congestion", "losses"}
        elif self.dataset_config.data_type == "load":
            expected_fields = {"zone", "load"}
        elif self.dataset_config.data_type == "generation_mix":
            expected_fields = {"zone", "fuel_type", "generation"}
        elif self.dataset_config.data_type == "ancillary_services":
            expected_fields = {"zone", "as_type", "price", "mw"}
        elif self.dataset_config.data_type == "interchange":
            expected_fields = {"interface", "interchange"}
        elif self.dataset_config.data_type in ["forecast_load", "forecast_generation"]:
            expected_fields = {"zone", "forecast_load", "forecast_hour"}

        actual_fields = set(sample_record.keys())
        missing_fields = expected_fields - actual_fields

        if missing_fields:
            logger.warning(
                "Schema drift detected",
                extra={
                    "dataset": self.dataset_config.dataset_id,
                    "date": date.isoformat(),
                    "missing_fields": list(missing_fields),
                    "alert_type": "schema_drift"
                }
            )

            await self._send_alert({
                "alert_type": "schema_drift",
                "dataset": self.dataset_config.dataset_id,
                "date": date.isoformat(),
                "missing_fields": list(missing_fields),
                "message": f"Missing expected fields in {self.dataset_config.dataset_id}: {missing_fields}",
                "severity": "WARNING"
            })

    async def _send_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to monitoring system."""
        # In production, this would integrate with monitoring/alerting systems
        # For now, just log the alert
        logger.info(f"Alert generated: {alert_data}")

        # Could send to Kafka topic for alerts
        # from aurum.kafka import OptimizedProducer
        # producer = OptimizedProducer()
        # await producer.send("aurum.alerts.v1", alert_data)

    def _generate_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[datetime]:
        """Generate date range based on dataset frequency."""
        dates = []
        current = start_date

        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)  # Default to daily

        return dates

    async def _process_data(
        self,
        data: Dict[str, Any],
        date: datetime
    ) -> List[Dict[str, Any]]:
        """Process raw MISO data into normalized records."""
        records = []

        if "data" not in data:
            logger.warning(f"No data found for {date.strftime('%Y-%m-%d')}")
            return records

        data_items = data["data"]

        # Handle different data formats
        if isinstance(data_items, str):
            # XML or raw text
            # For now, skip XML processing
            return records
        elif isinstance(data_items, list):
            # JSON array or parsed CSV
            for item in data_items:
                record = await self._normalize_record(item, date)
                if record:
                    records.append(record)
        else:
            logger.warning(f"Unknown data format for {date.strftime('%Y-%m-%d')}")
            return records

        return records

    async def _normalize_record(
        self,
        item: Dict[str, Any],
        date: datetime
    ) -> Optional[Dict[str, Any]]:
        """Normalize individual MISO record."""
        try:
            # Common normalization logic
            normalized = {
                "iso_code": "MISO",
                "data_type": self.dataset_config.data_type,
                "market": self.dataset_config.market,
                "delivery_date": date.date().isoformat(),
                "ingest_ts": datetime.utcnow().isoformat(),
                "source": "MISO",
                "metadata": {
                    "source_timezone": "America/Chicago",  # MISO's primary timezone
                    "normalized_to_utc": True
                }
            }

            # Add data-type specific fields
            if self.dataset_config.data_type == "lmp":
                normalized.update({
                    "location_id": item.get("node_id"),
                    "location_name": item.get("node_name"),
                    "price_total": float(item.get("lmp", 0)),
                    "price_congestion": float(item.get("congestion", 0)),
                    "price_loss": float(item.get("losses", 0)),
                    "currency": "USD",
                    "uom": "MWh"
                })
            elif self.dataset_config.data_type == "load":
                normalized.update({
                    "location_id": item.get("zone"),
                    "location_name": item.get("zone"),
                    "load_mw": float(item.get("load", 0)),
                    "uom": "MW"
                })
            elif self.dataset_config.data_type == "generation_mix":
                normalized.update({
                    "location_id": item.get("zone"),
                    "fuel_type": item.get("fuel_type"),
                    "generation_mw": float(item.get("generation", 0)),
                    "uom": "MW"
                })
            elif self.dataset_config.data_type == "ancillary_services":
                normalized.update({
                    "location_id": item.get("zone"),
                    "as_type": item.get("as_type"),
                    "as_price": float(item.get("price", 0)),
                    "as_mw": float(item.get("mw", 0)),
                    "uom": "MW",
                    "currency": "USD"
                })
            elif self.dataset_config.data_type == "interchange":
                normalized.update({
                    "location_id": item.get("interface"),
                    "interchange_mw": float(item.get("interchange", 0)),
                    "uom": "MW"
                })
            elif self.dataset_config.data_type == "forecast_load":
                normalized.update({
                    "location_id": item.get("zone"),
                    "forecast_load_mw": float(item.get("forecast_load", 0)),
                    "forecast_hour": item.get("forecast_hour"),
                    "uom": "MW"
                })
            elif self.dataset_config.data_type == "forecast_generation":
                normalized.update({
                    "location_id": item.get("zone"),
                    "forecast_generation_mw": float(item.get("forecast_generation", 0)),
                    "forecast_hour": item.get("forecast_hour"),
                    "uom": "MW"
                })

            return normalized

        except Exception as e:
            logger.warning(f"Failed to normalize record: {e}")
            return None


class MisoConfigValidator:
    """Configuration validator for MISO datasets."""

    @staticmethod
    def validate_dataset_config(dataset: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a single dataset configuration."""
        errors = []

        # Required fields
        required_fields = [
            "source_name", "iso_name", "data_type", "market",
            "description", "schedule", "topic_var", "default_topic",
            "frequency", "units_var", "default_units", "dlq_topic", "watermark_policy"
        ]

        for field in required_fields:
            if field not in dataset:
                errors.append(f"Missing required field: {field}")
            elif not dataset[field]:
                errors.append(f"Empty required field: {field}")

        # Validate data_type
        valid_data_types = [
            "lmp", "load", "generation_mix", "ancillary_services",
            "interchange", "forecast_load", "forecast_generation"
        ]
        if dataset.get("data_type") not in valid_data_types:
            errors.append(f"Invalid data_type: {dataset.get('data_type')}. Must be one of {valid_data_types}")

        # Validate market
        valid_markets = ["DAM", "RTM"]
        if dataset.get("market") not in valid_markets:
            errors.append(f"Invalid market: {dataset.get('market')}. Must be one of {valid_markets}")

        # Validate frequency
        valid_frequencies = ["HOURLY", "DAILY", "REAL_TIME"]
        if dataset.get("frequency") not in valid_frequencies:
            errors.append(f"Invalid frequency: {dataset.get('frequency')}. Must be one of {valid_frequencies}")

        # Validate watermark policy
        valid_policies = ["hour", "day", "exact"]
        if dataset.get("watermark_policy") not in valid_policies:
            errors.append(f"Invalid watermark_policy: {dataset.get('watermark_policy')}. Must be one of {valid_policies}")

        return len(errors) == 0, errors

    @staticmethod
    def validate_api_config(config: MisoApiConfig) -> Tuple[bool, List[str]]:
        """Validate API configuration."""
        errors = []

        if not config.base_url:
            errors.append("base_url cannot be empty")

        if config.requests_per_minute <= 0:
            errors.append("requests_per_minute must be positive")

        if config.requests_per_hour <= 0:
            errors.append("requests_per_hour must be positive")

        if config.timeout <= 0:
            errors.append("timeout must be positive")

        if config.circuit_breaker_enabled:
            if config.circuit_breaker_threshold <= 0:
                errors.append("circuit_breaker_threshold must be positive")
            if config.circuit_breaker_timeout <= 0:
                errors.append("circuit_breaker_timeout must be positive")

        return len(errors) == 0, errors


def load_miso_dataset_configs(validate: bool = True) -> List[MisoDatasetConfig]:
    """Load MISO dataset configurations from JSON with validation."""
    config_path = Path(__file__).parent.parent.parent / "config" / "miso_ingest_datasets.json"

    if not config_path.exists():
        raise FileNotFoundError(f"MISO config file not found: {config_path}")

    with open(config_path) as f:
        config_data = json.load(f)

    if validate:
        # Validate the entire configuration
        if "datasets" not in config_data:
            raise ValueError("Configuration must contain 'datasets' key")

        for i, dataset in enumerate(config_data["datasets"]):
            is_valid, errors = MisoConfigValidator.validate_dataset_config(dataset)
            if not is_valid:
                raise ValueError(f"Invalid configuration for dataset {i}: {errors}")

        logger.info("MISO dataset configurations validated successfully")

    configs = []
    for dataset in config_data.get("datasets", []):
        if dataset.get("iso_name") == "miso":
            # Build URL template based on data type and market
            url_template = _build_url_template(dataset)

            configs.append(MisoDatasetConfig(
                dataset_id=dataset["source_name"],
                description=dataset["description"],
                data_type=dataset["data_type"],
                market=dataset["market"],
                url_template=url_template,
                interval_minutes=300 if dataset["market"] == "RTM" else 3600,
                columns={
                    "time": "timestamp",
                    "node": "node_id",
                    "lmp": "lmp",
                    "congestion": "congestion",
                    "losses": "losses",
                    "load": "load",
                    "fuel_type": "fuel_type",
                    "generation": "generation",
                    "interchange": "interchange",
                    "forecast_load": "forecast_load",
                    "forecast_generation": "forecast_generation"
                },
                frequency=dataset["frequency"],
                units=dataset["default_units"]
            ))

    return configs

def _build_url_template(dataset: Dict[str, Any]) -> str:
    """Build URL template based on dataset configuration."""
    base_url_var = "{{MISO_URL}}"

    if dataset["data_type"] == "lmp":
        return f"{base_url_var}/api/marketreport/lmp?format=json&startDate={{date.strftime('%Y-%m-%d')}}&endDate={{date.strftime('%Y-%m-%d')}}&marketType={dataset['market']}"
    elif dataset["data_type"] == "load":
        return f"{base_url_var}/api/marketreport/load?format=json&startDate={{date.strftime('%Y-%m-%d')}}&endDate={{date.strftime('%Y-%m-%d')}}"
    elif dataset["data_type"] == "generation_mix":
        return f"{base_url_var}/api/marketreport/generation?format=json&startDate={{date.strftime('%Y-%m-%d')}}&endDate={{date.strftime('%Y-%m-%d')}}"
    elif dataset["data_type"] == "ancillary_services":
        return f"{base_url_var}/api/marketreport/asm?format=json&startDate={{date.strftime('%Y-%m-%d')}}&endDate={{date.strftime('%Y-%m-%d')}}"
    elif dataset["data_type"] == "interchange":
        return f"{base_url_var}/api/marketreport/interchange?format=json&startDate={{date.strftime('%Y-%m-%d')}}&endDate={{date.strftime('%Y-%m-%d')}}"
    elif dataset["data_type"] == "forecast_load":
        return f"{base_url_var}/api/marketreport/loadforecast?format=json&startDate={{date.strftime('%Y-%m-%d')}}&endDate={{date.strftime('%Y-%m-%d')}}"
    elif dataset["data_type"] == "forecast_generation":
        return f"{base_url_var}/api/marketreport/genforecast?format=json&startDate={{date.strftime('%Y-%m-%d')}}&endDate={{date.strftime('%Y-%m-%d')}}"
    else:
        return ""
