from __future__ import annotations

import hashlib
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pandas as pd
from opentelemetry import propagate

from aurum.api.config import CacheConfig
from aurum.api.scenario_models import ScenarioRunPriority, ScenarioRunStatus
from aurum.api.scenario_service import STORE as ScenarioStore
from aurum.api.service import invalidate_scenario_outputs_cache
from aurum.parsers.iceberg_writer import write_scenario_output
from aurum.scenarios.models import DriverType
from aurum.telemetry import configure_telemetry, get_tracer
from aurum.telemetry.context import get_request_id, set_request_id, reset_request_id

LOGGER = logging.getLogger(__name__)
EPOCH = date(1970, 1, 1)
DEFAULT_TENOR_WEIGHTS = {
    "MONTHLY": 1.0,
    "QUARTER": 1.05,
    "CALENDAR": 1.08,
}
TENOR_ORDER = ["MONTHLY", "QUARTER", "CALENDAR"]
FUEL_CURVE_WEIGHTS = {
    "natural_gas": 0.004,
    "coal": -0.003,
    "co2": 0.006,
    "oil": 0.005,
}
FLEET_RAMP_MONTHS = 24

try:  # Optional Prometheus metrics support
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Histogram,
        Gauge,
        REGISTRY,
        generate_latest,
        Info
    )  # type: ignore
except Exception:  # pragma: no cover - dependency not installed during unit tests
    Counter = None  # type: ignore[assignment]
    Histogram = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    Info = None  # type: ignore[assignment]
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    REGISTRY = None  # type: ignore[assignment]
    generate_latest = None  # type: ignore[assignment]

# Core request lifecycle metrics
REQUESTS_TOTAL = Counter("aurum_scenario_requests_total", "Scenario requests consumed") if Counter else None
REQUEST_SUCCESS = Counter("aurum_scenario_requests_success_total", "Successful scenario requests") if Counter else None
REQUEST_FAILURE = Counter("aurum_scenario_requests_failure_total", "Failed scenario requests") if Counter else None
REQUEST_DLQ = Counter("aurum_scenario_requests_dlq_total", "Scenario requests routed to DLQ") if Counter else None

# Processing duration histogram with more granular buckets
PROCESS_DURATION = Histogram(
    "aurum_scenario_processing_seconds",
    "Scenario request processing duration",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
    labelnames=["scenario_type", "tenant_id", "status"]
) if Histogram else None

# Queue and worker state metrics
QUEUE_SIZE = Gauge("aurum_scenario_queue_size", "Number of messages in processing queue") if Gauge else None
ACTIVE_WORKERS = Gauge("aurum_scenario_active_workers", "Number of active worker threads") if Gauge else None
CANCELLED_RUNS = Counter("aurum_scenario_cancelled_runs_total", "Total cancelled scenario runs") if Counter else None
TIMEOUT_RUNS = Counter("aurum_scenario_timeout_runs_total", "Total timed out scenario runs") if Counter else None

# Performance and resource metrics
MEMORY_USAGE = Gauge("aurum_scenario_memory_mb", "Memory usage in MB") if Gauge else None
CPU_USAGE = Gauge("aurum_scenario_cpu_percent", "CPU usage percentage") if Gauge else None

# Retry and backoff metrics
RETRY_ATTEMPTS = Counter("aurum_scenario_retry_attempts_total", "Total retry attempts") if Counter else None
RETRY_SUCCESS = Counter("aurum_scenario_retry_success_total", "Successful retries") if Counter else None
RETRY_EXHAUSTED = Counter("aurum_scenario_retry_exhausted_total", "Retries exhausted") if Counter else None

# Scenario type breakdown
SCENARIO_TYPE_COUNTER = Counter(
    "aurum_scenario_by_type_total",
    "Scenario requests by type",
    labelnames=["scenario_type", "tenant_id"]
) if Counter else None

# Worker info
WORKER_INFO = Info("aurum_scenario_worker_info", "Worker information") if Info else None

# Processing rate metrics
REQUESTS_PER_SECOND = Counter("aurum_scenario_requests_rate_total", "Request processing rate") if Counter else None
PROCESSING_RATE = Histogram(
    "aurum_scenario_processing_rate_per_second",
    "Processing rate (requests/second)",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0)
) if Histogram else None

# Poison pill and circuit breaker metrics
POISON_PILL_DETECTED = Counter("aurum_scenario_poison_pill_total", "Poison pills detected and sent to DLQ") if Counter else None
CIRCUIT_BREAKER_OPENED = Counter("aurum_scenario_circuit_breaker_opened_total", "Circuit breakers opened") if Counter else None
CIRCUIT_BREAKER_CLOSED = Counter("aurum_scenario_circuit_breaker_closed_total", "Circuit breakers closed") if Counter else None
CIRCUIT_BREAKER_REJECTED = Counter("aurum_scenario_circuit_breaker_rejected_total", "Messages rejected by circuit breaker") if Counter else None

# Backoff metrics
BACKOFF_DELAY = Histogram(
    "aurum_scenario_backoff_delay_seconds",
    "Backoff delay between retry attempts",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
    labelnames=["attempt"]
) if Histogram else None


SHUTDOWN_EVENT = threading.Event()
READINESS_EVENT = threading.Event()
_SIGNAL_INSTALLED = threading.Event()

# Poison-pill detection and circuit breaker state
_POISON_PILL_CACHE: Dict[str, Dict[str, Any]] = {}
_POISON_PILL_LOCK = threading.Lock()
_CIRCUIT_BREAKER_STATE: Dict[str, Dict[str, Any]] = {}
_CIRCUIT_BREAKER_LOCK = threading.Lock()


def _start_metrics_updater() -> None:
    """Start background task to update resource usage metrics."""
    if not MEMORY_USAGE and not CPU_USAGE:
        return

    try:
        import psutil
        process = psutil.Process()
    except ImportError:
        return

    def update_metrics():
        while not SHUTDOWN_EVENT.is_set():
            try:
                if MEMORY_USAGE:
                    try:
                        memory_mb = process.memory_info().rss / 1024 / 1024
                        MEMORY_USAGE.set(memory_mb)
                    except Exception:
                        pass

                if CPU_USAGE:
                    try:
                        cpu_percent = process.cpu_percent()
                        CPU_USAGE.set(cpu_percent)
                    except Exception:
                        pass

                # Sleep for 30 seconds
                SHUTDOWN_EVENT.wait(30)
            except Exception:
                pass

    thread = threading.Thread(target=update_metrics, name="metrics-updater", daemon=True)
    thread.start()


def _is_poison_pill(message_key: str, error_type: str) -> bool:
    """Check if a message is a poison pill that should be sent directly to DLQ.

    Args:
        message_key: Unique identifier for the message
        error_type: Type of error encountered

    Returns:
        True if message should be treated as poison pill
    """
    with _POISON_PILL_LOCK:
        current_time = time.time()
        cache_key = f"{message_key}:{error_type}"

        if cache_key not in _POISON_PILL_CACHE:
            _POISON_PILL_CACHE[cache_key] = {
                "failure_count": 0,
                "first_failure": current_time,
                "last_failure": current_time,
            }

        entry = _POISON_PILL_CACHE[cache_key]
        entry["failure_count"] += 1
        entry["last_failure"] = current_time

        # Check if this message type has failed too many times recently
        # Consider it poison if:
        # - More than 5 failures in the last 10 minutes, OR
        # - More than 10 failures total
        recent_failures = sum(
            1 for k, v in _POISON_PILL_CACHE.items()
            if k.endswith(f":{error_type}") and (current_time - v["last_failure"]) < 600  # 10 minutes
        )

        if (entry["failure_count"] > 10 or recent_failures > 5):
            LOGGER.warning(
                "Poison pill detected for %s:%s (failures: %d, recent: %d)",
                message_key,
                error_type,
                entry["failure_count"],
                recent_failures,
            )
            return True

        return False


def _record_failure_for_circuit_breaker(error_type: str, tenant_id: str) -> bool:
    """Record a failure and check if circuit breaker should trigger.

    Args:
        error_type: Type of error encountered
        tenant_id: Tenant identifier

    Returns:
        True if circuit breaker should trigger
    """
    with _CIRCUIT_BREAKER_LOCK:
        current_time = time.time()
        cb_key = f"{error_type}:{tenant_id}"

        if cb_key not in _CIRCUIT_BREAKER_STATE:
            _CIRCUIT_BREAKER_STATE[cb_key] = {
                "failure_count": 0,
                "last_failure": 0,
                "circuit_open": False,
                "circuit_opened_at": 0,
            }

        state = _CIRCUIT_BREAKER_STATE[cb_key]
        state["failure_count"] += 1
        state["last_failure"] = current_time

        # Circuit breaker logic:
        # - If 3 failures in 1 minute, open circuit for 2 minutes
        # - If 10 failures in 5 minutes, open circuit for 10 minutes
        if (state["failure_count"] >= 3 and (current_time - state["last_failure"]) < 60) or \
           (state["failure_count"] >= 10 and (current_time - state["last_failure"]) < 300):

            if not state["circuit_open"]:
                state["circuit_open"] = True
                state["circuit_opened_at"] = current_time
                LOGGER.warning(
                    "Circuit breaker opened for %s:%s (failures: %d)",
                    error_type,
                    tenant_id,
                    state["failure_count"]
                )
                if CIRCUIT_BREAKER_OPENED:
                    CIRCUIT_BREAKER_OPENED.inc()
                return True

        return False


def _is_circuit_open(error_type: str, tenant_id: str) -> bool:
    """Check if circuit breaker is open for this error type and tenant."""
    with _CIRCUIT_BREAKER_LOCK:
        cb_key = f"{error_type}:{tenant_id}"
        state = _CIRCUIT_BREAKER_STATE.get(cb_key)

        if not state or not state["circuit_open"]:
            return False

        current_time = time.time()
        circuit_duration = current_time - state["circuit_opened_at"]

        # Close circuit after appropriate timeout
        if circuit_duration > 600:  # 10 minutes max
            state["circuit_open"] = False
            state["failure_count"] = 0  # Reset failure count
            LOGGER.info("Circuit breaker closed for %s:%s", error_type, tenant_id)
            if CIRCUIT_BREAKER_CLOSED:
                CIRCUIT_BREAKER_CLOSED.inc()
            return False

        return True


def _install_signal_handlers() -> None:
    if _SIGNAL_INSTALLED.is_set():
        return

    def _handle_signal(signum, _frame):
        if not SHUTDOWN_EVENT.is_set():
            try:
                signal_name = signal.Signals(signum).name  # type: ignore[attr-defined]
            except Exception:  # pragma: no cover - defensive fallback
                signal_name = str(signum)
            LOGGER.info("Received %s; initiating graceful shutdown", signal_name)
            SHUTDOWN_EVENT.set()

    for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None)):
        if sig is None:
            continue
        try:
            signal.signal(sig, _handle_signal)
        except ValueError:
            LOGGER.debug("Unable to register handler for signal %s", sig, exc_info=True)
    _SIGNAL_INSTALLED.set()


def _start_probe_server(host: str, port: int):
    class _ProbeHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # pragma: no cover - suppress noisy logs
            LOGGER.debug("probe[%s]: " + format, self.address_string(), *args)

        def _write_response(self, status: int, body: bytes, content_type: str = "text/plain; charset=utf-8") -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # pragma: no cover - simple IO handler
            path = self.path.split("?", 1)[0]
            if path in {"/health", "/healthz"}:
                self._write_response(200, b"ok")
                return
            if path in {"/ready", "/readyz"}:
                if READINESS_EVENT.is_set() and not SHUTDOWN_EVENT.is_set():
                    self._write_response(200, b"ready")
                else:
                    self._write_response(503, b"not ready")
                return
            if path == "/metrics" and generate_latest and REGISTRY:
                output = generate_latest(REGISTRY)
                self._write_response(200, output, CONTENT_TYPE_LATEST)
                return
            if path == "/metrics":
                self._write_response(503, b"metrics unavailable")
                return
            self._write_response(404, b"not found")

    server = ThreadingHTTPServer((host, port), _ProbeHandler)
    thread = threading.Thread(target=server.serve_forever, name="worker-probes", daemon=True)
    thread.start()
    LOGGER.info("Probe server listening on %s:%s", host, port)
    return server


configure_telemetry("aurum-scenario-worker", enable_requests=False)
TRACER = get_tracer("aurum.scenarios.worker")


@dataclass
class ScenarioRequest:
    scenario_id: str
    tenant_id: str
    assumptions: list[dict]
    asof_date: Optional[date] = None
    curve_def_ids: list[str] | None = field(default=None)

    @classmethod
    def from_message(cls, payload: dict) -> "ScenarioRequest":
        scenario_id = str(payload.get("scenario_id") or "")
        tenant_id = str(payload.get("tenant_id") or "")
        assumptions = payload.get("assumptions") or []
        curve_defs_raw = payload.get("curve_def_ids")
        curve_def_ids: list[str] | None
        if isinstance(curve_defs_raw, Sequence) and not isinstance(curve_defs_raw, (str, bytes)):
            curve_def_ids = [str(item) for item in curve_defs_raw]
        elif curve_defs_raw:
            curve_def_ids = [str(curve_defs_raw)]
        else:
            curve_def_ids = None

        asof_raw = payload.get("asof_date")
        asof_date: Optional[date] = None
        if isinstance(asof_raw, date):
            asof_date = asof_raw
        elif isinstance(asof_raw, int):
            asof_date = EPOCH + timedelta(days=asof_raw)
        elif isinstance(asof_raw, str) and asof_raw:
            try:
                asof_date = date.fromisoformat(asof_raw)
            except ValueError:
                asof_date = None

        return cls(
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            assumptions=assumptions,
            asof_date=asof_date,
            curve_def_ids=curve_def_ids,
        )


class _KafkaHeaderGetter:
    @staticmethod
    def get(carrier: Dict[str, list[str]], key: str) -> list[str]:
        return carrier.get(key.lower(), [])

    @staticmethod
    def keys(carrier: Dict[str, list[str]]):  # type: ignore[override]
        return carrier.keys()


_HEADER_GETTER = _KafkaHeaderGetter()


def _decode_headers(raw_headers) -> Dict[str, list[str]]:
    carrier: Dict[str, list[str]] = {}
    if not raw_headers:
        return carrier
    for key, value in raw_headers:
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="ignore")
        key_lc = str(key).lower()
        if value is None:
            continue
        if isinstance(value, bytes):
            value_str = value.decode("utf-8", errors="ignore")
        else:
            value_str = str(value)
        carrier.setdefault(key_lc, []).append(value_str)
    return carrier


def _build_produce_headers(run_id: Optional[str], request_id: Optional[str]) -> list[tuple[str, bytes]]:
    headers: list[tuple[str, bytes]] = []
    if run_id:
        headers.append(("run_id", run_id.encode("utf-8")))
    if request_id:
        headers.append(("x-request-id", request_id.encode("utf-8")))
    carrier: Dict[str, str] = {}
    propagate.inject(carrier)
    for key, value in carrier.items():
        headers.append((key, value.encode("utf-8")))
    return headers


def _default_tenor_weights() -> dict[str, float]:
    return dict(DEFAULT_TENOR_WEIGHTS)


@dataclass(frozen=True)
class WorkerSettings:
    base_value: float = 50.0
    policy_weight: float = 0.1
    load_weight: float = 0.05
    tenor_multipliers: dict[str, float] = field(default_factory=_default_tenor_weights)

    @classmethod
    def from_env(cls) -> "WorkerSettings":
        def _float_env(name: str, default: float) -> float:
            raw = os.getenv(name)
            if raw is None or raw.strip() == "":
                return default
            try:
                return float(raw)
            except ValueError:
                LOGGER.warning("Invalid float override for %s: %s", name, raw)
                return default

        multipliers = _parse_tenor_weights(os.getenv("AURUM_SCENARIO_TENOR_WEIGHTS"))
        return cls(
            base_value=_float_env("AURUM_SCENARIO_BASE_VALUE", 50.0),
            policy_weight=_float_env("AURUM_SCENARIO_POLICY_WEIGHT", 0.1),
            load_weight=_float_env("AURUM_SCENARIO_LOAD_WEIGHT", 0.05),
            tenor_multipliers=multipliers,
        )


def _parse_tenor_weights(raw: Optional[str]) -> dict[str, float]:
    weights = _default_tenor_weights()
    if not raw:
        return weights
    entries = [segment.strip() for segment in raw.split(",") if segment.strip()]
    for entry in entries:
        if ":" not in entry:
            LOGGER.warning("Ignoring malformed tenor weight entry: %s", entry)
            continue
        key, value = entry.split(":", 1)
        key = key.strip().upper()
        try:
            weights[key] = float(value.strip())
        except ValueError:
            LOGGER.warning("Ignoring tenor weight with non-numeric value: %s", entry)
    return weights


def _load_avro_schema(path: str):  # pragma: no cover - integration path
    from confluent_kafka import avro  # type: ignore

    return avro.load(path)


def _producer(bootstrap: str, schema_registry_url: str):  # pragma: no cover - integration path
    from confluent_kafka.avro import AvroProducer  # type: ignore

    return AvroProducer({"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry_url})


def _consumer(bootstrap: str, group_id: str, schema_registry_url: str):  # pragma: no cover - integration path
    """Create an AvroConsumer to decode records using Schema Registry."""
    try:
        from confluent_kafka.avro import AvroConsumer  # type: ignore
    except Exception as exc:  # pragma: no cover - import failure at runtime only
        raise RuntimeError("AvroConsumer not available - install confluent-kafka[avro]") from exc

    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "schema.registry.url": schema_registry_url,
    }
    return AvroConsumer(conf)


def _schema_path(filename: str) -> str:
    base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    return os.path.join(base, "kafka", "schemas", filename)


def _maybe_emit_dlq(
    *,
    scenario_id: str,
    tenant_id: str,
    run_id: str | None,
    error: Exception,
) -> None:
    topic = os.getenv("AURUM_DLQ_TOPIC")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not topic or not bootstrap:
        return
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    try:  # pragma: no cover - requires confluent stack
        from confluent_kafka import avro  # type: ignore
        from confluent_kafka.avro import AvroProducer  # type: ignore
    except Exception:
        LOGGER.debug("DLQ emission skipped; confluent-kafka[avro] not available", exc_info=True)
        return

    dlq_schema = avro.load(_schema_path("ingest.error.v1.avsc"))
    producer = AvroProducer(
        {"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry_url},
        default_value_schema=dlq_schema,
    )

    context = {
        "component": "scenario-worker",
        "scenario_id": scenario_id,
        "tenant_id": tenant_id,
        "error_type": error.__class__.__name__,
        "error_module": error.__class__.__module__,
        "worker_host": os.getenv("HOSTNAME", "unknown"),
        "attempts": getattr(error, "attempts", "unknown") if hasattr(error, "attempts") else "unknown",
    }
    if run_id:
        context["run_id"] = run_id

    # Add stack trace if available
    stack_info = None
    try:
        import traceback
        stack_info = traceback.format_exception(type(error), error, error.__traceback__)
    except Exception:
        pass

    payload = {
        "source": "aurum.scenario.worker",
        "error_message": str(error),
        "error_details": {
            "type": error.__class__.__name__,
            "module": error.__class__.__module__,
            "args": error.args,
            "str": str(error),
        },
        "context": context,
        "ingest_ts": _timestamp_micros(datetime.now(timezone.utc)),
        "processing_metadata": {
            "worker_version": "1.0.0",
            "processing_duration_seconds": getattr(error, "processing_duration", 0) if hasattr(error, "processing_duration") else 0,
            "retry_attempts": getattr(error, "retry_attempts", 0) if hasattr(error, "retry_attempts") else 0,
        },
        "stack_trace": stack_info,
    }

    headers = _build_produce_headers(run_id, get_request_id())

    try:  # pragma: no cover - external system interaction
        producer.produce(topic=topic, value=payload, headers=headers)
        producer.flush(2)
        if REQUEST_DLQ:
            REQUEST_DLQ.inc()
    except Exception:
        LOGGER.debug("Failed to emit DLQ message", exc_info=True)


def _date_to_days(value: date) -> int:
    return (value - EPOCH).days


def _timestamp_micros(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def _current_ts() -> datetime:
    return datetime.now(timezone.utc)


def _backoff_delay(attempt: int, base_seconds: float, jitter: bool = True) -> float:
    """Calculate exponential backoff delay with optional jitter.

    Args:
        attempt: The current attempt number (1-based)
        base_seconds: Base delay in seconds
        jitter: Whether to add random jitter to prevent thundering herd

    Returns:
        Delay in seconds before next retry
    """
    attempt = max(attempt, 1)

    # Calculate exponential backoff
    delay = base_seconds * (2 ** (attempt - 1))

    # Add jitter to prevent thundering herd (up to 25% randomization)
    if jitter:
        import random
        jitter_factor = random.uniform(0.75, 1.25)
        delay = delay * jitter_factor

    # Cap the maximum delay at 5 minutes
    max_delay = 300.0
    return min(delay, max_delay)


def _resolve_curve_key(request: ScenarioRequest, scenario_id: str, tenant_id: str) -> str:
    if request.curve_def_ids:
        return request.curve_def_ids[0]
    seed = f"{tenant_id}:{scenario_id}:{request.asof_date or ''}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]


def _load_base_mid(curve_key: str, asof: Optional[date]) -> Optional[float]:
    table_name = os.getenv("AURUM_CURVE_LATEST_TABLE", "iceberg.market.curve_observation_latest")
    branch = os.getenv("AURUM_ICEBERG_BRANCH", "main")
    catalog_name = os.getenv("AURUM_ICEBERG_CATALOG", "nessie")
    warehouse = os.getenv("AURUM_S3_WAREHOUSE", "s3://aurum/curated/iceberg")
    props = {
        "uri": os.getenv("AURUM_NESSIE_URI", "http://nessie:19121/api/v1"),
        "warehouse": warehouse,
        "s3.endpoint": os.getenv("AURUM_S3_ENDPOINT"),
        "s3.access-key-id": os.getenv("AURUM_S3_ACCESS_KEY"),
        "s3.secret-access-key": os.getenv("AURUM_S3_SECRET_KEY"),
        "nessie.ref": branch,
    }
    props = {k: v for k, v in props.items() if v is not None}
    table_identifier = table_name if "@" in table_name else f"{table_name}@{branch}"
    try:
        from pyiceberg.catalog import load_catalog  # type: ignore
        from pyiceberg.expressions import And, EqualTo  # type: ignore
    except ModuleNotFoundError:
        return None

    try:
        catalog = load_catalog(catalog_name, **props)
        table = catalog.load_table(table_identifier)
        expr = None
        if curve_key:
            expr = EqualTo("curve_key", curve_key)
        if asof is not None:
            filter_asof = EqualTo("asof_date", asof)
            expr = filter_asof if expr is None else And(expr, filter_asof)
        scan = table.scan(row_filter=expr) if expr is not None else table.scan()
        if hasattr(scan, "limit"):
            scan = scan.limit(1000)
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas()
    except Exception as exc:  # pragma: no cover - integration failures
        LOGGER.debug("Failed to load base curve mid: %s", exc)
        return None

    if df.empty:
        return None
    column = None
    for candidate in ("mid", "value"):
        if candidate in df.columns:
            column = candidate
            break
    if column is None:
        return None
    series = df[column].dropna()
    if series.empty:
        return None
    return float(series.mean())


def _safe_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _reference_factor(name: str) -> float:
    if not name:
        return 0.0
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()
    bucket = int(digest[:6], 16) % 50
    return bucket / 1000.0


def _month_diff(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def _parse_effective_date(value: object) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _policy_effect(payload: dict, settings: WorkerSettings) -> float:
    policy_name = str(payload.get("policy_name", ""))
    return len(policy_name) * settings.policy_weight


def _load_growth_effect(payload: dict, request: ScenarioRequest, settings: WorkerSettings) -> float:
    growth = _safe_float(payload.get("annual_growth_pct"), 0.0)
    if growth == 0.0:
        return 0.0
    start_year = payload.get("start_year")
    end_year = payload.get("end_year")
    if isinstance(request.asof_date, date):
        current_year = request.asof_date.year
    elif isinstance(start_year, int):
        current_year = start_year
    elif isinstance(end_year, int):
        current_year = end_year
    else:
        current_year = datetime.now(timezone.utc).year

    if isinstance(start_year, int) and current_year < start_year:
        progress = 0.0
    else:
        effective_start = start_year if isinstance(start_year, int) else current_year
        effective_end = end_year if isinstance(end_year, int) and end_year >= effective_start else effective_start
        span = max(effective_end - effective_start + 1, 1)
        current_position = min(max(current_year, effective_start), effective_end)
        progress = (current_position - effective_start + 1) / span if span else 1.0
    effect = settings.load_weight * growth * progress

    curve_key = payload.get("reference_curve_key")
    if curve_key:
        baseline_value = _load_base_mid(curve_key, request.asof_date)
        if baseline_value is not None and settings.base_value:
            scale = baseline_value / settings.base_value
            effect *= scale
    return effect


def _fuel_curve_effect(payload: dict, request: ScenarioRequest, base_value: float) -> float:
    fuel = str(payload.get("fuel", "natural_gas")).lower()
    basis_points = _safe_float(payload.get("basis_points_adjustment"), 0.0)
    reference = str(payload.get("reference_series", ""))
    curve_key = payload.get("reference_curve_key") or reference
    baseline = _load_base_mid(curve_key, request.asof_date) if curve_key else None
    effective_base = baseline if baseline is not None else base_value
    fuel_weight = FUEL_CURVE_WEIGHTS.get(fuel, 0.0)
    basis_component = effective_base * (basis_points / 10_000.0)
    reference_component = _reference_factor(reference)
    return (fuel_weight * effective_base) + basis_component + reference_component


def _fleet_change_effect(payload: dict, asof: Optional[date]) -> float:
    capacity = _safe_float(payload.get("capacity_mw"), 0.0)
    if capacity <= 0.0:
        return 0.0
    change_type = str(payload.get("change_type", "add")).lower()
    sign = 1.0 if change_type == "add" else -1.0
    effective_date = _parse_effective_date(payload.get("effective_date"))
    reference_date = asof or effective_date or datetime.now(timezone.utc).date()
    months_ahead = 0
    if effective_date is not None:
        months_ahead = max(_month_diff(reference_date, effective_date), 0)
    ramp = min(months_ahead, FLEET_RAMP_MONTHS) / FLEET_RAMP_MONTHS
    return sign * (capacity / 100.0) * (1.0 + 0.5 * ramp)


def _driver_effect(
    assumption,
    *,
    request: ScenarioRequest,
    base_value: float,
    settings: WorkerSettings,
) -> float:
    driver_type = getattr(assumption, "driver_type", None)
    payload = getattr(assumption, "payload", {}) or {}
    if driver_type == DriverType.POLICY:
        return _policy_effect(payload, settings)
    if driver_type == DriverType.LOAD_GROWTH:
        return _load_growth_effect(payload, request, settings)
    if driver_type == DriverType.FUEL_CURVE:
        return _fuel_curve_effect(payload, request, base_value)
    if driver_type == DriverType.FLEET_CHANGE:
        return _fleet_change_effect(payload, request.asof_date)
    return 0.0


def _compute_value(
    scenario,
    request: ScenarioRequest,
    base_mid: Optional[float],
    settings: WorkerSettings,
) -> float:
    base_value = base_mid if base_mid is not None else settings.base_value
    total_effect = 0.0
    for assumption in getattr(scenario, "assumptions", []) or []:
        total_effect += _driver_effect(
            assumption,
            request=request,
            base_value=base_value,
            settings=settings,
        )
    return round(base_value + total_effect, 4)


def _build_attribution(
    scenario,
    request: ScenarioRequest,
    base_mid: Optional[float],
    settings: WorkerSettings,
) -> list[dict[str, float]]:
    base_value = base_mid if base_mid is not None else settings.base_value
    attribution: list[dict[str, float]] = []
    for assumption in getattr(scenario, "assumptions", []) or []:
        driver = getattr(assumption, "driver_type", None)
        delta = _driver_effect(
            assumption,
            request=request,
            base_value=base_value,
            settings=settings,
        )
        if abs(delta) < 1e-9:
            continue
        name = driver.value if isinstance(driver, DriverType) else str(driver)
        attribution.append({"component": name, "delta": round(delta, 4)})
    return attribution


def _ordered_tenor_keys(multipliers: dict[str, float]) -> list[str]:
    ordered = [tenor for tenor in TENOR_ORDER if tenor in multipliers]
    extras = [tenor for tenor in multipliers.keys() if tenor not in TENOR_ORDER]
    return ordered + extras


def _tenor_contract_and_label(contract_month: date, tenor_type: str) -> tuple[date, str]:
    tenor_upper = tenor_type.upper()
    if tenor_upper == "MONTHLY":
        return contract_month, contract_month.strftime("%Y-%m")
    if tenor_upper == "QUARTER":
        quarter_index = ((contract_month.month - 1) // 3) + 1
        start_month = (quarter_index - 1) * 3 + 1
        start_date = contract_month.replace(month=start_month, day=1)
        return start_date, f"Q{quarter_index} {contract_month.year}"
    if tenor_upper == "CALENDAR":
        start_date = contract_month.replace(month=1, day=1)
        return start_date, f"Calendar {contract_month.year}"
    return contract_month, contract_month.strftime("%Y-%m")


def _build_outputs(
    scenario,
    request: ScenarioRequest,
    *,
    run_id: Optional[str] = None,
    run_record: Optional[object] = None,
    settings: Optional[WorkerSettings] = None,
) -> list[dict[str, object]]:
    settings = settings or WorkerSettings.from_env()
    asof = request.asof_date or date.today()
    contract_month = asof.replace(day=1)
    computed_ts = _current_ts()
    curve_key = _resolve_curve_key(request, scenario.id, scenario.tenant_id)
    base_mid = _load_base_mid(curve_key, asof)
    base_value = _compute_value(scenario, request, base_mid, settings)
    attribution = _build_attribution(scenario, request, base_mid, settings) or None

    outputs: list[dict[str, object]] = []
    tenor_map = {key.upper(): value for key, value in settings.tenor_multipliers.items()}
    input_hash = None
    if run_record is not None:
        input_hash = getattr(run_record, "input_hash", None)

    for tenor_type in _ordered_tenor_keys(tenor_map):
        multiplier = tenor_map.get(tenor_type)
        if multiplier is None:
            continue
        tenor_contract_month, tenor_label = _tenor_contract_and_label(contract_month, tenor_type)
        value = round(base_value * multiplier, 4)
        band_lower = round(value * 0.98, 4)
        band_upper = round(value * 1.02, 4)
        if input_hash:
            version_hash = input_hash
        else:
            version_seed = f"{scenario.id}:{curve_key}:{tenor_type}:{tenor_label}:{value:.4f}:{run_id or ''}"
            version_hash = hashlib.sha256(version_seed.encode("utf-8")).hexdigest()[:16]
        outputs.append(
            {
                "asof_date": asof,
                "scenario_id": scenario.id,
                "tenant_id": scenario.tenant_id,
                "run_id": run_id,
                "curve_key": curve_key,
                "tenor_type": tenor_type,
                "contract_month": tenor_contract_month,
                "tenor_label": tenor_label,
                "metric": "mid",
                "value": value,
                "band_lower": band_lower,
                "band_upper": band_upper,
                "attribution": attribution,
                "version_hash": version_hash,
                "input_hash": input_hash,
                "run_key": getattr(run_record, "run_key", None) if run_record is not None else None,
                "computed_ts": computed_ts,
                "_ingest_ts": computed_ts,
            }
        )

    return outputs


def _append_to_iceberg(records: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    payload = list(records)
    if not payload:
        return []
    frame = pd.DataFrame(payload)
    subset_cols = [
        "scenario_id",
        "run_id",
        "tenor_type",
        "tenor_label",
        "metric",
        "asof_date",
    ]
    existing_cols = [col for col in subset_cols if col in frame.columns]
    if existing_cols:
        frame = frame.drop_duplicates(subset=existing_cols, keep="last")
    deduped_payload = frame.to_dict(orient="records")
    try:
        write_scenario_output(frame)
    except Exception as exc:  # pragma: no cover - integration error path
        LOGGER.warning("Failed to append scenario outputs to Iceberg: %s", exc)
        return deduped_payload
    try:
        cache_cfg = CacheConfig.from_env()
        keys = {
            (
                (record.get("tenant_id") or None),
                str(record.get("scenario_id")),
            )
            for record in payload
            if record.get("scenario_id")
        }
        for tenant_id, scenario_id in keys:
            invalidate_scenario_outputs_cache(cache_cfg, tenant_id, scenario_id)
    except Exception:  # pragma: no cover - cache invalidation best effort
        LOGGER.debug("Cache invalidation skipped", exc_info=True)
    return deduped_payload


def _publish_outputs(
    outputs: list[dict[str, object]],
    *,
    scenario_req,
    run_id: str,
    producer,
    value_schema,
    output_topic: str,
    request_id: str | None,
) -> bool:
    if _run_is_cancelled(
        scenario_id=scenario_req.scenario_id,
        run_id=run_id,
        tenant_id=scenario_req.tenant_id,
    ):
        LOGGER.info(
            "Run %s for scenario %s cancelled before writing outputs",
            run_id,
            scenario_req.scenario_id,
        )
        return False

    deduped_outputs = _append_to_iceberg(outputs)
    if not deduped_outputs:
        deduped_outputs = outputs

    headers_out = _build_produce_headers(run_id, request_id)
    for output in deduped_outputs:
        avro_payload = _to_avro_payload(output)
        producer.produce(
            topic=output_topic,
            value=avro_payload,
            value_schema=value_schema,
            headers=headers_out,
        )
    producer.flush(2)
    return True


def _to_avro_payload(record: dict[str, object]) -> dict[str, object]:
    asof_date = record["asof_date"]
    contract_month = record.get("contract_month")
    computed_ts = record["computed_ts"]
    attribution = record.get("attribution")
    if isinstance(attribution, list):
        avro_attr = [
            {"component": str(item.get("component")), "delta": float(item.get("delta", 0.0))}
            for item in attribution
        ]
    else:
        avro_attr = None
    return {
        "scenario_id": record["scenario_id"],
        "tenant_id": record["tenant_id"],
        "run_id": record.get("run_id"),
        "asof_date": _date_to_days(asof_date),
        "curve_key": record["curve_key"],
        "tenor_type": record["tenor_type"],
        "contract_month": _date_to_days(contract_month) if isinstance(contract_month, date) else None,
        "tenor_label": record["tenor_label"],
        "metric": record["metric"],
        "value": float(record["value"]),
        "band_lower": float(record["band_lower"]) if record.get("band_lower") is not None else None,
        "band_upper": float(record["band_upper"]) if record.get("band_upper") is not None else None,
        "attribution": avro_attr,
        "version_hash": record["version_hash"],
        "computed_ts": _timestamp_micros(computed_ts),
    }


def _fetch_run_record(
    *,
    scenario_id: Optional[str],
    run_id: Optional[str],
    tenant_id: Optional[str],
):
    if not (run_id and scenario_id and tenant_id):
        return None
    try:  # pragma: no cover - best effort lookup
        return ScenarioStore.get_run_for_scenario(  # type: ignore[arg-type]
            scenario_id,
            run_id,
            tenant_id=tenant_id,
        )
    except Exception:
        LOGGER.debug(
            "Scenario store lookup failed for run %s", run_id, exc_info=True
        )
        return None


def _normalize_run_state(state: object) -> Optional[ScenarioRunStatus]:
    if isinstance(state, ScenarioRunStatus):
        return state
    if isinstance(state, str):
        try:
            return ScenarioRunStatus(state.lower())
        except ValueError:
            return None
    return None


def _normalize_run_priority(priority: object) -> Optional[ScenarioRunPriority]:
    if isinstance(priority, ScenarioRunPriority):
        return priority
    if isinstance(priority, str):
        try:
            return ScenarioRunPriority(priority.lower())
        except ValueError:
            return None
    return None


def _run_is_cancelled(
    *,
    scenario_id: Optional[str],
    run_id: Optional[str],
    tenant_id: Optional[str],
) -> bool:
    record = _fetch_run_record(
        scenario_id=scenario_id,
        run_id=run_id,
        tenant_id=tenant_id,
    )
    if not record:
        return False
    state = _normalize_run_state(getattr(record, "state", None))
    return state == ScenarioRunStatus.CANCELLED


def _run_is_terminal(
    *,
    scenario_id: Optional[str],
    run_id: Optional[str],
    tenant_id: Optional[str],
) -> bool:
    record = _fetch_run_record(
        scenario_id=scenario_id,
        run_id=run_id,
        tenant_id=tenant_id,
    )
    if not record:
        return False
    state = _normalize_run_state(getattr(record, "state", None))
    return state in {
        ScenarioRunStatus.SUCCEEDED,
        ScenarioRunStatus.FAILED,
        ScenarioRunStatus.CANCELLED,
        ScenarioRunStatus.TIMEOUT,
    }


def _maybe_update_run_state(
    run_id: Optional[str],
    tenant_id: Optional[str],
    state: ScenarioRunStatus | str,
    *,
    scenario_id: Optional[str] = None,
) -> None:
    if not run_id or not tenant_id:
        return
    if isinstance(state, ScenarioRunStatus):
        state_value = state.value
    else:
        state_value = str(state)
    try:  # pragma: no cover - integration path
        if state_value.upper() != ScenarioRunStatus.CANCELLED.value.upper() and _run_is_cancelled(
            scenario_id=scenario_id,
            run_id=run_id,
            tenant_id=tenant_id,
        ):
            LOGGER.debug(
                "Skipping state transition for cancelled run %s -> %s",
                run_id,
                state_value,
            )
            return
        ScenarioStore.update_run_state(run_id, state=state_value, tenant_id=tenant_id)
    except Exception as exc:
        LOGGER.debug("Failed to update run %s to %s: %s", run_id, state_value, exc)


def run_worker():  # pragma: no cover - integration entrypoint
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is required")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    request_topic = os.getenv("AURUM_SCENARIO_REQUEST_TOPIC", "aurum.scenario.request.v1")
    output_topic = os.getenv("AURUM_SCENARIO_OUTPUT_TOPIC", "aurum.scenario.output.v1")
    group_id = os.getenv("AURUM_SCENARIO_WORKER_GROUP", "aurum-scenario-worker")

    _install_signal_handlers()
    READINESS_EVENT.clear()

    cons = _consumer(bootstrap, group_id, schema_registry_url)
    prod = _producer(bootstrap, schema_registry_url)
    value_schema = _load_avro_schema(_schema_path("scenario.output.v1.avsc"))

    http_port = os.getenv("AURUM_SCENARIO_HTTP_PORT") or os.getenv("AURUM_SCENARIO_METRICS_PORT")
    http_addr = os.getenv(
        "AURUM_SCENARIO_HTTP_ADDR",
        os.getenv("AURUM_SCENARIO_METRICS_ADDR", "0.0.0.0"),
    )
    probe_server = None
    if http_port:
        try:
            probe_server = _start_probe_server(http_addr, int(http_port))
        except Exception:
            LOGGER.warning(
                "Failed to start probe server on %s:%s",
                http_addr,
                http_port,
                exc_info=True,
            )

    settings = WorkerSettings.from_env()
    try:
        default_max_attempts = max(1, int(os.getenv("AURUM_SCENARIO_MAX_ATTEMPTS", "3")))
    except ValueError:
        default_max_attempts = 3
    try:
        backoff_seconds = max(0.1, float(os.getenv("AURUM_SCENARIO_RETRY_BACKOFF_SEC", "0.5")))
    except ValueError:
        backoff_seconds = 0.5

    # Start metrics updater for resource usage
    _start_metrics_updater()

    # Set worker info metric
    if WORKER_INFO:
        try:
            WORKER_INFO.info({
                "worker_id": group_id,
                "topics": f"{request_topic},{output_topic}",
                "bootstrap_servers": bootstrap.split(',')[0] if ',' in bootstrap else bootstrap,
                "version": "1.0.0",
            })
        except Exception:
            pass

    cons.subscribe([request_topic])
    READINESS_EVENT.set()

    try:
        # Track queue size periodically
        last_queue_check = 0
        queue_check_interval = 60  # Check every 60 seconds

        while not SHUTDOWN_EVENT.is_set():
            msg = cons.poll(1.0)
            if msg is None:
                if SHUTDOWN_EVENT.is_set():
                    break

                # Update queue size metric periodically
                current_time = time.time()
                if current_time - last_queue_check > queue_check_interval:
                    try:
                        # Note: Kafka doesn't provide direct queue size, so we'll estimate based on consumer lag
                        # For now, just set a placeholder value or use a different approach
                        if QUEUE_SIZE:
                            QUEUE_SIZE.set(0)  # Placeholder - would need Kafka admin client for actual lag
                    except Exception:
                        pass
                    last_queue_check = current_time

                continue
            if msg.error():  # type: ignore[attr-defined]
                continue
            payload = msg.value()  # type: ignore[assignment]
            if not isinstance(payload, dict):
                cons.commit(msg)
                continue

            raw_headers = msg.headers()  # type: ignore[attr-defined]
            carrier = _decode_headers(raw_headers)
            parent_context = propagate.extract(_HEADER_GETTER, carrier)

            # Extract correlation IDs from message headers
            run_values = carrier.get("run_id", [])
            run_id: Optional[str] = run_values[0] if run_values else None
            request_values = carrier.get("x-request-id", [])
            request_id = request_values[0] if request_values else None
            correlation_values = carrier.get("x-correlation-id", [])
            correlation_id = correlation_values[0] if correlation_values else request_id
            tenant_values = carrier.get("x-aurum-tenant", [])
            tenant_id = tenant_values[0] if tenant_values else scenario_req.tenant_id
            user_values = carrier.get("x-user-id", [])
            user_id = user_values[0] if user_values else None

            try:
                scenario_req = ScenarioRequest.from_message(payload)
            except Exception as exc:
                # Log error but continue processing
                log_structured(
                    "error",
                    "scenario_message_parse_failed",
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                    service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-scenario-worker"),
                )
                cons.commit(msg)
                continue

            # Set up correlation context for the worker processing
            from aurum.telemetry.context import correlation_context, log_structured
            import os
            with correlation_context(
                correlation_id=correlation_id,
                tenant_id=tenant_id,
                user_id=user_id,
                session_id=request_id
            ) as context:
                token = set_request_id(context["request_id"]) if context["request_id"] else None

                # Log message processing start
                log_structured(
                    "info",
                    "scenario_message_received",
                    message_key=f"{scenario_req.scenario_id}:{scenario_req.tenant_id}",
                    scenario_id=scenario_req.scenario_id,
                    tenant_id=scenario_req.tenant_id,
                    run_id=run_id,
                    driver_type=getattr(scenario_req, 'driver_type', 'unknown'),
                    service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-scenario-worker"),
                )

                try:
                    if not scenario_req.scenario_id or not scenario_req.tenant_id:
                        log_structured(
                            "warning",
                            "scenario_request_invalid",
                            error="missing_identifiers",
                            payload_size=len(str(payload)),
                            service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-scenario-worker"),
                        )
                        cons.commit(msg)
                        continue

                    if REQUESTS_TOTAL:
                        REQUESTS_TOTAL.inc()

                    # Track scenario type
                    scenario_type = getattr(scenario_req, 'driver_type', 'unknown') if hasattr(scenario_req, 'driver_type') else 'unknown'
                    if SCENARIO_TYPE_COUNTER:
                        try:
                            SCENARIO_TYPE_COUNTER.labels(scenario_type=scenario_type, tenant_id=scenario_req.tenant_id).inc()
                        except Exception:
                            SCENARIO_TYPE_COUNTER.inc()

                    run_record = _fetch_run_record(
                        scenario_id=scenario_req.scenario_id,
                        run_id=run_id,
                        tenant_id=scenario_req.tenant_id,
                    )
                    run_state = _normalize_run_state(getattr(run_record, "state", None) if run_record else None)
                    run_priority = _normalize_run_priority(getattr(run_record, "priority", None) if run_record else None)
                    attempt_limit = default_max_attempts
                    if run_record is not None:
                        max_retries = getattr(run_record, "max_retries", None)
                        if max_retries is not None:
                            try:
                                attempt_limit = max(1, 1 + int(max_retries))
                            except (TypeError, ValueError):
                                attempt_limit = default_max_attempts
                    if run_priority == ScenarioRunPriority.LOW:
                        attempt_limit = max(1, attempt_limit - 1)
                    elif run_priority == ScenarioRunPriority.CRITICAL:
                        attempt_limit = max(attempt_limit, default_max_attempts + 1)

                    if run_state == ScenarioRunStatus.CANCELLED:
                        log_structured(
                            "info",
                            "scenario_run_cancelled",
                            scenario_id=scenario_req.scenario_id,
                            run_id=run_id,
                            tenant_id=scenario_req.tenant_id,
                            service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-scenario-worker"),
                        )
                        if CANCELLED_RUNS:
                            CANCELLED_RUNS.inc()
                        cons.commit(msg)
                        continue

                    if run_state in {
                        ScenarioRunStatus.SUCCEEDED,
                        ScenarioRunStatus.FAILED,
                        ScenarioRunStatus.TIMEOUT,
                    }:
                        log_structured(
                            "info",
                            "scenario_run_already_terminal",
                            scenario_id=scenario_req.scenario_id,
                            run_id=run_id,
                            tenant_id=scenario_req.tenant_id,
                            state=run_state.value if run_state else None,
                            service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-scenario-worker"),
                        )
                        cons.commit(msg)
                        continue

                    _maybe_update_run_state(
                        run_id,
                        scenario_req.tenant_id,
                        ScenarioRunStatus.RUNNING,
                        scenario_id=scenario_req.scenario_id,
                    )

                    log_structured(
                        "info",
                        "scenario_processing_started",
                        scenario_id=scenario_req.scenario_id,
                        run_id=run_id,
                        tenant_id=scenario_req.tenant_id,
                        priority=run_priority.value if run_priority else None,
                        service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-scenario-worker"),
                    )

                    process_start = time.perf_counter()
                    attempts = 0
                    success = False
                    cancelled = False
                    last_exc: Optional[Exception] = None

                    with TRACER.start_as_current_span("scenario.process", context=parent_context) as span:
                        if span.is_recording():  # pragma: no branch
                            span.set_attribute("messaging.system", "kafka")
                            span.set_attribute("messaging.destination", request_topic)
                            span.set_attribute("messaging.destination_kind", "topic")
                            span.set_attribute("aurum.scenario_id", scenario_req.scenario_id)
                            span.set_attribute("aurum.tenant_id", scenario_req.tenant_id)
                            if run_id:
                                span.set_attribute("aurum.run_id", run_id)
                            if run_priority:
                                span.set_attribute("aurum.run_priority", run_priority.value)
                        delay = float(os.getenv("AURUM_SCENARIO_SIM_DELAY_SEC", "0.1"))
                        if delay:
                            time.sleep(delay)

                    while attempts < attempt_limit and not success:
                        if SHUTDOWN_EVENT.is_set():
                            LOGGER.info(
                                "Shutdown requested; cancelling scenario %s",
                                scenario_req.scenario_id,
                            )
                            cancelled = True
                            break
                        if _run_is_cancelled(
                            scenario_id=scenario_req.scenario_id,
                            run_id=run_id,
                            tenant_id=scenario_req.tenant_id,
                        ):
                            LOGGER.info(
                                "Run %s for scenario %s cancelled before processing",
                                run_id,
                                scenario_req.scenario_id,
                            )
                            cancelled = True
                            break
                        attempts += 1
                        try:
                            scenario = ScenarioStore.get_scenario(
                                scenario_req.scenario_id,
                                tenant_id=scenario_req.tenant_id,
                            )
                            if scenario is None:
                                raise RuntimeError(
                                    f"Scenario {scenario_req.scenario_id} not found for tenant {scenario_req.tenant_id}"
                                )

                            outputs = _build_outputs(
                                scenario,
                                scenario_req,
                                run_id=run_id,
                                run_record=run_record,
                                settings=settings,
                            )
                            if not _publish_outputs(
                                outputs,
                                scenario_req=scenario_req,
                                run_id=run_id,
                                producer=prod,
                                value_schema=value_schema,
                                output_topic=output_topic,
                                request_id=request_id,
                            ):
                                cancelled = True
                                break

                            _maybe_update_run_state(
                                run_id,
                                scenario_req.tenant_id,
                                ScenarioRunStatus.SUCCEEDED,
                                scenario_id=scenario_req.scenario_id,
                            )
                            if REQUEST_SUCCESS:
                                REQUEST_SUCCESS.inc()
                            success = True
                        except Exception as exc:  # pragma: no cover - integration failure path
                            last_exc = exc
                            error_type = exc.__class__.__name__

                            # Check if circuit breaker is open for this error type
                            if _is_circuit_open(error_type, scenario_req.tenant_id):
                                LOGGER.warning(
                                    "Circuit breaker open for %s:%s, sending directly to DLQ",
                                    error_type,
                                    scenario_req.tenant_id,
                                )
                                if CIRCUIT_BREAKER_REJECTED:
                                    CIRCUIT_BREAKER_REJECTED.inc()
                                _maybe_update_run_state(
                                    run_id,
                                    scenario_req.tenant_id,
                                    ScenarioRunStatus.FAILED,
                                    scenario_id=scenario_req.scenario_id,
                                )
                                _maybe_emit_dlq(
                                    scenario_id=scenario_req.scenario_id,
                                    tenant_id=scenario_req.tenant_id,
                                    run_id=run_id,
                                    error=exc,
                                )
                                if REQUEST_FAILURE:
                                    REQUEST_FAILURE.inc()
                                break

                            # Record failure for circuit breaker tracking
                            _record_failure_for_circuit_breaker(error_type, scenario_req.tenant_id)

                            LOGGER.error(
                                "Scenario worker attempt %s/%s failed for scenario %s: %s",
                                attempts,
                                attempt_limit,
                                scenario_req.scenario_id,
                                exc,
                            )
                            if span.is_recording():
                                span.record_exception(exc)

                            if attempts >= attempt_limit:
                                # Check if this is a poison pill
                                message_key = f"{scenario_req.scenario_id}:{scenario_req.tenant_id}"
                                if _is_poison_pill(message_key, error_type):
                                    LOGGER.error(
                                        "Poison pill detected for scenario %s, sending to DLQ",
                                        scenario_req.scenario_id,
                                    )
                                    if POISON_PILL_DETECTED:
                                        POISON_PILL_DETECTED.inc()
                                    _maybe_update_run_state(
                                        run_id,
                                        scenario_req.tenant_id,
                                        ScenarioRunStatus.FAILED,
                                        scenario_id=scenario_req.scenario_id,
                                    )
                                    _maybe_emit_dlq(
                                        scenario_id=scenario_req.scenario_id,
                                        tenant_id=scenario_req.tenant_id,
                                        run_id=run_id,
                                        error=exc,
                                    )
                                    if REQUEST_DLQ:
                                        REQUEST_DLQ.inc()
                                    if REQUEST_FAILURE:
                                        REQUEST_FAILURE.inc()
                                    if RETRY_EXHAUSTED:
                                        RETRY_EXHAUSTED.inc()
                                    break
                                else:
                                    _maybe_update_run_state(
                                        run_id,
                                        scenario_req.tenant_id,
                                        ScenarioRunStatus.FAILED,
                                        scenario_id=scenario_req.scenario_id,
                                    )
                                    _maybe_emit_dlq(
                                        scenario_id=scenario_req.scenario_id,
                                        tenant_id=scenario_req.tenant_id,
                                        run_id=run_id,
                                        error=exc,
                                    )
                                    if REQUEST_FAILURE:
                                        REQUEST_FAILURE.inc()
                                    if attempts >= attempt_limit:
                                        if RETRY_EXHAUSTED:
                                            RETRY_EXHAUSTED.inc()
                                    break

                            sleep_time = _backoff_delay(attempts, backoff_seconds)
                            if BACKOFF_DELAY:
                                try:
                                    BACKOFF_DELAY.labels(attempt=str(attempts)).observe(sleep_time)
                                except Exception:
                                    pass
                            time.sleep(sleep_time)
                            break

                    if cancelled:
                        _maybe_update_run_state(
                            run_id,
                            scenario_req.tenant_id,
                            ScenarioRunStatus.CANCELLED,
                            scenario_id=scenario_req.scenario_id,
                        )
                        LOGGER.info(
                            "Scenario run %s for scenario %s cancelled after %s attempt(s)",
                            run_id,
                            scenario_req.scenario_id,
                            attempts,
                        )
                        if CANCELLED_RUNS:
                            CANCELLED_RUNS.inc()
                if not success and not cancelled and last_exc is not None:
                    LOGGER.error(
                        "Scenario worker exhausted retries for scenario %s: %s",
                        scenario_req.scenario_id,
                        last_exc,
                    )
                    if RETRY_EXHAUSTED:
                        RETRY_EXHAUSTED.inc()
            finally:
                if token:
                    reset_request_id(token)

            # Record comprehensive metrics
            processing_time = max(time.perf_counter() - process_start, 0.0)
            status_label = "cancelled" if cancelled else ("succeeded" if success else "failed")

            if PROCESS_DURATION:
                try:
                    PROCESS_DURATION.labels(
                        scenario_type=scenario_type,
                        tenant_id=scenario_req.tenant_id,
                        status=status_label
                    ).observe(processing_time)
                except Exception:
                    try:
                        PROCESS_DURATION.observe(processing_time)
                    except Exception:
                        LOGGER.debug("Failed to record processing duration", exc_info=True)

            # Record retry metrics
            if attempts > 1:
                if RETRY_ATTEMPTS:
                    RETRY_ATTEMPTS.inc(attempts - 1)
                if success and attempts > 1:
                    if RETRY_SUCCESS:
                        RETRY_SUCCESS.inc()

                    # Record timeout metrics
                    if last_exc and "timeout" in str(last_exc).lower():
                        if TIMEOUT_RUNS:
                            TIMEOUT_RUNS.inc()

                    # Log processing completion
                    final_status = "cancelled" if cancelled else ("succeeded" if success else "failed")
                    log_structured(
                        "info",
                        "scenario_processing_completed",
                        scenario_id=scenario_req.scenario_id,
                        run_id=run_id,
                        tenant_id=scenario_req.tenant_id,
                        status=final_status,
                        attempts=attempts + 1,
                        processing_time_seconds=processing_time,
                        service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-scenario-worker"),
                    )

                    cons.commit(msg)
    finally:
        LOGGER.info("Scenario worker shutting down")
        READINESS_EVENT.clear()
        try:
            prod.flush(5)
        except Exception:
            LOGGER.debug("Failed to flush producer during shutdown", exc_info=True)
        try:
            cons.close()
        except Exception:
            LOGGER.debug("Failed to close consumer cleanly", exc_info=True)
        if probe_server is not None:
            try:
                probe_server.shutdown()
                probe_server.server_close()
            except Exception:
                LOGGER.debug("Failed to stop probe server", exc_info=True)
