from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, Optional, Sequence

import pandas as pd
from opentelemetry import propagate

from aurum.api.config import CacheConfig
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
    from prometheus_client import Counter, Histogram, start_http_server  # type: ignore
except Exception:  # pragma: no cover - dependency not installed during unit tests
    Counter = None  # type: ignore[assignment]
    Histogram = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]

REQUESTS_TOTAL = Counter("aurum_scenario_requests_total", "Scenario requests consumed") if Counter else None
REQUEST_SUCCESS = Counter("aurum_scenario_requests_success_total", "Successful scenario requests") if Counter else None
REQUEST_FAILURE = Counter("aurum_scenario_requests_failure_total", "Failed scenario requests") if Counter else None
REQUEST_DLQ = Counter("aurum_scenario_requests_dlq_total", "Scenario requests routed to DLQ") if Counter else None
PROCESS_DURATION = Histogram(
    "aurum_scenario_processing_seconds",
    "Scenario request processing duration",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
) if Histogram else None


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
    }
    if run_id:
        context["run_id"] = run_id

    payload = {
        "source": "aurum.scenario.worker",
        "error_message": str(error),
        "context": context,
        "ingest_ts": _timestamp_micros(datetime.now(timezone.utc)),
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


def _load_growth_effect(payload: dict, asof: Optional[date], settings: WorkerSettings) -> float:
    growth = _safe_float(payload.get("annual_growth_pct"), 0.0)
    if growth == 0.0:
        return 0.0
    start_year = payload.get("start_year")
    end_year = payload.get("end_year")
    if isinstance(asof, date):
        current_year = asof.year
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
    return settings.load_weight * growth * progress


def _fuel_curve_effect(payload: dict, base_value: float) -> float:
    fuel = str(payload.get("fuel", "natural_gas")).lower()
    basis_points = _safe_float(payload.get("basis_points_adjustment"), 0.0)
    reference = str(payload.get("reference_series", ""))
    fuel_weight = FUEL_CURVE_WEIGHTS.get(fuel, 0.0)
    basis_component = base_value * (basis_points / 10_000.0)
    reference_component = _reference_factor(reference)
    return (fuel_weight * base_value) + basis_component + reference_component


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
        return _load_growth_effect(payload, request.asof_date, settings)
    if driver_type == DriverType.FUEL_CURVE:
        return _fuel_curve_effect(payload, base_value)
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
    for tenor_type in _ordered_tenor_keys(tenor_map):
        multiplier = tenor_map.get(tenor_type)
        if multiplier is None:
            continue
        tenor_contract_month, tenor_label = _tenor_contract_and_label(contract_month, tenor_type)
        value = round(base_value * multiplier, 4)
        band_lower = round(value * 0.98, 4)
        band_upper = round(value * 1.02, 4)
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
                "computed_ts": computed_ts,
                "_ingest_ts": computed_ts,
            }
        )

    return outputs


def _append_to_iceberg(records: Iterable[dict[str, object]]) -> None:
    payload = list(records)
    if not payload:
        return
    frame = pd.DataFrame(payload)
    try:
        write_scenario_output(frame)
    except Exception as exc:  # pragma: no cover - integration error path
        LOGGER.warning("Failed to append scenario outputs to Iceberg: %s", exc)
        return
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


def _maybe_update_run_state(run_id: Optional[str], tenant_id: Optional[str], state: str) -> None:
    if not run_id or not tenant_id:
        return
    try:  # pragma: no cover - integration path
        ScenarioStore.update_run_state(run_id, state=state, tenant_id=tenant_id)
    except Exception as exc:
        LOGGER.debug("Failed to update run %s to %s: %s", run_id, state, exc)


def run_worker():  # pragma: no cover - integration entrypoint
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is required")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    request_topic = os.getenv("AURUM_SCENARIO_REQUEST_TOPIC", "aurum.scenario.request.v1")
    output_topic = os.getenv("AURUM_SCENARIO_OUTPUT_TOPIC", "aurum.scenario.output.v1")
    group_id = os.getenv("AURUM_SCENARIO_WORKER_GROUP", "aurum-scenario-worker")

    cons = _consumer(bootstrap, group_id, schema_registry_url)
    prod = _producer(bootstrap, schema_registry_url)
    value_schema = _load_avro_schema(_schema_path("scenario.output.v1.avsc"))

    metrics_port = os.getenv("AURUM_SCENARIO_METRICS_PORT")
    if start_http_server and metrics_port:
        addr = os.getenv("AURUM_SCENARIO_METRICS_ADDR", "0.0.0.0")
        try:
            start_http_server(int(metrics_port), addr=addr)
            LOGGER.info("Prometheus metrics exposed on %s:%s", addr, metrics_port)
        except Exception:
            LOGGER.warning("Failed to start Prometheus metrics listener on %s:%s", addr, metrics_port, exc_info=True)

    settings = WorkerSettings.from_env()
    try:
        max_attempts = max(1, int(os.getenv("AURUM_SCENARIO_MAX_ATTEMPTS", "3")))
    except ValueError:
        max_attempts = 3
    try:
        backoff_seconds = max(0.1, float(os.getenv("AURUM_SCENARIO_RETRY_BACKOFF_SEC", "0.5")))
    except ValueError:
        backoff_seconds = 0.5

    cons.subscribe([request_topic])

    try:
        while True:
            msg = cons.poll(1.0)
            if msg is None:
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
            run_values = carrier.get("run_id", [])
            run_id: Optional[str] = run_values[0] if run_values else None
            request_values = carrier.get("x-request-id", [])
            request_id = request_values[0] if request_values else None
            token = set_request_id(request_id) if request_id else None
            try:
                scenario_req = ScenarioRequest.from_message(payload)
                if not scenario_req.scenario_id or not scenario_req.tenant_id:
                    LOGGER.warning("Scenario request missing identifiers: %s", payload)
                    cons.commit(msg)
                    continue

                if REQUESTS_TOTAL:
                    REQUESTS_TOTAL.inc()

                _maybe_update_run_state(run_id, scenario_req.tenant_id, "RUNNING")

                process_start = time.perf_counter()
                attempts = 0
                success = False
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
                    delay = float(os.getenv("AURUM_SCENARIO_SIM_DELAY_SEC", "0.1"))
                    if delay:
                        time.sleep(delay)

                    while attempts < max_attempts and not success:
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
                                settings=settings,
                            )
                            _append_to_iceberg(outputs)

                            headers_out = _build_produce_headers(run_id, request_id)
                            for output in outputs:
                                avro_payload = _to_avro_payload(output)
                                prod.produce(
                                    topic=output_topic,
                                    value=avro_payload,
                                    value_schema=value_schema,
                                    headers=headers_out,
                                )
                            prod.flush(2)

                            _maybe_update_run_state(run_id, scenario_req.tenant_id, "SUCCEEDED")
                            if REQUEST_SUCCESS:
                                REQUEST_SUCCESS.inc()
                            success = True
                        except Exception as exc:  # pragma: no cover - integration failure path
                            last_exc = exc
                            LOGGER.error(
                                "Scenario worker attempt %s/%s failed for scenario %s: %s",
                                attempts,
                                max_attempts,
                                scenario_req.scenario_id,
                                exc,
                            )
                            if span.is_recording():
                                span.record_exception(exc)
                            if attempts >= max_attempts:
                                _maybe_update_run_state(run_id, scenario_req.tenant_id, "FAILED")
                                _maybe_emit_dlq(
                                    scenario_id=scenario_req.scenario_id,
                                    tenant_id=scenario_req.tenant_id,
                                    run_id=run_id,
                                    error=exc,
                                )
                                if REQUEST_FAILURE:
                                    REQUEST_FAILURE.inc()
                                break

                            sleep_time = backoff_seconds * (2 ** (attempts - 1))
                            time.sleep(sleep_time)

                if not success and last_exc is not None:
                    LOGGER.error(
                        "Scenario worker exhausted retries for scenario %s: %s",
                        scenario_req.scenario_id,
                        last_exc,
                    )
            finally:
                if token:
                    reset_request_id(token)
            if PROCESS_DURATION:
                try:
                    PROCESS_DURATION.observe(max(time.perf_counter() - process_start, 0.0))
                except Exception:
                    LOGGER.debug("Failed to record processing duration", exc_info=True)
            cons.commit(msg)
    finally:
        cons.close()
