"""Production Airflow DAGs for external provider ingestion with idempotent backfills.

This module defines four production DAGs covering EIA, NOAA, FRED, and ISO (CAISO)
feeds. Each DAG provides:
- Scheduled incremental ingestion with retries and SLA monitoring.
- Optional idempotent backfill execution via `dag_run.conf` payloads.
- Consistent logging and checkpoint usage to avoid duplicate loads.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback
from aurum.external.collect.checkpoints import CheckpointStore, PostgresCheckpointStore
from aurum.external.providers import (
    DailyQuota,
    EiaApiClient,
    EiaCollector,
    FredApiClient,
    FredCollector,
    NoaaApiClient,
    NoaaCollector,
    NoaaRateLimiter,
    load_eia_dataset_configs,
    load_fred_dataset_configs,
    load_noaa_dataset_configs,
)
from aurum.external.providers.caiso_collectors import (
    CaisoKafkaConfig,
    CaisoOasisCollector,
)
from aurum.external.runner import (
    OBS_TOPIC,
    CATALOG_TOPIC,
    _build_http_collector,
    _build_kafka_collector,
)
from aurum.data_ingestion.watermark_store import WatermarkStore

UTC = timezone.utc
LOG = logging.getLogger("aurum.airflow.prod_ingestion")

BACKFILL_CATALOG_TOPIC = "aurum.ext.series_catalog.upsert.backfill.v1"
BACKFILL_OBS_TOPIC = "aurum.ext.timeseries.obs.backfill.v1"
DEFAULT_ISO_DATA_TYPES = ("lmp", "load", "asm")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _parse_iso_datetime(value: str, *, param_name: str) -> datetime:
    """Parse ISO8601 datetime strings into aware datetimes."""
    if not value:
        raise AirflowFailException(f"Missing required {param_name} for backfill")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:  # pragma: no cover - defensive
        raise AirflowFailException(
            f"Invalid datetime '{value}' for {param_name}; expected ISO 8601"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_list(value: Optional[Any]) -> Optional[List[str]]:
    """Normalise an arbitrary value into a list of lower-cased strings."""
    if value is None:
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        items = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value]
    else:
        items = [str(value).strip()]
    normalized = [item.lower() for item in items if item]
    return normalized or None


def _ensure_env(vars_required: Sequence[str], provider: str) -> None:
    missing = [var for var in vars_required if not os.getenv(var)]
    if missing:
        raise AirflowFailException(
            f"Missing required environment variables for {provider}: {', '.join(missing)}"
        )


def _build_checkpoint_store() -> CheckpointStore:
    dsn = (
        os.getenv("AURUM_COLLECTOR_CHECKPOINT_DSN")
        or os.getenv("AURUM_APP_DB_DSN")
        or os.getenv("AURUM_TIMESCALE_DSN")
    )
    if not dsn:
        raise AirflowFailException(
            "Postgres checkpoint store DSN not configured; set AURUM_COLLECTOR_CHECKPOINT_DSN or AURUM_APP_DB_DSN"
        )
    return PostgresCheckpointStore(dsn=dsn)


def _close_collectors(collectors: Iterable[Any]) -> None:
    for collector in collectors:
        if not collector:
            continue
        try:
            collector.flush()
        except Exception:  # pragma: no cover - best effort
            LOG.warning("Failed to flush collector", exc_info=True)
        try:
            collector.close()
        except Exception:  # pragma: no cover - best effort
            LOG.warning("Failed to close collector", exc_info=True)


async def _get_watermark(source: str, table: str) -> Optional[datetime]:
    store = WatermarkStore()
    value = await store.get_watermark(source, table)
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    return None


async def _set_watermark(source: str, table: str, value: datetime, *, metadata: Optional[Dict[str, Any]] = None) -> None:
    store = WatermarkStore()
    await store.set_watermark(source, table, value.astimezone(UTC), metadata=metadata or {})


# ---------------------------------------------------------------------------
# Provider-specific ingestion helpers
# ---------------------------------------------------------------------------

def _filter_eia_datasets(datasets: Sequence[str] | None) -> List[Any]:
    configs = load_eia_dataset_configs()
    if not datasets:
        return configs
    desired = {item.lower() for item in datasets}
    matched: List[Any] = []
    for cfg in configs:
        keys = {
            cfg.source_name.lower(),
            (cfg.dataset_code or "").lower(),
        }
        if desired & keys:
            matched.append(cfg)
    if not matched:
        LOG.warning("No EIA datasets matched filter", extra={"filter": sorted(desired)})
        return []
    return matched


def _run_eia_incremental(window_hours: int, datasets: Sequence[str] | None) -> Dict[str, int]:
    _ensure_env(["EIA_API_KEY"], "EIA")
    api_key = os.environ["EIA_API_KEY"]
    base_url = os.getenv("EIA_API_BASE_URL", "https://api.eia.gov/v2/")

    catalog_collector = _build_kafka_collector("eia-prod-catalog", CATALOG_TOPIC, "ExtSeriesCatalogUpsertV1.avsc")
    obs_collector = _build_kafka_collector("eia-prod-obs", OBS_TOPIC, "ExtTimeseriesObsV1.avsc")
    checkpoint_store = _build_checkpoint_store()

    http_collector = _build_http_collector("eia-http", base_url)
    api_client = EiaApiClient(http_collector, api_key=api_key)

    now = datetime.now(tz=UTC)
    start = now - timedelta(hours=max(1, window_hours))

    total_catalog = 0
    total_obs = 0

    try:
        for dataset in _filter_eia_datasets(datasets):
            collector = EiaCollector(
                dataset,
                api_client=api_client,
                catalog_collector=catalog_collector,
                observation_collector=obs_collector,
                checkpoint_store=checkpoint_store,
            )
            total_catalog += collector.sync_catalog()
            total_obs += collector.ingest_observations(start=start, end=now)
    finally:
        _close_collectors((catalog_collector, obs_collector, http_collector))

    return {
        "catalog_records": total_catalog,
        "observation_records": total_obs,
        "window_hours": window_hours,
    }


def _run_eia_backfill(start: datetime, end: datetime, chunk_hours: int, datasets: Sequence[str] | None) -> Dict[str, int]:
    _ensure_env(["EIA_API_KEY"], "EIA")
    api_key = os.environ["EIA_API_KEY"]
    base_url = os.getenv("EIA_API_BASE_URL", "https://api.eia.gov/v2/")

    catalog_collector = _build_kafka_collector("eia-backfill-catalog", BACKFILL_CATALOG_TOPIC, "ExtSeriesCatalogUpsertV1.avsc")
    obs_collector = _build_kafka_collector("eia-backfill-obs", BACKFILL_OBS_TOPIC, "ExtTimeseriesObsV1.avsc")
    checkpoint_store = _build_checkpoint_store()

    http_collector = _build_http_collector("eia-http", base_url)
    api_client = EiaApiClient(http_collector, api_key=api_key)

    total_catalog = 0
    total_obs = 0

    window = timedelta(hours=max(1, chunk_hours))

    try:
        for dataset in _filter_eia_datasets(datasets):
            collector = EiaCollector(
                dataset,
                api_client=api_client,
                catalog_collector=catalog_collector,
                observation_collector=obs_collector,
                checkpoint_store=checkpoint_store,
            )
            total_catalog += collector.sync_catalog()
            cursor = start
            while cursor < end:
                chunk_end = min(cursor + window, end)
                total_obs += collector.ingest_observations(start=cursor, end=chunk_end)
                cursor = chunk_end
    finally:
        _close_collectors((catalog_collector, obs_collector, http_collector))

    return {
        "catalog_records": total_catalog,
        "observation_records": total_obs,
        "chunks": max(1, int(((end - start).total_seconds() + window.total_seconds() - 1) // window.total_seconds())),
    }


def _filter_fred_datasets(datasets: Sequence[str] | None) -> List[Any]:
    configs = load_fred_dataset_configs()
    if not datasets:
        return configs
    desired = {item.lower() for item in datasets}
    matched: List[Any] = []
    for cfg in configs:
        keys = {cfg.series_id.lower(), cfg.source_name.lower()}
        if desired & keys:
            matched.append(cfg)
    if not matched:
        LOG.warning("No FRED datasets matched filter", extra={"filter": sorted(desired)})
        return []
    return matched


def _run_fred_incremental(window_hours: int, datasets: Sequence[str] | None) -> Dict[str, int]:
    _ensure_env(["FRED_API_KEY"], "FRED")
    api_key = os.environ["FRED_API_KEY"]
    base_url = os.getenv("FRED_API_BASE_URL", "https://api.stlouisfed.org/")

    catalog_collector = _build_kafka_collector("fred-prod-catalog", CATALOG_TOPIC, "ExtSeriesCatalogUpsertV1.avsc")
    obs_collector = _build_kafka_collector("fred-prod-obs", OBS_TOPIC, "ExtTimeseriesObsV1.avsc")
    checkpoint_store = _build_checkpoint_store()
    http_collector = _build_http_collector("fred-http", base_url)
    api_client = FredApiClient(http_collector, api_key=api_key)

    now = datetime.now(tz=UTC)
    start = now - timedelta(hours=max(1, window_hours))

    total_catalog = 0
    total_obs = 0

    try:
        for dataset in _filter_fred_datasets(datasets):
            collector = FredCollector(
                dataset,
                api_client=api_client,
                catalog_collector=catalog_collector,
                observation_collector=obs_collector,
                checkpoint_store=checkpoint_store,
            )
            total_catalog += collector.sync_catalog()
            total_obs += collector.ingest_observations(start=start, end=now)
    finally:
        _close_collectors((catalog_collector, obs_collector, http_collector))

    return {
        "catalog_records": total_catalog,
        "observation_records": total_obs,
        "window_hours": window_hours,
    }


def _run_fred_backfill(start: datetime, end: datetime, chunk_hours: int, datasets: Sequence[str] | None) -> Dict[str, int]:
    _ensure_env(["FRED_API_KEY"], "FRED")
    api_key = os.environ["FRED_API_KEY"]
    base_url = os.getenv("FRED_API_BASE_URL", "https://api.stlouisfed.org/")

    catalog_collector = _build_kafka_collector("fred-backfill-catalog", BACKFILL_CATALOG_TOPIC, "ExtSeriesCatalogUpsertV1.avsc")
    obs_collector = _build_kafka_collector("fred-backfill-obs", BACKFILL_OBS_TOPIC, "ExtTimeseriesObsV1.avsc")
    checkpoint_store = _build_checkpoint_store()
    http_collector = _build_http_collector("fred-http", base_url)
    api_client = FredApiClient(http_collector, api_key=api_key)

    total_catalog = 0
    total_obs = 0
    window = timedelta(hours=max(1, chunk_hours))

    try:
        for dataset in _filter_fred_datasets(datasets):
            collector = FredCollector(
                dataset,
                api_client=api_client,
                catalog_collector=catalog_collector,
                observation_collector=obs_collector,
                checkpoint_store=checkpoint_store,
            )
            total_catalog += collector.sync_catalog()
            cursor = start
            while cursor < end:
                chunk_end = min(cursor + window, end)
                total_obs += collector.ingest_observations(start=cursor, end=chunk_end)
                cursor = chunk_end
    finally:
        _close_collectors((catalog_collector, obs_collector, http_collector))

    return {
        "catalog_records": total_catalog,
        "observation_records": total_obs,
    }


def _filter_noaa_datasets(datasets: Sequence[str] | None) -> List[Any]:
    configs = load_noaa_dataset_configs()
    if not datasets:
        return configs
    desired = {item.lower() for item in datasets}
    matched: List[Any] = []
    for cfg in configs:
        keys = {cfg.dataset_id.lower(), cfg.dataset.lower()}
        if desired & keys:
            matched.append(cfg)
    if not matched:
        LOG.warning("No NOAA datasets matched filter", extra={"filter": sorted(desired)})
        return []
    return matched


def _build_noaa_client() -> NoaaApiClient:
    token = os.getenv("NOAA_GHCND_TOKEN") or os.getenv("NOAA_TOKEN")
    if not token:
        raise AirflowFailException("NOAA_GHCND_TOKEN environment variable is required")
    base_url = os.getenv("NOAA_API_BASE_URL", "https://www.ncdc.noaa.gov/cdo-web/api/v2")
    rate = float(os.getenv("NOAA_RATE_LIMIT_RPS", "5"))
    quota_limit = os.getenv("NOAA_DAILY_QUOTA")
    quota = DailyQuota(limit=int(quota_limit)) if quota_limit else None
    rate_limiter = NoaaRateLimiter(rate_per_sec=rate)
    http_collector = _build_http_collector("noaa-http", base_url)
    return NoaaApiClient(http_collector, token=token, rate_limiter=rate_limiter, quota=quota, base_url=base_url)


def _run_noaa_incremental(window_hours: int, datasets: Sequence[str] | None) -> Dict[str, int]:
    api_client = _build_noaa_client()
    catalog_collector = _build_kafka_collector("noaa-prod-catalog", CATALOG_TOPIC, "ExtSeriesCatalogUpsertV1.avsc")
    obs_collector = _build_kafka_collector("noaa-prod-obs", OBS_TOPIC, "ExtTimeseriesObsV1.avsc")
    checkpoint_store = _build_checkpoint_store()

    total_catalog = 0
    total_obs = 0
    now = datetime.now(tz=UTC)
    start = now - timedelta(hours=max(1, window_hours))

    try:
        for dataset in _filter_noaa_datasets(datasets):
            collector = NoaaCollector(
                dataset,
                api_client=api_client,
                catalog_collector=catalog_collector,
                observation_collector=obs_collector,
                checkpoint_store=checkpoint_store,
            )
            total_catalog += collector.sync_catalog()
            total_obs += collector.ingest_observations(start=start, end=now)
    finally:
        _close_collectors((catalog_collector, obs_collector, api_client.collector))

    return {
        "catalog_records": total_catalog,
        "observation_records": total_obs,
    }


def _run_noaa_backfill(start: datetime, end: datetime, chunk_hours: int, datasets: Sequence[str] | None) -> Dict[str, int]:
    api_client = _build_noaa_client()
    catalog_collector = _build_kafka_collector("noaa-backfill-catalog", BACKFILL_CATALOG_TOPIC, "ExtSeriesCatalogUpsertV1.avsc")
    obs_collector = _build_kafka_collector("noaa-backfill-obs", BACKFILL_OBS_TOPIC, "ExtTimeseriesObsV1.avsc")
    checkpoint_store = _build_checkpoint_store()

    total_catalog = 0
    total_obs = 0
    window = timedelta(hours=max(1, chunk_hours))

    try:
        for dataset in _filter_noaa_datasets(datasets):
            collector = NoaaCollector(
                dataset,
                api_client=api_client,
                catalog_collector=catalog_collector,
                observation_collector=obs_collector,
                checkpoint_store=checkpoint_store,
            )
            total_catalog += collector.sync_catalog()
            cursor = start
            while cursor < end:
                chunk_end = min(cursor + window, end)
                total_obs += collector.ingest_observations(start=cursor, end=chunk_end)
                cursor = chunk_end
    finally:
        _close_collectors((catalog_collector, obs_collector, api_client.collector))

    return {
        "catalog_records": total_catalog,
        "observation_records": total_obs,
    }


def _build_caiso_collector() -> CaisoOasisCollector:
    base_url = os.getenv("CAISO_OASIS_BASE_URL", "https://oasis.caiso.com/oasisapi/")
    bootstrap = os.getenv("AURUM_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        raise AirflowFailException("AURUM_KAFKA_BOOTSTRAP_SERVERS environment variable is required for ISO ingestion")
    schema_registry = os.getenv("AURUM_SCHEMA_REGISTRY_URL")
    kafka_cfg = CaisoKafkaConfig(bootstrap_servers=bootstrap, schema_registry_url=schema_registry)
    http_collector = _build_http_collector("caiso-http", base_url)
    return CaisoOasisCollector(http_collector=http_collector, kafka_cfg=kafka_cfg)


def _ingest_iso_dataset(collector: CaisoOasisCollector, data_type: str, start: datetime, end: datetime) -> int:
    kwargs = {"start_utc": start, "end_utc": end, "market_run_id": "RTM"}
    if data_type == "lmp":
        return collector.ingest_prc_lmp(**kwargs)
    if data_type == "load":
        return collector.ingest_load_and_forecast(**kwargs)
    if data_type == "asm":
        return collector.ingest_as_results(**kwargs)
    raise AirflowFailException(f"Unsupported ISO data_type '{data_type}'")


def _run_iso_incremental(window_hours: int, data_types: Sequence[str]) -> Dict[str, Any]:
    collector = _build_caiso_collector()
    now = datetime.now(tz=UTC)
    start = now - timedelta(hours=max(1, window_hours))

    totals: Dict[str, int] = {}
    try:
        for data_type in data_types:
            watermark = asyncio.run(_get_watermark("iso_caiso_prod", data_type))
            effective_start = watermark if watermark and watermark < now else start
            if effective_start >= now:
                LOG.info("ISO incremental ingestion already up to date", extra={"data_type": data_type})
                totals[data_type] = 0
                continue
            processed = _ingest_iso_dataset(collector, data_type, effective_start, now)
            totals[data_type] = processed
            asyncio.run(_set_watermark("iso_caiso_prod", data_type, now, metadata={"window_hours": window_hours}))
    finally:
        _close_collectors((collector.http,))  # type: ignore[attr-defined]

    totals["window_hours"] = window_hours
    return totals


def _run_iso_backfill(start: datetime, end: datetime, chunk_hours: int, data_types: Sequence[str]) -> Dict[str, Any]:
    collector = _build_caiso_collector()
    window = timedelta(hours=max(1, chunk_hours))
    totals: Dict[str, int] = {dtype: 0 for dtype in data_types}

    try:
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + window, end)
            for data_type in data_types:
                watermark = asyncio.run(_get_watermark("iso_caiso_backfill", data_type))
                if watermark and chunk_end <= watermark:
                    LOG.info(
                        "Skipping ISO chunk; watermark covers window",
                        extra={"data_type": data_type, "chunk_start": cursor.isoformat(), "chunk_end": chunk_end.isoformat()},
                    )
                    continue
                processed = _ingest_iso_dataset(collector, data_type, cursor, chunk_end)
                totals[data_type] += processed
                asyncio.run(_set_watermark("iso_caiso_backfill", data_type, chunk_end, metadata={"chunk_hours": chunk_hours}))
            cursor = chunk_end
    finally:
        _close_collectors((collector.http,))  # type: ignore[attr-defined]

    totals["chunks"] = max(1, int(((end - start).total_seconds() + window.total_seconds() - 1) // window.total_seconds()))
    return totals


# ---------------------------------------------------------------------------
# Airflow task callables
# ---------------------------------------------------------------------------

def _incremental_wrapper(provider: str, window_hours: int, data_types: Sequence[str] | None = None, datasets_var: Optional[str] = None, **context: Any) -> None:
    datasets: Optional[Sequence[str]] = None
    if datasets_var:
        from airflow.models import Variable

        raw = Variable.get(datasets_var, default_var="")
        datasets = _normalize_list(raw)
    LOG.info("Starting incremental ingestion", extra={"provider": provider, "window_hours": window_hours, "datasets": datasets})
    if provider == "eia":
        result = _run_eia_incremental(window_hours, datasets)
    elif provider == "fred":
        result = _run_fred_incremental(window_hours, datasets)
    elif provider == "noaa":
        result = _run_noaa_incremental(window_hours, datasets)
    elif provider == "iso":
        result = _run_iso_incremental(window_hours, data_types or DEFAULT_ISO_DATA_TYPES)
    else:
        raise AirflowFailException(f"Unsupported provider '{provider}' for incremental ingestion")
    LOG.info("Incremental ingestion complete", extra={"provider": provider, "stats": result})


def _backfill_wrapper(provider: str, default_chunk_hours: int, data_types: Sequence[str] | None = None, datasets_var: Optional[str] = None, **context: Any) -> None:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    trigger = conf.get("backfill")
    if not trigger:
        LOG.info("Backfill not requested; skipping", extra={"provider": provider})
        return

    start_str = conf.get("start") or conf.get("backfill_start")
    end_str = conf.get("end") or conf.get("backfill_end")
    start = _parse_iso_datetime(start_str, param_name="backfill_start")
    end = _parse_iso_datetime(end_str, param_name="backfill_end")
    if end <= start:
        raise AirflowFailException("backfill_end must be later than backfill_start")

    chunk_hours = int(conf.get("chunk_hours") or conf.get("chunk_size_hours") or default_chunk_hours)
    datasets = conf.get("datasets")
    if datasets is None and datasets_var:
        from airflow.models import Variable

        raw = Variable.get(datasets_var, default_var="")
        datasets = raw
    dataset_filter = _normalize_list(datasets)
    LOG.info(
        "Starting backfill",
        extra={
            "provider": provider,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "chunk_hours": chunk_hours,
            "datasets": dataset_filter,
        },
    )

    if provider == "eia":
        result = _run_eia_backfill(start, end, chunk_hours, dataset_filter)
    elif provider == "fred":
        result = _run_fred_backfill(start, end, chunk_hours, dataset_filter)
    elif provider == "noaa":
        result = _run_noaa_backfill(start, end, chunk_hours, dataset_filter)
    elif provider == "iso":
        result = _run_iso_backfill(start, end, chunk_hours, data_types or DEFAULT_ISO_DATA_TYPES)
    else:
        raise AirflowFailException(f"Unsupported provider '{provider}' for backfill")

    LOG.info("Backfill completed", extra={"provider": provider, "stats": result})


# ---------------------------------------------------------------------------
# DAG definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProviderDagConfig:
    dag_id: str
    provider: str
    schedule: str
    window_hours: int
    chunk_hours: int
    sla: timedelta
    tags: Sequence[str]
    datasets_variable: Optional[str] = None
    iso_data_types: Sequence[str] = DEFAULT_ISO_DATA_TYPES


PROVIDER_DAG_CONFIGS: Sequence[ProviderDagConfig] = (
    ProviderDagConfig(
        dag_id="prod_ingest_eia",
        provider="eia",
        schedule="0 * * * *",
        window_hours=24,
        chunk_hours=24,
        sla=timedelta(hours=2),
        tags=("aurum", "external", "eia"),
        datasets_variable="aurum_eia_datasets",
    ),
    ProviderDagConfig(
        dag_id="prod_ingest_fred",
        provider="fred",
        schedule="30 6 * * *",
        window_hours=24,
        chunk_hours=24,
        sla=timedelta(hours=3),
        tags=("aurum", "external", "fred"),
        datasets_variable="aurum_fred_datasets",
    ),
    ProviderDagConfig(
        dag_id="prod_ingest_noaa",
        provider="noaa",
        schedule="0 7 * * *",
        window_hours=24,
        chunk_hours=24,
        sla=timedelta(hours=6),
        tags=("aurum", "external", "noaa"),
        datasets_variable="aurum_noaa_datasets",
    ),
    ProviderDagConfig(
        dag_id="prod_ingest_iso",
        provider="iso",
        schedule="*/30 * * * *",
        window_hours=2,
        chunk_hours=6,
        sla=timedelta(hours=1),
        tags=("aurum", "iso", "caiso"),
        iso_data_types=DEFAULT_ISO_DATA_TYPES,
    ),
)


def _build_dag(config: ProviderDagConfig) -> DAG:
    default_args = {
        "owner": "aurum-data",
        "depends_on_past": False,
        "email_on_failure": True,
        "email": ["aurum-ops@example.com"],
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=60),
        "execution_timeout": timedelta(hours=4),
        "sla": config.sla,
    }

    dag = DAG(
        dag_id=config.dag_id,
        description=f"Production ingestion pipeline for {config.provider.upper()} data",
        default_args=default_args,
        schedule_interval=config.schedule,
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        tags=list(config.tags),
    )

    start = EmptyOperator(task_id="start", dag=dag)

    # Keep PythonOperator wiring but structure remains the same; future step can
    # call into DAGFactory helpers for parity once backfill/incremental are exposed.
    ingest = PythonOperator(
        task_id="incremental_ingest",
        python_callable=_incremental_wrapper,
        op_kwargs={
            "provider": config.provider,
            "window_hours": config.window_hours,
            "data_types": config.iso_data_types,
            "datasets_var": config.datasets_variable,
        },
        dag=dag,
    )

    backfill = PythonOperator(
        task_id="idempotent_backfill",
        python_callable=_backfill_wrapper,
        op_kwargs={
            "provider": config.provider,
            "default_chunk_hours": config.chunk_hours,
            "data_types": config.iso_data_types,
            "datasets_var": config.datasets_variable,
        },
        dag=dag,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> ingest >> backfill >> end

    dag.on_failure_callback = build_failure_callback(source=f"aurum.airflow.prod_ingest.{config.provider}")
    return dag


for dag_config in PROVIDER_DAG_CONFIGS:
    dag_instance = _build_dag(dag_config)
    globals()[dag_config.dag_id] = dag_instance

__all__ = [config.dag_id for config in PROVIDER_DAG_CONFIGS]
