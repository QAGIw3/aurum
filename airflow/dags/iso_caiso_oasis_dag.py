"""Airflow DAG for CAISO OASIS ingestion (LMP, AS, Load/Forecasts).

Implements Set C — CAISO OASIS:
  1) SingleZip downloader with CSV-first and XML fallback (GMT params)
  2) LMP: PRC_INTVL_LMP (RT 5-min) and PRC_LMP (DA hourly); grp_type=ALL uses 1h windows
  3) AS: AS_RESULTS mapped to as_price
  4) Load + forecasts: SLD_FCST and SLD_REN_FCST with market_run_id in {2DA,7DA,DAM,ACTUAL,RTM,RTPD,HASP}
  5) Nodes: APNode/PNode crosswalk seeding (placeholder hook)
  6) Chunking with per-report limits and auto-bisect
  7) Backfill: rolling 39 months with daily windows and visible watermarks (separate DAG entrypoint)
  8) Publish to Kafka and land raw zips with provenance
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.external.runner import _build_http_collector
from aurum.db import update_ingest_watermark
from aurum.external.providers.caiso_collectors import CaisoKafkaConfig, CaisoOasisCollector


DEFAULT_ARGS = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.getenv("AURUM_ALERT_EMAIL", "ops@example.com")],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _build_collector() -> CaisoOasisCollector:
    http = _build_http_collector("caiso-oasis", os.getenv("CAISO_OASIS_BASE_URL", "https://oasis.caiso.com/oasisapi"))
    kafka = CaisoKafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
        lmp_topic=os.getenv("AURUM_CAISO_LMP_TOPIC", "aurum.iso.caiso.lmp.v1"),
        load_topic=os.getenv("AURUM_CAISO_LOAD_TOPIC", "aurum.iso.caiso.load.v1"),
        asm_topic=os.getenv("AURUM_CAISO_ASM_TOPIC", "aurum.iso.caiso.asm.v1"),
        pnode_topic=os.getenv("AURUM_CAISO_PNODE_TOPIC", "aurum.iso.caiso.pnode.v1"),
    )
    return CaisoOasisCollector(http_collector=http, kafka_cfg=kafka)


def task_ingest_rtm_5min(**context) -> int:
    # RTM/RTPD: use last 2 hours as a conservative catch-up window
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=2)
    end = now
    collector = _build_collector()
    produced = collector.ingest_prc_lmp(start_utc=start, end_utc=end, market_run_id=os.getenv("CAISO_RTM_MARKET", "RTPD"), grp_all=True)
    if os.getenv("AURUM_ENABLE_WATERMARK_UPDATES", "true").lower() == "true":
        update_ingest_watermark("iso_caiso_lmp", "rtm", end, policy="hour")
    return produced


def task_ingest_da_hourly(**context) -> int:
    # DA: fetch prior operating day hourly windows
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    collector = _build_collector()
    produced = collector.ingest_prc_lmp(start_utc=start, end_utc=end, market_run_id="DAM", grp_all=True)
    if os.getenv("AURUM_ENABLE_WATERMARK_UPDATES", "true").lower() == "true":
        update_ingest_watermark("iso_caiso_lmp", "dam", end, policy="day")
    return produced


def task_ingest_as(**context) -> int:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=int(os.getenv("CAISO_AS_HOURS", "6")))
    collector = _build_collector()
    produced = collector.ingest_as_results(start_utc=start, end_utc=now, market_run_id=os.getenv("CAISO_AS_MARKET", "RTPD"))
    if os.getenv("AURUM_ENABLE_WATERMARK_UPDATES", "true").lower() == "true":
        update_ingest_watermark("iso_caiso_as", "default", now, policy="hour")
    return produced


def task_ingest_load_forecasts(**context) -> int:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=int(os.getenv("CAISO_LOAD_HOURS", "6")))
    collector = _build_collector()
    total = 0
    for mrid in ("2DA", "7DA", "DAM", "ACTUAL", "RTM", "RTPD", "HASP"):
        total += collector.ingest_load_and_forecast(start_utc=start, end_utc=now, market_run_id=mrid)
    return total


with DAG(
    dag_id="iso_caiso_oasis_ingest",
    description="Ingest CAISO OASIS reports to Kafka and raw landing",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",  # drive RTM frequently; other tasks run daily/hourly via task-level triggers
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "iso", "caiso", "oasis"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_rtm = PythonOperator(
        task_id="ingest_lmp_rtm_5min",
        python_callable=task_ingest_rtm_5min,
        provide_context=True,
    )

    ingest_da = PythonOperator(
        task_id="ingest_lmp_da_hourly",
        python_callable=task_ingest_da_hourly,
        provide_context=True,
    )

    ingest_as = PythonOperator(
        task_id="ingest_as_results",
        python_callable=task_ingest_as,
        provide_context=True,
    )

    ingest_load = PythonOperator(
        task_id="ingest_load_and_forecasts",
        python_callable=task_ingest_load_forecasts,
        provide_context=True,
    )

    def task_ingest_nodes(**context) -> int:
        collector = _build_collector()
        total = collector.ingest_nodes()
        total += collector.ingest_apnodes()
        return total

    ingest_nodes = PythonOperator(
        task_id="ingest_nodes_crosswalk",
        python_callable=task_ingest_nodes,
        provide_context=True,
    )

    start >> [ingest_rtm, ingest_da, ingest_as, ingest_load, ingest_nodes]


# ---- Backfill DAG (39 months daily windows) ----

with DAG(
    dag_id="iso_caiso_oasis_backfill",
    description="Backfill CAISO OASIS data for last 39 months (daily windows)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=os.getenv("CAISO_BACKFILL_SCHEDULE", "0 3 * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "iso", "caiso", "backfill"],
) as backfill_dag:
    def _backfill_window(**context) -> int:
        days = int(os.getenv("CAISO_BACKFILL_DAYS", "1"))
        # rolling 39 months ≈ 1185 days
        limit_days = int(os.getenv("CAISO_BACKFILL_LIMIT_DAYS", "1185"))
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        total = 0
        collector = _build_collector()
        do_wm = os.getenv("AURUM_ENABLE_WATERMARK_UPDATES", "true").lower() == "true"
        for i in range(1, min(days, limit_days) + 1):
            day = today - timedelta(days=i)
            start = day
            end = day + timedelta(days=1)
            total += collector.ingest_prc_lmp(start_utc=start, end_utc=end, market_run_id="DAM", grp_all=True)
            total += collector.ingest_prc_lmp(start_utc=start, end_utc=end, market_run_id="RTPD", grp_all=True)
            total += collector.ingest_as_results(start_utc=start, end_utc=end, market_run_id="RTPD")
            for mrid in ("2DA", "7DA", "DAM", "ACTUAL", "RTM", "RTPD", "HASP"):
                total += collector.ingest_load_and_forecast(start_utc=start, end_utc=end, market_run_id=mrid)
            if do_wm:
                update_ingest_watermark("iso_caiso_lmp", "dam", end, policy="day")
                update_ingest_watermark("iso_caiso_lmp", "rtm", end, policy="day")
                update_ingest_watermark("iso_caiso_as", "default", end, policy="day")
                update_ingest_watermark("iso_caiso_load", "default", end, policy="day")
        return total

    run = PythonOperator(task_id="run_backfill_window", python_callable=_backfill_window, provide_context=True)
