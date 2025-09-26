
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable
from aurum.airflow_utils import iso as iso_utils

# Generated DAG from:
#   - config/iso_dynamic_dag.sample.json
# DO NOT EDIT MANUALLY - Regenerate with: python3 scripts/airflow/generate_dynamic_dag.py

DEFAULT_ARGS = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=45),
}

DATASETS = [
{"iso": "miso", "dataset": "lmp", "description": "MISO real-time LMP via SeaTunnel", "job_name": "miso_lmp_to_kafka", "task_prefix": "miso_rt", "source_name": "miso_rt_lmp", "pool": "api_miso", "watermark_policy": "hour", "env": {"MISO_URL": "{{ var.value.get('aurum_miso_rt_url') }}", "MISO_MARKET": "REAL_TIME", "MISO_INTERVAL_SECONDS": "300", "MISO_TOPIC": "{{ var.value.get('aurum_miso_topic', 'aurum.iso.miso.lmp.v1') }}", "MISO_TIME_FORMAT": "{{ var.value.get('aurum_miso_time_format', 'yyyy-MM-dd HH:mm:ss') }}", "MISO_TIME_COLUMN": "{{ var.value.get('aurum_miso_time_column', 'Time') }}", "MISO_NODE_COLUMN": "{{ var.value.get('aurum_miso_node_column', 'CPNode') }}", "MISO_NODE_ID_COLUMN": "{{ var.value.get('aurum_miso_node_id_column', 'CPNode ID') }}", "MISO_LMP_COLUMN": "{{ var.value.get('aurum_miso_lmp_column', 'LMP') }}", "MISO_CONGESTION_COLUMN": "{{ var.value.get('aurum_miso_congestion_column', 'MCC') }}", "MISO_LOSS_COLUMN": "{{ var.value.get('aurum_miso_loss_column', 'MLC') }}"}},
        {"iso": "isone", "dataset": "lmp", "description": "ISO-NE day-ahead LMP via SeaTunnel", "job_name": "isone_lmp_to_kafka", "task_prefix": "isone_da", "source_name": "isone_da_lmp", "pool": "api_isone", "watermark_policy": "hour", "env": {"ISONE_URL": "{{ var.value.get('aurum_isone_da_url') }}", "ISONE_MARKET": "DAY_AHEAD", "ISONE_INTERVAL_SECONDS": "3600", "ISONE_TOPIC": "{{ var.value.get('aurum_isone_topic', 'aurum.iso.isone.lmp.v1') }}", "ISONE_TIME_FORMAT": "{{ var.value.get('aurum_isone_time_format', 'yyyy-MM-dd HH:mm:ss') }}", "ISONE_LOCATION_COLUMN": "{{ var.value.get('aurum_isone_location_column', 'LocationID') }}", "ISONE_LMP_COLUMN": "{{ var.value.get('aurum_isone_lmp_column', 'LMP') }}", "ISONE_ENERGY_COLUMN": "{{ var.value.get('aurum_isone_energy_column', 'EnergyComponent') }}", "ISONE_CONGESTION_COLUMN": "{{ var.value.get('aurum_isone_congestion_column', 'CongestionComponent') }}", "ISONE_LOSS_COLUMN": "{{ var.value.get('aurum_isone_loss_column', 'LossComponent') }}"}},
        {"iso": "pjm", "dataset": "lmp", "description": "PJM day-ahead LMP via SeaTunnel", "job_name": "pjm_lmp_to_kafka", "task_prefix": "pjm_da", "source_name": "pjm_da_lmp", "pool": "api_pjm", "watermark_policy": "hour", "env": {"PJM_TOPIC": "{{ var.value.get('aurum_pjm_topic', 'aurum.iso.pjm.lmp.v1') }}", "PJM_INTERVAL_START": "{{ data_interval_start.in_timezone('America/New_York').isoformat() }}", "PJM_INTERVAL_END": "{{ data_interval_end.in_timezone('America/New_York').isoformat() }}", "PJM_ROW_LIMIT": "{{ var.value.get('aurum_pjm_row_limit', '10000') }}", "PJM_MARKET": "DAY_AHEAD", "PJM_LOCATION_TYPE": "{{ var.value.get('aurum_pjm_location_type', 'NODE') }}", "PJM_ENDPOINT": "{{ var.value.get('aurum_pjm_endpoint', 'https://api.pjm.com/api/v1/da_hrl_lmps') }}", "PJM_SUBJECT": "{{ var.value.get('aurum_pjm_subject', 'aurum.iso.pjm.lmp.v1-value') }}", "ISO_LOCATION_REGISTRY": "{{ var.value.get('aurum_iso_location_registry', 'config/iso_nodes.csv') }}"}}
]

with DAG(
    dag_id="dynamic_iso_ingest",
    description="Dynamic ISO ingestion DAG generated from configuration",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "ingestion", "dynamic", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=build_preflight_callable(
            required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
        ),
    )

    # Register sources best-effort
    sources = []
    for ds in DATASETS:
        iso_code = (ds.get("iso") or "").lower()
        dataset = (ds.get("dataset") or "").lower()
        src_name = ds.get("source_name") or (f"iso.{iso_code}.{dataset}" if iso_code and dataset else ds.get("name", "unknown"))
        sources.append(
            iso_utils.IngestSource(
                name=src_name,
                description=ds.get("description", src_name),
                schedule=ds.get("schedule"),
                target=ds.get("topic"),
            )
        )
    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(tuple(sources)),
    )

    tails = []
    for ds in DATASETS:
        iso_code = (ds.get("iso") or "").lower()
        dataset = (ds.get("dataset") or "").lower()
        job_name = ds.get("job_name") or ("{}_{}_to_kafka".format(iso_code, dataset))
        task_prefix = ds.get("task_prefix") or job_name
        source_name = ds.get("source_name") or ("iso.{}.{}".format(iso_code, dataset))
        pool = ds.get("pool")
        env_map = ds.get("env", {})
        env_entries = [f"{k}=\"{v}\"" for k, v in env_map.items()]
        render, execute, watermark = iso_utils.create_seatunnel_ingest_chain(
            task_prefix,
            job_name=job_name,
            source_name=source_name,
            env_entries=env_entries,
            pool=pool,
            watermark_policy=ds.get("watermark_policy", "hour"),
        )
        preflight >> register_sources >> render >> execute >> watermark
        tails.append(watermark)

    if tails:
        tails >> end
    else:
        register_sources >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.dynamic_iso_ingest")
