"""Example DAG: PythonOperator mapping over op_kwargs for chunked backfills.

Demonstrates Airflow dynamic task mapping using PythonOperator.partial().expand()
to process logical time windows in parallel. Useful for backfilling historical
data by chunks.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def _generate_chunks(start: datetime, end: datetime, step: timedelta) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = []
    current = start
    while current < end:
        nxt = min(current + step, end)
        chunks.append({"start": current.isoformat(), "end": nxt.isoformat()})
        current = nxt
    return chunks


def process_backfill_chunk(*, start: str, end: str, dataset: str = "unknown") -> Dict[str, Any]:
    # This would run your ingest/transform for the given window.
    # Use idempotency markers or watermark checks as appropriate.
    print(f"Processing backfill chunk for {dataset} from {start} to {end}")
    # Simulate light work; in real code, call into your library.
    return {"dataset": dataset, "start": start, "end": end, "status": "ok"}


with DAG(
    dag_id="mapped_backfill_chunks_example",
    description="Chunked backfill using PythonOperator dynamic mapping",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "examples", "mapping"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Build chunk windows: last 24 hours broken into 6-hour ranges
    # In a real backfill, compute from DAG run config or variable inputs
    now = datetime(2024, 1, 2)  # static example for deterministic parse
    windows = _generate_chunks(now - timedelta(hours=24), now, timedelta(hours=6))

    mapped = (
        PythonOperator.partial(
            task_id="process_chunk",
            python_callable=process_backfill_chunk,
        )
        .expand(op_kwargs=[{**w, "dataset": "example_stream"} for w in windows])
    )

    start >> mapped >> end

