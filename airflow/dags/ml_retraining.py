"""Airflow DAG to periodically retrain and register forecasting models.

Reads a CSV path and column details from Airflow Variables and uses the
lightweight ML platform in `aurum.ml` to pick the best baseline forecaster,
fit on all available data, and register it in the local filesystem registry.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

_SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if _SRC_PATH and _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from aurum.airflow_utils import build_failure_callback, build_preflight_callable


DEFAULT_ARGS = {
    "owner": "aurum-ml",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=30),
}


def _run_retraining(**_: object) -> None:
    """Load variables, fetch data from CSV, retrain and register the best model."""
    from airflow.models import Variable  # type: ignore
    import pandas as pd
    from aurum.ml.retraining import retrain_best_forecaster

    csv_path = Variable.get("aurum_ml_train_csv")
    column = Variable.get("aurum_ml_train_column")
    time_index = Variable.get("aurum_ml_time_index", default_var=None)
    model_name = Variable.get("aurum_ml_model_name", default_var="energy_price_forecaster")
    horizon = int(Variable.get("aurum_ml_horizon", default_var="24"))
    initial = int(Variable.get("aurum_ml_initial", default_var="100"))
    step = int(Variable.get("aurum_ml_step", default_var="1"))
    freq = Variable.get("aurum_ml_freq", default_var=None)

    def _fetch() -> pd.Series:
        parse = [time_index] if time_index else None
        df = pd.read_csv(csv_path, parse_dates=parse)
        s = df[column]
        if time_index and time_index in df.columns:
            s.index = pd.to_datetime(df[time_index])
        return s

    outcome = retrain_best_forecaster(
        _fetch,
        model_name=model_name,
        horizon=horizon,
        initial_train_size=initial,
        step=step,
        freq_hint=freq,
    )
    print("[ml_retraining]", outcome)


with DAG(
    dag_id="ml_retraining",
    description="Periodic ML retraining and registration",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # Daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "ml", "retraining"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=build_preflight_callable(
            required_variables=("aurum_ml_train_csv", "aurum_ml_train_column"),
            optional_variables=(
                "aurum_ml_time_index",
                "aurum_ml_model_name",
                "aurum_ml_horizon",
                "aurum_ml_initial",
                "aurum_ml_step",
                "aurum_ml_freq",
            ),
        ),
    )

    retrain = PythonOperator(task_id="retrain", python_callable=_run_retraining)

    end = EmptyOperator(task_id="end")

    start >> preflight >> retrain >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ml_retraining")

__all__ = ["dag"]

