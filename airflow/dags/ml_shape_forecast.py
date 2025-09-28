"""Airflow DAG: Shape-based forecast and CSV artifact export.

This DAG loads a univariate time series from a CSV, runs the
NearestNeighborShapeForecaster, and writes predictions to a CSV path provided
via Airflow Variables.

Airflow Variables
- Required:
  - aurum_ml_shape_csv: path to input CSV
  - aurum_ml_shape_column: column name containing the series values
  - aurum_ml_shape_output_csv: path to write forecast CSV
- Optional:
  - aurum_ml_shape_time_index: timestamp column name (sets series index)
  - aurum_ml_shape_window: int window size (default 24)
  - aurum_ml_shape_horizon: int horizon (default 6)
  - aurum_ml_shape_freq: pandas frequency alias (e.g., H)
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

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
    "execution_timeout": timedelta(minutes=20),
}


def _run_shape_forecast(**_: object) -> None:
    from airflow.models import Variable  # type: ignore
    import pandas as pd
    from aurum.ml.forecasting.shape import NearestNeighborShapeForecaster

    csv_path = Variable.get("aurum_ml_shape_csv")
    column = Variable.get("aurum_ml_shape_column")
    out_csv = Variable.get("aurum_ml_shape_output_csv")
    time_index = Variable.get("aurum_ml_shape_time_index", default_var=None)
    window = int(Variable.get("aurum_ml_shape_window", default_var="24"))
    horizon = int(Variable.get("aurum_ml_shape_horizon", default_var="6"))
    freq = Variable.get("aurum_ml_shape_freq", default_var=None)

    parse = [time_index] if time_index else None
    df = pd.read_csv(csv_path, parse_dates=parse)
    s = df[column]
    if time_index and time_index in df.columns:
        s.index = pd.to_datetime(df[time_index])

    model = NearestNeighborShapeForecaster(window_size=window, horizon=horizon)
    model.fit(s)
    res = model.forecast(freq=freq)
    out_df = pd.DataFrame({"timestamp": res.predictions.index, "prediction": res.predictions.values})
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    out_df.to_csv(out_csv, index=False)
    # Log match info for observability
    print(
        f"[ml_shape_forecast] match_start={res.match_start} match_end={res.match_end} distance={res.match_distance:.4f}"
    )


with DAG(
    dag_id="ml_shape_forecast",
    description="Shape-based curve forecast with CSV artifact",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",  # hourly by default
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "ml", "forecast"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_ml_shape_csv",
                "aurum_ml_shape_column",
                "aurum_ml_shape_output_csv",
            ),
            optional_variables=(
                "aurum_ml_shape_time_index",
                "aurum_ml_shape_window",
                "aurum_ml_shape_horizon",
                "aurum_ml_shape_freq",
            ),
        ),
    )

    run = PythonOperator(task_id="shape_forecast", python_callable=_run_shape_forecast)

    end = EmptyOperator(task_id="end")

    start >> preflight >> run >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ml_shape_forecast")

__all__ = ["dag"]

