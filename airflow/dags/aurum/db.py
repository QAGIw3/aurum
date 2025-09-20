from __future__ import annotations

from datetime import datetime
from typing import Any


def register_ingest_source(name: str, *, description: str, schedule: str, target: str) -> None:  # pragma: no cover - stub
    # In Airflow pods we only need this for DAG import-time; no-op is fine.
    return None


def update_ingest_watermark(source_name: str, field: str, value: datetime) -> None:  # pragma: no cover - stub
    # No-op stub for DAG import-time; runtime tasks call real implementations.
    return None


