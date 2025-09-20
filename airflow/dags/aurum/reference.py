from __future__ import annotations

"""Stub subpackage for `aurum.reference` to satisfy DAG imports.

This file exposes a minimal `eia_catalog.get_dataset` used by
`ingest_public_feeds.py` at parse-time. At runtime, the real
implementation is available under `/opt/airflow/src/aurum/reference`.
"""

from dataclasses import dataclass


@dataclass
class _Dataset:
    default_frequency: str | None = None


# Deprecated single-file reference stub kept for backward compatibility


