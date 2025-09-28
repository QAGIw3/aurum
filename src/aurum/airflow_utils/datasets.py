"""Centralized helpers for Airflow Dataset URIs used across DAGs.

This module avoids duplication of dataset URI strings in DAGs and provides
simple builders for common namespaces (iso, noaa, ingest, triggers). It does
not require Airflow's Dataset API at import time; it only returns URI strings.
"""

from __future__ import annotations

from typing import Final


def dataset_uri(*parts: str) -> str:
    """Build a dataset URI under the ``dataset://aurum`` scheme.

    Example: ``dataset_uri("ingest", "iso", "miso", "lmp")`` ->
    ``dataset://aurum/ingest/iso/miso/lmp``
    """
    norm = [str(p).strip("/ ") for p in parts if str(p).strip("/ ")]
    return "dataset://aurum/" + "/".join(norm)


def iso_trigger(iso_code: str, name: str) -> str:
    return dataset_uri("triggers", "iso", iso_code.lower(), name)


def iso_ingest(iso_code: str, name: str) -> str:
    return dataset_uri("ingest", "iso", iso_code.lower(), name)


def noaa_trigger(name: str) -> str:
    return dataset_uri("triggers", "noaa", name)


def noaa_ingest(name: str) -> str:
    return dataset_uri("ingest", "noaa", name)


def ingest(name: str) -> str:
    return dataset_uri("ingest", name)


class URIS:
    """Commonly used dataset URIs as constants for convenience."""

    # PJM PNODES
    TRIGGER_PJM_PNODES_DAILY: Final[str] = dataset_uri("triggers", "pjm_pnodes_daily")
    INGEST_PJM_PNODES: Final[str] = dataset_uri("ingest", "pjm_pnodes")

    # PJM Day-Ahead LMP
    TRIGGER_PJM_DA_WINDOW: Final[str] = dataset_uri("triggers", "pjm_da_window_ready")
    INGEST_PJM_DA_LMP: Final[str] = dataset_uri("ingest", "pjm_da_lmp")

    # ISO LMP streams (generic)
    INGEST_ISO_LMP_RAW: Final[str] = dataset_uri("ingest", "iso_lmp_raw")
    INGEST_ISO_LMP_TIMESCALE: Final[str] = dataset_uri("ingest", "iso_lmp_timescale")

    # EIA series streams
    INGEST_EIA_SERIES_RAW: Final[str] = dataset_uri("ingest", "eia_series_raw")
    INGEST_EIA_SERIES_TIMESCALE: Final[str] = dataset_uri("ingest", "eia_series_timescale")

