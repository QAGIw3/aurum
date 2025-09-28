"""Airflow dynamic DAG loader.

Discovers workflow configs under a configurable directory and registers them
as Airflow DAGs at import time. Intended to be imported by the scheduler.
"""

from __future__ import annotations

import os
import logging
from typing import Dict

from aurum.workflow.dynamic_dags import build_dags_from_dir

logger = logging.getLogger(__name__)


def _discover_config_dir() -> str:
    # Allow override; fallback to repository config path
    return os.getenv("AURUM_WORKFLOW_CONFIG_DIR", "config/workflows")


def register(globals_dict: Dict[str, object]) -> None:
    cfg_dir = _discover_config_dir()
    dags = build_dags_from_dir(cfg_dir)
    if not dags:
        logger.info("No dynamic DAGs discovered under %s", cfg_dir)
    globals_dict.update(dags)


# Register on import
register(globals())

