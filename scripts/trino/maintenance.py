#!/usr/bin/env python3
"""Run warehouse maintenance jobs on demand."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import asdict
from typing import Literal

from aurum.core import AurumSettings
from aurum.performance.warehouse import WarehouseMaintenanceCoordinator

JobName = Literal["iceberg", "timescale", "clickhouse", "all"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run warehouse maintenance jobs")
    parser.add_argument(
        "--job",
        choices=["iceberg", "timescale", "clickhouse", "all"],
        default="iceberg",
        help="Maintenance job to run (defaults to iceberg)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    return parser.parse_args()


async def _run_job(job: JobName) -> None:
    settings = AurumSettings.from_env()
    coordinator = WarehouseMaintenanceCoordinator(settings)

    if job == "all":
        await coordinator.run_once()
    elif job == "iceberg":
        await coordinator.run_iceberg_once()
    elif job == "timescale":
        await coordinator.run_timescale_once()
    elif job == "clickhouse":
        await coordinator.run_clickhouse_once()
    else:  # pragma: no cover - defensive guard
        raise ValueError(f"Unsupported maintenance job: {job}")

    metrics = asdict(coordinator.metrics)
    logging.info("Maintenance metrics: %s", metrics)


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        asyncio.run(_run_job(args.job))
    except KeyboardInterrupt:
        logging.warning("Maintenance interrupted")
        return 130
    except Exception:  # pragma: no cover - surfaced to shell for observability
        logging.exception("Maintenance job failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
