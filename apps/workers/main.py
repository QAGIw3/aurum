"""Worker application with single task runner."""
from __future__ import annotations

import logging
from typing import Any, Dict

from celery import Celery

from libs.common.config import get_settings
from libs.storage import TimescaleSeriesRepo, PostgresMetaRepo

logger = logging.getLogger(__name__)

# Initialize Celery app
settings = get_settings()
celery_app = Celery(
    "aurum-workers",
    broker=settings.workers.broker_url,
    backend=settings.workers.result_backend,
)

# Celery configuration
celery_app.conf.update(
    task_default_queue=settings.workers.default_queue,
    task_time_limit=settings.workers.task_timeout,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=True,
)


@celery_app.task(bind=True, name="process_scenario")
def process_scenario_task(self, scenario_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Process scenario execution task."""
    try:
        logger.info(f"Processing scenario {scenario_id}")
        
        # This would contain the actual scenario processing logic
        result = {
            "scenario_id": scenario_id,
            "status": "completed",
            "results": {
                "processed_at": "2024-01-01T00:00:00Z",
                "parameters": parameters,
            }
        }
        
        logger.info(f"Scenario {scenario_id} completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Scenario {scenario_id} failed: {e}")
        self.retry(countdown=60, max_retries=3)


@celery_app.task(bind=True, name="ingest_data")
def ingest_data_task(self, source: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Data ingestion task."""
    try:
        logger.info(f"Ingesting data from {source}")
        
        # This would contain the actual ingestion logic
        result = {
            "source": source,
            "status": "completed",
            "records_processed": 1000,  # Example
            "processed_at": "2024-01-01T00:00:00Z",
        }
        
        logger.info(f"Data ingestion from {source} completed")
        return result
        
    except Exception as e:
        logger.error(f"Data ingestion from {source} failed: {e}")
        self.retry(countdown=300, max_retries=2)


if __name__ == "__main__":
    # Start worker
    celery_app.start()