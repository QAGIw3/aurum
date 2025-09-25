"""Central Celery app configuration used by API async offloading.

Configuration via env vars (reasonable defaults for dev):
- AURUM_CELERY_BROKER_URL (default: redis://localhost:6379/0)
- AURUM_CELERY_RESULT_BACKEND (default: redis://localhost:6379/1)
- AURUM_CELERY_TASK_DEFAULT_QUEUE (optional default queue name)
"""
from __future__ import annotations

import os
from celery import Celery
from typing import Optional

_celery_app: Optional[Celery] = None


def get_celery_app() -> Celery:
    global _celery_app
    if _celery_app is not None:
        return _celery_app

    broker_url = os.getenv("AURUM_CELERY_BROKER_URL", "redis://localhost:6379/0")
    result_backend = os.getenv("AURUM_CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
    default_queue = os.getenv("AURUM_CELERY_TASK_DEFAULT_QUEUE", "default")

    app = Celery("aurum-api", broker=broker_url, backend=result_backend)

    # Minimal config; projects can extend via Celery config files if desired
    app.conf.update(
        task_default_queue=default_queue,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone=os.getenv("TZ", "UTC"),
        enable_utc=True,
    )

    _celery_app = app
    return app

