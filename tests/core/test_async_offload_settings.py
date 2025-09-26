from __future__ import annotations

import pytest

from aurum.core.settings import SimplifiedSettings


@pytest.fixture
def clear_env(monkeypatch):
    keys = [
        "AURUM_API_OFFLOAD_ENABLED",
        "AURUM_API_OFFLOAD_USE_STUB",
        "AURUM_API_OFFLOAD_CELERY_BROKER_URL",
        "AURUM_API_OFFLOAD_CELERY_RESULT_BACKEND",
        "AURUM_API_OFFLOAD_DEFAULT_QUEUE",
        "AURUM_CELERY_BROKER_URL",
        "AURUM_CELERY_RESULT_BACKEND",
        "AURUM_CELERY_TASK_DEFAULT_QUEUE",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def test_async_offload_defaults_use_stub(clear_env):
    settings = SimplifiedSettings()
    offload = settings.async_offload
    assert offload.enabled is False
    assert offload.use_stub is True
    assert offload.broker_url == "redis://localhost:6379/0"
    assert offload.result_backend == "redis://localhost:6379/1"
    assert offload.default_queue == "default"


def test_async_offload_custom_env(monkeypatch, clear_env):
    monkeypatch.setenv("AURUM_API_OFFLOAD_ENABLED", "1")
    monkeypatch.setenv("AURUM_API_OFFLOAD_USE_STUB", "false")
    monkeypatch.setenv("AURUM_API_OFFLOAD_CELERY_BROKER_URL", "redis://redis.example/5")
    monkeypatch.setenv("AURUM_API_OFFLOAD_CELERY_RESULT_BACKEND", "redis://redis.example/6")
    monkeypatch.setenv("AURUM_API_OFFLOAD_DEFAULT_QUEUE", "cpu-bound")

    settings = SimplifiedSettings()
    offload = settings.async_offload

    assert offload.enabled is True
    assert offload.use_stub is False
    assert offload.broker_url == "redis://redis.example/5"
    assert offload.result_backend == "redis://redis.example/6"
    assert offload.default_queue == "cpu-bound"
