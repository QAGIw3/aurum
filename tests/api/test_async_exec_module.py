from __future__ import annotations

import importlib
import sys

import pytest


ASYNCHRONOUS_MODULE_PREFIX = "aurum.api.async_exec"


def _reload_async_exec(monkeypatch, **env):
    keys = {
        "AURUM_API_OFFLOAD_USE_STUB",
        "AURUM_API_OFFLOAD_ENABLED",
    }
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    for module_name in list(sys.modules):
        if module_name == ASYNCHRONOUS_MODULE_PREFIX or module_name.startswith(f"{ASYNCHRONOUS_MODULE_PREFIX}."):
            sys.modules.pop(module_name)
    return importlib.import_module(ASYNCHRONOUS_MODULE_PREFIX)


def test_offload_disabled_flag(monkeypatch):
    module = _reload_async_exec(monkeypatch, AURUM_API_OFFLOAD_ENABLED="0")
    assert module.OFFLOAD_ENABLED is False
    _reload_async_exec(monkeypatch)


def test_force_stub(monkeypatch):
    module = _reload_async_exec(monkeypatch, AURUM_API_OFFLOAD_USE_STUB="1")
    assert module.OFFLOAD_ENABLED is False
    task_id = module.run_job_async("demo", {})
    assert task_id.startswith("dev-")
    _reload_async_exec(monkeypatch)
