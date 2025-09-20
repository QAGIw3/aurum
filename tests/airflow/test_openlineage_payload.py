from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


class DummyTI:
    def __init__(self, xcom_data):
        self._xcom_data = xcom_data
        self.pushed = {}

    def xcom_pull(self, task_ids: str, key: str):
        return self._xcom_data.get(task_ids, {}).get(key)

    def xcom_push(self, key: str, value):
        self.pushed[key] = value


class DummyResponse:
    def raise_for_status(self):
        return None


def test_emit_openlineage_builds_expected_payload(monkeypatch):
    class DummyDAG:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyOperator:
        def __init__(self, *args, **kwargs):
            pass

        def __rshift__(self, other):  # pragma: no cover - simple stub
            return other

        def __lshift__(self, other):  # pragma: no cover - simple stub
            return other

    airflow_stub = types.ModuleType("airflow")
    airflow_stub.DAG = DummyDAG
    airflow_stub.__path__ = []  # type: ignore[attr-defined]
    sys.modules.setdefault("airflow", airflow_stub)
    sys.modules.setdefault("airflow.models", types.ModuleType("airflow.models"))
    baseoperator_stub = types.ModuleType("airflow.models.baseoperator")
    baseoperator_stub.chain = lambda *args, **kwargs: None  # type: ignore[attr-defined]
    sys.modules["airflow.models.baseoperator"] = baseoperator_stub
    empty_stub = types.ModuleType("airflow.operators.empty")
    empty_stub.EmptyOperator = DummyOperator  # type: ignore[attr-defined]
    sys.modules["airflow.operators.empty"] = empty_stub
    python_stub = types.ModuleType("airflow.operators.python")
    python_stub.PythonOperator = DummyOperator  # type: ignore[attr-defined]
    sys.modules["airflow.operators.python"] = python_stub

    module_path = Path("airflow/dags/ingest_vendor_curves_eod.py").resolve()
    spec = importlib.util.spec_from_file_location("aurum.ingest_vendor_curves_eod", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    emit_openlineage_events = module.emit_openlineage_events
    endpoint = "http://openlineage.example/api"
    monkeypatch.setenv("OPENLINEAGE_ENDPOINT", endpoint)
    monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "aurum")
    monkeypatch.setenv("OPENLINEAGE_JOB_NAME", "airflow.test")
    monkeypatch.setenv("AURUM_CODE_VERSION", "abc123")
    monkeypatch.setenv("OPENLINEAGE_TENANT", "aurum-prod")
    monkeypatch.setenv("AURUM_VENDOR_TASK_IDS", "parse_pw,parse_eugp,parse_rp")

    file_a = "/tmp/vendor/PW.xlsx"
    file_b = "/tmp/vendor/EUGP.xlsx"
    xcom_data = {
        "parse_pw": {"input_files": [file_a], "rows": 10},
        "parse_eugp": {"input_files": [file_b], "rows": 5},
        "parse_rp": {"input_files": [], "rows": 0},
    }
    ti = DummyTI(xcom_data)

    captured = {}

    def fake_post(url, json, timeout):  # type: ignore[override]
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return DummyResponse()

    def fake_resolve(self, strict=False):  # type: ignore[override]
        return Path(str(self)) if str(self).startswith("/") else Path.cwd() / str(self)

    def fake_as_uri(self):  # type: ignore[override]
        return f"file://{self}"

    monkeypatch.setattr(Path, "resolve", fake_resolve, raising=False)
    monkeypatch.setattr(Path, "as_uri", fake_as_uri, raising=False)
    monkeypatch.setattr("requests.post", fake_post)

    context = {"ds": "2024-01-02", "run_id": "test-run", "ti": ti}
    emit_openlineage_events(**context)

    assert captured["url"] == endpoint
    event = captured["json"]
    assert event["run"]["runId"] == "test-run"

    facets = event["run"]["facets"]["aurumMetadata"]
    assert facets["codeVersion"] == "abc123"
    assert facets["asOfDate"] == "2024-01-02"
    assert facets["tenant"] == "aurum-prod"

    input_names = {entry["name"] for entry in event["inputs"]}
    assert f"file://{file_a}" in input_names
    assert f"file://{file_b}" in input_names

    output = event["outputs"][0]
    stats = output["facets"]["outputStatistics"]
    assert stats["rowCount"] == 15

    assert ti.pushed["openlineage_status"] == "sent"
