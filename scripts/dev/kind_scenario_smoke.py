#!/usr/bin/env python3
"""End-to-end scenario pipeline smoke test against the kind stack."""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg

API_BASE = os.getenv("KIND_SCENARIO_API_BASE", "http://api.aurum.localtest.me:8085")
TENANT_NAME = os.getenv("KIND_SCENARIO_TENANT_NAME", "kind-smoke")
TENANT_ID = os.getenv("KIND_SCENARIO_TENANT_ID", "00000000-0000-0000-0000-000000000042")
TENANT_HEADER = os.getenv("KIND_SCENARIO_TENANT_HEADER", "X-Aurum-Tenant")
POSTGRES_DSN = os.getenv(
    "KIND_SCENARIO_POSTGRES_DSN",
    "postgresql://aurum:aurum@localhost:5432/aurum",
)
SCENARIO_NAME = os.getenv("KIND_SCENARIO_NAME", "Kind Smoke Scenario")
CODE_VERSION = os.getenv("KIND_SCENARIO_CODE_VERSION", "kind-smoke")
ASSUMPTION_POLICY_NAME = os.getenv("KIND_SCENARIO_POLICY_NAME", "Policy Smoke Test")
POLL_TIMEOUT = int(os.getenv("KIND_SCENARIO_TIMEOUT", "180"))
POLL_INTERVAL = float(os.getenv("KIND_SCENARIO_POLL", "5"))


@dataclass
class Tenant:
    id: str
    name: str


def ensure_tenant(dsn: str, tenant_id: str, tenant_name: str) -> Tenant:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tenant (id, name)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (tenant_id, tenant_name),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Failed to upsert tenant")
        return Tenant(id=str(row[0]), name=tenant_name)


def _request(method: str, path: str, *, payload: Optional[dict] = None, tenant_id: Optional[str] = None) -> Dict[str, Any]:
    url = f"{API_BASE.rstrip('/')}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Accept": "application/json",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    if tenant_id:
        headers[TENANT_HEADER] = tenant_id
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: {exc.code} {detail}")
    except URLError as exc:  # noqa: PERF203 - easier to surface networking failures
        raise RuntimeError(f"{method} {url} failed: {exc}") from exc
    if not body:
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Unable to decode JSON response from {url}: {exc}") from exc


def create_scenario(tenant: Tenant) -> str:
    payload = {
        "tenant_id": tenant.id,
        "name": SCENARIO_NAME,
        "description": "Kind cluster scenario smoke test",
        "assumptions": [
            {
                "driver_type": "policy",
                "payload": {
                    "policy_name": ASSUMPTION_POLICY_NAME,
                    "start_year": time.gmtime().tm_year + 1,
                },
                "version": "v1",
            }
        ],
    }
    response = _request("POST", "/v1/scenarios", payload=payload, tenant_id=tenant.id)
    try:
        scenario_id = response["data"]["scenario_id"]
    except (KeyError, TypeError) as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Unexpected create scenario response: {response}") from exc
    return scenario_id


def trigger_run(scenario_id: str, tenant: Tenant) -> str:
    payload = {"code_version": CODE_VERSION}
    response = _request(
        "POST",
        f"/v1/scenarios/{scenario_id}/run",
        payload=payload,
        tenant_id=tenant.id,
    )
    try:
        run_id = response["data"]["run_id"]
    except (KeyError, TypeError) as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Unexpected run response: {response}") from exc
    return run_id


def wait_for_run(scenario_id: str, run_id: str, tenant: Tenant) -> str:
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        response = _request(
            "GET",
            f"/v1/scenarios/{scenario_id}/runs",
            tenant_id=tenant.id,
        )
        runs = response.get("data") or []
        for run in runs:
            if run.get("run_id") == run_id:
                state = run.get("state", "").upper()
                if state in {"SUCCEEDED", "FAILED", "ERRORED"}:
                    return state
        time.sleep(POLL_INTERVAL)
    raise RuntimeError(f"Run {run_id} did not complete within {POLL_TIMEOUT} seconds")


def wait_for_outputs(scenario_id: str, tenant: Tenant) -> tuple[int, int]:
    deadline = time.time() + POLL_TIMEOUT
    outputs = metrics = 0
    while time.time() < deadline:
        out_resp = _request(
            "GET",
            f"/v1/scenarios/{scenario_id}/outputs?limit=5",
            tenant_id=tenant.id,
        )
        outputs = len(out_resp.get("data") or [])
        met_resp = _request(
            "GET",
            f"/v1/scenarios/{scenario_id}/metrics/latest",
            tenant_id=tenant.id,
        )
        metrics = len(met_resp.get("data") or [])
        if outputs > 0 and metrics > 0:
            return outputs, metrics
        time.sleep(POLL_INTERVAL)
    raise RuntimeError(
        f"Scenario {scenario_id} produced no outputs/metrics after {POLL_TIMEOUT} seconds"
    )


def main() -> int:
    print("[kind-scenario] ensuring tenant exists")
    tenant = ensure_tenant(POSTGRES_DSN, TENANT_ID, TENANT_NAME)
    print(f"[kind-scenario] tenant id={tenant.id} name={tenant.name}")

    scenario_id = create_scenario(tenant)
    print(f"[kind-scenario] created scenario {scenario_id}")

    run_id = trigger_run(scenario_id, tenant)
    print(f"[kind-scenario] triggered run {run_id}")

    state = wait_for_run(scenario_id, run_id, tenant)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Scenario run {run_id} finished with state {state}")
    print(f"[kind-scenario] run {run_id} succeeded")

    outputs, metrics = wait_for_outputs(scenario_id, tenant)
    print(
        f"[kind-scenario] API returned outputs={outputs}, metrics={metrics} for scenario {scenario_id}"
    )
    print("[kind-scenario] smoke test complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
