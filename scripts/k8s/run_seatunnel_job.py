#!/usr/bin/env python
"""Create and optionally wait for a Kubernetes Job to run a SeaTunnel config.

This script assumes it's running in-cluster (e.g., from an Airflow pod) with
service account permissions to create Jobs in the current namespace.

Strategy:
- SeaTunnel config is expected to be pre-rendered at
  /workspace/seatunnel/jobs/generated/<job_name>.conf on the node, and is
  exposed to the Job via a hostPath volume mounted at /workspace.
- The Job uses the official SeaTunnel image to execute the config.
"""
from __future__ import annotations

import argparse
import os
import time
import uuid
from typing import Any, Dict

from aurum.compat import requests
from pathlib import Path


DEFAULT_IMAGE = os.environ.get("SEATUNNEL_IMAGE", "apache/seatunnel:2.3.3")
DEFAULT_NAMESPACE = os.environ.get("AURUM_K8S_NAMESPACE", "aurum-dev")
SERVICE_HOST = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
SERVICE_PORT = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
API_BASE = f"https://{SERVICE_HOST}:{SERVICE_PORT}"
SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _headers() -> Dict[str, str]:
    token = Path(SA_TOKEN_PATH).read_text(encoding="utf-8").strip()  # type: ignore[name-defined]
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _create_job(namespace: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE}/apis/batch/v1/namespaces/{namespace}/jobs"
    resp = requests.post(url, headers=_headers(), json=body, timeout=10, verify=SA_CA_PATH)
    resp.raise_for_status()
    return resp.json()


def _get_job(namespace: str, name: str) -> Dict[str, Any]:
    url = f"{API_BASE}/apis/batch/v1/namespaces/{namespace}/jobs/{name}"
    resp = requests.get(url, headers=_headers(), timeout=10, verify=SA_CA_PATH)
    resp.raise_for_status()
    return resp.json()


def _build_job_body(job_name: str, image: str, namespace: str, *, config_path: str | None = None) -> Dict[str, Any]:
    # Config path expected on the node hostPath; exposed under /workspace in the container
    if not config_path:
        config_path = f"/workspace/seatunnel/jobs/generated/{job_name}.conf"
    cmd = [
        "/bin/bash",
        "-lc",
        f"/opt/seatunnel/bin/seatunnel.sh --config {config_path}",
    ]
    name = f"seatunnel-{job_name}-{uuid.uuid4().hex[:6]}".lower()
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 600,
            "template": {
                "metadata": {"labels": {"app": "seatunnel-run", "job": job_name}},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "seatunnel",
                            "image": image,
                            "command": cmd,
                            "volumeMounts": [
                                {"name": "workspace", "mountPath": "/workspace"}
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "workspace",
                            "hostPath": {"path": "/workspace", "type": "Directory"},
                        }
                    ],
                },
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SeaTunnel config via Kubernetes Job")
    parser.add_argument("--job-name", required=True, help="Logical job name (used for config filename)")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--config-path", help="Absolute path to config inside Job container (default /workspace/seatunnel/jobs/generated/<job>.conf)")
    args = parser.parse_args()

    body = _build_job_body(args.job_name, args.image, args.namespace, config_path=args.config_path)
    created = _create_job(args.namespace, body)
    job_name = created["metadata"]["name"]
    print(f"Created job {job_name} in namespace {args.namespace}")

    if not args.wait:
        return 0

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        job = _get_job(args.namespace, job_name)
        status = job.get("status", {})
        if status.get("succeeded", 0) >= 1:
            print(f"Job {job_name} succeeded")
            return 0
        if status.get("failed", 0) >= 1:
            print(f"Job {job_name} failed")
            return 1
        time.sleep(5)
    print(f"Timed out waiting for job {job_name}")
    return 1


if __name__ == "__main__":  # pragma: no cover
    from pathlib import Path

    raise SystemExit(main())


