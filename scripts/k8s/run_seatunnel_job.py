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
from typing import Any, Dict, List, Optional

from aurum.compat import requests
from pathlib import Path


DEFAULT_IMAGE = os.environ.get("SEATUNNEL_IMAGE", "apache/seatunnel:2.3.3")
DEFAULT_NAMESPACE = os.environ.get("AURUM_K8S_NAMESPACE", "aurum-dev")
SERVICE_HOST = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
SERVICE_PORT = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
API_BASE = f"https://{SERVICE_HOST}:{SERVICE_PORT}"
SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
DEFAULT_API_RETRIES = int(os.environ.get("AURUM_K8S_API_RETRIES", "3"))
DEFAULT_API_BACKOFF = float(os.environ.get("AURUM_K8S_API_BACKOFF", "2.0"))
DEFAULT_LOG_TAIL = int(os.environ.get("AURUM_K8S_LOG_TAIL", "400"))


def _headers() -> Dict[str, str]:
    token = Path(SA_TOKEN_PATH).read_text(encoding="utf-8").strip()  # type: ignore[name-defined]
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _api_request(
    method: str,
    url: str,
    *,
    json: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
    retries: int = DEFAULT_API_RETRIES,
    backoff: float = DEFAULT_API_BACKOFF,
) -> Any:
    attempt = 0
    while attempt < max(1, retries):
        attempt += 1
        try:
            response = requests.request(  # type: ignore[call-arg]
                method,
                url,
                headers=_headers(),
                json=json,
                params=params,
                timeout=timeout,
                verify=SA_CA_PATH,
            )
            response.raise_for_status()
            return response
        except Exception as exc:  # pragma: no cover - network failure path
            if attempt >= retries:
                raise
            wait = backoff * attempt
            print(f"Kubernetes API {method} {url} failed (attempt {attempt}/{retries}): {exc}; retrying in {wait:.1f}s")
            time.sleep(wait)


def _create_job(namespace: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE}/apis/batch/v1/namespaces/{namespace}/jobs"
    response = _api_request("POST", url, json=body)
    return response.json()


def _get_job(namespace: str, name: str) -> Dict[str, Any]:
    url = f"{API_BASE}/apis/batch/v1/namespaces/{namespace}/jobs/{name}"
    response = _api_request("GET", url)
    return response.json()


def _list_job_pods(namespace: str, job_name: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/api/v1/namespaces/{namespace}/pods"
    params = {"labelSelector": f"job-name={job_name}"}
    response = _api_request("GET", url, params=params)
    payload = response.json()
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _print_pod_logs(namespace: str, pod_name: str, *, container: str = "seatunnel", tail_lines: int = DEFAULT_LOG_TAIL) -> None:
    params = {"container": container, "tailLines": str(tail_lines), "timestamps": "true"}
    url = f"{API_BASE}/api/v1/namespaces/{namespace}/pods/{pod_name}/log"
    try:
        response = _api_request("GET", url, params=params, timeout=30.0, retries=1)
        logs = response.text.strip()
    except Exception as exc:  # pragma: no cover - best effort logging
        print(f"Failed to fetch logs for pod {pod_name}: {exc}")
        return

    separator = "-" * 80
    print(separator)
    print(f"Logs for pod {pod_name} (container={container}):")
    if logs:
        print(logs)
    else:
        print("<no log output received>")
    print(separator)


def _dump_failure_details(namespace: str, job_name: str, *, tail_lines: int = DEFAULT_LOG_TAIL) -> None:
    pods = _list_job_pods(namespace, job_name)
    if not pods:
        print(f"No pods found for job {job_name}")
        return
    for pod in pods:
        metadata = pod.get("metadata", {}) if isinstance(pod, dict) else {}
        pod_name = metadata.get("name")
        if not pod_name:
            continue
        _print_pod_logs(namespace, pod_name, tail_lines=tail_lines)


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


def _delete_job(namespace: str, name: str) -> None:
    url = f"{API_BASE}/apis/batch/v1/namespaces/{namespace}/jobs/{name}"
    try:
        _api_request(
            "DELETE",
            url,
            params={"propagationPolicy": "Background"},
            timeout=10.0,
            retries=1,
        )
        print(f"Deleted job {name} in namespace {namespace}")
    except Exception as exc:  # pragma: no cover - cleanup best effort
        print(f"Failed to delete job {name}: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SeaTunnel config via Kubernetes Job")
    parser.add_argument("--job-name", required=True, help="Logical job name (used for config filename)")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--config-path", help="Absolute path to config inside Job container (default /workspace/seatunnel/jobs/generated/<job>.conf)")
    parser.add_argument("--cleanup", action="store_true", help="Delete the Kubernetes Job after completion")
    parser.add_argument(
        "--log-tail",
        type=int,
        default=DEFAULT_LOG_TAIL,
        help="Tail N lines from failing pods (default: %(default)s)",
    )
    args = parser.parse_args()
    args.log_tail = max(0, args.log_tail)

    body = _build_job_body(args.job_name, args.image, args.namespace, config_path=args.config_path)
    created = _create_job(args.namespace, body)
    job_name = created.get("metadata", {}).get("name")
    if not job_name:
        raise RuntimeError("Kubernetes API did not return a job name")
    print(f"Created job {job_name} in namespace {args.namespace}")

    if not args.wait:
        if args.cleanup:
            _delete_job(args.namespace, job_name)
        return 0

    deadline = time.time() + args.timeout
    poll_interval = 5
    while time.time() < deadline:
        job = _get_job(args.namespace, job_name)
        status = job.get("status", {}) if isinstance(job, dict) else {}
        if status.get("succeeded", 0) >= 1:
            print(f"Job {job_name} succeeded")
            if args.cleanup:
                _delete_job(args.namespace, job_name)
            return 0
        if status.get("failed", 0) >= 1:
            print(f"Job {job_name} reported failure status")
            _dump_failure_details(args.namespace, job_name, tail_lines=args.log_tail)
            if args.cleanup:
                _delete_job(args.namespace, job_name)
            return 1
        time.sleep(poll_interval)

    print(f"Timed out waiting for job {job_name}")
    _dump_failure_details(args.namespace, job_name, tail_lines=args.log_tail)
    if args.cleanup:
        _delete_job(args.namespace, job_name)
    return 1


if __name__ == "__main__":  # pragma: no cover
    from pathlib import Path

    raise SystemExit(main())
