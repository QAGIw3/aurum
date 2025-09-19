"""Bootstrap script for Aurum dev stack.

Creates the MinIO bucket, lakeFS repository, and optional Nessie namespaces
so engineers can start iterating without manual setup.
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Iterable, Optional

import boto3
import botocore.exceptions
from botocore.client import BaseClient

from aurum.compat import requests


S3_ENDPOINT = os.getenv("AURUM_S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ["AURUM_S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["AURUM_S3_SECRET_KEY"]
S3_BUCKET = os.getenv("AURUM_S3_BUCKET", "aurum")

LAKEFS_ENDPOINT = os.getenv("LAKEFS_API_ENDPOINT", "http://lakefs:8000/api/v1")
LAKEFS_REPO = os.getenv("LAKEFS_REPO", "aurum")
LAKEFS_NAMESPACE = os.getenv("LAKEFS_STORAGE_NAMESPACE", f"s3://{S3_BUCKET}")
LAKEFS_USER = os.environ["LAKEFS_ROOT_USER"]
LAKEFS_PASSWORD = os.environ["LAKEFS_ROOT_PASSWORD"]

NESSIE_ENDPOINT = os.getenv("NESSIE_API_ENDPOINT", "http://nessie:19121/api/v1")
NESSIE_NAMESPACES = [ns.strip() for ns in os.getenv("NESSIE_NAMESPACES", "market,ops,lineage").split(",") if ns.strip()]

try:
    SESSION = requests.Session()
    SESSION.auth = (LAKEFS_USER, LAKEFS_PASSWORD)
    SESSION.headers.update({"Content-Type": "application/json"})
    _REQUESTS_ERROR: Optional[Exception] = None
except ModuleNotFoundError as exc:  # pragma: no cover - environment without requests
    SESSION = None
    _REQUESTS_ERROR = exc


def _require_requests() -> None:
    if _REQUESTS_ERROR is not None:
        raise ModuleNotFoundError(
            "The 'requests' package is required to use the bootstrap script. Install it via 'pip install requests'."
        ) from _REQUESTS_ERROR


def wait_for_http(url: str, status: int = 200, attempts: int = 30, delay: float = 2.0) -> None:
    _require_requests()
    last_exc: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == status:
                return
        except Exception as exc:  # pragma: no cover - network-only path
            last_exc = exc
        time.sleep(delay)
    raise RuntimeError(f"Service at {url} did not become ready") from last_exc


def get_s3_client() -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=os.getenv("AURUM_S3_REGION", "us-east-1"),
    )

def ensure_bucket(s3: BaseClient) -> None:
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' already exists")
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "404":
            print(f"Creating bucket '{S3_BUCKET}'")
            create_params = {"Bucket": S3_BUCKET}
            region = os.getenv("AURUM_S3_REGION", "us-east-1")
            if region != "us-east-1":
                create_params["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3.create_bucket(**create_params)
        else:
            raise



def ensure_bucket_versioning(s3: BaseClient) -> None:
    status = s3.get_bucket_versioning(Bucket=S3_BUCKET)
    if status.get("Status") == "Enabled":
        print(f"Bucket '{S3_BUCKET}' versioning already enabled")
        return

    print(f"Enabling versioning for bucket '{S3_BUCKET}'")
    s3.put_bucket_versioning(
        Bucket=S3_BUCKET,
        VersioningConfiguration={"Status": "Enabled"},
    )


def ensure_lakefs_repo() -> None:
    _require_requests()
    if SESSION is None:  # pragma: no cover - guarded by _require_requests
        raise ModuleNotFoundError("requests.Session unavailable")

    payload = {
        "name": LAKEFS_REPO,
        "id": LAKEFS_REPO,
        "storage_namespace": LAKEFS_NAMESPACE.rstrip("/") + "/",
        "description": "Aurum development repository",
    }
    url = f"{LAKEFS_ENDPOINT}/repositories"
    response = SESSION.post(url, data=json.dumps(payload), timeout=10)
    if response.status_code == 201:
        print(f"Created lakeFS repo '{LAKEFS_REPO}' -> {LAKEFS_NAMESPACE}")
    elif response.status_code == 409:
        print(f"lakeFS repo '{LAKEFS_REPO}' already exists")
    elif response.status_code == 200:
        print(f"lakeFS repo '{LAKEFS_REPO}' already existed (200)")
    else:
        raise RuntimeError(f"Failed to create lakeFS repository: {response.status_code} {response.text}")


def ensure_nessie_namespaces(namespaces: Iterable[str]) -> None:
    _require_requests()
    headers = {"Content-Type": "application/json"}
    for namespace in namespaces:
        elements = [part for part in namespace.replace("/", ".").split(".") if part]
        if not elements:
            continue
        payload = {"namespace": {"elements": elements}}
        url = f"{NESSIE_ENDPOINT}/namespaces"
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        if response.status_code in (200, 201, 409):
            status_msg = "created" if response.status_code in (200, 201) else "exists"
            print(f"Nessie namespace '{namespace}' {status_msg}")
        else:
            raise RuntimeError(
                f"Failed to create Nessie namespace '{namespace}': {response.status_code} {response.text}"
            )


def main() -> int:
    wait_for_http(f"{S3_ENDPOINT}/minio/health/ready")
    wait_for_http(f"{LAKEFS_ENDPOINT}/config")
    wait_for_http(f"{NESSIE_ENDPOINT}/config")

    s3 = get_s3_client()
    ensure_bucket(s3)
    ensure_bucket_versioning(s3)
    ensure_lakefs_repo()
    ensure_nessie_namespaces(NESSIE_NAMESPACES)

    print("Bootstrap complete.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"Bootstrap failed: {exc}", file=sys.stderr)
        raise
