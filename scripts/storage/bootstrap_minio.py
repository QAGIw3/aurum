#!/usr/bin/env python
"""Bootstrap MinIO/S3 buckets with versioning, encryption, and lifecycle policies."""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable

import boto3
from botocore.exceptions import ClientError

DEFAULT_CONFIG = Path("config/storage/minio_buckets.json")


@dataclass
class BucketPlan:
    name: str
    versioning: bool
    encryption: str | None
    kms_key_id: str | None
    lifecycle: Dict[str, Any] | None


def _load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Bucket config not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_env(value: str | None) -> str | None:
    if not value:
        return value
    if value.startswith("${") and value.endswith("}"):
        env_name = value[2:-1]
        resolved = os.getenv(env_name)
        if resolved is None:
            raise RuntimeError(f"Environment variable {env_name} not set for placeholder {value}")
        return resolved
    env_value = os.getenv(value)
    if env_value is not None:
        return env_value
    return value


def _build_plans(config: dict[str, Any]) -> Iterable[BucketPlan]:
    defaults = config.get("defaults", {})
    default_versioning = bool(defaults.get("versioning", True))
    default_encryption = defaults.get("encryption")

    for entry in config.get("buckets", []):
        name = entry["name"]
        versioning = bool(entry.get("versioning", default_versioning))
        encryption = entry.get("encryption", default_encryption)
        kms_key_env = entry.get("kms_key_env")
        kms_key_direct = entry.get("kms_key")
        kms_key_id = _resolve_env(kms_key_direct) if kms_key_direct else _resolve_env(kms_key_env) if kms_key_env else None
        lifecycle = entry.get("lifecycle")
        yield BucketPlan(name=name, versioning=versioning, encryption=encryption, kms_key_id=kms_key_id, lifecycle=lifecycle)


def _create_bucket_if_missing(s3, bucket: BucketPlan, region: str | None) -> None:
    try:
        s3.head_bucket(Bucket=bucket.name)
        print(f"[skip] bucket {bucket.name} already exists")
        return
    except ClientError as exc:
        error_code = exc.response["Error"].get("Code", "")
        if error_code not in {"404", "NoSuchBucket"}:
            raise

    create_kwargs: dict[str, Any] = {"Bucket": bucket.name}
    if region and region != "us-east-1":  # us-east-1 requires no location constraint
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}

    s3.create_bucket(**create_kwargs)
    print(f"[create] bucket {bucket.name}")


def _ensure_versioning(s3, bucket: BucketPlan) -> None:
    if not bucket.versioning:
        return
    s3.put_bucket_versioning(
        Bucket=bucket.name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    print(f"[versioning] enabled for {bucket.name}")


def _ensure_encryption(s3, bucket: BucketPlan) -> None:
    if not bucket.encryption:
        return
    if bucket.encryption.upper() == "SSE-S3":
        rules = [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
    elif bucket.encryption.upper() == "SSE-KMS":
        if not bucket.kms_key_id:
            raise RuntimeError(f"Bucket {bucket.name} requires kms_key for SSE-KMS")
        rules = [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "aws:kms",
                    "KMSMasterKeyID": bucket.kms_key_id,
                }
            }
        ]
    else:
        raise ValueError(f"Unsupported encryption mode: {bucket.encryption}")

    s3.put_bucket_encryption(
        Bucket=bucket.name,
        ServerSideEncryptionConfiguration={"Rules": rules},
    )
    print(f"[encryption] {bucket.encryption.upper()} set for {bucket.name}")


def _ensure_lifecycle(s3, bucket: BucketPlan) -> None:
    if not bucket.lifecycle:
        return
    rules = bucket.lifecycle.get("rules")
    if not rules:
        return
    rule_defs: list[dict[str, Any]] = []
    for rule in rules:
        definition: dict[str, Any] = {
            "ID": rule.get("id", f"{bucket.name}-rule"),
            "Status": rule.get("status", "Enabled"),
        }
        if "transitions" in rule:
            definition["Transitions"] = [
                {"Days": item["days"], "StorageClass": item["storage_class"]}
                for item in rule["transitions"]
            ]
        if "expiration" in rule:
            definition["Expiration"] = {"Days": rule["expiration"]["days"]}
        if "noncurrent_version_expiration" in rule:
            definition["NoncurrentVersionExpiration"] = {
                "NoncurrentDays": rule["noncurrent_version_expiration"]["noncurrent_days"]
            }
        rule_defs.append(definition)

    if rule_defs:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket.name,
            LifecycleConfiguration={"Rules": rule_defs},
        )
        print(f"[lifecycle] configured for {bucket.name}")


def _apply_bucket_plan(s3, bucket: BucketPlan, region: str | None) -> None:
    _create_bucket_if_missing(s3, bucket, region)
    _ensure_versioning(s3, bucket)
    _ensure_encryption(s3, bucket)
    _ensure_lifecycle(s3, bucket)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create/configure MinIO buckets using boto3")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to bucket JSON config")
    parser.add_argument("--endpoint", default=os.getenv("AURUM_S3_ENDPOINT"), help="S3/MinIO endpoint URL")
    parser.add_argument("--region", default=os.getenv("AURUM_S3_REGION", "us-east-1"), help="Bucket region")
    parser.add_argument("--access-key", default=os.getenv("AURUM_S3_ACCESS_KEY"), help="Access key")
    parser.add_argument("--secret-key", default=os.getenv("AURUM_S3_SECRET_KEY"), help="Secret key")
    args = parser.parse_args(list(argv) if argv is not None else None)

    config = _load_config(args.config)
    plans = list(_build_plans(config))

    session = boto3.session.Session(
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
    )
    s3 = session.client("s3", endpoint_url=args.endpoint)

    try:
        for plan in plans:
            _apply_bucket_plan(s3, plan, args.region)
    except ClientError as exc:
        print(f"Error applying plan: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
