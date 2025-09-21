from __future__ import annotations

from pathlib import Path

import pytest


def test_minio_bucket_plan_resolution(monkeypatch):
    pytest.importorskip("boto3")
    import scripts.storage.bootstrap_minio as bootstrap

    config = bootstrap._load_config(Path("config/storage/minio_buckets.json"))
    monkeypatch.setenv("AURUM_CURATED_KMS_KEY", "kms-test-key")

    plans = list(bootstrap._build_plans(config))
    names = {plan.name for plan in plans}
    assert {"aurum-raw", "aurum-curated", "aurum-quarantine"}.issubset(names)

    raw_plan = next(plan for plan in plans if plan.name == "aurum-raw")
    assert raw_plan.versioning is True
    assert raw_plan.encryption.upper() == "SSE-S3"

    curated_plan = next(plan for plan in plans if plan.name == "aurum-curated")
    assert curated_plan.encryption.upper() == "SSE-KMS"
    assert curated_plan.kms_key_id == "kms-test-key"
