#!/usr/bin/env python3
"""Register NOAA Avro schemas for all configured NOAA topics.

Reads topics from config/noaa_ingest_datasets.json and registers the schema
in kafka/schemas/noaa.weather.v1.avsc under each subject `${topic}-value`.

Env vars:
- SCHEMA_REGISTRY_URL (required)

Usage:
  python scripts/noaa/register_schemas.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Iterable, Set

import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "config" / "noaa_ingest_datasets.json"
SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "noaa.weather.v1.avsc"


def _load_topics_from_config(path: Path) -> Set[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    topics: Set[str] = set()
    for ds in data.get("datasets", []):
        topic = ds.get("topic")
        if isinstance(topic, str) and topic.strip():
            topics.add(topic.strip())
    return topics


def _register_subject(schema_registry: str, subject: str, schema_json: str) -> bool:
    url = f"{schema_registry.rstrip('/')}/subjects/{subject}/versions"
    payload = {"schema": schema_json}
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
    if resp.status_code in (200, 201):
        return True
    # If already registered with identical schema, Schema Registry may still return 200/201
    # For other cases, print error and return False
    try:
        detail = resp.json()
    except Exception:
        detail = {"text": resp.text}
    print(f"❌ Failed to register {subject}: {resp.status_code} {detail}")
    return False


def main(argv: Iterable[str] | None = None) -> int:
    schema_registry = os.getenv("SCHEMA_REGISTRY_URL")
    if not schema_registry:
        print("SCHEMA_REGISTRY_URL must be set")
        return 1

    if not CONFIG_PATH.exists():
        print(f"Config not found: {CONFIG_PATH}")
        return 1
    if not SCHEMA_PATH.exists():
        print(f"Schema not found: {SCHEMA_PATH}")
        return 1

    topics = _load_topics_from_config(CONFIG_PATH)
    if not topics:
        print("No NOAA topics found in config; nothing to register")
        return 0

    schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
    # Minify for Schema Registry payload
    schema_obj = json.loads(schema_text)
    schema_json = json.dumps(schema_obj, separators=(",", ":"))

    ok = 0
    # Also ensure incremental subjects are present for the generic external pipeline
    incremental_subjects = [
        "aurum.ext.series_catalog.upsert.incremental.v1-value",
        "aurum.ext.timeseries.obs.incremental.v1-value",
    ]
    for topic in sorted(topics):
        subject = f"{topic}-value"
        print(f"Registering subject: {subject}")
        if _register_subject(schema_registry, subject, schema_json):
            ok += 1
            print(f"✅ Registered {subject}")

    # Register incremental subjects using the external timeseries schema where appropriate
    # Catalog upsert schema
    ext_catalog_path = REPO_ROOT / "kafka" / "schemas" / "ExtSeriesCatalogUpsertV1.avsc"
    ext_obs_path = REPO_ROOT / "kafka" / "schemas" / "ExtTimeseriesObsV1.avsc"
    if ext_catalog_path.exists() and ext_obs_path.exists():
        ext_catalog_json = json.dumps(json.loads(ext_catalog_path.read_text(encoding='utf-8')), separators=(",", ":"))
        ext_obs_json = json.dumps(json.loads(ext_obs_path.read_text(encoding='utf-8')), separators=(",", ":"))
        mapping = {
            incremental_subjects[0]: ext_catalog_json,
            incremental_subjects[1]: ext_obs_json,
        }
        for subject, schema_json2 in mapping.items():
            print(f"Registering subject: {subject}")
            if _register_subject(schema_registry, subject, schema_json2):
                ok += 1
                print(f"✅ Registered {subject}")

    print(f"Done. Registered {ok}/{len(topics)} subjects.")
    return 0 if ok == len(topics) else 2


if __name__ == "__main__":
    sys.exit(main())
