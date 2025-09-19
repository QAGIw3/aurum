#!/usr/bin/env bash
set -euo pipefail

IMAGE="python:3.11-slim"
WORKDIR="/workspace"

echo "Running tests inside ${IMAGE}..." >&2
docker run --rm -v "$(pwd)":${WORKDIR} -w ${WORKDIR} \
  ${IMAGE} bash -lc "pip install --no-cache-dir -U pip && pip install --no-cache-dir -r requirements-dev.txt && pytest -q"

