#!/usr/bin/env python3
"""Wrapper to invoke the packaged backfill helper."""
from __future__ import annotations

from aurum.scripts.ingest.backfill import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
