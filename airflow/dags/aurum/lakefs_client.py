from __future__ import annotations

"""Parse-time stubs for lakeFS client functions used by vendor DAGs.

These stubs allow Airflow to import DAGs without the full Aurum package
installed. Task code should rely on the real implementation mounted at
runtime under `/opt/airflow/src/aurum/lakefs_client.py`.
"""

from typing import Optional, Dict


def ensure_branch(repo: str, branch: str, source_branch: str = "main") -> None:  # pragma: no cover - stub
    return None


def commit_branch(repo: str, branch: str, message: str, *, metadata: Optional[Dict[str, str]] = None) -> str:  # pragma: no cover - stub
    return "stub-commit-id"


def tag_commit(repo: str, tag_name: str, commit_id: str) -> None:  # pragma: no cover - stub
    return None


