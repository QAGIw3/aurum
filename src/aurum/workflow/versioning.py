"""Workflow version registry utilities."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

REGISTRY_ENV_VAR = "AURUM_WORKFLOW_REGISTRY"
DEFAULT_REGISTRY_PATH = Path("config/workflows/registry.json")


@dataclass
class VersionEvent:
    """Audit record for version promotions or rollbacks."""

    version: str
    action: str  # promote | rollback
    timestamp: str
    user: Optional[str] = None
    git_sha: Optional[str] = None
    config_path: Optional[str] = None
    notes: Optional[str] = None
    previous_version: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        payload = {
            "version": self.version,
            "action": self.action,
            "timestamp": self.timestamp,
        }
        if self.user:
            payload["user"] = self.user
        if self.git_sha:
            payload["git_sha"] = self.git_sha
        if self.config_path:
            payload["config_path"] = self.config_path
        if self.notes:
            payload["notes"] = self.notes
        if self.previous_version:
            payload["previous_version"] = self.previous_version
        if self.metadata:
            payload["metadata"] = self.metadata
        return payload


class WorkflowVersionRegistry:
    """Manages workflow DAG version state with optional audit history."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else DEFAULT_REGISTRY_PATH
        self._data: Dict[str, Any] = {"workflows": {}}
        self.load()

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def load(self) -> None:
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as handle:
                raw = json.load(handle)
            if isinstance(raw, dict) and "workflows" in raw:
                self._data = raw
            else:
                self._data = {"workflows": raw if isinstance(raw, dict) else {}}
        else:
            self._data = {"workflows": {}}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump(self._data, handle, indent=2, sort_keys=True)

    # ------------------------------------------------------------------
    # Registry operations
    # ------------------------------------------------------------------
    def _entry(self, dag_id: str) -> Dict[str, Any]:
        workflows = self._data.setdefault("workflows", {})
        entry = workflows.get(dag_id)
        if entry is None:
            entry = {"history": []}
            workflows[dag_id] = entry
        entry.setdefault("history", [])
        return entry

    def get_active_version(self, dag_id: str) -> Optional[str]:
        entry = self._data.get("workflows", {}).get(dag_id)
        if not entry:
            return None
        return entry.get("active_version")

    def list_versions(self, dag_id: Optional[str] = None) -> Dict[str, Any]:
        if dag_id:
            entry = self._data.get("workflows", {}).get(dag_id, {})
            return {dag_id: entry}
        return self._data.get("workflows", {})

    def record_event(self, dag_id: str, event: VersionEvent) -> None:
        entry = self._entry(dag_id)
        entry.setdefault("history", []).append(event.as_dict())
        if event.action == "promote" or event.action == "rollback":
            entry["active_version"] = event.version
        self.save()

    def promote(
        self,
        dag_id: str,
        version: str,
        *,
        user: Optional[str] = None,
        git_sha: Optional[str] = None,
        config_path: Optional[str] = None,
        notes: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> VersionEvent:
        current = self.get_active_version(dag_id)
        event = VersionEvent(
            version=version,
            action="promote",
            timestamp=datetime.utcnow().isoformat(),
            user=user,
            git_sha=git_sha,
            config_path=config_path,
            notes=notes,
            previous_version=current,
            metadata=metadata or {},
        )
        self.record_event(dag_id, event)
        return event

    def rollback(
        self,
        dag_id: str,
        version: str,
        *,
        user: Optional[str] = None,
        notes: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> VersionEvent:
        entry = self._entry(dag_id)
        history = entry.get("history", [])
        target = next((item for item in history if item.get("version") == version), None)
        if target is None:
            raise ValueError(f"Unknown version '{version}' for {dag_id}")
        current = entry.get("active_version")
        event = VersionEvent(
            version=version,
            action="rollback",
            timestamp=datetime.utcnow().isoformat(),
            user=user,
            notes=notes,
            previous_version=current,
            metadata=metadata or {},
        )
        self.record_event(dag_id, event)
        return event


def registry_path_from_env() -> Path:
    value = os.getenv(REGISTRY_ENV_VAR)
    if value:
        return Path(value)
    return DEFAULT_REGISTRY_PATH


def load_registry(path: Path | str | None = None) -> WorkflowVersionRegistry:
    return WorkflowVersionRegistry(path or registry_path_from_env())
