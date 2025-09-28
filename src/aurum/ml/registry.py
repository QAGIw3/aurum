"""Lightweight filesystem model registry for MLOps workflows.

Models are stored under a base directory (default: artifacts/models), with a
structure: {base}/{name}/{version}/ containing model.pkl and metadata.json.
Serialization uses joblib if available, else pickle.
"""
from __future__ import annotations

import json
import os
import pickle
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


try:  # pragma: no cover - optional dependency
    import joblib as _joblib  # type: ignore
except Exception:  # pragma: no cover - fallback
    _joblib = None  # type: ignore[assignment]


@dataclass(frozen=True)
class ModelMetadata:
    name: str
    version: str
    created_at: str
    metrics: Mapping[str, float]
    extra: Mapping[str, Any]


class ModelRegistry:
    def __init__(self, base_dir: str | Path = "artifacts/models") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _model_dir(self, name: str, version: str) -> Path:
        return self.base_dir / name / version

    def save(self, name: str, version: str, model: Any, *, metrics: Optional[Mapping[str, float]] = None, **extra: Any) -> Path:
        d = self._model_dir(name, version)
        d.mkdir(parents=True, exist_ok=True)
        # Serialize
        model_path = d / "model.pkl"
        if _joblib is not None:
            _joblib.dump(model, model_path)
        else:
            with open(model_path, "wb") as f:
                pickle.dump(model, f)
        # Metadata
        meta = ModelMetadata(
            name=name,
            version=version,
            created_at=datetime.now(timezone.utc).isoformat(),
            metrics=dict(metrics or {}),
            extra=dict(extra),
        )
        with open(d / "metadata.json", "w") as f:
            json.dump(asdict(meta), f, indent=2)
        return d

    def load(self, name: str, version: str) -> Any:
        d = self._model_dir(name, version)
        model_path = d / "model.pkl"
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
        if _joblib is not None:
            return _joblib.load(model_path)
        with open(model_path, "rb") as f:
            return pickle.load(f)

    def metadata(self, name: str, version: str) -> ModelMetadata:
        d = self._model_dir(name, version)
        meta_path = d / "metadata.json"
        if not meta_path.exists():
            raise FileNotFoundError(f"Metadata not found: {meta_path}")
        with open(meta_path, "r") as f:
            data = json.load(f)
        return ModelMetadata(**data)

    def list_versions(self, name: str) -> list[str]:
        d = self.base_dir / name
        if not d.exists():
            return []
        return sorted([p.name for p in d.iterdir() if p.is_dir()])

    def latest(self, name: str) -> tuple[str, ModelMetadata] | None:
        versions = self.list_versions(name)
        if not versions:
            return None
        latest_version = versions[-1]
        return latest_version, self.metadata(name, latest_version)


__all__ = ["ModelRegistry", "ModelMetadata"]

