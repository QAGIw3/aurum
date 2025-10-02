from __future__ import annotations

import hashlib
import json
from importlib import import_module
from typing import Any, Dict, Iterable, Tuple

from fastapi import APIRouter, HTTPException

from aurum.libs.common.config import AurumSettings, get_settings


router = APIRouter(prefix="/_internal", tags=["internal"])


SENSITIVE_KEYS = ("password", "secret", "token", "api_key", "key", "private")


def _redact(value: Any) -> Any:
    return "***" if isinstance(value, str) else None


def _is_sensitive_key(key: str) -> bool:
    k = key.lower()
    return any(part in k for part in SENSITIVE_KEYS)


def _deep_copy_redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if _is_sensitive_key(k):
                out[k] = _redact(v)
            else:
                out[k] = _deep_copy_redact(v)
        return out
    if isinstance(obj, list):
        return [_deep_copy_redact(v) for v in obj]
    return obj


def _settings_to_dict(settings: AurumSettings) -> Dict[str, Any]:
    try:
        return settings.model_dump()
    except Exception:
        # Fallback for legacy objects
        if hasattr(settings, "dict"):
            return settings.dict()  # type: ignore[no-any-return]
        try:
            return json.loads(json.dumps(settings, default=lambda o: getattr(o, "__dict__", str(o))))
        except Exception:
            return {}


def _flatten(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    items: Dict[str, Any] = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(_flatten(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


def _stable_hash(payload: Dict[str, Any]) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


@router.get("/settings/snapshot")
async def settings_snapshot() -> Dict[str, Any]:
    settings = get_settings()
    if settings.environment.lower() in {"production", "prod"} and not settings.debug:
        raise HTTPException(status_code=404)
    raw = _settings_to_dict(settings)
    redacted = _deep_copy_redact(raw)
    return {
        "environment": settings.environment,
        "snapshot": redacted,
        "hash": _stable_hash(redacted),
    }


@router.get("/settings/parity")
async def settings_parity() -> Dict[str, Any]:
    settings = get_settings()
    if settings.environment.lower() in {"production", "prod"} and not settings.debug:
        raise HTTPException(status_code=404)

    unified_raw = _settings_to_dict(settings)

    legacy_raw: Dict[str, Any] = {}
    legacy_available = False
    try:
        legacy_mod = import_module("aurum.core.settings")
        legacy_get_settings = getattr(legacy_mod, "get_settings", None)
        if callable(legacy_get_settings):
            legacy_settings = legacy_get_settings()
            legacy_raw = _settings_to_dict(legacy_settings)
            legacy_available = True
    except Exception:
        legacy_available = False

    if not legacy_available:
        return {
            "legacy_available": False,
            "parity": None,
            "note": "Legacy settings module not available; parity check skipped.",
        }

    # Redact for output; compare on non-sensitive keys only
    unified_flat = _flatten(unified_raw)
    legacy_flat = _flatten(legacy_raw)

    def _non_sensitive_items(flat: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in flat.items() if not any(part in k.lower() for part in SENSITIVE_KEYS)}

    unified_cmp = _non_sensitive_items(unified_flat)
    legacy_cmp = _non_sensitive_items(legacy_flat)

    diffs: Dict[str, Tuple[Any, Any]] = {}
    keys = set(unified_cmp) | set(legacy_cmp)
    for key in sorted(keys):
        if unified_cmp.get(key) != legacy_cmp.get(key):
            diffs[key] = (unified_cmp.get(key), legacy_cmp.get(key))

    return {
        "legacy_available": True,
        "parity_ok": len(diffs) == 0,
        "diff_count": len(diffs),
        "diffs": diffs,
    }


