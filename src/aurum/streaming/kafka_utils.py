"""Shared Kafka header utilities for normalization and propagation."""
from __future__ import annotations

from typing import Any, Dict, Mapping, List, Tuple


def normalise_headers(headers: Mapping[str, Any]) -> List[Tuple[str, bytes | None]]:
    """Normalize heterogeneous header values to (str, bytes|None) pairs.

    - Ensures header keys are strings
    - Serializes complex values to JSON-ish bytes when possible, else str bytes
    """
    import json

    norm: List[Tuple[str, bytes | None]] = []
    for k, v in (headers or {}).items():
        key = str(k)
        if v is None:
            norm.append((key, None))
            continue
        if isinstance(v, (bytes, bytearray)):
            norm.append((key, bytes(v)))
        elif isinstance(v, str):
            norm.append((key, v.encode("utf-8")))
        else:
            try:
                norm.append((key, json.dumps(v).encode("utf-8")))
            except Exception:
                norm.append((key, str(v).encode("utf-8")))
    return norm


def decode_headers(raw_headers) -> Dict[str, List[str]]:
    """Decode Kafka headers into a lowercase multi-map of str -> [str]."""
    carrier: Dict[str, List[str]] = {}
    if not raw_headers:
        return carrier
    for key, value in raw_headers:
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="ignore")
        key_lc = str(key).lower()
        if value is None:
            continue
        if isinstance(value, bytes):
            value_str = value.decode("utf-8", errors="ignore")
        else:
            value_str = str(value)
        carrier.setdefault(key_lc, []).append(value_str)
    return carrier


def build_produce_headers(run_id: str | None, request_id: str | None) -> List[Tuple[str, bytes]]:
    """Build standard outbound headers including OTEL propagation."""
    headers: List[Tuple[str, bytes]] = []
    if run_id:
        headers.append(("run_id", run_id.encode("utf-8")))
    if request_id:
        headers.append(("x-request-id", request_id.encode("utf-8")))

    # Inject OpenTelemetry propagation if available
    try:
        from opentelemetry import propagate  # type: ignore
        carrier: Dict[str, str] = {}
        propagate.inject(carrier)
        for key, value in carrier.items():
            headers.append((key, value.encode("utf-8")))
    except Exception:
        # Propagation optional
        pass

    return headers


