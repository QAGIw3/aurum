"""Compatibility layer for telemetry context helpers with API-specific utilities."""

from __future__ import annotations

import re
from typing import Iterable, Mapping, Optional, Tuple, Union

from aurum.telemetry.context import *  # noqa: F401,F403


class TenantIdValidationError(ValueError):
    """Raised when a tenant identifier fails validation constraints."""


_TENANT_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,63})$")
_TENANT_HEADER_DEFAULT = "x-aurum-tenant"

HeadersInput = Union[
    Mapping[str, str],
    Iterable[Tuple[str, str]],
    Iterable[Tuple[bytes, bytes]],
]


def normalize_tenant_id(raw: Optional[str]) -> Optional[str]:
    """Normalise and validate a tenant identifier.

    Returns a lower-cased tenant id with surrounding whitespace removed. ``None``
    is returned when no tenant is provided. Raises ``TenantIdValidationError``
    when the identifier contains unsupported characters or exceeds the
    supported length (1-64 characters).
    """

    if raw is None:
        return None

    candidate = raw.strip().lower()
    if not candidate:
        return None

    if len(candidate) > 64 or not _TENANT_PATTERN.match(candidate):
        raise TenantIdValidationError(
            "tenant identifiers must be 1-64 characters of lowercase letters, numbers, '.', '_' or '-'"
        )
    return candidate


def _get_header_value(headers: HeadersInput, target: str) -> Optional[str]:
    target_lower = target.lower()

    if isinstance(headers, Mapping):
        for key, value in headers.items():
            if key.lower() == target_lower:
                return value
        return None

    for key, value in headers:
        if isinstance(key, bytes):
            key_lower = key.decode("latin-1").lower()
            value_text = value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
        else:
            key_lower = key.lower()
            value_text = value if isinstance(value, str) else str(value)

        if key_lower == target_lower:
            return value_text
    return None


def extract_tenant_id_from_headers(
    headers: HeadersInput,
    *,
    header_name: str = _TENANT_HEADER_DEFAULT,
) -> Optional[str]:
    """Extract and normalise the tenant identifier from ASGI or HTTP headers."""

    value = _get_header_value(headers, header_name)
    return normalize_tenant_id(value)


__all__ = [name for name in globals().keys() if not name.startswith("_")]
