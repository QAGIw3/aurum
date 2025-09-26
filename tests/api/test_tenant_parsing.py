from __future__ import annotations

import pytest

from aurum.api.telemetry.context import (
    TenantIdValidationError,
    extract_tenant_id_from_headers,
    normalize_tenant_id,
)


def test_normalize_tenant_id_lowercases_and_strips() -> None:
    assert normalize_tenant_id(" Tenant-ABC ") == "tenant-abc"


def test_normalize_tenant_id_returns_none_for_empty() -> None:
    assert normalize_tenant_id(None) is None
    assert normalize_tenant_id("   ") is None


def test_normalize_tenant_id_rejects_invalid_values() -> None:
    with pytest.raises(TenantIdValidationError):
        normalize_tenant_id("bad tenant!")


def test_extract_tenant_from_asgi_headers() -> None:
    headers = [(b"x-aurum-tenant", b"Tenant_ONE"), (b"content-type", b"application/json")]
    assert extract_tenant_id_from_headers(headers) == "tenant_one"


def test_extract_tenant_from_mapping() -> None:
    headers = {"X-Aurum-Tenant": "Tenant_TWO"}
    assert extract_tenant_id_from_headers(headers) == "tenant_two"


def test_extract_tenant_invalid_raises() -> None:
    headers = {"X-Aurum-Tenant": "no spaces!"}
    with pytest.raises(TenantIdValidationError):
        extract_tenant_id_from_headers(headers)
