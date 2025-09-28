from __future__ import annotations

import pytest

from aurum.tenancy import (
    InMemoryTenantStore,
    TenantConfiguration,
    TenantIsolationController,
    TenantManager,
    TenantQuota,
    TenantQuotaExceeded,
)


def _manager(**kwargs) -> TenantManager:
    return TenantManager(
        store=InMemoryTenantStore(),
        isolation=TenantIsolationController(tuple()),
        **kwargs,
    )


def test_provision_tenant_applies_default_quota() -> None:
    manager = _manager(default_quotas={"requests": {"hard_limit": 100}})
    tenant = manager.provision_tenant("acme").tenant
    assert "requests" in tenant.quotas
    assert tenant.quotas["requests"].hard_limit == 100


def test_record_usage_enforces_quota() -> None:
    manager = _manager()
    manager.provision_tenant("acme")
    manager.apply_quota("acme", TenantQuota(name="requests", hard_limit=5))

    snapshot = manager.record_usage("acme", "requests", 3)
    assert snapshot.metrics["requests"] == 3

    with pytest.raises(TenantQuotaExceeded):
        manager.record_usage("acme", "requests", 5)


def test_ensure_tenant_uses_baseline_configuration() -> None:
    baseline = TenantConfiguration(plan="enterprise", features={"realtime": True})
    manager = _manager(baseline_configuration=baseline)
    tenant = manager.ensure_tenant("globex")
    assert tenant.configuration.plan == "enterprise"
    assert tenant.configuration.features.get("realtime") is True
