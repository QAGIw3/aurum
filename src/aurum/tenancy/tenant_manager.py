"""Tenant control-plane services for Aurum's multi-tenant platform."""
from __future__ import annotations

import copy
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Protocol, Sequence

from aurum.telemetry.context import (
    TenantIdValidationError,
    log_structured,
    normalize_tenant_id,
)

from .isolation import IsolationFailure, TenantIsolationController

LOGGER = logging.getLogger(__name__)


class TenantError(RuntimeError):
    """Base class for tenant management errors."""


class TenantProvisioningError(TenantError):
    """Raised when provisioning a tenant fails."""


class TenantNotFound(TenantError):
    """Raised when a tenant cannot be located."""


class TenantStateTransitionError(TenantError):
    """Raised when an invalid lifecycle transition is attempted."""


class TenantQuotaExceeded(TenantError):
    """Raised when a tenant exceeds a configured quota."""


class TenantLifecycleState(str, Enum):
    """Lifecycle states for a tenant."""

    PROVISIONING = "provisioning"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    OFFBOARDING = "offboarding"
    DEPROVISIONED = "deprovisioned"


@dataclass
class TenantQuota:
    """Represents a resource quota enforced per tenant."""

    name: str
    hard_limit: Optional[float]
    soft_limit: Optional[float] = None
    burst_limit: Optional[float] = None
    period: str = "monthly"
    unit: str = "requests"
    usage: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def register(self, amount: float) -> None:
        new_usage = self.usage + amount
        if self.soft_limit is not None and new_usage > self.soft_limit:
            log_structured(
                "warning",
                "tenant_quota_soft_limit",
                quota=self.name,
                soft_limit=self.soft_limit,
                usage=new_usage,
            )
        if self.hard_limit is not None and new_usage > self.hard_limit:
            raise TenantQuotaExceeded(
                f"Quota {self.name} exceeded: {new_usage} > {self.hard_limit}"
            )
        if self.burst_limit is not None and amount > self.burst_limit:
            raise TenantQuotaExceeded(
                f"Burst quota {self.name} exceeded: {amount} > {self.burst_limit}"
            )
        self.usage = new_usage

    def reset(self) -> None:
        self.usage = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "hard_limit": self.hard_limit,
            "soft_limit": self.soft_limit,
            "burst_limit": self.burst_limit,
            "period": self.period,
            "unit": self.unit,
            "usage": self.usage,
            "metadata": copy.deepcopy(self.metadata),
        }


@dataclass
class TenantUsageSnapshot:
    """Point-in-time usage summary for a tenant."""

    tenant_id: str
    recorded_at: datetime
    metrics: Dict[str, float]


@dataclass
class TenantConfiguration:
    """Per-tenant configuration, feature flags, and customization hooks."""

    plan: str = "standard"
    database_schema: Optional[str] = None
    compute_pool: Optional[str] = None
    billing_account: Optional[str] = None
    contact_email: Optional[str] = None
    features: MutableMapping[str, bool] = field(default_factory=dict)
    settings: MutableMapping[str, Any] = field(default_factory=dict)
    metadata: MutableMapping[str, Any] = field(default_factory=dict)
    labels: MutableMapping[str, str] = field(default_factory=dict)
    customizations: MutableMapping[str, Any] = field(default_factory=dict)
    data_retention_days: Optional[int] = None
    export_formats: Sequence[str] = field(default_factory=lambda: ("parquet", "csv"))

    def apply_overrides(self, overrides: Mapping[str, Any]) -> None:
        if not overrides:
            return
        for key in ("plan", "database_schema", "compute_pool", "billing_account", "contact_email", "data_retention_days"):
            if key in overrides and overrides[key] is not None:
                setattr(self, key, overrides[key])
        feature_updates = overrides.get("features") or {}
        self.features.update({str(k): bool(v) for k, v in feature_updates.items()})
        settings_updates = overrides.get("settings") or {}
        self.settings.update(settings_updates)
        metadata_updates = overrides.get("metadata") or {}
        self.metadata.update(metadata_updates)
        label_updates = overrides.get("labels") or {}
        self.labels.update({str(k): str(v) for k, v in label_updates.items()})
        customization_updates = overrides.get("customizations") or {}
        self.customizations.update(customization_updates)
        if "export_formats" in overrides and overrides["export_formats"]:
            self.export_formats = tuple(overrides["export_formats"])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan": self.plan,
            "database_schema": self.database_schema,
            "compute_pool": self.compute_pool,
            "billing_account": self.billing_account,
            "contact_email": self.contact_email,
            "features": dict(self.features),
            "settings": dict(self.settings),
            "metadata": dict(self.metadata),
            "labels": dict(self.labels),
            "customizations": dict(self.customizations),
            "data_retention_days": self.data_retention_days,
            "export_formats": list(self.export_formats),
        }


@dataclass
class TenantRecord:
    """Canonical representation of a tenant."""

    tenant_id: str
    display_name: str
    status: TenantLifecycleState
    configuration: TenantConfiguration
    quotas: Dict[str, TenantQuota] = field(default_factory=dict)
    metadata: MutableMapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    suspended_at: Optional[datetime] = None
    offboarded_at: Optional[datetime] = None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "display_name": self.display_name,
            "status": self.status.value,
            "configuration": self.configuration.to_dict(),
            "quotas": {name: quota.to_dict() for name, quota in self.quotas.items()},
            "metadata": copy.deepcopy(dict(self.metadata)),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "suspended_at": self.suspended_at.isoformat() if self.suspended_at else None,
            "offboarded_at": self.offboarded_at.isoformat() if self.offboarded_at else None,
        }


@dataclass
class TenantProvisioningResult:
    """Result payload returned when provisioning a tenant."""

    tenant: TenantRecord
    warnings: Sequence[str] = field(default_factory=tuple)


class TenantStore(Protocol):
    """Persistence abstraction for tenant metadata."""

    def create(self, record: TenantRecord) -> TenantRecord:
        ...

    def update(self, record: TenantRecord) -> TenantRecord:
        ...

    def delete(self, tenant_id: str) -> None:
        ...

    def get(self, tenant_id: str) -> Optional[TenantRecord]:
        ...

    def list(self) -> Iterable[TenantRecord]:
        ...


class InMemoryTenantStore:
    """Simple in-memory tenant store useful for tests and demos."""

    def __init__(self) -> None:
        self._records: Dict[str, TenantRecord] = {}

    def create(self, record: TenantRecord) -> TenantRecord:
        if record.tenant_id in self._records:
            raise TenantProvisioningError(f"Tenant {record.tenant_id} already exists")
        self._records[record.tenant_id] = copy.deepcopy(record)
        return copy.deepcopy(record)

    def update(self, record: TenantRecord) -> TenantRecord:
        if record.tenant_id not in self._records:
            raise TenantNotFound(record.tenant_id)
        self._records[record.tenant_id] = copy.deepcopy(record)
        return copy.deepcopy(record)

    def delete(self, tenant_id: str) -> None:
        self._records.pop(tenant_id, None)

    def get(self, tenant_id: str) -> Optional[TenantRecord]:
        stored = self._records.get(tenant_id)
        return copy.deepcopy(stored) if stored else None

    def list(self) -> Iterable[TenantRecord]:
        for record in self._records.values():
            yield copy.deepcopy(record)


class TenantLifecycleHooks:
    """Extension points for custom automation during lifecycle transitions."""

    def before_provision(self, tenant: TenantRecord) -> None:
        return None

    def after_provision(self, tenant: TenantRecord) -> None:
        return None

    def before_suspend(self, tenant: TenantRecord, reason: Optional[str]) -> None:
        return None

    def after_suspend(self, tenant: TenantRecord, reason: Optional[str]) -> None:
        return None

    def before_resume(self, tenant: TenantRecord) -> None:
        return None

    def after_resume(self, tenant: TenantRecord) -> None:
        return None

    def before_offboard(self, tenant: TenantRecord, reason: Optional[str]) -> None:
        return None

    def after_offboard(self, tenant: TenantRecord, reason: Optional[str]) -> None:
        return None


class TenantBillingAdapter:
    """Integrates tenant usage with the billing and invoicing subsystem."""

    def __init__(self, currency: str = "USD", default_rates: Optional[Mapping[str, float]] = None) -> None:
        self.currency = currency
        self._rates = dict(default_rates or {})
        self._accruals: Dict[str, float] = {}

    def set_rate(self, metric: str, rate: float) -> None:
        self._rates[metric] = rate

    def register_tenant(self, tenant: TenantRecord) -> None:
        self._accruals.setdefault(tenant.tenant_id, 0.0)
        log_structured("info", "tenant_billing_registered", tenant_id=tenant.tenant_id)

    def record_usage(
        self,
        tenant: TenantRecord,
        metric: str,
        amount: float,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> float:
        rate = self._resolve_rate(tenant, metric)
        charge = rate * amount
        self._accruals[tenant.tenant_id] = self._accruals.get(tenant.tenant_id, 0.0) + charge
        log_structured(
            "info",
            "tenant_usage_recorded",
            tenant_id=tenant.tenant_id,
            metric=metric,
            amount=amount,
            charge=charge,
            currency=self.currency,
            context=dict(metadata or {}),
        )
        return charge

    def _resolve_rate(self, tenant: TenantRecord, metric: str) -> float:
        tenant_rates = tenant.configuration.metadata.get("billing_rates", {})
        if isinstance(tenant_rates, Mapping) and metric in tenant_rates:
            return float(tenant_rates[metric])
        return float(self._rates.get(metric, 0.0))

    def update_plan(self, tenant: TenantRecord) -> None:
        plan_rates = tenant.configuration.metadata.get("plan_rates")
        if isinstance(plan_rates, Mapping):
            for metric, rate in plan_rates.items():
                self.set_rate(metric, float(rate))

    def close_account(self, tenant: TenantRecord) -> float:
        balance = self._accruals.pop(tenant.tenant_id, 0.0)
        log_structured(
            "info",
            "tenant_billing_closed",
            tenant_id=tenant.tenant_id,
            outstanding_balance=balance,
            currency=self.currency,
        )
        return balance

    def current_balance(self, tenant_id: str) -> float:
        return self._accruals.get(tenant_id, 0.0)


class TenantAnalyticsAdapter:
    """Aggregates metrics across tenants with permission checks."""

    def __init__(self, *, require_roles: Sequence[str] = ("admin",)) -> None:
        self.require_roles = {role.lower() for role in require_roles}
        self._store: Optional[TenantStore] = None

    def bind_store(self, store: TenantStore) -> None:
        self._store = store

    def is_authorized(self, actor: Mapping[str, Any]) -> bool:
        groups = actor.get("groups") if isinstance(actor, Mapping) else None
        if groups is None:
            return False
        normalized = {str(group).lower() for group in groups}
        return bool(self.require_roles & normalized)

    def run(
        self,
        query: str,
        tenant_ids: Sequence[str],
        *,
        context: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        if self._store is None:
            raise TenantError("Analytics adapter is not bound to a tenant store")
        records = [self._store.get(tid) for tid in tenant_ids]
        filtered = [record for record in records if record is not None]
        plan_counts = Counter(record.configuration.plan for record in filtered)
        state_counts = Counter(record.status.value for record in filtered)
        usage = {
            record.tenant_id: {name: quota.usage for name, quota in record.quotas.items()}
            for record in filtered
        }
        return {
            "query": query,
            "evaluated_tenants": [record.tenant_id for record in filtered],
            "plans": dict(plan_counts),
            "states": dict(state_counts),
            "usage": usage,
            "context": dict(context or {}),
        }


@dataclass
class TenantManager:
    """Coordinates tenant provisioning, isolation, quotas, and billing."""

    store: TenantStore
    isolation: TenantIsolationController
    billing: Optional[TenantBillingAdapter] = None
    analytics: Optional[TenantAnalyticsAdapter] = None
    hooks: TenantLifecycleHooks = field(default_factory=TenantLifecycleHooks)
    baseline_configuration: Optional[TenantConfiguration] = None
    default_quotas: Optional[Mapping[str, TenantQuota | Mapping[str, Any]]] = None

    def __post_init__(self) -> None:
        if self.analytics:
            self.analytics.bind_store(self.store)
        self._baseline_configuration = copy.deepcopy(self.baseline_configuration)
        self._default_quotas = self._normalize_quotas(self.default_quotas)

    def provision_tenant(
        self,
        tenant_id: str,
        display_name: Optional[str] = None,
        *,
        configuration: Optional[TenantConfiguration] = None,
        quotas: Optional[Mapping[str, TenantQuota | Mapping[str, Any]]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> TenantProvisioningResult:
        normalized = self._normalize_tenant_id(tenant_id)
        if self.store.get(normalized) is not None:
            raise TenantProvisioningError(f"Tenant {normalized} already exists")
        if configuration is not None:
            config = configuration
        elif self._baseline_configuration is not None:
            config = copy.deepcopy(self._baseline_configuration)
        else:
            config = TenantConfiguration()
        if not config.database_schema:
            config.database_schema = f"tenant_{normalized}"
        record = TenantRecord(
            tenant_id=normalized,
            display_name=display_name or normalized,
            status=TenantLifecycleState.PROVISIONING,
            configuration=config,
            quotas=self._tenant_quotas_or_default(quotas),
            metadata=dict(metadata or {}),
        )
        self.hooks.before_provision(record)
        stored = self.store.create(record)
        try:
            self.isolation.prepare_tenant(stored)
        except IsolationFailure as exc:
            stored.status = TenantLifecycleState.DEPROVISIONED
            stored.updated_at = datetime.utcnow()
            self.store.update(stored)
            raise TenantProvisioningError(str(exc)) from exc
        if self.billing:
            self.billing.register_tenant(stored)
            self.billing.update_plan(stored)
        self.hooks.after_provision(stored)
        stored.status = TenantLifecycleState.ACTIVE
        stored.updated_at = datetime.utcnow()
        stored = self.store.update(stored)
        log_structured(
            "info",
            "tenant_provisioned",
            tenant_id=stored.tenant_id,
            plan=stored.configuration.plan,
        )
        return TenantProvisioningResult(tenant=stored)

    def update_configuration(
        self,
        tenant_id: str,
        overrides: Mapping[str, Any],
    ) -> TenantRecord:
        tenant = self._require_tenant(tenant_id)
        tenant.configuration.apply_overrides(overrides)
        tenant.updated_at = datetime.utcnow()
        updated = self.store.update(tenant)
        if self.billing:
            self.billing.update_plan(updated)
        log_structured(
            "info",
            "tenant_configuration_updated",
            tenant_id=updated.tenant_id,
            overrides=dict(overrides),
        )
        return updated

    def set_feature_flag(self, tenant_id: str, feature: str, enabled: bool) -> TenantRecord:
        return self.update_configuration(tenant_id, {"features": {feature: enabled}})

    def apply_quota(self, tenant_id: str, quota: TenantQuota) -> TenantRecord:
        tenant = self._require_tenant(tenant_id)
        tenant.quotas[quota.name] = quota
        tenant.updated_at = datetime.utcnow()
        updated = self.store.update(tenant)
        log_structured(
            "info",
            "tenant_quota_updated",
            tenant_id=tenant_id,
            quota=quota.to_dict(),
        )
        return updated

    def record_usage(
        self,
        tenant_id: str,
        metric: str,
        amount: float,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> TenantUsageSnapshot:
        tenant = self._require_tenant(tenant_id)
        quota = tenant.quotas.get(metric)
        if quota is not None:
            quota.register(amount)
        tenant.updated_at = datetime.utcnow()
        updated = self.store.update(tenant)
        if self.billing:
            self.billing.record_usage(updated, metric, amount, metadata)
        usage_snapshot = TenantUsageSnapshot(
            tenant_id=updated.tenant_id,
            recorded_at=datetime.utcnow(),
            metrics={metric: updated.quotas.get(metric).usage if metric in updated.quotas else amount},
        )
        log_structured(
            "info",
            "tenant_usage_snapshot",
            tenant_id=tenant_id,
            metric=metric,
            amount=amount,
            snapshot=usage_snapshot.metrics,
        )
        return usage_snapshot

    def export_tenant(self, tenant_id: str) -> Dict[str, Any]:
        tenant = self._require_tenant(tenant_id)
        export_payload = {
            "generated_at": datetime.utcnow().isoformat(),
            "tenant": tenant.snapshot(),
        }
        log_structured("info", "tenant_exported", tenant_id=tenant_id)
        return export_payload

    def import_tenant(self, payload: Mapping[str, Any], *, overwrite: bool = False) -> TenantProvisioningResult:
        tenant_snapshot = payload.get("tenant") if isinstance(payload, Mapping) else None
        if not isinstance(tenant_snapshot, Mapping):
            raise TenantProvisioningError("Invalid tenant export payload")
        tenant_id = tenant_snapshot.get("tenant_id")
        if not isinstance(tenant_id, str):
            raise TenantProvisioningError("Export payload missing tenant_id")
        if self.store.get(tenant_id) is not None and not overwrite:
            raise TenantProvisioningError(f"Tenant {tenant_id} already exists")
        configuration = TenantConfiguration()
        configuration.apply_overrides(tenant_snapshot.get("configuration", {}))
        quotas = {
            name: TenantQuota(
                name=name,
                hard_limit=quota_data.get("hard_limit"),
                soft_limit=quota_data.get("soft_limit"),
                burst_limit=quota_data.get("burst_limit"),
                period=quota_data.get("period", "monthly"),
                unit=quota_data.get("unit", "requests"),
                usage=quota_data.get("usage", 0.0),
                metadata=quota_data.get("metadata", {}),
            )
            for name, quota_data in (tenant_snapshot.get("quotas") or {}).items()
        }
        record = TenantRecord(
            tenant_id=tenant_id,
            display_name=tenant_snapshot.get("display_name", tenant_id),
            status=TenantLifecycleState(tenant_snapshot.get("status", TenantLifecycleState.ACTIVE.value)),
            configuration=configuration,
            quotas=quotas,
            metadata=tenant_snapshot.get("metadata", {}),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        if overwrite and self.store.get(tenant_id):
            self.store.delete(tenant_id)
        result = self.provision_tenant(
            tenant_id,
            record.display_name,
            configuration=record.configuration,
            quotas=record.quotas,
            metadata=record.metadata,
        )
        log_structured("info", "tenant_imported", tenant_id=tenant_id)
        return result

    def suspend_tenant(self, tenant_id: str, *, reason: Optional[str] = None) -> TenantRecord:
        tenant = self._require_tenant(tenant_id)
        if tenant.status != TenantLifecycleState.ACTIVE:
            raise TenantStateTransitionError("Only active tenants can be suspended")
        self.hooks.before_suspend(tenant, reason)
        tenant.status = TenantLifecycleState.SUSPENDED
        tenant.suspended_at = datetime.utcnow()
        tenant.updated_at = tenant.suspended_at
        updated = self.store.update(tenant)
        self.hooks.after_suspend(updated, reason)
        log_structured("info", "tenant_suspended", tenant_id=tenant_id, reason=reason)
        return updated

    def resume_tenant(self, tenant_id: str) -> TenantRecord:
        tenant = self._require_tenant(tenant_id)
        if tenant.status != TenantLifecycleState.SUSPENDED:
            raise TenantStateTransitionError("Only suspended tenants can be resumed")
        self.hooks.before_resume(tenant)
        tenant.status = TenantLifecycleState.ACTIVE
        tenant.suspended_at = None
        tenant.updated_at = datetime.utcnow()
        updated = self.store.update(tenant)
        self.hooks.after_resume(updated)
        log_structured("info", "tenant_resumed", tenant_id=tenant_id)
        return updated

    def offboard_tenant(self, tenant_id: str, *, reason: Optional[str] = None, retain_metadata: bool = False) -> TenantRecord:
        tenant = self._require_tenant(tenant_id)
        if tenant.status not in {TenantLifecycleState.ACTIVE, TenantLifecycleState.SUSPENDED}:
            raise TenantStateTransitionError("Tenant must be active or suspended to offboard")
        self.hooks.before_offboard(tenant, reason)
        tenant.status = TenantLifecycleState.OFFBOARDING
        tenant.offboarded_at = datetime.utcnow()
        tenant.updated_at = tenant.offboarded_at
        updated = self.store.update(tenant)
        try:
            self.isolation.teardown_tenant(updated)
        finally:
            if not retain_metadata:
                updated.metadata.clear()
            if self.billing:
                self.billing.close_account(updated)
            updated.status = TenantLifecycleState.DEPROVISIONED
            updated.updated_at = datetime.utcnow()
            updated = self.store.update(updated)
            self.hooks.after_offboard(updated, reason)
            log_structured("info", "tenant_offboarded", tenant_id=tenant_id, reason=reason)
        return updated

    def delete_tenant(self, tenant_id: str) -> None:
        if self.store.get(tenant_id) is None:
            return
        self.store.delete(tenant_id)
        log_structured("info", "tenant_deleted", tenant_id=tenant_id)

    def list_tenants(self) -> Sequence[TenantRecord]:
        return list(self.store.list())

    def get_cross_tenant_analytics(
        self,
        actor: Mapping[str, Any],
        query: str,
        tenant_ids: Optional[Sequence[str]] = None,
        *,
        context: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        if self.analytics is None:
            raise TenantError("Cross-tenant analytics not configured")
        if not self.analytics.is_authorized(actor):
            raise PermissionError("Actor lacks permission for cross-tenant analytics")
        selected = tenant_ids or [tenant.tenant_id for tenant in self.store.list()]
        payload = self.analytics.run(query, selected, context=context)
        log_structured(
            "info",
            "tenant_cross_analytics",
            actor=dict(actor),
            query=query,
            tenants=selected,
        )
        return payload

    def ensure_tenant(self, tenant_id: str) -> TenantRecord:
        tenant = self.store.get(tenant_id)
        if tenant is not None:
            return tenant
        result = self.provision_tenant(tenant_id)
        return result.tenant

    def get_tenant(self, tenant_id: str) -> Optional[TenantRecord]:
        return self.store.get(tenant_id)

    def _normalize_tenant_id(self, tenant_id: str) -> str:
        try:
            normalized = normalize_tenant_id(tenant_id)
        except TenantIdValidationError as exc:  # pragma: no cover - validation up-stack
            raise TenantProvisioningError(str(exc)) from exc
        if not normalized:
            raise TenantProvisioningError("tenant_id is required")
        return normalized

    def _require_tenant(self, tenant_id: str) -> TenantRecord:
        tenant = self.store.get(tenant_id)
        if tenant is None:
            raise TenantNotFound(tenant_id)
        return tenant

    def _normalize_quotas(
        self,
        quotas: Optional[Mapping[str, TenantQuota | Mapping[str, Any]]],
    ) -> Dict[str, TenantQuota]:
        if not quotas:
            return {}
        normalized: Dict[str, TenantQuota] = {}
        for name, quota in quotas.items():
            if isinstance(quota, TenantQuota):
                normalized[name] = quota
            else:
                normalized[name] = TenantQuota(
                    name=name,
                    hard_limit=quota.get("hard_limit"),
                    soft_limit=quota.get("soft_limit"),
                    burst_limit=quota.get("burst_limit"),
                    period=quota.get("period", "monthly"),
                    unit=quota.get("unit", "requests"),
                    usage=quota.get("usage", 0.0),
                    metadata=quota.get("metadata", {}),
                )
        return normalized

    def _tenant_quotas_or_default(
        self,
        quotas: Optional[Mapping[str, TenantQuota | Mapping[str, Any]]],
    ) -> Dict[str, TenantQuota]:
        normalized = self._normalize_quotas(quotas)
        if normalized:
            return normalized
        return copy.deepcopy(self._default_quotas) if getattr(self, "_default_quotas", None) else {}


__all__ = [
    "TenantManager",
    "TenantStore",
    "InMemoryTenantStore",
    "TenantRecord",
    "TenantConfiguration",
    "TenantProvisioningResult",
    "TenantLifecycleState",
    "TenantLifecycleHooks",
    "TenantQuota",
    "TenantUsageSnapshot",
    "TenantBillingAdapter",
    "TenantAnalyticsAdapter",
    "TenantError",
    "TenantProvisioningError",
    "TenantNotFound",
    "TenantStateTransitionError",
    "TenantQuotaExceeded",
]
