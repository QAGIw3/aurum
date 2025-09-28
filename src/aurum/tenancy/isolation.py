"""Isolation primitives that enforce Aurum's multi-tenant safety guarantees."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, MutableMapping, Optional, Protocol, Sequence, runtime_checkable

from aurum.db.base import DatabaseClient, DatabaseError, SupportsExecute
from aurum.telemetry.context import log_structured

if TYPE_CHECKING:  # pragma: no cover - circular import protections
    from .tenant_manager import TenantConfiguration, TenantRecord

LOGGER = logging.getLogger(__name__)


class IsolationFailure(RuntimeError):
    """Raised when a tenant isolation step cannot be completed."""


@runtime_checkable
class DataIsolationStrategy(Protocol):
    """Contract for enforcing tenant-level data isolation."""

    def prepare(self, tenant: "TenantRecord") -> None:
        ...

    def session_settings(self, tenant_id: str) -> Mapping[str, str]:
        ...

    def teardown(self, tenant: "TenantRecord") -> None:
        ...


@runtime_checkable
class ComputeIsolationStrategy(Protocol):
    """Contract for isolating compute and background workloads per tenant."""

    def assign(self, tenant: "TenantRecord") -> Optional[str]:
        ...

    def release(self, tenant: "TenantRecord") -> None:
        ...

    def scale(self, tenant: "TenantRecord", workload_hint: Optional[str] = None) -> None:
        ...


@dataclass
class TenantIsolationStrategy:
    """Bundle data and compute strategies so they can be orchestrated together."""

    data: Optional[DataIsolationStrategy] = None
    compute: Optional[ComputeIsolationStrategy] = None

    def provision(self, tenant: "TenantRecord") -> None:
        if self.data:
            self.data.prepare(tenant)
        if self.compute:
            self.compute.assign(tenant)

    def teardown(self, tenant: "TenantRecord") -> None:
        if self.compute:
            self.compute.release(tenant)
        if self.data:
            self.data.teardown(tenant)


@dataclass
class TenantIsolationController:
    """Coordinate multiple isolation strategies across data and compute planes."""

    strategies: Sequence[TenantIsolationStrategy] = field(default_factory=tuple)

    def prepare_tenant(self, tenant: "TenantRecord") -> None:
        for strategy in self.strategies:
            strategy.provision(tenant)
            log_structured(
                "info",
                "tenant_isolation_applied",
                tenant_id=tenant.tenant_id,
                strategy=strategy.__class__.__name__,
            )

    def teardown_tenant(self, tenant: "TenantRecord") -> None:
        for strategy in reversed(self.strategies):
            try:
                strategy.teardown(tenant)
            finally:
                log_structured(
                    "info",
                    "tenant_isolation_removed",
                    tenant_id=tenant.tenant_id,
                    strategy=strategy.__class__.__name__,
                )

    def session_settings(self, tenant_id: str) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        for strategy in self.strategies:
            if strategy.data:
                merged.update(strategy.data.session_settings(tenant_id))
        return merged

    def configure_connection(
        self,
        connection: SupportsExecute,
        tenant_id: str,
        *,
        scope: str = "LOCAL",
    ) -> None:
        scope_clause = scope.upper()
        if scope_clause not in {"LOCAL", "SESSION"}:
            raise IsolationFailure(f"Unsupported SET scope: {scope}")
        settings = self.session_settings(tenant_id)
        for setting, value in settings.items():
            try:
                connection.execute(
                    f"SET {scope_clause} {setting} = %s",
                    (value,),
                )
            except Exception as exc:  # pragma: no cover - driver specific
                raise IsolationFailure(
                    f"Failed to apply session setting {setting} for tenant {tenant_id}: {exc}"
                ) from exc

    def scale_tenant(self, tenant: "TenantRecord", workload_hint: Optional[str] = None) -> None:
        for strategy in self.strategies:
            if strategy.compute:
                strategy.compute.scale(tenant, workload_hint)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str) -> str:
    parts = identifier.split(".")
    quoted = []
    for part in parts:
        if part.startswith('"') and part.endswith('"'):
            quoted.append(part)
            continue
        if not _IDENTIFIER_RE.match(part):
            raise IsolationFailure(f"Unsafe identifier: {identifier}")
        quoted.append(f'"{part}"')
    return ".".join(quoted)


@dataclass
class RowLevelSecurityIsolation:
    """Enforce tenant isolation through PostgreSQL row-level security policies."""

    database: Optional[DatabaseClient] = None
    tables: Sequence[str] = field(default_factory=tuple)
    tenant_column: str = "tenant_id"
    policy_prefix: str = "aurum_tenant_rls"
    setting_name: str = "app.current_tenant"

    def _connect(self) -> Optional[SupportsExecute]:
        if self.database is None:
            LOGGER.debug("Skipping RLS setup because no database client was provided")
            return None
        try:
            return self.database.connect()
        except DatabaseError as exc:  # pragma: no cover - relies on runtime DB
            raise IsolationFailure(f"Database connection failed: {exc}") from exc

    def _ensure_rls(self, conn: SupportsExecute, table: str) -> None:
        qualified = _quote_identifier(table)
        tenant_col = _quote_identifier(self.tenant_column)
        policy_name = _quote_identifier(f"{self.policy_prefix}_{table.replace('.', '_')}")
        statements = [
            f"ALTER TABLE {qualified} ENABLE ROW LEVEL SECURITY",
            f"CREATE POLICY IF NOT EXISTS {policy_name} ON {qualified} USING ({tenant_col} = current_setting('{self.setting_name}')::text)",
        ]
        for statement in statements:
            try:
                conn.execute(statement)
            except Exception as exc:  # pragma: no cover - driver specific
                raise IsolationFailure(
                    f"Failed to apply RLS policy on {table}: {exc}"
                ) from exc

    def prepare(self, tenant: "TenantRecord") -> None:
        connection = self._connect()
        if not connection or not self.tables:
            return
        try:
            for table in self.tables:
                self._ensure_rls(connection, table)
        finally:
            close = getattr(connection, "close", None)
            if callable(close):  # pragma: no cover - optional cleanup
                close()

    def session_settings(self, tenant_id: str) -> Dict[str, str]:
        return {self.setting_name: tenant_id}

    def teardown(self, tenant: "TenantRecord") -> None:
        if not self.tables:
            return
        connection = self._connect()
        if not connection:
            return
        try:
            for table in self.tables:
                policy_name = _quote_identifier(f"{self.policy_prefix}_{table.replace('.', '_')}")
                qualified = _quote_identifier(table)
                try:
                    connection.execute(
                        f"DROP POLICY IF EXISTS {policy_name} ON {qualified}"
                    )
                except Exception as exc:  # pragma: no cover - driver specific
                    LOGGER.warning(
                        "Failed to drop RLS policy",
                        exc_info=exc,
                    )
        finally:
            close = getattr(connection, "close", None)
            if callable(close):  # pragma: no cover
                close()


@dataclass
class SchemaPerTenantIsolation:
    """Provision dedicated schemas to achieve strong namespace isolation."""

    database: Optional[DatabaseClient] = None
    template: str = "tenant_{tenant_id}"
    grant_read_role: Optional[str] = None
    grant_write_role: Optional[str] = None

    def prepare(self, tenant: "TenantRecord") -> None:
        if self.database is None:
            LOGGER.debug("Skipping schema provisioning because no database client was provided")
            return
        schema_name = self.template.format(tenant_id=tenant.tenant_id)
        try:
            conn = self.database.connect()
        except DatabaseError as exc:  # pragma: no cover - depends on runtime DB
            raise IsolationFailure(f"Schema provisioning failed: {exc}") from exc
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema_name)}")
            if self.grant_read_role:
                conn.execute(
                    f"GRANT USAGE ON SCHEMA {_quote_identifier(schema_name)} TO {_quote_identifier(self.grant_read_role)}"
                )
            if self.grant_write_role:
                conn.execute(
                    f"GRANT ALL PRIVILEGES ON SCHEMA {_quote_identifier(schema_name)} TO {_quote_identifier(self.grant_write_role)}"
                )
        except Exception as exc:  # pragma: no cover - driver specific
            raise IsolationFailure(
                f"Failed to create schema for tenant {tenant.tenant_id}: {exc}"
            ) from exc
        finally:
            close = getattr(conn, "close", None)
            if callable(close):  # pragma: no cover
                close()

    def session_settings(self, tenant_id: str) -> Dict[str, str]:
        schema_name = self.template.format(tenant_id=tenant_id)
        return {"search_path": f"{schema_name}, public"}

    def teardown(self, tenant: "TenantRecord") -> None:
        if self.database is None:
            return
        schema_name = self.template.format(tenant_id=tenant.tenant_id)
        try:
            conn = self.database.connect()
        except DatabaseError:
            return
        try:
            conn.execute(f"DROP SCHEMA IF EXISTS {_quote_identifier(schema_name)} CASCADE")
        finally:
            close = getattr(conn, "close", None)
            if callable(close):  # pragma: no cover
                close()


@dataclass
class WorkloadPoolIsolation:
    """Assign tenants to compute pools to isolate background and streaming jobs."""

    pools: Sequence[str]
    default_pool: Optional[str] = None
    assignments: MutableMapping[str, str] = field(default_factory=dict)

    def assign(self, tenant: "TenantRecord") -> Optional[str]:
        if tenant.tenant_id in self.assignments:
            return self.assignments[tenant.tenant_id]
        pool = tenant.configuration.compute_pool or self._pick_pool()
        if pool is None:
            LOGGER.debug("No compute pool available for tenant %s", tenant.tenant_id)
            return None
        self.assignments[tenant.tenant_id] = pool
        log_structured(
            "info",
            "tenant_compute_pool_assigned",
            tenant_id=tenant.tenant_id,
            pool=pool,
        )
        return pool

    def _pick_pool(self) -> Optional[str]:
        if self.default_pool:
            return self.default_pool
        return self.pools[0] if self.pools else None

    def release(self, tenant: "TenantRecord") -> None:
        pool = self.assignments.pop(tenant.tenant_id, None)
        if pool:
            log_structured(
                "info",
                "tenant_compute_pool_released",
                tenant_id=tenant.tenant_id,
                pool=pool,
            )

    def scale(self, tenant: "TenantRecord", workload_hint: Optional[str] = None) -> None:
        pool = self.assignments.get(tenant.tenant_id)
        log_structured(
            "info",
            "tenant_compute_pool_scale",
            tenant_id=tenant.tenant_id,
            pool=pool,
            hint=workload_hint,
        )

    def session_settings(self, tenant_id: str) -> Mapping[str, str]:
        return {}


__all__ = [
    "IsolationFailure",
    "DataIsolationStrategy",
    "ComputeIsolationStrategy",
    "TenantIsolationStrategy",
    "TenantIsolationController",
    "RowLevelSecurityIsolation",
    "SchemaPerTenantIsolation",
    "WorkloadPoolIsolation",
]
