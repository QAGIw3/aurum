"""Scenario storage abstraction supporting in-memory and Postgres backends."""

from __future__ import annotations

import os
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import hashlib
import json
import re

try:
    from opentelemetry import propagate
except ImportError:  # pragma: no cover - optional dependency
    propagate = None  # type: ignore[assignment]

try:  # Runtime optional dependency for code-generated models
    from aurum.schema_registry.codegen import ContractModelError, get_model
except Exception:  # pragma: no cover - validation is optional in some runtimes
    ContractModelError = RuntimeError  # type: ignore[assignment]
    get_model = None  # type: ignore[assignment]

from aurum.scenarios import DriverType, ScenarioAssumption
from aurum.telemetry import get_tracer
from aurum.telemetry.context import get_request_id

from .scenario_models import (
    ScenarioRunPriority,
    ScenarioRunStatus,
    ScenarioStatus,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


TRACER = get_tracer("aurum.api.scenario")


if get_model:
    try:
        _SCENARIO_REQUEST_MODEL = get_model("aurum.scenario.request.v1-value")
    except Exception:  # pragma: no cover - fallback when contracts are unavailable
        _SCENARIO_REQUEST_MODEL = None
else:  # pragma: no cover - happens if pydantic/contracts are unavailable
    _SCENARIO_REQUEST_MODEL = None


@dataclass
class ScenarioRecord:
    id: str
    tenant_id: str
    name: str
    description: Optional[str]
    assumptions: List[ScenarioAssumption] = field(default_factory=list)
    status: ScenarioStatus = ScenarioStatus.CREATED
    created_at: datetime = field(default_factory=_now)
    updated_at: Optional[datetime] = None
    unique_key: str = ""
    version: int = 1
    tags: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        status_value = self.status.value if isinstance(self.status, ScenarioStatus) else str(self.status)
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "name": self.name,
            "description": self.description,
            "assumptions": [_assumption_to_payload(assumption) for assumption in self.assumptions],
            "status": status_value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "unique_key": self.unique_key,
            "version": self.version,
            "tags": list(self.tags),
            "parameters": dict(self.parameters),
            "metadata": dict(self.metadata),
        }


@dataclass
class ScenarioRunRecord:
    run_id: str
    scenario_id: str
    state: ScenarioRunStatus = ScenarioRunStatus.QUEUED
    created_at: datetime = field(default_factory=_now)
    code_version: Optional[str] = None
    seed: Optional[int] = None
    run_key: Optional[str] = None
    input_hash: Optional[str] = None
    priority: ScenarioRunPriority = ScenarioRunPriority.NORMAL
    retry_count: int = 0
    max_retries: int = 3
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    queued_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    environment: Dict[str, str] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.state in {
            ScenarioRunStatus.SUCCEEDED,
            ScenarioRunStatus.FAILED,
            ScenarioRunStatus.CANCELLED,
            ScenarioRunStatus.TIMEOUT,
        }

    def as_dict(self) -> Dict[str, Any]:
        state_value = self.state.value if isinstance(self.state, ScenarioRunStatus) else str(self.state)
        priority_value = self.priority.value if isinstance(self.priority, ScenarioRunPriority) else str(self.priority)
        return {
            "run_id": self.run_id,
            "scenario_id": self.scenario_id,
            "state": state_value,
            "created_at": self.created_at,
            "code_version": self.code_version,
            "seed": self.seed,
            "run_key": self.run_key,
            "input_hash": self.input_hash,
            "priority": priority_value,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "queued_at": self.queued_at,
            "cancelled_at": self.cancelled_at,
            "environment": dict(self.environment),
            "parameters": dict(self.parameters),
        }


_SCENARIO_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(value: str) -> str:
    slug = _SCENARIO_SLUG_RE.sub("-", value.lower()).strip("-")
    return slug or "scenario"


def _compute_scenario_unique_key(tenant_id: str, name: str) -> str:
    return f"{tenant_id}:{_slugify(name)}"


def _stable_dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _assumption_to_payload(assumption: ScenarioAssumption) -> Dict[str, Any]:
    try:
        return assumption.dict()
    except AttributeError:  # pragma: no cover - defensive path for non-pydantic
        return dict(assumption)  # type: ignore[arg-type]


def _compute_run_fingerprint(
    scenario: ScenarioRecord,
    *,
    parameters: Optional[Dict[str, Any]] = None,
    environment: Optional[Dict[str, str]] = None,
    code_version: Optional[str] = None,
    seed: Optional[int] = None,
) -> Tuple[str, str]:
    """Return (input_hash, run_key) for a scenario execution."""

    payload = {
        "tenant_id": scenario.tenant_id,
        "scenario_id": scenario.id,
        "unique_key": scenario.unique_key or _compute_scenario_unique_key(scenario.tenant_id, scenario.name),
        "assumptions": [_assumption_to_payload(assumption) for assumption in scenario.assumptions],
        "parameters": parameters or {},
        "environment": environment or {},
        "code_version": code_version,
        "seed": seed,
        "scenario_version": scenario.version,
    }
    canonical = _stable_dumps(payload)
    input_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    run_key = f"{scenario.id}:{input_hash}"
    return input_hash, run_key


def _ensure_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _ensure_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


@dataclass
class PpaContractRecord:
    id: str
    tenant_id: str
    instrument_id: Optional[str]
    terms: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class BaseScenarioStore:
    def create_scenario(
        self,
        tenant_id: str,
        name: str,
        description: Optional[str],
        assumptions: List[ScenarioAssumption],
    ) -> ScenarioRecord:
        raise NotImplementedError

    def get_scenario(self, scenario_id: str, *, tenant_id: Optional[str] = None) -> Optional[ScenarioRecord]:
        raise NotImplementedError

    def list_scenarios(
        self,
        tenant_id: Optional[str],
        *,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ScenarioRecord]:
        raise NotImplementedError

    def create_run(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        code_version: Optional[str],
        seed: Optional[int],
        parameters: Optional[Dict[str, Any]] = None,
        environment: Optional[Dict[str, str]] = None,
        priority: ScenarioRunPriority = ScenarioRunPriority.NORMAL,
        max_retries: int = 3,
        idempotency_key: Optional[str] = None,
    ) -> ScenarioRunRecord:
        raise NotImplementedError

    def get_run_for_scenario(
        self,
        scenario_id: str,
        run_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        raise NotImplementedError

    def list_runs(
        self,
        tenant_id: Optional[str],
        *,
        scenario_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ScenarioRunRecord]:
        raise NotImplementedError

    def reset(self) -> None:  # pragma: no cover - only meaningful for in-memory
        pass

    def update_run_state(
        self,
        run_id: str,
        *,
        state: str,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        raise NotImplementedError

    def delete_scenario(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> bool:
        raise NotImplementedError

    # PPA contract operations

    def create_ppa_contract(
        self,
        tenant_id: str,
        *,
        instrument_id: Optional[str] = None,
        terms: Optional[Dict[str, Any]] = None,
    ) -> PpaContractRecord:
        raise NotImplementedError

    def get_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> Optional[PpaContractRecord]:
        raise NotImplementedError

    def list_ppa_contracts(
        self,
        tenant_id: Optional[str],
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> List[PpaContractRecord]:
        raise NotImplementedError

    def update_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
        instrument_id: Optional[str] = None,
        terms: Optional[Dict[str, Any]] = None,
    ) -> Optional[PpaContractRecord]:
        raise NotImplementedError

    def delete_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> bool:
        raise NotImplementedError


class InMemoryScenarioStore(BaseScenarioStore):
    def __init__(self) -> None:
        self._scenarios: Dict[str, ScenarioRecord] = {}
        self._runs: Dict[str, ScenarioRunRecord] = {}
        self._runs_by_key: Dict[str, str] = {}
        self._runs_by_idempotency: Dict[str, str] = {}
        self._ppa_contracts: Dict[str, PpaContractRecord] = {}

    def create_scenario(
        self,
        tenant_id: str,
        name: str,
        description: Optional[str],
        assumptions: List[ScenarioAssumption],
    ) -> ScenarioRecord:
        scenario_id = str(uuid.uuid4())
        unique_key = _compute_scenario_unique_key(tenant_id, name)
        for record in self._scenarios.values():
            if record.tenant_id == tenant_id and record.unique_key == unique_key:
                raise ValueError("Scenario with the same name already exists for tenant")
        now = _now()
        record = ScenarioRecord(
            id=scenario_id,
            tenant_id=tenant_id,
            name=name,
            description=description,
            assumptions=list(assumptions),
            status=ScenarioStatus.CREATED,
            created_at=now,
            updated_at=now,
            unique_key=unique_key,
        )
        self._scenarios[scenario_id] = record
        return record

    def get_scenario(self, scenario_id: str, *, tenant_id: Optional[str] = None) -> Optional[ScenarioRecord]:
        record = self._scenarios.get(scenario_id)
        if record is None:
            return None
        if tenant_id and record.tenant_id != tenant_id:
            return None
        return record

    def list_scenarios(
        self,
        tenant_id: Optional[str],
        *,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ScenarioRecord]:
        matches = list(self._scenarios.values())
        if tenant_id:
            matches = [record for record in matches if record.tenant_id == tenant_id]
        if status:
            status_enum: Optional[ScenarioStatus]
            if isinstance(status, ScenarioStatus):
                status_enum = status
            else:
                try:
                    status_enum = ScenarioStatus(str(status).lower())
                except ValueError:
                    status_enum = None
            if status_enum is not None:
                matches = [record for record in matches if record.status == status_enum]
        matches.sort(key=lambda rec: rec.created_at, reverse=True)
        return matches[offset : offset + limit]

    def create_run(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        code_version: Optional[str],
        seed: Optional[int],
        parameters: Optional[Dict[str, Any]] = None,
        environment: Optional[Dict[str, str]] = None,
        priority: ScenarioRunPriority = ScenarioRunPriority.NORMAL,
        max_retries: int = 3,
        idempotency_key: Optional[str] = None,
    ) -> ScenarioRunRecord:
        scenario = self.get_scenario(scenario_id, tenant_id=tenant_id)
        if scenario is None:
            raise ValueError("Scenario not found")

        fingerprint, run_key = _compute_run_fingerprint(
            scenario,
            parameters=parameters,
            environment=environment,
            code_version=code_version,
            seed=seed,
        )

        if idempotency_key and idempotency_key in self._runs_by_idempotency:
            existing_id = self._runs_by_idempotency[idempotency_key]
            existing_run = self._runs.get(existing_id)
            if existing_run is not None:
                return existing_run

        if run_key in self._runs_by_key:
            existing_id = self._runs_by_key[run_key]
            existing_run = self._runs.get(existing_id)
            if existing_run is not None:
                return existing_run

        run_id = str(uuid.uuid4())
        now = _now()
        record = ScenarioRunRecord(
            run_id=run_id,
            scenario_id=scenario_id,
            state=ScenarioRunStatus.QUEUED,
            created_at=now,
            queued_at=now,
            code_version=code_version,
            seed=seed,
            input_hash=fingerprint,
            run_key=run_key,
            priority=priority,
            max_retries=max_retries,
            parameters=dict(parameters or {}),
            environment=dict(environment or {}),
        )
        self._runs[run_id] = record
        self._runs_by_key[run_key] = run_id
        if idempotency_key:
            self._runs_by_idempotency[idempotency_key] = run_id
        try:
            _maybe_emit_scenario_request(scenario, record)
        except Exception:
            pass
        return record

    def create_ppa_contract(
        self,
        tenant_id: str,
        *,
        instrument_id: Optional[str] = None,
        terms: Optional[Dict[str, Any]] = None,
    ) -> PpaContractRecord:
        contract_id = str(uuid.uuid4())
        record = PpaContractRecord(
            id=contract_id,
            tenant_id=tenant_id,
            instrument_id=instrument_id,
            terms=dict(terms or {}),
            created_at=_now(),
            updated_at=_now(),
        )
        self._ppa_contracts[contract_id] = record
        return record

    def get_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> Optional[PpaContractRecord]:
        record = self._ppa_contracts.get(contract_id)
        if record is None:
            return None
        if tenant_id and record.tenant_id != tenant_id:
            return None
        return record

    def list_ppa_contracts(
        self,
        tenant_id: Optional[str],
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> List[PpaContractRecord]:
        records = list(self._ppa_contracts.values())
        if tenant_id:
            records = [rec for rec in records if rec.tenant_id == tenant_id]
        records.sort(key=lambda rec: rec.created_at, reverse=True)
        return records[offset : offset + limit]

    def update_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
        instrument_id: Optional[str] = None,
        terms: Optional[Dict[str, Any]] = None,
    ) -> Optional[PpaContractRecord]:
        record = self.get_ppa_contract(contract_id, tenant_id=tenant_id)
        if record is None:
            return None
        if instrument_id is not None:
            record.instrument_id = instrument_id
        if terms is not None:
            record.terms = dict(terms)
        record.updated_at = _now()
        self._ppa_contracts[contract_id] = record
        return record

    def delete_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> bool:
        record = self.get_ppa_contract(contract_id, tenant_id=tenant_id)
        if record is None:
            return False
        self._ppa_contracts.pop(contract_id, None)
        return True

    def reset(self) -> None:  # pragma: no cover - test helper
        self._scenarios.clear()
        self._runs.clear()
        self._runs_by_key.clear()
        self._runs_by_idempotency.clear()
        self._ppa_contracts.clear()

    def get_run_for_scenario(
        self,
        scenario_id: str,
        run_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        record = self._runs.get(run_id)
        if record and record.scenario_id == scenario_id:
            scenario = self._scenarios.get(scenario_id)
            if tenant_id and scenario and scenario.tenant_id != tenant_id:
                return None
            if isinstance(record.state, str):
                try:
                    record.state = ScenarioRunStatus(record.state.lower())
                except ValueError:
                    record.state = ScenarioRunStatus.QUEUED
            return record
        return None

    def list_runs(
        self,
        tenant_id: Optional[str],
        *,
        scenario_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ScenarioRunRecord]:
        if isinstance(state, ScenarioRunStatus):
            state_enum: Optional[ScenarioRunStatus] = state
        elif state is None:
            state_enum = None
        else:
            try:
                state_enum = ScenarioRunStatus(str(state).lower())
            except ValueError:
                state_enum = None

        runs: List[ScenarioRunRecord] = []
        for run in self._runs.values():
            run_state = run.state
            if isinstance(run_state, str):
                try:
                    run_state = ScenarioRunStatus(run_state.lower())
                except ValueError:
                    run_state = ScenarioRunStatus.QUEUED
                    run.state = run_state
            scenario_match = scenario_id is None or run.scenario_id == scenario_id
            state_match = state_enum is None or run_state == state_enum
            scenario_exists = self._scenarios.get(run.scenario_id) is not None
            tenant_match = (
                not tenant_id
                or (
                    scenario_exists
                    and self._scenarios[run.scenario_id].tenant_id == tenant_id
                )
            )
            if scenario_match and state_match and scenario_exists and tenant_match:
                runs.append(run)
        runs.sort(key=lambda rec: rec.created_at, reverse=True)
        return runs[offset : offset + limit]

    def update_run_state(
        self,
        run_id: str,
        *,
        state: str,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        run = self._runs.get(run_id)
        if run is None:
            return None
        scenario = self._scenarios.get(run.scenario_id)
        if tenant_id and scenario and scenario.tenant_id != tenant_id:
            return None
        if isinstance(state, ScenarioRunStatus):
            new_state = state
        else:
            try:
                new_state = ScenarioRunStatus(state.lower())
            except ValueError:
                raise ValueError(f"Unsupported scenario run state: {state}")

        now = _now()
        if new_state == ScenarioRunStatus.RUNNING and run.started_at is None:
            run.started_at = now
        if new_state in {
            ScenarioRunStatus.SUCCEEDED,
            ScenarioRunStatus.FAILED,
            ScenarioRunStatus.TIMEOUT,
            ScenarioRunStatus.CANCELLED,
        }:
            if run.completed_at is None:
                run.completed_at = now
            if run.started_at and run.duration_seconds is None:
                run.duration_seconds = (run.completed_at - run.started_at).total_seconds()
            if new_state == ScenarioRunStatus.CANCELLED:
                run.cancelled_at = now
        run.state = new_state
        self._runs[run_id] = run
        return run

    def delete_scenario(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> bool:
        record = self._scenarios.get(scenario_id)
        if record is None:
            return False
        if tenant_id and record.tenant_id != tenant_id:
            return False
        self._scenarios.pop(scenario_id, None)
        self._runs = {
            run_id: run
            for run_id, run in self._runs.items()
            if run.scenario_id != scenario_id
        }
        valid_ids = set(self._runs.keys())
        self._runs_by_key = {
            key: run_id for key, run_id in self._runs_by_key.items() if run_id in valid_ids
        }
        self._runs_by_idempotency = {
            key: run_id
            for key, run_id in self._runs_by_idempotency.items()
            if run_id in valid_ids
        }
        return True


class PostgresScenarioStore(BaseScenarioStore):
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        # Lazy import psycopg to avoid hard dep in unit tests
        from importlib import import_module  # type: ignore

        self._psycopg = import_module("psycopg")
        rows = import_module("psycopg.rows")
        self._dict_row = getattr(rows, "dict_row")

    @contextmanager
    def _connect(self):
        conn = self._psycopg.connect(self._dsn, row_factory=self._dict_row)
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _set_tenant(cursor, tenant_id: Optional[str]) -> None:
        if not tenant_id:
            return
        cursor.execute("SET LOCAL app.current_tenant = %s", (tenant_id,))

    @staticmethod
    def _row_to_contract(row: Optional[Dict[str, Any]]) -> Optional[PpaContractRecord]:
        if row is None:
            return None
        terms = row.get("terms") or {}
        if isinstance(terms, str):
            try:
                terms = json.loads(terms)
            except json.JSONDecodeError:
                terms = {}
        return PpaContractRecord(
            id=row["id"],
            tenant_id=row["tenant_id"],
            instrument_id=row.get("instrument_id"),
            terms=terms,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def create_scenario(
        self,
        tenant_id: str,
        name: str,
        description: Optional[str],
        assumptions: List[ScenarioAssumption],
    ) -> ScenarioRecord:
        scenario_id = str(uuid.uuid4())
        unique_key = _compute_scenario_unique_key(tenant_id, name)
        status_value = ScenarioStatus.CREATED.value
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    "SELECT 1 FROM scenario WHERE tenant_id = %s AND lower(name) = lower(%s) LIMIT 1",
                    (tenant_id, name),
                )
                if cur.fetchone():
                    raise ValueError("Scenario with the same name already exists for tenant")
                cur.execute(
                    """
                    INSERT INTO scenario (id, tenant_id, name, description, status, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING created_at, updated_at, status
                    """,
                    (scenario_id, tenant_id, name, description, status_value, "aurum-api"),
                )
                created_row = cur.fetchone() or {}
                created_at = created_row.get("created_at", _now())
                updated_at = created_row.get("updated_at", created_at)

                for assumption in assumptions:
                    driver_name = assumption.driver_type.value
                    driver_description = None
                    payload = assumption.payload or {}
                    if assumption.driver_type == DriverType.POLICY:
                        driver_description = payload.get("policy_name")
                    cur.execute(
                        """
                        INSERT INTO scenario_driver (id, name, type, description)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (name, type) DO UPDATE SET description = COALESCE(EXCLUDED.description, scenario_driver.description)
                        RETURNING id
                        """,
                        (str(uuid.uuid4()), driver_name, assumption.driver_type.value, driver_description),
                    )
                    driver_id = cur.fetchone()["id"]

                    cur.execute(
                        """
                        INSERT INTO scenario_assumption_value (id, scenario_id, driver_id, payload, version)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (scenario_id, driver_id, version) DO UPDATE SET payload = EXCLUDED.payload
                        """,
                        (
                            str(uuid.uuid4()),
                            scenario_id,
                            driver_id,
                            json.dumps(payload),
                            assumption.version,
                        ),
                    )

            conn.commit()

        return ScenarioRecord(
            id=scenario_id,
            tenant_id=tenant_id,
            name=name,
            description=description,
            assumptions=assumptions,
            status=ScenarioStatus.CREATED,
            created_at=created_at,
            updated_at=updated_at,
            unique_key=unique_key,
        )

    def get_scenario(self, scenario_id: str, *, tenant_id: Optional[str] = None) -> Optional[ScenarioRecord]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    SELECT
                        id,
                        tenant_id,
                        name,
                        description,
                        status,
                        created_at,
                        updated_at,
                        COALESCE(parameters, '{}'::JSONB) AS parameters,
                        COALESCE(tags, '[]'::JSONB) AS tags,
                        COALESCE(version, 1) AS version
                    FROM scenario
                    WHERE id = %s
                    """,
                    (scenario_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None

                cur.execute(
                    """
                    SELECT d.type, sav.payload, sav.version
                    FROM scenario_assumption_value sav
                    JOIN scenario_driver d ON d.id = sav.driver_id
                    WHERE sav.scenario_id = %s
                    ORDER BY d.type
                    """,
                    (scenario_id,),
                )
                assumptions: List[ScenarioAssumption] = []
                for assumption_row in cur.fetchall():
                    driver_type = DriverType(assumption_row["type"])
                    payload = assumption_row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    assumptions.append(
                        ScenarioAssumption(
                            driver_type=driver_type,
                            payload=payload,
                            version=assumption_row.get("version"),
                        )
                    )

        status_value = str(row.get("status", "created")).lower()
        try:
            status_enum = ScenarioStatus(status_value)
        except ValueError:
            status_enum = ScenarioStatus.CREATED

        return ScenarioRecord(
            id=row["id"],
            tenant_id=row["tenant_id"],
            name=row["name"],
            description=row["description"],
            assumptions=assumptions,
            status=status_enum,
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
            unique_key=_compute_scenario_unique_key(row["tenant_id"], row["name"]),
            parameters=_ensure_dict(row.get("parameters")),
            tags=_ensure_list(row.get("tags")),
            version=row.get("version", 1),
        )

    def list_scenarios(
        self,
        tenant_id: Optional[str],
        *,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ScenarioRecord]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                params: list[Any] = []
                clauses: list[str] = []
                if tenant_id:
                    clauses.append("tenant_id = %s")
                    params.append(tenant_id)
                status_value: Optional[str] = None
                if status:
                    if isinstance(status, ScenarioStatus):
                        status_value = status.value
                    else:
                        try:
                            status_value = ScenarioStatus(str(status).lower()).value
                        except ValueError:
                            status_value = None
                    if status_value:
                        clauses.append("status = %s")
                        params.append(status_value)
                where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                params.extend([limit, offset])
                base_query = """
                    SELECT id, tenant_id, name, description, status, created_at, updated_at,
                           COALESCE(parameters, '{}'::JSONB) AS parameters,
                           COALESCE(tags, '[]'::JSONB) AS tags,
                           COALESCE(version, 1) AS version
                    FROM scenario
                """
                if where_clause:
                    base_query += f"\n{where_clause}"
                base_query += """
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """
                cur.execute(base_query, tuple(params))
                scenario_rows = cur.fetchall()
                if not scenario_rows:
                    return []

                scenario_ids = [row["id"] for row in scenario_rows]
                assumptions_map: Dict[str, List[ScenarioAssumption]] = defaultdict(list)
                cur.execute(
                    """
                    SELECT sav.scenario_id, d.type, sav.payload, sav.version
                    FROM scenario_assumption_value sav
                    JOIN scenario_driver d ON d.id = sav.driver_id
                    WHERE sav.scenario_id = ANY(%s)
                    ORDER BY sav.scenario_id, d.type
                    """,
                    (scenario_ids,),
                )
                for assumption_row in cur.fetchall():
                    driver_type = DriverType(assumption_row["type"])
                    payload = assumption_row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    assumptions_map[assumption_row["scenario_id"]].append(
                        ScenarioAssumption(
                            driver_type=driver_type,
                            payload=payload,
                            version=assumption_row.get("version"),
                        )
                    )

        records: List[ScenarioRecord] = []
        for row in scenario_rows:
            status_value = str(row.get("status", "created")).lower()
            try:
                status_enum = ScenarioStatus(status_value)
            except ValueError:
                status_enum = ScenarioStatus.CREATED
            records.append(
                ScenarioRecord(
                    id=row["id"],
                    tenant_id=row["tenant_id"],
                    name=row["name"],
                    description=row["description"],
                    assumptions=assumptions_map.get(row["id"], []),
                    status=status_enum,
                    created_at=row["created_at"],
                    updated_at=row.get("updated_at"),
                    unique_key=_compute_scenario_unique_key(row["tenant_id"], row["name"]),
                    parameters=_ensure_dict(row.get("parameters")),
                    tags=_ensure_list(row.get("tags")),
                    version=row.get("version", 1),
                )
            )
        return records

    def create_run(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        code_version: Optional[str],
        seed: Optional[int],
        parameters: Optional[Dict[str, Any]] = None,
        environment: Optional[Dict[str, str]] = None,
        priority: ScenarioRunPriority = ScenarioRunPriority.NORMAL,
        max_retries: int = 3,
        idempotency_key: Optional[str] = None,
    ) -> ScenarioRunRecord:
        scenario = self.get_scenario(scenario_id, tenant_id=tenant_id)
        if scenario is None:
            raise ValueError("Scenario not found")

        version_hash, run_key = _compute_run_fingerprint(
            scenario,
            parameters=parameters,
            environment=environment,
            code_version=code_version,
            seed=seed,
        )

        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    SELECT id, scenario_id, state, code_version, seed, submitted_at, started_at, completed_at, error
                    FROM model_run
                    WHERE scenario_id = %s AND version_hash = %s
                    ORDER BY submitted_at DESC
                    LIMIT 1
                    """,
                    (scenario_id, version_hash),
                )
                existing = cur.fetchone()
                if existing:
                    existing_state = str(existing.get("state", "queued")).lower()
                    try:
                        state_enum = ScenarioRunStatus(existing_state)
                    except ValueError:
                        state_enum = ScenarioRunStatus.QUEUED
                    record = ScenarioRunRecord(
                        run_id=existing["id"],
                        scenario_id=existing.get("scenario_id", scenario_id),
                        state=state_enum,
                        created_at=existing.get("submitted_at", _now()),
                        code_version=existing.get("code_version"),
                        seed=existing.get("seed"),
                        run_key=run_key,
                        input_hash=version_hash,
                        priority=priority,
                        max_retries=max_retries,
                        started_at=existing.get("started_at"),
                        completed_at=existing.get("completed_at"),
                        error_message=existing.get("error"),
                        parameters=dict(parameters or {}),
                        environment=dict(environment or {}),
                    )
                    if (
                        record.started_at
                        and record.completed_at
                        and record.duration_seconds is None
                    ):
                        record.duration_seconds = (
                            record.completed_at - record.started_at
                        ).total_seconds()
                    record.queued_at = record.created_at
                    return record

                run_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO model_run (id, scenario_id, curve_def_id, code_version, seed, state, version_hash)
                    VALUES (%s, %s, NULL, %s, %s, %s, %s)
                    RETURNING submitted_at
                    """,
                    (
                        run_id,
                        scenario_id,
                        code_version,
                        seed,
                        ScenarioRunStatus.QUEUED.value,
                        version_hash,
                    ),
                )
                submitted_row = cur.fetchone() or {}
                submitted_at = submitted_row.get("submitted_at", _now())
            conn.commit()

        run = ScenarioRunRecord(
            run_id=run_id,
            scenario_id=scenario_id,
            state=ScenarioRunStatus.QUEUED,
            created_at=submitted_at,
            code_version=code_version,
            seed=seed,
            run_key=run_key,
            input_hash=version_hash,
            priority=priority,
            max_retries=max_retries,
            queued_at=submitted_at,
            parameters=dict(parameters or {}),
            environment=dict(environment or {}),
        )
        try:
            if scenario is not None:
                _maybe_emit_scenario_request(scenario, run)
        except Exception:
            pass
        return run

    def delete_scenario(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute("DELETE FROM model_run WHERE scenario_id = %s", (scenario_id,))
                cur.execute("DELETE FROM scenario WHERE id = %s", (scenario_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted > 0

    def get_run_for_scenario(
        self,
        scenario_id: str,
        run_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    SELECT id, scenario_id, state, code_version, seed, version_hash,
                           submitted_at, started_at, completed_at, error
                    FROM model_run
                    WHERE id = %s AND scenario_id = %s
                    """,
                    (run_id, scenario_id),
                )
                row = cur.fetchone()
                if row is None:
                    return None

        state_value = str(row.get("state", "queued")).lower()
        try:
            state_enum = ScenarioRunStatus(state_value)
        except ValueError:
            state_enum = ScenarioRunStatus.QUEUED

        record = ScenarioRunRecord(
            run_id=row["id"],
            scenario_id=row["scenario_id"],
            state=state_enum,
            created_at=row.get("submitted_at", _now()),
            code_version=row.get("code_version"),
            seed=row.get("seed"),
            input_hash=row.get("version_hash"),
            run_key=f"{row['scenario_id']}:{row.get('version_hash')}" if row.get("version_hash") else None,
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            error_message=row.get("error"),
        )
        record.queued_at = record.created_at
        if (
            record.started_at
            and record.completed_at
            and record.duration_seconds is None
        ):
            record.duration_seconds = (
                record.completed_at - record.started_at
            ).total_seconds()
        return record

    def list_runs(
        self,
        tenant_id: Optional[str],
        *,
        scenario_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ScenarioRunRecord]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                params: List[object] = []
                query = [
                    "SELECT mr.id, mr.scenario_id, mr.state, mr.code_version, mr.seed,",
                    "       mr.version_hash, mr.submitted_at, mr.started_at, mr.completed_at, mr.error",
                    "FROM model_run mr",
                    "JOIN scenario s ON s.id = mr.scenario_id",
                    "WHERE 1 = 1",
                ]
                if tenant_id:
                    query.append("AND s.tenant_id = current_setting('app.current_tenant')::UUID")
                if scenario_id:
                    query.append("AND mr.scenario_id = %s")
                    params.append(scenario_id)
                state_value: Optional[str] = None
                if state:
                    if isinstance(state, ScenarioRunStatus):
                        state_value = state.value
                    else:
                        try:
                            state_value = ScenarioRunStatus(str(state).lower()).value
                        except ValueError:
                            state_value = None
                    if state_value:
                        query.append("AND mr.state = %s")
                        params.append(state_value)
                query.append("ORDER BY mr.submitted_at DESC")
                query.append("LIMIT %s OFFSET %s")
                params.extend([limit, offset])
                cur.execute("\n".join(query), tuple(params))
                rows = cur.fetchall()

        records: List[ScenarioRunRecord] = []
        for row in rows:
            state_value = str(row.get("state", "queued")).lower()
            try:
                state_enum = ScenarioRunStatus(state_value)
            except ValueError:
                state_enum = ScenarioRunStatus.QUEUED
            record = ScenarioRunRecord(
                run_id=row["id"],
                scenario_id=row["scenario_id"],
                state=state_enum,
                created_at=row.get("submitted_at", _now()),
                code_version=row.get("code_version"),
                seed=row.get("seed"),
                input_hash=row.get("version_hash"),
                run_key=f"{row['scenario_id']}:{row.get('version_hash')}" if row.get("version_hash") else None,
                started_at=row.get("started_at"),
                completed_at=row.get("completed_at"),
                error_message=row.get("error"),
            )
            record.queued_at = record.created_at
            if (
                record.started_at
                and record.completed_at
                and record.duration_seconds is None
            ):
                record.duration_seconds = (
                    record.completed_at - record.started_at
                ).total_seconds()
            records.append(record)

        return records

    def update_run_state(
        self,
        run_id: str,
        *,
        state: str,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        if isinstance(state, ScenarioRunStatus):
            new_state = state
        else:
            try:
                new_state = ScenarioRunStatus(str(state).lower())
            except ValueError:
                raise ValueError(f"Unsupported scenario run state: {state}")

        now = _now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    SELECT mr.id, mr.scenario_id, mr.code_version, mr.seed, mr.submitted_at, mr.state,
                           mr.version_hash, mr.started_at, mr.completed_at, mr.error, s.tenant_id
                    FROM model_run mr
                    JOIN scenario s ON s.id = mr.scenario_id
                    WHERE mr.id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None

                scenario_id = row["scenario_id"]
                row_tenant = row["tenant_id"]
                started_at_value = row.get("started_at")
                completed_at_value = row.get("completed_at")

                if new_state == ScenarioRunStatus.RUNNING and started_at_value is None:
                    started_at_value = now
                if new_state in {
                    ScenarioRunStatus.SUCCEEDED,
                    ScenarioRunStatus.FAILED,
                    ScenarioRunStatus.CANCELLED,
                    ScenarioRunStatus.TIMEOUT,
                } and completed_at_value is None:
                    completed_at_value = now

                cur.execute(
                    """
                    UPDATE model_run
                    SET state = %s,
                        started_at = %s,
                        completed_at = %s
                    WHERE id = %s
                    RETURNING scenario_id, state, code_version, seed, version_hash,
                              submitted_at, started_at, completed_at, error
                    """,
                    (
                        new_state.value,
                        started_at_value,
                        completed_at_value,
                        run_id,
                    ),
                )
                updated_row = cur.fetchone()
            conn.commit()

        if updated_row is None:
            return None

        updated_state_value = str(updated_row.get("state", "queued")).lower()
        try:
            updated_state = ScenarioRunStatus(updated_state_value)
        except ValueError:
            updated_state = ScenarioRunStatus.QUEUED

        record = ScenarioRunRecord(
            run_id=run_id,
            scenario_id=scenario_id,
            state=updated_state,
            created_at=updated_row.get("submitted_at", _now()),
            code_version=updated_row.get("code_version"),
            seed=updated_row.get("seed"),
            input_hash=updated_row.get("version_hash"),
            run_key=f"{scenario_id}:{updated_row.get('version_hash')}" if updated_row.get("version_hash") else None,
            started_at=updated_row.get("started_at"),
            completed_at=updated_row.get("completed_at"),
            error_message=updated_row.get("error"),
        )
        record.queued_at = record.created_at
        if (
            record.started_at
            and record.completed_at
            and record.duration_seconds is None
        ):
            record.duration_seconds = (
                record.completed_at - record.started_at
            ).total_seconds()

        _maybe_emit_status_alert(
            tenant_id=str(row_tenant) if row_tenant else None,
            scenario_id=scenario_id,
            run_id=run_id,
            state=updated_state.value,
        )
        return record

    def create_ppa_contract(
        self,
        tenant_id: str,
        *,
        instrument_id: Optional[str] = None,
        terms: Optional[Dict[str, Any]] = None,
    ) -> PpaContractRecord:
        contract_id = str(uuid.uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    INSERT INTO ppa_contract (id, tenant_id, instrument_id, terms)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, tenant_id, instrument_id, terms, created_at, updated_at
                    """,
                    (
                        contract_id,
                        tenant_id,
                        instrument_id,
                        json.dumps(terms or {}, default=str),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        record = self._row_to_contract(row)
        if record is None:
            raise RuntimeError("Failed to create PPA contract")
        return record

    def get_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> Optional[PpaContractRecord]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    SELECT id, tenant_id, instrument_id, terms, created_at, updated_at
                    FROM ppa_contract
                    WHERE id = %s
                    """,
                    (contract_id,),
                )
                row = cur.fetchone()
        return self._row_to_contract(row)

    def list_ppa_contracts(
        self,
        tenant_id: Optional[str],
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> List[PpaContractRecord]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                params: List[Any] = []
                where = ""
                if tenant_id:
                    where = "WHERE tenant_id = current_setting('app.current_tenant')::UUID"
                params.extend([limit, offset])
                cur.execute(
                    f"""
                    SELECT id, tenant_id, instrument_id, terms, created_at, updated_at
                    FROM ppa_contract
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [
            record
            for record in (self._row_to_contract(row) for row in rows)
            if record is not None
        ]

    def update_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
        instrument_id: Optional[str] = None,
        terms: Optional[Dict[str, Any]] = None,
    ) -> Optional[PpaContractRecord]:
        updates: List[str] = []
        params: List[Any] = []
        if instrument_id is not None:
            updates.append("instrument_id = %s")
            params.append(instrument_id)
        if terms is not None:
            updates.append("terms = %s")
            params.append(json.dumps(terms, default=str))
        if not updates:
            return self.get_ppa_contract(contract_id, tenant_id=tenant_id)
        updates.append("updated_at = NOW()")
        params.append(contract_id)
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    f"""
                    UPDATE ppa_contract
                    SET {', '.join(updates)}
                    WHERE id = %s
                    RETURNING id, tenant_id, instrument_id, terms, created_at, updated_at
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
            conn.commit()
        return self._row_to_contract(row)

    def delete_ppa_contract(
        self,
        contract_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute("DELETE FROM ppa_contract WHERE id = %s", (contract_id,))
                deleted = cur.rowcount
            conn.commit()
        return bool(deleted)


def _maybe_emit_status_alert(*, tenant_id: Optional[str], scenario_id: str, run_id: str, state: str) -> None:
    enabled = os.getenv("AURUM_SCENARIO_STATUS_ENABLED", "0").lower() in {"1", "true", "yes"}
    if not enabled:
        return
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        return
    topic = os.getenv("AURUM_ALERT_TOPIC", "aurum.alert.v1")
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "kafka", "schemas", "alert.v1.avsc"
    )
    severity = "INFO" if state in {"QUEUED", "RUNNING", "SUCCEEDED"} else "CRITICAL"
    try:  # pragma: no cover - integration only
        from confluent_kafka import avro  # type: ignore
        from confluent_kafka.avro import AvroProducer  # type: ignore

        schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        value_schema = avro.load(schema_path)
        producer = AvroProducer(
            {"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry_url},
            default_value_schema=value_schema,
        )
        payload = {
            "alert_id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "category": "PIPELINE",
            "severity": severity,
            "source": "aurum.api.scenario",
            "message": f"Scenario run state: scenario={scenario_id} run={run_id} -> {state}",
            "payload": json.dumps({"scenario_id": scenario_id, "run_id": run_id, "state": state}),
            "created_ts": int(_now().timestamp() * 1_000_000),
        }
        producer.produce(topic=topic, value=payload)
        producer.flush(2)
    except Exception:
        return


def _create_store() -> BaseScenarioStore:
    dsn = os.getenv("AURUM_APP_DB_DSN")
    if dsn:
        try:
            return PostgresScenarioStore(dsn)
        except Exception:
            # fallback to in-memory if driver missing/misconfigured
            return InMemoryScenarioStore()
    return InMemoryScenarioStore()


STORE: BaseScenarioStore = _create_store()


# ---- Kafka emission helpers (optional; integration paths) ----

def _schema_path(filename: str) -> str:
    base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    return os.path.join(base, "kafka", "schemas", filename)


def _scenario_request_payload(scenario: ScenarioRecord, run: ScenarioRunRecord) -> Dict[str, Any]:
    assumptions_payload = [
        {
            "assumption_id": assumption.version or str(uuid.uuid4()),
            "type": assumption.driver_type.value,
            "payload": json.dumps(assumption.payload or {}),
            "version": assumption.version,
        }
        for assumption in scenario.assumptions
    ]

    payload_data: Dict[str, Any] = {
        "scenario_id": scenario.id,
        "tenant_id": scenario.tenant_id,
        "requested_by": os.getenv("AURUM_REQUESTED_BY", "api"),
        "asof_date": None,
        "curve_def_ids": [],
        "assumptions": assumptions_payload,
        "submitted_ts": _now(),
    }

    model = _SCENARIO_REQUEST_MODEL
    if model is not None:
        try:
            payload_obj = model(**payload_data)
            payload_data = payload_obj.model_dump(mode="python")
        except Exception as exc:  # pragma: no cover - fall back to raw payload
            if isinstance(exc, ContractModelError):
                pass

    submitted_ts = payload_data.get("submitted_ts")
    if isinstance(submitted_ts, datetime):
        payload_data["submitted_ts"] = int(submitted_ts.timestamp() * 1_000_000)

    return payload_data


def _maybe_emit_scenario_request(scenario: ScenarioRecord, run: ScenarioRunRecord) -> None:
    # Default to enabled unless explicitly disabled
    if os.getenv("AURUM_SCENARIO_REQUESTS_ENABLED", "1").lower() in {"0", "false", "no"}:
        return
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        return
    topic = os.getenv("AURUM_SCENARIO_REQUEST_TOPIC", "aurum.scenario.request.v1")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    try:  # pragma: no cover
        from confluent_kafka import avro  # type: ignore
        from confluent_kafka.avro import AvroProducer  # type: ignore

        value_schema = avro.load(_schema_path("scenario.request.v1.avsc"))
        producer = AvroProducer(
            {"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry_url},
            default_value_schema=value_schema,
        )
        payload = _scenario_request_payload(scenario, run)
        with TRACER.start_as_current_span("scenario.request.produce") as span:
            if span.is_recording():  # pragma: no branch
                span.set_attribute("messaging.system", "kafka")
                span.set_attribute("messaging.destination", topic)
                span.set_attribute("messaging.destination_kind", "topic")
                span.set_attribute("aurum.scenario_id", scenario.id)
                span.set_attribute("aurum.run_id", run.run_id)
                span.set_attribute("tenant.id", scenario.tenant_id)
            carrier: Dict[str, str] = {}
            if propagate is not None:
                propagate.inject(carrier)
            headers = [("run_id", run.run_id.encode("utf-8"))]
            request_id = get_request_id()
            if request_id:
                headers.append(("x-request-id", request_id.encode("utf-8")))
            for key, value in carrier.items():
                headers.append((key, value.encode("utf-8")))
            producer.produce(topic=topic, value=payload, headers=headers)
            producer.flush(2)
    except Exception:
        return
