"""Scenario storage abstraction supporting in-memory and Postgres backends."""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import json

from aurum.scenarios import DriverType, ScenarioAssumption


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ScenarioRecord:
    id: str
    tenant_id: str
    name: str
    description: Optional[str]
    assumptions: List[ScenarioAssumption]
    status: str
    created_at: datetime


@dataclass
class ScenarioRunRecord:
    run_id: str
    scenario_id: str
    state: str
    created_at: datetime
    code_version: Optional[str] = None
    seed: Optional[int] = None


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

    def create_run(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        code_version: Optional[str],
        seed: Optional[int],
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


class InMemoryScenarioStore(BaseScenarioStore):
    def __init__(self) -> None:
        self._scenarios: Dict[str, ScenarioRecord] = {}
        self._runs: Dict[str, ScenarioRunRecord] = {}

    def create_scenario(
        self,
        tenant_id: str,
        name: str,
        description: Optional[str],
        assumptions: List[ScenarioAssumption],
    ) -> ScenarioRecord:
        scenario_id = str(uuid.uuid4())
        record = ScenarioRecord(
            id=scenario_id,
            tenant_id=tenant_id,
            name=name,
            description=description,
            assumptions=assumptions,
            status="CREATED",
            created_at=_now(),
        )
        self._scenarios[scenario_id] = record
        return record

    def get_scenario(self, scenario_id: str, *, tenant_id: Optional[str] = None) -> Optional[ScenarioRecord]:
        return self._scenarios.get(scenario_id)

    def create_run(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        code_version: Optional[str],
        seed: Optional[int],
    ) -> ScenarioRunRecord:
        run_id = str(uuid.uuid4())
        record = ScenarioRunRecord(
            run_id=run_id,
            scenario_id=scenario_id,
            state="QUEUED",
            created_at=_now(),
            code_version=code_version,
            seed=seed,
        )
        self._runs[run_id] = record
        try:
            scenario = self.get_scenario(scenario_id, tenant_id=tenant_id)
            if scenario is not None:
                _maybe_emit_scenario_request(scenario, record)
        except Exception:
            pass
        return record

    def get_run_for_scenario(
        self,
        scenario_id: str,
        run_id: str,
        *,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        record = self._runs.get(run_id)
        if record and record.scenario_id == scenario_id:
            return record
        return None

    def reset(self) -> None:
        self._scenarios.clear()
        self._runs.clear()

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
        run.state = state
        self._runs[run_id] = run
        return run


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

    def create_scenario(
        self,
        tenant_id: str,
        name: str,
        description: Optional[str],
        assumptions: List[ScenarioAssumption],
    ) -> ScenarioRecord:
        scenario_id = str(uuid.uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    INSERT INTO scenario (id, tenant_id, name, description, created_by)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING created_at
                    """,
                    (scenario_id, tenant_id, name, description, "aurum-api"),
                )
                created_at_row = cur.fetchone()
                created_at = created_at_row["created_at"] if created_at_row else _now()

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
            status="CREATED",
            created_at=created_at,
        )

    def get_scenario(self, scenario_id: str, *, tenant_id: Optional[str] = None) -> Optional[ScenarioRecord]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """SELECT id, tenant_id, name, description, created_at FROM scenario WHERE id = %s""",
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

        return ScenarioRecord(
            id=row["id"],
            tenant_id=row["tenant_id"],
            name=row["name"],
            description=row["description"],
            assumptions=assumptions,
            status="CREATED",
            created_at=row["created_at"],
        )

    def create_run(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        code_version: Optional[str],
        seed: Optional[int],
    ) -> ScenarioRunRecord:
        run_id = str(uuid.uuid4())
        version_hash = str(uuid.uuid4())
        state = "QUEUED"
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                cur.execute(
                    """
                    INSERT INTO model_run (id, scenario_id, curve_def_id, code_version, seed, state, version_hash)
                    VALUES (%s, %s, NULL, %s, %s, %s, %s)
                    RETURNING submitted_at
                    """,
                    (run_id, scenario_id, code_version, seed, state, version_hash),
                )
                submitted_at = cur.fetchone()["submitted_at"]
            conn.commit()

        run = ScenarioRunRecord(
            run_id=run_id,
            scenario_id=scenario_id,
            state=state,
            created_at=submitted_at,
            code_version=code_version,
            seed=seed,
        )
        try:
            scenario = self.get_scenario(scenario_id, tenant_id=tenant_id)
            if scenario is not None:
                _maybe_emit_scenario_request(scenario, run)
        except Exception:
            pass
        return run

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
                    SELECT id, scenario_id, state, code_version, seed, submitted_at
                    FROM model_run
                    WHERE id = %s AND scenario_id = %s
                    """,
                    (run_id, scenario_id),
                )
                row = cur.fetchone()
                if row is None:
                    return None

        return ScenarioRunRecord(
            run_id=row["id"],
            scenario_id=row["scenario_id"],
            state=row["state"],
            created_at=row["submitted_at"],
            code_version=row.get("code_version"),
            seed=row.get("seed"),
        )

    def update_run_state(
        self,
        run_id: str,
        *,
        state: str,
        tenant_id: Optional[str] = None,
    ) -> Optional[ScenarioRunRecord]:
        now = _now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._set_tenant(cur, tenant_id)
                # fetch current run and scenario tenant
                cur.execute(
                    """
                    SELECT mr.id, mr.scenario_id, mr.code_version, mr.seed, mr.submitted_at, mr.state, s.tenant_id
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

                # update state with timestamps
                if state == "RUNNING":
                    cur.execute(
                        "UPDATE model_run SET state=%s, started_at=COALESCE(started_at, %s) WHERE id=%s",
                        (state, now, run_id),
                    )
                elif state in ("SUCCEEDED", "FAILED"):
                    cur.execute(
                        "UPDATE model_run SET state=%s, completed_at=COALESCE(completed_at, %s) WHERE id=%s",
                        (state, now, run_id),
                    )
                else:
                    cur.execute("UPDATE model_run SET state=%s WHERE id=%s", (state, run_id))

                conn.commit()

                updated = ScenarioRunRecord(
                    run_id=row["id"],
                    scenario_id=scenario_id,
                    state=state,
                    created_at=row["submitted_at"],
                    code_version=row.get("code_version"),
                    seed=row.get("seed"),
                )

        _maybe_emit_status_alert(
            tenant_id=str(row_tenant) if row_tenant else None,
            scenario_id=scenario_id,
            run_id=run_id,
            state=state,
        )
        return updated


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
        assumptions_payload = [
            {
                "assumption_id": a.version or str(uuid.uuid4()),
                "type": a.driver_type.value,
                "payload": json.dumps(a.payload or {}),
                "version": a.version,
            }
            for a in scenario.assumptions
        ]
        payload = {
            "scenario_id": scenario.id,
            "tenant_id": scenario.tenant_id,
            "requested_by": os.getenv("AURUM_REQUESTED_BY", "api"),
            "asof_date": None,
            "curve_def_ids": [],
            "assumptions": assumptions_payload,
            "submitted_ts": int(_now().timestamp() * 1_000_000),
        }
        headers = [("run_id", run.run_id.encode("utf-8"))]
        producer.produce(topic=topic, value=payload, headers=headers)
        producer.flush(2)
    except Exception:
        return
