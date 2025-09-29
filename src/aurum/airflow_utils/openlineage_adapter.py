"""Utilities to instrument Airflow DAGs with OpenLineage emissions."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional

try:  # pragma: no cover - optional dependency
    from openlineage.client import OpenLineageClient
    from openlineage.client.facet import BaseFacet
    from openlineage.client.run import Dataset, Job, Run, RunEvent
except Exception:  # pragma: no cover - avoid hard dependency during tests
    OpenLineageClient = None  # type: ignore
    BaseFacet = object  # type: ignore
    Dataset = object  # type: ignore
    Job = object  # type: ignore
    Run = object  # type: ignore
    RunEvent = object  # type: ignore

from .lineage import LineageDataset, LineageEventPayload

logger = logging.getLogger(__name__)


@dataclass
class OpenLineageConfig:
    """Configuration for Airflow → OpenLineage emission."""

    endpoint: Optional[str] = None
    namespace: str = "aurum"
    timeout_seconds: float = 10.0
    default_producer: str = "urn:aurum:openlineage:emitter"
    schema_url: str = "https://openlineage.io/spec/1-0-5/OpenLineage.json"
    job_facets: Dict[str, Any] = field(default_factory=dict)
    run_facets: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "OpenLineageConfig":
        config_path = os.getenv("AURUM_OPENLINEAGE_CONFIG")
        data: Dict[str, Any] = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        endpoint = os.getenv("OPENLINEAGE_URL", data.get("openlineage_url"))
        namespace = os.getenv("OPENLINEAGE_NAMESPACE", data.get("namespace", "aurum"))
        timeout = float(os.getenv("OPENLINEAGE_TIMEOUT", data.get("timeout_seconds", 10)))
        producer = os.getenv("OPENLINEAGE_PRODUCER", data.get("default_producer", "urn:aurum:openlineage:emitter"))
        schema_url = os.getenv("OPENLINEAGE_SCHEMA_URL", data.get("schema_url", cls.schema_url))
        job_facets = data.get("job_facets", {})
        run_facets = data.get("run_facets", {})
        return cls(
            endpoint=endpoint,
            namespace=namespace,
            timeout_seconds=timeout,
            default_producer=producer,
            schema_url=schema_url,
            job_facets=job_facets,
            run_facets=run_facets,
        )


class AirflowOpenLineageAdapter:
    """Adapter that emits OpenLineage events from Airflow callbacks."""

    def __init__(self, config: Optional[OpenLineageConfig] = None) -> None:
        self.config = config or OpenLineageConfig.from_env()
        self._client = self._build_client()

    def _build_client(self) -> Optional[OpenLineageClient]:
        if OpenLineageClient is None or not self.config.endpoint:
            return None
        try:
            return OpenLineageClient(url=self.config.endpoint)
        except Exception as exc:  # pragma: no cover - initialization failure reported once
            logger.warning("Failed to initialise OpenLineage client: %s", exc)
            return None

    @property
    def enabled(self) -> bool:
        return bool(self.config.endpoint)

    def emit_from_context(
        self,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        event_type: str,
        inputs: Iterable[LineageDataset] = (),
        outputs: Iterable[LineageDataset] = (),
        extra_run_facets: Optional[Mapping[str, Any]] = None,
        extra_job_facets: Optional[Mapping[str, Any]] = None,
    ) -> Optional[LineageEventPayload]:
        if not self.enabled:
            return None

        payload = LineageEventPayload(
            event_time=datetime.now(timezone.utc).isoformat(),
            event_type=event_type,
            job={
                "namespace": self.config.namespace,
                "name": f"{dag_id}.{task_id}",
                "facets": {**self.config.job_facets, **(extra_job_facets or {})},
            },
            run={
                "runId": run_id,
                "facets": {**self.config.run_facets, **(extra_run_facets or {})},
            },
            inputs=tuple(inputs),
            outputs=tuple(outputs),
            producer=self.config.default_producer,
            schema_url=self.config.schema_url,
        )

        if self._client is None:
            return self._emit_via_http(payload)

        try:
            self._emit_via_sdk(payload)
        except Exception as exc:
            logger.error("OpenLineage SDK emission failed; falling back to HTTP: %s", exc)
            return self._emit_via_http(payload)
        return payload

    def _emit_via_sdk(self, payload: LineageEventPayload) -> None:
        if self._client is None:  # pragma: no cover - defensive guard
            raise RuntimeError("OpenLineage client unavailable")
        event = self._convert_to_run_event(payload)
        self._client.emit(event)

    def _emit_via_http(self, payload: LineageEventPayload) -> Optional[LineageEventPayload]:
        from .lineage import emit_lineage_event

        return emit_lineage_event(
            endpoint=self.config.endpoint,
            namespace=self.config.namespace,
            job_name=payload.job["name"],
            run_id=payload.run["runId"],
            event_type=payload.event_type,
            inputs=payload.inputs,
            outputs=payload.outputs,
            extra_run_facets=payload.run.get("facets"),
            extra_job_facets=payload.job.get("facets"),
            timeout=self.config.timeout_seconds,
        )

    def _convert_to_run_event(self, payload: LineageEventPayload) -> RunEvent:
        if RunEvent is object:  # pragma: no cover - OpenLineage SDK not installed
            raise RuntimeError("openlineage-python not installed")

        run = Run(
            runId=payload.run.get("runId"),
            facets=self._convert_facets(payload.run.get("facets", {})),
        )
        job = Job(
            namespace=payload.job.get("namespace"),
            name=payload.job.get("name"),
            facets=self._convert_facets(payload.job.get("facets", {})),
        )
        inputs = [
            Dataset(
                namespace=i.namespace,
                name=i.name,
                facets=self._convert_facets(i.facets),
            )
            for i in payload.inputs
        ]
        outputs = [
            Dataset(
                namespace=o.namespace,
                name=o.name,
                facets=self._convert_facets(o.facets),
            )
            for o in payload.outputs
        ]
        return RunEvent(
            eventTime=payload.event_time,
            eventType=payload.event_type,
            run=run,
            job=job,
            producer=payload.producer,
            schemaURL=payload.schema_url,
            inputs=inputs,
            outputs=outputs,
        )

    def _convert_facets(self, facets: Mapping[str, Any]) -> Dict[str, BaseFacet]:
        if BaseFacet is object:  # pragma: no cover - OpenLineage SDK not installed
            return dict(facets)

        converted: Dict[str, BaseFacet] = {}
        for key, value in facets.items():
            if isinstance(value, BaseFacet):
                converted[key] = value
                continue
            if isinstance(value, Mapping) and value.get("_type"):
                # custom serialized facet
                converted[key] = BaseFacet.from_dict(value)  # type: ignore[attr-defined]
            else:
                converted[key] = BaseFacet.from_dict({"_type": "CustomFacet", **value})  # type: ignore[attr-defined]
        return converted


def load_default_adapter() -> AirflowOpenLineageAdapter:
    """Convenience factory for DAG files."""

    return AirflowOpenLineageAdapter()


def airflow_callback_factory(
    *,
    event_type: str,
    adapter: Optional[AirflowOpenLineageAdapter] = None,
):
    """Return an Airflow task callback that emits an OpenLineage event."""

    adapter = adapter or load_default_adapter()

    def _callback(context: Mapping[str, Any]) -> Optional[LineageEventPayload]:
        dag_id = context["dag"].dag_id  # type: ignore[assignment]
        task = context["task"].task_id  # type: ignore[assignment]
        run_id = context["run_id"]
        ti = context.get("ti")
        extra_run_facets: MutableMapping[str, Any] = {}
        extra_job_facets: MutableMapping[str, Any] = {}
        if ti is not None:
            try:
                extra_run_facets["airflow"] = {
                    "type": "CustomFacet",
                    "tryNumber": ti.try_number,
                    "state": ti.state,
                }
            except AttributeError:
                pass
        return adapter.emit_from_context(
            dag_id=dag_id,
            task_id=task,
            run_id=run_id,
            event_type=event_type,
            extra_run_facets=extra_run_facets,
            extra_job_facets=extra_job_facets,
        )

    return _callback


__all__ = [
    "AirflowOpenLineageAdapter",
    "OpenLineageConfig",
    "airflow_callback_factory",
    "load_default_adapter",
]
