"""Helpers for emitting OpenLineage-compatible events from Airflow."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

try:  # pragma: no cover - optional dependency on requests
    import requests
except Exception:  # pragma: no cover - allow validation without requests installed
    requests = None  # type: ignore


__all__ = ["LineageDataset", "LineageEventPayload", "emit_lineage_event"]


@dataclass(frozen=True)
class LineageDataset:
    """Specification for a dataset emitted in a lineage event."""

    namespace: str
    name: str
    facets: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LineageEventPayload:
    """Representation of an OpenLineage event body."""

    event_time: str
    event_type: str
    job: Mapping[str, Any]
    run: Mapping[str, Any]
    inputs: Sequence[LineageDataset] = field(default_factory=tuple)
    outputs: Sequence[LineageDataset] = field(default_factory=tuple)
    producer: str = "https://github.com/aurum-data-platform"
    schema_url: str = "https://openlineage.io/spec/1-0-0/OpenLineage.json"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "eventTime": self.event_time,
            "eventType": self.event_type,
            "job": dict(self.job),
            "run": dict(self.run),
            "producer": self.producer,
            "schemaURL": self.schema_url,
            "inputs": [
                {
                    "namespace": ds.namespace,
                    "name": ds.name,
                    "facets": dict(ds.facets),
                }
                for ds in self.inputs
            ],
            "outputs": [
                {
                    "namespace": ds.namespace,
                    "name": ds.name,
                    "facets": dict(ds.facets),
                }
                for ds in self.outputs
            ],
        }


def _default_run_facets(run_id: str, *, nominal_time: Optional[str] = None) -> Dict[str, Any]:
    facets: Dict[str, Any] = {
        "runId": run_id,
        "facets": {
            "aurumMetadata": {
                "type": "CustomFacet",
                "codeVersion": os.getenv("AURUM_CODE_VERSION") or os.getenv("GIT_COMMIT") or "unknown",
            }
        },
    }
    if nominal_time:
        facets["facets"]["nominalTime"] = {
            "type": "NominalTimeRunFacet",
            "nominalTime": nominal_time,
        }
    return facets


def _http_post(url: str, payload: Mapping[str, Any], timeout: float) -> requests.Response:
    if requests is None:  # pragma: no cover - requests optional in some envs
        raise RuntimeError("requests dependency not available; cannot emit lineage")
    response = requests.post(url, json=payload, timeout=timeout)
    response.raise_for_status()
    return response


def emit_lineage_event(
    *,
    endpoint: Optional[str],
    namespace: str,
    job_name: str,
    run_id: str,
    event_type: str = "COMPLETE",
    inputs: Iterable[LineageDataset] = (),
    outputs: Iterable[LineageDataset] = (),
    extra_run_facets: Optional[Mapping[str, Any]] = None,
    extra_job_facets: Optional[Mapping[str, Any]] = None,
    timeout: float = 10.0,
) -> Optional[LineageEventPayload]:
    """Send a lineage event to the configured OpenLineage endpoint.

    Returns the structured payload for observability and unit tests. When no
    endpoint is provided the function is a no-op and returns ``None``.
    """

    if not endpoint:
        return None

    nominal_ds = os.getenv("EXECUTION_DATE")
    run_facets = _default_run_facets(run_id, nominal_time=nominal_ds)
    if extra_run_facets:
        run_facets["facets"].update(dict(extra_run_facets))

    job_payload: MutableMapping[str, Any] = {
        "namespace": namespace,
        "name": job_name,
    }
    if extra_job_facets:
        job_payload["facets"] = dict(extra_job_facets)

    payload = LineageEventPayload(
        event_time=datetime.now(timezone.utc).isoformat(),
        event_type=event_type,
        job=job_payload,
        run=run_facets,
        inputs=tuple(inputs),
        outputs=tuple(outputs),
    )

    data = payload.to_dict()

    try:
        _http_post(endpoint, data, timeout)
    except Exception as exc:
        raise RuntimeError(f"Failed to emit OpenLineage event: {exc}") from exc

    return payload

