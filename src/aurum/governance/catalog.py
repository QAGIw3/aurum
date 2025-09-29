"""Integration layer for interacting with OpenMetadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


@dataclass
class ColumnDefinition:
    name: str
    data_type: str
    description: Optional[str] = None
    tags: Sequence[str] = ()

    def to_openmetadata(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "name": self.name,
            "dataType": self.data_type,
        }
        if self.description:
            data["description"] = self.description
        if self.tags:
            data["tags"] = [
                {"tagFQN": tag, "source": "Tag"}
                for tag in self.tags
            ]
        return data


@dataclass
class DatasetMetadata:
    name: str
    service: str
    database: str
    schema: str
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: Sequence[str] = ()
    columns: Sequence[ColumnDefinition] = ()
    table_type: str = "Regular"
    properties: Dict[str, Any] = field(default_factory=dict)

    def fully_qualified_name(self) -> str:
        return f"{self.service}.{self.database}.{self.schema}.{self.name}"

    def to_openmetadata(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "tableType": self.table_type,
            "databaseSchema": {
                "name": f"{self.service}.{self.database}.{self.schema}",
            },
            "columns": [col.to_openmetadata() for col in self.columns],
        }
        if self.description:
            payload["description"] = self.description
        if self.owner:
            payload["owner"] = {"name": self.owner, "type": "user"}
        if self.tags:
            payload["tags"] = [{"tagFQN": tag, "source": "Tag"} for tag in self.tags]
        if self.properties:
            payload["customProperties"] = self.properties
        return payload


@dataclass
class ImpactedEntity:
    type: str
    fqn: str
    level: int
    attributes: Dict[str, Any] = field(default_factory=dict)


class CatalogService:
    """HTTP client for OpenMetadata catalog interactions."""

    def __init__(
        self,
        *,
        server_url: str,
        auth_token: Optional[str] = None,
        timeout: int = 10,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.auth_token = auth_token

    def _headers(self, extra: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        if extra:
            headers.update(extra)
        return headers

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        url = f"{self.server_url}{path}"
        headers = kwargs.pop("headers", {})
        kwargs["headers"] = self._headers(headers)
        kwargs.setdefault("timeout", self.timeout)
        response = self.session.request(method, url, **kwargs)
        try:
            response.raise_for_status()
        except Exception:
            logger.error("OpenMetadata request failed [%s %s]: %s", method, url, response.text)
            raise
        return response

    def get_dataset(self, fqn: str, *, fields: Sequence[str] = ()) -> Dict[str, Any]:
        params = {}
        if fields:
            params["fields"] = ",".join(fields)
        encoded = quote(fqn, safe="")
        response = self._request("GET", f"/v1/tables/name/{encoded}", params=params)
        return response.json()

    def upsert_dataset(self, metadata: DatasetMetadata) -> Dict[str, Any]:
        payload = metadata.to_openmetadata()
        payload["fullyQualifiedName"] = metadata.fully_qualified_name()

        try:
            response = self._request("POST", "/v1/tables", json=payload)
            return response.json()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 409:
                logger.info("Dataset exists; issuing PATCH for %s", metadata.fully_qualified_name())
                details = exc.response.json()
                table_id = details.get("id") or self.get_dataset(metadata.fully_qualified_name()).get("id")
                if not table_id:
                    raise RuntimeError("Existing dataset lacks id for patch") from exc
                patch_ops: List[Dict[str, Any]] = []
                if metadata.description:
                    patch_ops.append({"op": "replace", "path": "/description", "value": metadata.description})
                if metadata.tags:
                    patch_ops.append({"op": "replace", "path": "/tags", "value": payload.get("tags", [])})
                if metadata.columns:
                    patch_ops.append({"op": "replace", "path": "/columns", "value": payload.get("columns", [])})
                if metadata.properties:
                    patch_ops.append(
                        {"op": "replace", "path": "/customProperties", "value": metadata.properties}
                    )
                if patch_ops:
                    response = self._request(
                        "PATCH",
                        f"/v1/tables/{table_id}",
                        headers={"Content-Type": "application/json-patch+json"},
                        json=patch_ops,
                    )
                    return response.json()
                return details
            raise

    def apply_tags(self, fqn: str, tags: Iterable[str]) -> Dict[str, Any]:
        table = self.get_dataset(fqn, fields=("tags",))
        table_id = table.get("id")
        if not table_id:
            raise RuntimeError(f"Dataset {fqn} does not have an id")
        existing = {tag.get("tagFQN") for tag in table.get("tags", [])}
        patch_ops: List[Dict[str, Any]] = []
        for tag in tags:
            if tag in existing:
                continue
            patch_ops.append({"op": "add", "path": "/tags/-", "value": {"tagFQN": tag, "source": "Tag"}})
        if not patch_ops:
            return table
        response = self._request(
            "PATCH",
            f"/v1/tables/{table_id}",
            headers={"Content-Type": "application/json-patch+json"},
            json=patch_ops,
        )
        return response.json()

    def list_downstream(self, fqn: str, *, depth: int = 2) -> List[ImpactedEntity]:
        encoded = quote(fqn, safe="")
        params = {"upstreamDepth": 0, "downstreamDepth": depth}
        response = self._request("GET", f"/v1/lineage/table/name/{encoded}", params=params)
        data = response.json()
        return self._parse_lineage_entities(data.get("downstreamEdges", []))

    def impact_analysis(
        self,
        fqn: str,
        *,
        change_spec: Mapping[str, Any],
        depth: int = 2,
    ) -> Dict[str, Any]:
        impacted = self.list_downstream(fqn, depth=depth)
        severity = change_spec.get("severity", "medium")
        reason = change_spec.get("reason", "schema_change")
        return {
            "subject": fqn,
            "severity": severity,
            "reason": reason,
            "impacted": [entity.__dict__ for entity in impacted],
        }

    def record_schema_version(
        self,
        fqn: str,
        *,
        schema_hash: str,
        diff_summary: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        table = self.get_dataset(fqn, fields=("id", "version", "changeDescription"))
        table_id = table.get("id")
        if not table_id:
            raise RuntimeError(f"Dataset {fqn} missing id; cannot record schema version")
        patch_ops = [
            {"op": "add", "path": "/customProperties/schema_hash", "value": schema_hash},
        ]
        if diff_summary:
            patch_ops.append(
                {"op": "add", "path": "/customProperties/schema_diff", "value": diff_summary}
            )
        response = self._request(
            "PATCH",
            f"/v1/tables/{table_id}",
            headers={"Content-Type": "application/json-patch+json"},
            json=patch_ops,
        )
        return response.json()

    def _parse_lineage_entities(self, edges: Sequence[Mapping[str, Any]]) -> List[ImpactedEntity]:
        impacted: List[ImpactedEntity] = []
        for edge in edges:
            node = edge.get("toEntity") or edge.get("entity")
            if not node:
                continue
            fqn = node.get("fullyQualifiedName") or node.get("fqn")
            if not fqn:
                continue
            impacted.append(
                ImpactedEntity(
                    type=node.get("type", "Unknown"),
                    fqn=fqn,
                    level=edge.get("lineageDetails", {}).get("edge", {}).get("weight", 1),
                    attributes={"confidence": edge.get("lineageDetails", {}).get("confidence", 1.0)},
                )
            )
        return impacted


__all__ = [
    "CatalogService",
    "ColumnDefinition",
    "DatasetMetadata",
    "ImpactedEntity",
]
