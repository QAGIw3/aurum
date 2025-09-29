"""Schema evolution tracking utilities."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .catalog import CatalogService, ColumnDefinition, DatasetMetadata


@dataclass
class ColumnSchema:
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None

    @classmethod
    def from_openmetadata(cls, column: Mapping[str, Any]) -> "ColumnSchema":
        return cls(
            name=column.get("name"),
            data_type=column.get("dataType", "UNKNOWN"),
            nullable=bool(column.get("nullable", True)),
            description=column.get("description"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dataType": self.data_type,
            "nullable": self.nullable,
            "description": self.description,
        }


@dataclass
class SchemaSnapshot:
    columns: List[ColumnSchema] = field(default_factory=list)

    def to_payload(self) -> List[Dict[str, Any]]:
        return [column.to_dict() for column in self.columns]

    def hash(self) -> str:
        encoded = json.dumps(self.to_payload(), sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


@dataclass
class SchemaDiff:
    added: List[ColumnSchema] = field(default_factory=list)
    removed: List[ColumnSchema] = field(default_factory=list)
    changed: List[Dict[str, Any]] = field(default_factory=list)

    def summary(self) -> Dict[str, Any]:
        return {
            "added": [col.to_dict() for col in self.added],
            "removed": [col.to_dict() for col in self.removed],
            "changed": self.changed,
        }

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.changed)


class SchemaEvolutionTracker:
    """Derives schema snapshots and diffs, then records them in the catalog."""

    def __init__(self, catalog: CatalogService) -> None:
        self.catalog = catalog

    def snapshot_from_metadata(self, dataset: Mapping[str, Any]) -> SchemaSnapshot:
        columns = dataset.get("columns", [])
        return SchemaSnapshot([ColumnSchema.from_openmetadata(col) for col in columns])

    def snapshot_from_definition(self, definition: DatasetMetadata) -> SchemaSnapshot:
        cols = [
            ColumnSchema(name=col.name, data_type=col.data_type, description=col.description)
            for col in definition.columns
        ]
        return SchemaSnapshot(cols)

    def diff(self, old: SchemaSnapshot, new: SchemaSnapshot) -> SchemaDiff:
        old_map = {col.name: col for col in old.columns}
        new_map = {col.name: col for col in new.columns}

        added = [new_map[name] for name in new_map.keys() - old_map.keys()]
        removed = [old_map[name] for name in old_map.keys() - new_map.keys()]

        changed: List[Dict[str, Any]] = []
        for name in new_map.keys() & old_map.keys():
            old_col = old_map[name]
            new_col = new_map[name]
            delta: Dict[str, Any] = {}
            if old_col.data_type != new_col.data_type:
                delta["dataType"] = {"from": old_col.data_type, "to": new_col.data_type}
            if old_col.nullable != new_col.nullable:
                delta["nullable"] = {"from": old_col.nullable, "to": new_col.nullable}
            if old_col.description != new_col.description:
                delta["description"] = {"from": old_col.description, "to": new_col.description}
            if delta:
                changed.append({"column": name, "changes": delta})

        return SchemaDiff(added=added, removed=removed, changed=changed)

    def record(
        self,
        fqn: str,
        *,
        new_snapshot: SchemaSnapshot,
        previous_snapshot: Optional[SchemaSnapshot] = None,
    ) -> Dict[str, Any]:
        old_snapshot = previous_snapshot
        if old_snapshot is None:
            dataset = self.catalog.get_dataset(fqn, fields=("columns",))
            if dataset:
                old_snapshot = self.snapshot_from_metadata(dataset)

        if old_snapshot is None:
            diff = SchemaDiff()
        else:
            diff = self.diff(old_snapshot, new_snapshot)

        schema_hash = new_snapshot.hash()
        diff_summary = None if diff.is_empty else diff.summary()
        return self.catalog.record_schema_version(
            fqn,
            schema_hash=schema_hash,
            diff_summary=diff_summary,
        )


__all__ = [
    "ColumnSchema",
    "SchemaDiff",
    "SchemaEvolutionTracker",
    "SchemaSnapshot",
]
