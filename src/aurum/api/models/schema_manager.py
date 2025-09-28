"""Schema management utilities for the Aurum API."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum

from pydantic import BaseModel, Field

from .base import AurumBaseModel


class SchemaType(str, Enum):
    """Types of schemas managed by the system."""
    REQUEST = "request"
    RESPONSE = "response"
    EVENT = "event"
    CONFIG = "config"
    INTERNAL = "internal"


class SchemaFormat(str, Enum):
    """Schema formats supported."""
    JSON_SCHEMA = "json_schema"
    OPENAPI = "openapi"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class SchemaValidationLevel(str, Enum):
    """Validation strictness levels."""
    STRICT = "strict"          # All validation rules enforced
    LAX = "lax"               # Some validation rules relaxed
    DISABLED = "disabled"     # No validation performed


@dataclass
class SchemaMetadata:
    """Metadata about a schema."""
    name: str
    version: str
    schema_type: SchemaType
    format: SchemaFormat
    description: str
    created_at: datetime
    updated_at: datetime
    owner: str
    tags: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    validation_level: SchemaValidationLevel = SchemaValidationLevel.STRICT

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "version": self.version,
            "schema_type": self.schema_type.value,
            "format": self.format.value,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "owner": self.owner,
            "tags": self.tags,
            "dependencies": self.dependencies,
            "validation_level": self.validation_level.value,
        }


class SchemaValidationResult(AurumBaseModel):
    """Result of schema validation."""
    valid: bool = Field(description="Whether validation passed")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
    validated_at: datetime = Field(default_factory=datetime.utcnow, description="When validation occurred")
    schema_version: Optional[str] = Field(None, description="Version of schema used for validation")

    def add_error(self, error: str) -> None:
        """Add a validation error."""
        self.errors.append(error)
        self.valid = False

    def add_warning(self, warning: str) -> None:
        """Add a validation warning."""
        self.warnings.append(warning)


class SchemaManager:
    """Manager for API schemas and validation."""

    def __init__(self):
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.metadata: Dict[str, SchemaMetadata] = {}
        self._load_builtin_schemas()

    def _load_builtin_schemas(self) -> None:
        """Load built-in schemas from configuration files."""
        config_dir = Path(__file__).resolve().parents[3] / "config"

        # Load schema files
        schema_files = [
            "eia_ingest_datasets.schema.json",
            "cpi_ingest_datasets.schema.json",
            "fred_ingest_datasets.schema.json",
            "iso_ingest_datasets.schema.json",
        ]

        for schema_file in schema_files:
            schema_path = config_dir / schema_file
            if schema_path.exists():
                try:
                    with open(schema_path, 'r') as f:
                        schema_data = json.load(f)

                    schema_name = schema_file.replace(".schema.json", "").replace("_", "-")
                    schema_key = f"{schema_name}:v1"

                    self.schemas[schema_key] = schema_data
                    self.metadata[schema_key] = SchemaMetadata(
                        name=schema_name,
                        version="v1",
                        schema_type=SchemaType.CONFIG,
                        format=SchemaFormat.JSON_SCHEMA,
                        description=f"Schema for {schema_name} configuration",
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                        owner="system",
                        tags=["config", schema_name],
                    )
                except Exception:
                    pass  # Skip invalid schema files

    def register_schema(
        self,
        name: str,
        version: str,
        schema_data: Dict[str, Any],
        metadata: SchemaMetadata
    ) -> None:
        """Register a schema with the manager."""
        schema_key = f"{name}:{version}"
        self.schemas[schema_key] = schema_data
        self.metadata[schema_key] = metadata

    def get_schema(self, name: str, version: str = "latest") -> Optional[Dict[str, Any]]:
        """Get a schema by name and version."""
        if version == "latest":
            # Find latest version
            matching_schemas = [k for k in self.schemas.keys() if k.startswith(f"{name}:")]
            if not matching_schemas:
                return None
            latest_key = max(matching_schemas, key=lambda k: self.metadata.get(k, SchemaMetadata("", "", SchemaType.INTERNAL, SchemaFormat.JSON_SCHEMA, "", datetime.utcnow(), datetime.utcnow(), "")).version)
            return self.schemas.get(latest_key)

        schema_key = f"{name}:{version}"
        return self.schemas.get(schema_key)

    def get_schema_metadata(self, name: str, version: str = "latest") -> Optional[SchemaMetadata]:
        """Get schema metadata."""
        if version == "latest":
            # Find latest version
            matching_schemas = [k for k in self.metadata.keys() if k.startswith(f"{name}:")]
            if not matching_schemas:
                return None
            latest_key = max(matching_schemas, key=lambda k: self.metadata[k].version)
            return self.metadata.get(latest_key)

        schema_key = f"{name}:{version}"
        return self.metadata.get(schema_key)

    def validate_data(self, name: str, data: Any, version: str = "latest") -> SchemaValidationResult:
        """Validate data against a schema."""
        try:
            import jsonschema
        except ImportError:
            return SchemaValidationResult(
                valid=False,
                errors=["jsonschema package not available"]
            )

        schema = self.get_schema(name, version)
        if not schema:
            return SchemaValidationResult(
                valid=False,
                errors=[f"Schema {name}:{version} not found"]
            )

        result = SchemaValidationResult(
            schema_version=version if version != "latest" else "latest"
        )

        try:
            jsonschema.validate(instance=data, schema=schema)
            result.valid = True
        except jsonschema.ValidationError as e:
            result.add_error(f"Validation failed at {'.'.join(str(p) for p in e.absolute_path)}: {e.message}")
        except Exception as e:
            result.add_error(f"Validation error: {str(e)}")

        return result

    def list_schemas(self, schema_type: Optional[SchemaType] = None) -> List[SchemaMetadata]:
        """List all registered schemas, optionally filtered by type."""
        schemas = list(self.metadata.values())

        if schema_type:
            schemas = [s for s in schemas if s.schema_type == schema_type]

        return sorted(schemas, key=lambda s: (s.name, s.version))

    def get_schema_dependencies(self, name: str, version: str = "latest") -> List[str]:
        """Get dependencies for a schema."""
        metadata = self.get_schema_metadata(name, version)
        if metadata:
            return metadata.dependencies
        return []

    def check_compatibility(self, from_version: str, to_version: str, schema_name: str) -> Dict[str, Any]:
        """Check compatibility between two schema versions."""
        from_schema = self.get_schema(schema_name, from_version)
        to_schema = self.get_schema(schema_name, to_version)

        if not from_schema or not to_schema:
            return {"compatible": False, "reason": "Schema not found"}

        # Simple compatibility check - in a real implementation this would be more sophisticated
        compatibility = {
            "compatible": True,
            "breaking_changes": [],
            "added_fields": [],
            "removed_fields": [],
            "type_changes": [],
        }

        # This is a simplified check - real implementation would compare schema structures
        # For now, assume all changes are compatible unless explicitly marked as breaking

        return compatibility


# Global schema manager instance
_schema_manager = SchemaManager()


def get_schema_manager() -> SchemaManager:
    """Get the global schema manager instance."""
    return _schema_manager


def validate_api_schema(schema_name: str, data: Any, version: str = "latest") -> SchemaValidationResult:
    """Validate API data against schema."""
    return get_schema_manager().validate_data(schema_name, data, version)


def get_api_schema(schema_name: str, version: str = "latest") -> Optional[Dict[str, Any]]:
    """Get API schema."""
    return get_schema_manager().get_schema(schema_name, version)


__all__ = [
    "SchemaType",
    "SchemaFormat",
    "SchemaValidationLevel",
    "SchemaMetadata",
    "SchemaValidationResult",
    "SchemaManager",
    "get_schema_manager",
    "validate_api_schema",
    "get_api_schema",
]
