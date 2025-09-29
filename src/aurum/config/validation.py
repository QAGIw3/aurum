"""
Configuration validation and schema enforcement for the Advanced Configuration Management System.

This module provides:
- Schema registry for different configuration namespaces
- Validation of configuration against schemas
- Type coercion and conversion
- JSON Schema export for documentation
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, Union
from pathlib import Path

from pydantic import BaseModel, Field, ValidationError, validator
from pydantic.json_schema import GenerateJsonSchema

logger = logging.getLogger(__name__)


@dataclass
class SchemaDefinition:
    """Definition of a configuration schema."""
    name: str
    schema: Dict[str, Any]
    version: str = "1.0"
    description: str = ""
    required: List[str] = field(default_factory=list)
    examples: List[Dict[str, Any]] = field(default_factory=list)


class SchemaRegistry:
    """Registry for configuration schemas."""

    def __init__(self):
        self._schemas: Dict[str, SchemaDefinition] = {}
        self._register_builtin_schemas()

    def register_schema(self, schema_def: SchemaDefinition) -> None:
        """Register a schema definition."""
        self._schemas[schema_def.name] = schema_def
        logger.info(f"Registered schema: {schema_def.name} v{schema_def.version}")

    def get_schema(self, name: str) -> Optional[SchemaDefinition]:
        """Get a schema definition by name."""
        return self._schemas.get(name)

    def list_schemas(self) -> List[str]:
        """List all registered schema names."""
        return list(self._schemas.keys())

    def export_json_schema(self, schema_name: str, path: str) -> None:
        """Export a schema as JSON Schema to file."""
        schema_def = self.get_schema(schema_name)
        if not schema_def:
            raise ValueError(f"Schema '{schema_name}' not found")

        # Convert our internal schema format to JSON Schema
        json_schema = self._convert_to_json_schema(schema_def)

        with open(path, 'w') as f:
            json.dump(json_schema, f, indent=2)

    def _convert_to_json_schema(self, schema_def: SchemaDefinition) -> Dict[str, Any]:
        """Convert internal schema format to JSON Schema."""
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": schema_def.name,
            "description": schema_def.description,
            "version": schema_def.version,
            "type": "object",
            "properties": {},
            "required": schema_def.required
        }

        # Convert properties (simplified conversion)
        for prop_name, prop_schema in schema_def.schema.get("properties", {}).items():
            json_schema["properties"][prop_name] = self._convert_property_schema(prop_schema)

        return json_schema

    def _convert_property_schema(self, prop_schema: Any) -> Any:
        """Convert a property schema to JSON Schema format."""
        if isinstance(prop_schema, dict):
            result = {}
            for key, value in prop_schema.items():
                if key == "type":
                    result[key] = value
                elif key == "description":
                    result[key] = value
                elif key == "default":
                    result[key] = value
                elif key == "enum":
                    result[key] = value
                elif key == "items":
                    result[key] = self._convert_property_schema(value)
                elif key == "properties":
                    result[key] = {}
                    for prop_name, prop_def in value.items():
                        result[key][prop_name] = self._convert_property_schema(prop_def)
                else:
                    result[key] = value
            return result
        return prop_schema

    def _register_builtin_schemas(self) -> None:
        """Register built-in schemas for common configuration namespaces."""

        # API configuration schema
        api_schema = SchemaDefinition(
            name="api",
            description="API server configuration",
            version="1.0",
            required=["title", "version"],
            schema={
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "API title"},
                    "version": {"type": "string", "description": "API version"},
                    "host": {"type": "string", "description": "API host", "default": "0.0.0.0"},
                    "port": {"type": "integer", "description": "API port", "default": 8000},
                    "debug": {"type": "boolean", "description": "Debug mode", "default": False},
                    "docs_url": {"type": "string", "description": "OpenAPI docs URL", "default": "/docs"},
                    "redoc_url": {"type": "string", "description": "ReDoc URL", "default": "/redoc"},
                    "openapi_url": {"type": "string", "description": "OpenAPI schema URL", "default": "/openapi.json"},
                    "root_path": {"type": "string", "description": "Root path for reverse proxy"},
                    "rate_limit": {
                        "type": "object",
                        "properties": {
                            "requests_per_minute": {"type": "integer", "default": 100},
                            "burst_limit": {"type": "integer", "default": 10}
                        }
                    }
                }
            }
        )

        # Redis configuration schema
        redis_schema = SchemaDefinition(
            name="redis",
            description="Redis configuration",
            version="1.0",
            schema={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "Redis URL", "default": "redis://localhost:6379"},
                    "host": {"type": "string", "description": "Redis host", "default": "localhost"},
                    "port": {"type": "integer", "description": "Redis port", "default": 6379},
                    "db": {"type": "integer", "description": "Redis database", "default": 0},
                    "password": {"type": "string", "description": "Redis password"},
                    "ssl": {"type": "boolean", "description": "Use SSL", "default": False},
                    "connection_pool": {
                        "type": "object",
                        "properties": {
                            "max_connections": {"type": "integer", "default": 20},
                            "retry_on_timeout": {"type": "boolean", "default": True}
                        }
                    }
                }
            }
        )

        # Database configuration schema
        database_schema = SchemaDefinition(
            name="database",
            description="Database configuration",
            version="1.0",
            schema={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "Database URL"},
                    "host": {"type": "string", "description": "Database host", "default": "localhost"},
                    "port": {"type": "integer", "description": "Database port", "default": 5432},
                    "database": {"type": "string", "description": "Database name"},
                    "username": {"type": "string", "description": "Database username"},
                    "password": {"type": "string", "description": "Database password"},
                    "ssl": {"type": "boolean", "description": "Use SSL", "default": False},
                    "pool": {
                        "type": "object",
                        "properties": {
                            "min_size": {"type": "integer", "default": 1},
                            "max_size": {"type": "integer", "default": 10},
                            "timeout": {"type": "number", "default": 30.0}
                        }
                    }
                }
            }
        )

        # Security configuration schema
        security_schema = SchemaDefinition(
            name="security",
            description="Security configuration",
            version="1.0",
            schema={
                "type": "object",
                "properties": {
                    "secret_key": {"type": "string", "description": "Secret key for JWT signing"},
                    "algorithm": {"type": "string", "description": "JWT algorithm", "default": "HS256"},
                    "access_token_expire_minutes": {"type": "integer", "description": "Access token expiry", "default": 30},
                    "refresh_token_expire_days": {"type": "integer", "description": "Refresh token expiry", "default": 7},
                    "cors": {
                        "type": "object",
                        "properties": {
                            "origins": {"type": "array", "items": {"type": "string"}, "default": ["*"]},
                            "methods": {"type": "array", "items": {"type": "string"}, "default": ["GET", "POST"]},
                            "headers": {"type": "array", "items": {"type": "string"}, "default": ["*"]}
                        }
                    },
                    "rate_limiting": {
                        "type": "object",
                        "properties": {
                            "enabled": {"type": "boolean", "default": True},
                            "requests_per_minute": {"type": "integer", "default": 100}
                        }
                    }
                }
            }
        )

        # Feature flags configuration schema
        feature_flags_schema = SchemaDefinition(
            name="feature_flags",
            description="Feature flags configuration",
            version="1.0",
            schema={
                "type": "object",
                "properties": {
                    "default_ttl_seconds": {"type": "integer", "description": "Default TTL for feature flags", "default": 3600, "minimum": 60},
                    "cache_enabled": {"type": "boolean", "description": "Enable feature flag caching", "default": True},
                    "cache_ttl_seconds": {"type": "integer", "description": "Cache TTL for feature flags", "default": 300, "minimum": 60},
                    "redis": {
                        "type": "object",
                        "properties": {
                            "key_prefix": {"type": "string", "description": "Redis key prefix", "default": "aurum:feature_flags"},
                            "ttl_seconds": {"type": "integer", "description": "Redis TTL", "default": 3600, "minimum": 60}
                        }
                    },
                    "overrides": {
                        "type": "object",
                        "description": "Environment-specific feature flag overrides",
                        "additionalProperties": {"type": "boolean"}
                    },
                    "rollout": {
                        "type": "object",
                        "properties": {
                            "default_percentage": {"type": "number", "description": "Default rollout percentage", "default": 100.0, "minimum": 0.0, "maximum": 100.0},
                            "gradual_rollout_enabled": {"type": "boolean", "description": "Enable gradual rollout", "default": False},
                            "rollout_increment_hours": {"type": "integer", "description": "Hours between rollout increments", "default": 24, "minimum": 1}
                        }
                    },
                    "analytics": {
                        "type": "object",
                        "properties": {
                            "enabled": {"type": "boolean", "description": "Enable feature flag analytics", "default": True},
                            "retention_days": {"type": "integer", "description": "Analytics retention period", "default": 30, "minimum": 1},
                            "sampling_rate": {"type": "number", "description": "Analytics sampling rate", "default": 1.0, "minimum": 0.0, "maximum": 1.0}
                        }
                    }
                }
            }
        )

        # Register all schemas
        for schema in [api_schema, redis_schema, database_schema, security_schema, feature_flags_schema]:
            self.register_schema(schema)


class ConfigValidator:
    """Validates configuration against registered schemas."""

    def __init__(self, schema_registry: SchemaRegistry, strict_mode: bool = False):
        self._schema_registry = schema_registry
        self._strict_mode = strict_mode
        self._validation_errors: List[str] = []

    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration against all applicable schemas."""
        self._validation_errors = []
        is_valid = True

        # Validate each namespace against its schema
        for namespace in self._schema_registry.list_schemas():
            if namespace in config:
                namespace_config = config[namespace]
                if not self._validate_namespace(namespace, namespace_config):
                    is_valid = False

        return is_valid

    def _validate_namespace(self, namespace: str, config: Any) -> bool:
        """Validate a specific namespace configuration."""
        schema_def = self._schema_registry.get_schema(namespace)
        if not schema_def:
            if self._strict_mode:
                self._validation_errors.append(f"No schema found for namespace '{namespace}'")
                return False
            return True  # Skip validation if no schema

        try:
            # Basic JSON Schema validation (simplified)
            self._validate_against_schema(config, schema_def.schema)
            return True
        except Exception as e:
            self._validation_errors.append(f"Validation failed for namespace '{namespace}': {str(e)}")
            return False

    def _validate_against_schema(self, config: Any, schema: Dict[str, Any]) -> None:
        """Validate config against schema (simplified validation)."""
        if not isinstance(config, dict):
            raise ValueError(f"Expected object, got {type(config).__name__}")

        properties = schema.get("properties", {})
        required = schema.get("required", [])

        # Check required fields
        for field in required:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")

        # Validate field types and constraints
        for field_name, field_value in config.items():
            if field_name in properties:
                field_schema = properties[field_name]
                self._validate_field(field_name, field_value, field_schema)

    def _validate_field(self, field_name: str, value: Any, schema: Dict[str, Any]) -> None:
        """Validate a single field against its schema."""
        field_type = schema.get("type")

        if field_type == "string" and not isinstance(value, str):
            raise ValueError(f"Field '{field_name}' must be a string")
        elif field_type == "integer" and not isinstance(value, int):
            raise ValueError(f"Field '{field_name}' must be an integer")
        elif field_type == "number" and not isinstance(value, (int, float)):
            raise ValueError(f"Field '{field_name}' must be a number")
        elif field_type == "boolean" and not isinstance(value, bool):
            raise ValueError(f"Field '{field_name}' must be a boolean")
        elif field_type == "array" and not isinstance(value, list):
            raise ValueError(f"Field '{field_name}' must be an array")
        elif field_type == "object" and not isinstance(value, dict):
            raise ValueError(f"Field '{field_name}' must be an object")

        # Validate enum values
        enum_values = schema.get("enum")
        if enum_values and value not in enum_values:
            raise ValueError(f"Field '{field_name}' must be one of: {enum_values}")

        # Validate nested objects
        if field_type == "object" and isinstance(value, dict):
            nested_properties = schema.get("properties", {})
            for nested_field, nested_value in value.items():
                if nested_field in nested_properties:
                    self._validate_field(f"{field_name}.{nested_field}", nested_value, nested_properties[nested_field])

        # Validate arrays
        if field_type == "array" and isinstance(value, list):
            items_schema = schema.get("items", {})
            for i, item in enumerate(value):
                self._validate_field(f"{field_name}[{i}]", item, items_schema)

    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors."""
        return self._validation_errors.copy()

    def coerce_types(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Attempt to coerce configuration values to correct types."""
        result = config.copy()

        for namespace in self._schema_registry.list_schemas():
            if namespace in result:
                result[namespace] = self._coerce_namespace_types(namespace, result[namespace])

        return result

    def _coerce_namespace_types(self, namespace: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Coerce types for a namespace configuration."""
        schema_def = self._schema_registry.get_schema(namespace)
        if not schema_def:
            return config

        result = config.copy()
        properties = schema_def.schema.get("properties", {})

        for field_name, field_value in config.items():
            if field_name in properties:
                field_schema = properties[field_name]
                result[field_name] = self._coerce_field_type(field_value, field_schema)

        return result

    def _coerce_field_type(self, value: Any, schema: Dict[str, Any]) -> Any:
        """Coerce a field value to the correct type."""
        field_type = schema.get("type")

        try:
            if field_type == "string":
                return str(value)
            elif field_type == "integer":
                return int(value)
            elif field_type == "number":
                return float(value)
            elif field_type == "boolean":
                if isinstance(value, str):
                    return value.lower() in ("true", "1", "yes", "on")
                return bool(value)
            elif field_type == "object" and isinstance(value, dict):
                # Recursively coerce nested objects
                nested_properties = schema.get("properties", {})
                result = {}
                for nested_field, nested_value in value.items():
                    if nested_field in nested_properties:
                        result[nested_field] = self._coerce_field_type(nested_value, nested_properties[nested_field])
                    else:
                        result[nested_field] = nested_value
                return result
            elif field_type == "array" and isinstance(value, list):
                # Coerce array items
                items_schema = schema.get("items", {})
                return [self._coerce_field_type(item, items_schema) for item in value]
        except (ValueError, TypeError):
            # If coercion fails, return original value
            pass

        return value


# Global schema registry instance
_schema_registry = SchemaRegistry()
_config_validator = ConfigValidator(_schema_registry)


def get_schema_registry() -> SchemaRegistry:
    """Get the global schema registry."""
    return _schema_registry


def get_config_validator(strict_mode: bool = False) -> ConfigValidator:
    """Get the global config validator."""
    return ConfigValidator(_schema_registry, strict_mode)


def validate_and_coerce_config(config: Dict[str, Any], strict_mode: bool = False) -> Dict[str, Any]:
    """Validate and coerce configuration types."""
    validator = get_config_validator(strict_mode)

    if not validator.validate_config(config):
        errors = validator.get_validation_errors()
        raise ValueError(f"Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors))

    return validator.coerce_types(config)


def export_all_schemas(output_dir: str) -> None:
    """Export all schemas as JSON Schema files."""
    registry = get_schema_registry()
    output_path = Path(output_dir)

    for schema_name in registry.list_schemas():
        schema_file = output_path / f"{schema_name}_schema.json"
        registry.export_json_schema(schema_name, str(schema_file))
        logger.info(f"Exported schema {schema_name} to {schema_file}")
