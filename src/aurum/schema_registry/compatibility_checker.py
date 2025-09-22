"""Schema compatibility checking for evolution validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from enum import Enum

from .registry_manager import SchemaCompatibilityMode


class CompatibilityIssue(str, Enum):
    """Types of schema compatibility issues."""
    FIELD_REMOVED = "FIELD_REMOVED"              # Required field removed
    FIELD_TYPE_CHANGED = "FIELD_TYPE_CHANGED"    # Field type changed incompatibly
    FIELD_DEFAULT_CHANGED = "FIELD_DEFAULT_CHANGED"  # Default value changed
    FIELD_OPTIONAL_TO_REQUIRED = "FIELD_OPTIONAL_TO_REQUIRED"  # Field became required
    FIELD_REQUIRED_TO_OPTIONAL = "FIELD_REQUIRED_TO_OPTIONAL"  # Field became optional
    ENUM_VALUE_REMOVED = "ENUM_VALUE_REMOVED"    # Enum value removed
    UNION_MEMBER_REMOVED = "UNION_MEMBER_REMOVED"  # Union member removed
    SCHEMA_TYPE_CHANGED = "SCHEMA_TYPE_CHANGED"  # Schema type changed


@dataclass
class CompatibilityResult:
    """Result of schema compatibility check."""

    is_compatible: bool
    mode: SchemaCompatibilityMode
    messages: List[str] = field(default_factory=list)
    issues: List[CompatibilityIssue] = field(default_factory=list)
    breaking_changes: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    def add_issue(self, issue: CompatibilityIssue, message: str) -> None:
        """Add a compatibility issue.

        Args:
            issue: Type of compatibility issue
            message: Description of the issue
        """
        self.issues.append(issue)
        self.messages.append(message)

        if issue in [
            CompatibilityIssue.FIELD_REMOVED,
            CompatibilityIssue.FIELD_TYPE_CHANGED,
            CompatibilityIssue.SCHEMA_TYPE_CHANGED
        ]:
            self.breaking_changes.append(message)

    def add_recommendation(self, recommendation: str) -> None:
        """Add a recommendation for fixing compatibility issues.

        Args:
            recommendation: Recommendation text
        """
        self.recommendations.append(recommendation)

    def is_backward_compatible(self) -> bool:
        """Check if schema is backward compatible.

        Returns:
            True if backward compatible
        """
        return self.mode in [
            SchemaCompatibilityMode.BACKWARD,
            SchemaCompatibilityMode.FULL,
            SchemaCompatibilityMode.BACKWARD_TRANSITIVE,
            SchemaCompatibilityMode.FULL_TRANSITIVE
        ]

    def is_forward_compatible(self) -> bool:
        """Check if schema is forward compatible.

        Returns:
            True if forward compatible
        """
        return self.mode in [
            SchemaCompatibilityMode.FORWARD,
            SchemaCompatibilityMode.FULL,
            SchemaCompatibilityMode.FORWARD_TRANSITIVE,
            SchemaCompatibilityMode.FULL_TRANSITIVE
        ]

    def has_breaking_changes(self) -> bool:
        """Check if there are breaking changes.

        Returns:
            True if breaking changes detected
        """
        return len(self.breaking_changes) > 0

    def summary(self) -> str:
        """Get compatibility summary.

        Returns:
            Human-readable summary
        """
        if self.is_compatible:
            return f"Schema is compatible ({self.mode.value})"

        summary = f"Schema is NOT compatible ({self.mode.value})"
        if self.breaking_changes:
            summary += f" - {len(self.breaking_changes)} breaking changes"
        if self.recommendations:
            summary += f" - {len(self.recommendations)} recommendations"

        return summary


class CompatibilityChecker:
    """Check schema compatibility and evolution rules."""

    def __init__(self):
        """Initialize compatibility checker."""
        self.supported_types = {
            "null", "boolean", "int", "long", "float", "double",
            "bytes", "string", "record", "enum", "array", "map", "union", "fixed"
        }

    def check_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        mode: SchemaCompatibilityMode = SchemaCompatibilityMode.BACKWARD
    ) -> CompatibilityResult:
        """Check compatibility between two schemas.

        Args:
            old_schema: Existing schema
            new_schema: New schema to check
            mode: Compatibility mode to use

        Returns:
            Compatibility check result
        """
        result = CompatibilityResult(
            is_compatible=True,
            mode=mode
        )

        try:
            # Check basic schema structure
            if old_schema.get("type") != new_schema.get("type"):
                if old_schema.get("type") == "record" or new_schema.get("type") == "record":
                    result.add_issue(
                        CompatibilityIssue.SCHEMA_TYPE_CHANGED,
                        "Schema type changed between record and non-record"
                    )
                    result.is_compatible = False

            # Check record compatibility
            if old_schema.get("type") == "record" and new_schema.get("type") == "record":
                self._check_record_compatibility(old_schema, new_schema, result)

            # Check enum compatibility
            elif old_schema.get("type") == "enum" and new_schema.get("type") == "enum":
                self._check_enum_compatibility(old_schema, new_schema, result)

            # Check union compatibility
            elif self._is_union_type(old_schema) and self._is_union_type(new_schema):
                self._check_union_compatibility(old_schema, new_schema, result)

            # Check array compatibility
            elif old_schema.get("type") == "array" and new_schema.get("type") == "array":
                self._check_array_compatibility(old_schema, new_schema, result)

            # Check map compatibility
            elif old_schema.get("type") == "map" and new_schema.get("type") == "map":
                self._check_map_compatibility(old_schema, new_schema, result)

            # For other types, check basic compatibility
            else:
                self._check_basic_compatibility(old_schema, new_schema, result)

        except Exception as e:
            result.is_compatible = False
            result.add_issue(
                CompatibilityIssue.SCHEMA_TYPE_CHANGED,
                f"Error checking compatibility: {e}"
            )

        # Add recommendations for backward compatibility
        if result.is_backward_compatible() and result.has_breaking_changes():
            result.add_recommendation(
                "Consider adding default values for new fields to maintain backward compatibility"
            )

        if result.is_forward_compatible() and result.has_breaking_changes():
            result.add_recommendation(
                "Consider making new fields optional to maintain forward compatibility"
            )

        return result

    def _check_record_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        result: CompatibilityResult
    ) -> None:
        """Check compatibility between record schemas.

        Args:
            old_schema: Old record schema
            new_schema: New record schema
            result: Compatibility result to update
        """
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

        # Check for removed fields
        for field_name, old_field in old_fields.items():
            if field_name not in new_fields:
                # Field was removed - this is a breaking change
                if not self._is_field_optional(old_field):
                    result.add_issue(
                        CompatibilityIssue.FIELD_REMOVED,
                        f"Required field '{field_name}' was removed"
                    )
                    result.is_compatible = False
            else:
                # Field exists, check type compatibility
                new_field = new_fields[field_name]
                self._check_field_compatibility(old_field, new_field, result)

        # Check for new fields
        for field_name, new_field in new_fields.items():
            if field_name not in old_fields:
                # New field added - may affect forward compatibility
                if not self._is_field_optional(new_field):
                    if result.is_forward_compatible():
                        result.add_issue(
                            CompatibilityIssue.FIELD_OPTIONAL_TO_REQUIRED,
                            f"New required field '{field_name}' may break forward compatibility"
                        )
                        result.is_compatible = False

    def _check_field_compatibility(
        self,
        old_field: Dict[str, Any],
        new_field: Dict[str, Any],
        result: CompatibilityResult
    ) -> None:
        """Check compatibility between two fields.

        Args:
            old_field: Old field definition
            new_field: New field definition
            result: Compatibility result to update
        """
        field_name = old_field["name"]

        # Check if field became required when it was optional
        old_optional = self._is_field_optional(old_field)
        new_optional = self._is_field_optional(new_field)

        if old_optional and not new_optional:
            result.add_issue(
                CompatibilityIssue.FIELD_OPTIONAL_TO_REQUIRED,
                f"Field '{field_name}' changed from optional to required"
            )
            result.is_compatible = False

        # Check type compatibility
        old_type = old_field["type"]
        new_type = new_field["type"]

        if not self._types_compatible(old_type, new_type):
            result.add_issue(
                CompatibilityIssue.FIELD_TYPE_CHANGED,
                f"Field '{field_name}' type changed from {old_type} to {new_type}"
            )
            result.is_compatible = False

        # Check default value changes
        if self._default_value_changed(old_field, new_field):
            result.add_issue(
                CompatibilityIssue.FIELD_DEFAULT_CHANGED,
                f"Field '{field_name}' default value changed"
            )
            if not old_optional:
                result.is_compatible = False

    def _check_enum_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        result: CompatibilityResult
    ) -> None:
        """Check compatibility between enum schemas.

        Args:
            old_schema: Old enum schema
            new_schema: New enum schema
            result: Compatibility result to update
        """
        old_symbols = set(old_schema.get("symbols", []))
        new_symbols = set(new_schema.get("symbols", []))

        # Check for removed symbols
        removed_symbols = old_symbols - new_symbols
        if removed_symbols:
            result.add_issue(
                CompatibilityIssue.ENUM_VALUE_REMOVED,
                f"Enum symbols removed: {removed_symbols}"
            )
            result.is_compatible = False

        # Check for default value
        old_default = old_schema.get("default")
        new_default = new_schema.get("default")

        if old_default and new_default and old_default != new_default:
            result.add_issue(
                CompatibilityIssue.ENUM_VALUE_REMOVED,
                f"Enum default value changed from '{old_default}' to '{new_default}'"
            )
            result.is_compatible = False

    def _check_union_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        result: CompatibilityResult
    ) -> None:
        """Check compatibility between union schemas.

        Args:
            old_schema: Old union schema
            new_schema: New union schema
            result: Compatibility result to update
        """
        old_types = self._normalize_union_types(old_schema)
        new_types = self._normalize_union_types(new_schema)

        # Check for removed union members
        removed_types = old_types - new_types
        if removed_types:
            result.add_issue(
                CompatibilityIssue.UNION_MEMBER_REMOVED,
                f"Union members removed: {removed_types}"
            )
            result.is_compatible = False

    def _check_array_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        result: CompatibilityResult
    ) -> None:
        """Check compatibility between array schemas.

        Args:
            old_schema: Old array schema
            new_schema: New array schema
            result: Compatibility result to update
        """
        old_items = old_schema.get("items")
        new_items = new_schema.get("items")

        if old_items and new_items:
            if not self._types_compatible(old_items, new_items):
                result.add_issue(
                    CompatibilityIssue.FIELD_TYPE_CHANGED,
                    f"Array item type changed from {old_items} to {new_items}"
                )
                result.is_compatible = False

    def _check_map_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        result: CompatibilityResult
    ) -> None:
        """Check compatibility between map schemas.

        Args:
            old_schema: Old map schema
            new_schema: New map schema
            result: Compatibility result to update
        """
        old_values = old_schema.get("values")
        new_values = new_schema.get("values")

        if old_values and new_values:
            if not self._types_compatible(old_values, new_values):
                result.add_issue(
                    CompatibilityIssue.FIELD_TYPE_CHANGED,
                    f"Map value type changed from {old_values} to {new_values}"
                )
                result.is_compatible = False

    def _check_basic_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        result: CompatibilityResult
    ) -> None:
        """Check basic compatibility for non-complex types.

        Args:
            old_schema: Old schema
            new_schema: New schema
            result: Compatibility result to update
        """
        old_type = old_schema.get("type")
        new_type = new_schema.get("type")

        if old_type != new_type:
            result.add_issue(
                CompatibilityIssue.FIELD_TYPE_CHANGED,
                f"Type changed from {old_type} to {new_type}"
            )
            result.is_compatible = False

    def _is_field_optional(self, field: Dict[str, Any]) -> bool:
        """Check if a field is optional.

        Args:
            field: Field definition

        Returns:
            True if field is optional
        """
        field_type = field.get("type", [])

        # Field is optional if type is union with null
        if isinstance(field_type, list):
            return "null" in field_type

        return False

    def _types_compatible(self, old_type: Any, new_type: Any) -> bool:
        """Check if two types are compatible.

        Args:
            old_type: Old type definition
            new_type: New type definition

        Returns:
            True if types are compatible
        """
        # Handle union types
        if isinstance(old_type, list) or isinstance(new_type, list):
            old_types = self._normalize_union_types(old_type)
            new_types = self._normalize_union_types(new_type)
            return old_types.issubset(new_types) or new_types.issubset(old_types)

        # Handle named types
        if isinstance(old_type, str) and isinstance(new_type, str):
            # Same type is always compatible
            if old_type == new_type:
                return True

            # String to bytes is not compatible
            if old_type == "string" and new_type == "bytes":
                return False
            if old_type == "bytes" and new_type == "string":
                return False

            # Numeric type promotions
            numeric_promotions = {
                "int": ["long", "float", "double"],
                "long": ["float", "double"],
                "float": ["double"]
            }

            if old_type in numeric_promotions and new_type in numeric_promotions[old_type]:
                return True

            return False

        # Handle complex types
        if isinstance(old_type, dict) and isinstance(new_type, dict):
            return old_type.get("type") == new_type.get("type")

        return False

    def _default_value_changed(self, old_field: Dict[str, Any], new_field: Dict[str, Any]) -> bool:
        """Check if default value changed.

        Args:
            old_field: Old field definition
            new_field: New field definition

        Returns:
            True if default value changed
        """
        old_default = old_field.get("default")
        new_default = new_field.get("default")

        # Both have defaults
        if old_default is not None and new_default is not None:
            return old_default != new_default

        # One has default, other doesn't
        if old_default is not None or new_default is not None:
            return True

        return False

    def _is_union_type(self, schema: Dict[str, Any]) -> bool:
        """Check if schema represents a union type.

        Args:
            schema: Schema to check

        Returns:
            True if union type
        """
        return isinstance(schema.get("type"), list)

    def _normalize_union_types(self, schema: Any) -> Set[str]:
        """Normalize union type to set of types.

        Args:
            schema: Schema or type list

        Returns:
            Set of normalized types
        """
        if isinstance(schema, list):
            types = schema
        elif isinstance(schema, dict) and schema.get("type") == "union":
            types = schema.get("types", [])
        else:
            types = [schema]

        normalized = set()
        for t in types:
            if isinstance(t, dict):
                if t.get("type") in ["record", "enum", "array", "map", "fixed"]:
                    normalized.add(t.get("type"))
                else:
                    normalized.add(str(t))
            else:
                normalized.add(str(t))

        return normalized
