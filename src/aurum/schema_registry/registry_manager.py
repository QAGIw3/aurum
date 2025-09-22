"""Schema Registry management and compatibility enforcement."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import requests
from dataclasses import dataclass, field
from enum import Enum

from ..logging import StructuredLogger, LogLevel, create_logger
from .contracts import (
    ContractError,
    SchemaNotFoundError,
    SubjectContracts,
    SubjectNotDefinedError,
    SubjectPatternError,
)


class SchemaCompatibilityMode(str, Enum):
    """Schema Registry compatibility modes."""
    NONE = "NONE"              # No compatibility checks
    BACKWARD = "BACKWARD"      # Consumers using new schema can read data written by old schema
    FORWARD = "FORWARD"        # Consumers using old schema can read data written by new schema
    FULL = "FULL"             # Both backward and forward compatibility
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"  # Backward compatibility across multiple versions
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"    # Forward compatibility across multiple versions
    FULL_TRANSITIVE = "FULL_TRANSITIVE"         # Full compatibility across multiple versions


@dataclass
class SchemaInfo:
    """Information about a registered schema."""
    subject: str
    version: int
    schema_id: int
    schema: Dict[str, Any]
    compatibility_mode: SchemaCompatibilityMode
    registered_at: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "subject": self.subject,
            "version": self.version,
            "schema_id": self.schema_id,
            "schema": self.schema,
            "compatibility_mode": self.compatibility_mode.value,
            "registered_at": self.registered_at
        }


@dataclass
class SchemaRegistryConfig:
    """Configuration for Schema Registry."""
    base_url: str
    timeout_seconds: int = 30
    max_retries: int = 3
    default_compatibility_mode: SchemaCompatibilityMode = SchemaCompatibilityMode.BACKWARD
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_verify: bool = True

    # Subject naming configuration
    subject_prefix: str = "aurum"
    subject_suffix: str = "v1"
    contracts_path: Optional[str] = None
    enforce_contracts: bool = True
    fail_on_missing_contract: bool = True
    validate_contract_schema: bool = True

    # Compatibility enforcement
    enforce_compatibility: bool = True
    fail_on_incompatible: bool = True

    # Validation
    validate_schema: bool = True
    validate_references: bool = True


class SchemaRegistryError(Exception):
    """Base exception for Schema Registry operations."""
    pass


class SchemaRegistryConnectionError(SchemaRegistryError):
    """Connection error to Schema Registry."""
    pass


class SchemaCompatibilityError(SchemaRegistryError):
    """Schema compatibility error."""
    pass


class SubjectRegistrationError(SchemaRegistryError):
    """Error registering schema subject."""
    pass


class SchemaRegistryManager:
    """Manage Schema Registry operations and enforce compatibility."""

    def __init__(self, config: SchemaRegistryConfig):
        """Initialize Schema Registry manager.

        Args:
            config: Schema Registry configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="schema_registry_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.schema_registry.events",
            dataset="schema_management"
        )

        # Session for HTTP requests
        self.session = requests.Session()
        if config.username and config.password:
            self.session.auth = (config.username, config.password)

        self.session.verify = config.ssl_verify

        # Cache for registered schemas
        self._schema_cache: Dict[str, SchemaInfo] = {}
        self._contracts: Optional[SubjectContracts] = None

        if config.enforce_contracts:
            default_contract_path = Path(__file__).resolve().parents[3] / "kafka" / "schemas" / "contracts.yml"
            contracts_path = Path(config.contracts_path or default_contract_path)

            if contracts_path.exists():
                try:
                    self._contracts = SubjectContracts(contracts_path)
                    self.logger.log(
                        LogLevel.INFO,
                        "Loaded Avro subject contracts",
                        "schema_contracts_loaded",
                        contracts_path=str(contracts_path),
                        subject_count=len(self._contracts.list_subjects()),
                    )
                except ContractError as exc:
                    if config.fail_on_missing_contract:
                        raise SchemaRegistryError(f"Failed to load contract catalog: {exc}") from exc
                    self.logger.log(
                        LogLevel.WARNING,
                        f"Contract catalog could not be loaded: {exc}",
                        "schema_contracts_load_failed",
                        contracts_path=str(contracts_path),
                        error=str(exc),
                    )
            elif config.fail_on_missing_contract:
                raise SchemaRegistryError(f"Contract catalog not found at {contracts_path}")
            else:
                self.logger.log(
                    LogLevel.WARNING,
                    "Contract catalog missing but enforcement disabled",
                    "schema_contracts_missing",
                    contracts_path=str(contracts_path),
                )

        self.logger.log(
            LogLevel.INFO,
            f"Initialized Schema Registry manager for {config.base_url}",
            "schema_registry_initialized",
            base_url=config.base_url,
            default_compatibility=config.default_compatibility_mode.value
        )

    def register_subject(
        self,
        subject: str,
        schema: Dict[str, Any],
        compatibility_mode: Optional[SchemaCompatibilityMode] = None
    ) -> SchemaInfo:
        """Register a schema subject.

        Args:
            subject: Schema subject name
            schema: Avro schema definition
            compatibility_mode: Compatibility mode for this subject

        Returns:
            Schema registration information

        Raises:
            SubjectRegistrationError: If registration fails
            SchemaCompatibilityError: If schema is incompatible
        """
        if compatibility_mode is None:
            compatibility_mode = self.config.default_compatibility_mode

        # Enforce subject contracts if configured
        contract = None
        if self._contracts and self.config.enforce_contracts:
            try:
                contract = self._contracts.get(subject)
                self._contracts.validate_subject_name(subject)
            except SubjectNotDefinedError as exc:
                if self.config.fail_on_missing_contract:
                    raise SubjectRegistrationError(
                        f"Subject '{subject}' is not defined in the contract catalog"
                    ) from exc
                self.logger.log(
                    LogLevel.WARNING,
                    f"Skipping contract enforcement for undefined subject {subject}",
                    "schema_contract_missing",
                    subject=subject,
                )
            except SubjectPatternError as exc:
                raise SubjectRegistrationError(str(exc)) from exc

            if contract:
                try:
                    contract_mode = SchemaCompatibilityMode(contract.compatibility)
                except ValueError as exc:
                    raise SchemaCompatibilityError(
                        f"Unsupported compatibility '{contract.compatibility}' for subject {subject}"
                    ) from exc

                if compatibility_mode != contract_mode:
                    raise SchemaCompatibilityError(
                        f"Requested compatibility {compatibility_mode.value} does not match contract {contract_mode.value}"
                    )

                if self.config.validate_contract_schema:
                    try:
                        frozen_schema = self._contracts.load_schema(subject)
                    except SchemaNotFoundError as exc:
                        if self.config.fail_on_missing_contract:
                            raise SubjectRegistrationError(str(exc)) from exc
                        self.logger.log(
                            LogLevel.WARNING,
                            f"Frozen schema missing for subject {subject}: {exc}",
                            "schema_contract_schema_missing",
                            subject=subject,
                            error=str(exc),
                        )
                    else:
                        if frozen_schema != schema:
                            raise SubjectRegistrationError(
                                f"Provided schema for subject {subject} does not match frozen contract"
                            )

        # Validate schema structure
        if self.config.validate_schema:
            self._validate_schema(schema)

        # Check compatibility if enforcement is enabled
        if self.config.enforce_compatibility:
            existing_schema = self.get_latest_schema(subject)
            if existing_schema:
                compatibility_result = self.check_compatibility(
                    subject,
                    schema,
                    existing_schema.schema
                )
                if not compatibility_result.is_compatible and self.config.fail_on_incompatible:
                    raise SchemaCompatibilityError(
                        f"Schema incompatible with existing subject {subject}: {compatibility_result.messages}"
                    )

        # Register the subject
        try:
            # Register schema
            schema_response = self._register_schema(subject, schema)
            schema_id = schema_response["id"]
            version = schema_response["version"]

            # Set compatibility mode
            self._set_compatibility_mode(subject, compatibility_mode)

            # Create schema info
            schema_info = SchemaInfo(
                subject=subject,
                version=version,
                schema_id=schema_id,
                schema=schema,
                compatibility_mode=compatibility_mode,
                registered_at=str(datetime.now())
            )

            # Cache the schema
            self._schema_cache[subject] = schema_info

            self.logger.log(
                LogLevel.INFO,
                f"Registered schema subject {subject} version {version}",
                "schema_subject_registered",
                subject=subject,
                version=version,
                schema_id=schema_id,
                compatibility_mode=compatibility_mode.value
            )

            return schema_info

        except requests.RequestException as e:
            raise SchemaRegistryConnectionError(f"Failed to register subject {subject}: {e}")

    def get_latest_schema(self, subject: str) -> Optional[SchemaInfo]:
        """Get the latest version of a schema subject.

        Args:
            subject: Schema subject name

        Returns:
            Latest schema information or None if not found
        """
        if subject in self._schema_cache:
            return self._schema_cache[subject]

        try:
            response = self.session.get(
                urljoin(self.config.base_url, f"subjects/{subject}/versions/latest"),
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()

            data = response.json()
            schema_info = SchemaInfo(
                subject=subject,
                version=data["version"],
                schema_id=data["id"],
                schema=data["schema"],
                compatibility_mode=SchemaCompatibilityMode(data.get("compatibility", "BACKWARD")),
                registered_at=data.get("registered_at", "")
            )

            self._schema_cache[subject] = schema_info
            return schema_info

        except requests.RequestException:
            return None

    def check_compatibility(
        self,
        subject: str,
        new_schema: Dict[str, Any],
        existing_schema: Optional[Dict[str, Any]] = None
    ) -> "CompatibilityResult":
        """Check compatibility between schemas.

        Args:
            subject: Schema subject name
            new_schema: New schema to check
            existing_schema: Existing schema (optional, will fetch if not provided)

        Returns:
            Compatibility check result
        """
        if existing_schema is None:
            existing_info = self.get_latest_schema(subject)
            if not existing_info:
                # If no existing schema, consider it compatible
                return CompatibilityResult(
                    is_compatible=True,
                    mode=SchemaCompatibilityMode.BACKWARD,
                    messages=["No existing schema to check compatibility against"]
                )
            existing_schema = existing_info.schema

        # Get compatibility mode for the subject
        compatibility_mode = self._get_compatibility_mode(subject)

        # Perform compatibility check
        try:
            response = self.session.post(
                urljoin(self.config.base_url, f"compatibility/subjects/{subject}/versions/latest"),
                json=new_schema,
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()

            result_data = response.json()
            is_compatible = result_data.get("is_compatible", True)

            return CompatibilityResult(
                is_compatible=is_compatible,
                mode=compatibility_mode,
                messages=result_data.get("messages", [])
            )

        except requests.RequestException as e:
            # If compatibility check fails, assume incompatible
            return CompatibilityResult(
                is_compatible=False,
                mode=compatibility_mode,
                messages=[f"Compatibility check failed: {e}"]
            )

    def _register_schema(self, subject: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Register schema with Schema Registry.

        Args:
            subject: Schema subject name
            schema: Avro schema

        Returns:
            Schema registration response

        Raises:
            SubjectRegistrationError: If registration fails
        """
        try:
            response = self.session.post(
                urljoin(self.config.base_url, "subjects/{subject}/versions"),
                json={"schema": json.dumps(schema)},
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            raise SubjectRegistrationError(f"Failed to register schema for {subject}: {e}")

    def _set_compatibility_mode(self, subject: str, mode: SchemaCompatibilityMode) -> None:
        """Set compatibility mode for a subject.

        Args:
            subject: Schema subject name
            mode: Compatibility mode
        """
        try:
            response = self.session.put(
                urljoin(self.config.base_url, f"config/{subject}"),
                json={"compatibility": mode.value},
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()

        except requests.RequestException as e:
            self.logger.log(
                LogLevel.WARNING,
                f"Failed to set compatibility mode for {subject}: {e}",
                "compatibility_mode_set_failed",
                subject=subject,
                mode=mode.value
            )

    def _get_compatibility_mode(self, subject: str) -> SchemaCompatibilityMode:
        """Get compatibility mode for a subject.

        Args:
            subject: Schema subject name

        Returns:
            Compatibility mode
        """
        try:
            response = self.session.get(
                urljoin(self.config.base_url, f"config/{subject}"),
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()

            data = response.json()
            return SchemaCompatibilityMode(data.get("compatibilityLevel", "BACKWARD"))

        except requests.RequestException:
            # Return default if we can't get the mode
            return self.config.default_compatibility_mode

    def _validate_schema(self, schema: Dict[str, Any]) -> None:
        """Validate Avro schema.

        Args:
            schema: Avro schema to validate

        Raises:
            SubjectRegistrationError: If schema is invalid
        """
        # Basic validation - check required fields
        required_fields = ["type", "name", "fields"]
        if schema.get("type") == "record":
            missing_fields = [field for field in required_fields if field not in schema]
            if missing_fields:
                raise SubjectRegistrationError(f"Invalid schema: missing required fields: {missing_fields}")

        # Validate field definitions
        if "fields" in schema:
            for field in schema["fields"]:
                if not isinstance(field, dict) or "name" not in field or "type" not in field:
                    raise SubjectRegistrationError(f"Invalid field definition: {field}")

    def get_registry_status(self) -> Dict[str, Any]:
        """Get Schema Registry status.

        Returns:
            Status information
        """
        try:
            # Get subjects
            response = self.session.get(
                urljoin(self.config.base_url, "subjects"),
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()
            subjects = response.json()

            # Get schema count
            response = self.session.get(
                urljoin(self.config.base_url, "schemas"),
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()
            schemas = response.json()

            return {
                "total_subjects": len(subjects),
                "total_schemas": len(schemas),
                "subjects": subjects[:10],  # Limit for readability
                "base_url": self.config.base_url,
                "default_compatibility": self.config.default_compatibility_mode.value,
                "enforce_compatibility": self.config.enforce_compatibility
            }

        except requests.RequestException as e:
            return {
                "error": str(e),
                "base_url": self.config.base_url
            }
