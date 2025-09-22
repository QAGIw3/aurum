"""Tests for Schema Registry management."""

from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path
import types

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

# Stub optional Kafka dependencies so schema registry imports succeed without the extras
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("confluent_kafka.avro", MagicMock())
sys.modules.setdefault("confluent_kafka.schema_registry", MagicMock())
sys.modules.setdefault("confluent_kafka.schema_registry.avro", MagicMock())

# Provide a lightweight logging module to avoid importing heavy dependencies
logging_stub = types.ModuleType("aurum.logging")
logging_stub.StructuredLogger = MagicMock()
logging_stub.LogLevel = MagicMock()
logging_stub.create_logger = MagicMock(return_value=MagicMock())
sys.modules.setdefault("aurum.logging", logging_stub)

from aurum.schema_registry import (
    SchemaRegistryManager,
    SchemaRegistryConfig,
    SchemaCompatibilityMode,
    CompatibilityChecker,
    CompatibilityResult,
    SchemaInfo,
    SchemaRegistryError,
    SchemaRegistryConnectionError,
    SubjectRegistrationError,
    SchemaCompatibilityError
)
from aurum.schema_registry.contracts import (
    SubjectContracts,
    SubjectNotDefinedError,
    SubjectPatternError,
)


class TestSchemaRegistryConfig:
    def test_config_creation(self) -> None:
        """Test Schema Registry configuration creation."""
        config = SchemaRegistryConfig(
            base_url="http://localhost:8081",
            default_compatibility_mode=SchemaCompatibilityMode.BACKWARD,
            timeout_seconds=30,
            username="user",
            password="pass"
        )

        assert config.base_url == "http://localhost:8081"
        assert config.default_compatibility_mode == SchemaCompatibilityMode.BACKWARD
        assert config.timeout_seconds == 30
        assert config.username == "user"
        assert config.password == "pass"
        assert config.enforce_compatibility is True
        assert config.fail_on_incompatible is True
        assert config.enforce_contracts is True
        assert config.fail_on_missing_contract is True

    def test_config_defaults(self) -> None:
        """Test configuration defaults."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")

        assert config.timeout_seconds == 30
        assert config.default_compatibility_mode == SchemaCompatibilityMode.BACKWARD
        assert config.username is None
        assert config.password is None
        assert config.enforce_compatibility is True
        assert config.enforce_contracts is True


class TestCompatibilityChecker:
    def test_checker_init(self) -> None:
        """Test compatibility checker initialization."""
        checker = CompatibilityChecker()

        assert len(checker.supported_types) > 0
        assert "string" in checker.supported_types
        assert "record" in checker.supported_types

    def test_backward_compatible_record(self) -> None:
        """Test backward compatible record schema changes."""
        checker = CompatibilityChecker()

        # Old schema - simple record
        old_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"}
            ]
        }

        # New schema - added optional field
        new_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "description", "type": ["null", "string"], "default": None}
            ]
        }

        result = checker.check_compatibility(old_schema, new_schema, SchemaCompatibilityMode.BACKWARD)

        assert result.is_compatible is True
        assert result.is_backward_compatible() is True
        assert len(result.breaking_changes) == 0

    def test_incompatible_record_changes(self) -> None:
        """Test incompatible record schema changes."""
        checker = CompatibilityChecker()

        # Old schema
        old_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "count", "type": "int"}
            ]
        }

        # New schema - changed field type (breaking change)
        new_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "count", "type": "string"}  # Changed from int to string
            ]
        }

        result = checker.check_compatibility(old_schema, new_schema, SchemaCompatibilityMode.BACKWARD)

        assert result.is_compatible is False
        assert len(result.breaking_changes) > 0
        assert "FIELD_TYPE_CHANGED" in [issue.value for issue in result.issues]

    def test_enum_compatibility(self) -> None:
        """Test enum schema compatibility."""
        checker = CompatibilityChecker()

        # Old enum
        old_schema = {
            "type": "enum",
            "name": "Status",
            "symbols": ["ACTIVE", "INACTIVE", "PENDING"]
        }

        # New enum - added symbol (compatible)
        new_schema = {
            "type": "enum",
            "name": "Status",
            "symbols": ["ACTIVE", "INACTIVE", "PENDING", "COMPLETED"]
        }

        result = checker.check_compatibility(old_schema, new_schema, SchemaCompatibilityMode.BACKWARD)

        assert result.is_compatible is True

    def test_enum_incompatibility(self) -> None:
        """Test enum schema incompatibility."""
        checker = CompatibilityChecker()

        # Old enum
        old_schema = {
            "type": "enum",
            "name": "Status",
            "symbols": ["ACTIVE", "INACTIVE", "PENDING"]
        }

        # New enum - removed symbol (incompatible)
        new_schema = {
            "type": "enum",
            "name": "Status",
            "symbols": ["ACTIVE", "INACTIVE"]  # Removed PENDING
        }

        result = checker.check_compatibility(old_schema, new_schema, SchemaCompatibilityMode.BACKWARD)

        assert result.is_compatible is False
        assert len(result.breaking_changes) > 0
        assert "ENUM_VALUE_REMOVED" in [issue.value for issue in result.issues]

    def test_union_compatibility(self) -> None:
        """Test union type compatibility."""
        checker = CompatibilityChecker()

        # Old union
        old_schema = {"type": ["null", "string"]}

        # New union - added type (compatible)
        new_schema = {"type": ["null", "string", "int"]}

        result = checker.check_compatibility(old_schema, new_schema, SchemaCompatibilityMode.BACKWARD)

        assert result.is_compatible is True

    def test_forward_compatibility(self) -> None:
        """Test forward compatibility checking."""
        checker = CompatibilityChecker()

        # Old schema - optional field
        old_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": ["null", "string"], "default": None}
            ]
        }

        # New schema - made field required (breaks forward compatibility)
        new_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"}  # Now required
            ]
        }

        result = checker.check_compatibility(old_schema, new_schema, SchemaCompatibilityMode.FORWARD)

        assert result.is_compatible is False
        assert result.is_forward_compatible() is True  # Mode allows forward compatibility check

    def test_type_compatibility(self) -> None:
        """Test basic type compatibility."""
        checker = CompatibilityChecker()

        # Compatible types
        assert checker._types_compatible("string", "string") is True
        assert checker._types_compatible("int", "long") is True  # Numeric promotion
        assert checker._types_compatible("long", "float") is True
        assert checker._types_compatible("float", "double") is True

        # Incompatible types
        assert checker._types_compatible("string", "int") is False
        assert checker._types_compatible("int", "string") is False

        # Union types
        assert checker._types_compatible(["null", "string"], ["null", "string", "int"]) is True
        assert checker._types_compatible(["null", "string"], ["null", "int"]) is False


class TestSchemaRegistryManager:
    def test_manager_init(self) -> None:
        """Test Schema Registry manager initialization."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        assert manager.config == config
        assert manager.session is not None
        assert len(manager._schema_cache) == 0

    @patch('requests.Session')
    def test_register_subject_success(self, mock_session) -> None:
        """Test successful subject registration."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock successful registration response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": 1, "version": 1}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_response
        mock_session_instance.get.return_value = mock_response
        mock_session_instance.put.return_value = mock_response
        manager.session = mock_session_instance

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [{"name": "id", "type": "string"}]
        }

        result = manager.register_subject("test.subject", schema)

        assert result.subject == "test.subject"
        assert result.version == 1
        assert result.schema_id == 1
        assert result.compatibility_mode == SchemaCompatibilityMode.BACKWARD

    @patch('requests.Session')
    def test_register_subject_failure(self, mock_session) -> None:
        """Test subject registration failure."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock failed registration response
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = Exception("Registration failed")

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_response
        manager.session = mock_session_instance

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [{"name": "id", "type": "string"}]
        }

        with pytest.raises(SubjectRegistrationError):
            manager.register_subject("test.subject", schema)

    @patch('requests.Session')
    def test_get_latest_schema_success(self, mock_session) -> None:
        """Test successful schema retrieval."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "subject": "test.subject",
            "version": 1,
            "id": 1,
            "schema": {"type": "record", "name": "TestRecord", "fields": []},
            "compatibility": "BACKWARD"
        }

        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        manager.session = mock_session_instance

        result = manager.get_latest_schema("test.subject")

        assert result is not None
        assert result.subject == "test.subject"
        assert result.version == 1
        assert result.schema_id == 1

    @patch('requests.Session')
    def test_get_latest_schema_not_found(self, mock_session) -> None:
        """Test schema retrieval when not found."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock 404 response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("Not found")

        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        manager.session = mock_session_instance

        result = manager.get_latest_schema("nonexistent.subject")

        assert result is None

    @patch('requests.Session')
    def test_check_compatibility_success(self, mock_session) -> None:
        """Test successful compatibility check."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock successful compatibility check
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"is_compatible": True}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_response
        manager.session = mock_session_instance

        schema = {"type": "record", "name": "TestRecord", "fields": []}
        result = manager.check_compatibility("test.subject", schema)

        assert result.is_compatible is True
        assert len(result.messages) == 0

    @patch('requests.Session')
    def test_check_compatibility_incompatible(self, mock_session) -> None:
        """Test incompatible schema check."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock incompatible response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "is_compatible": False,
            "messages": ["Field 'name' is required in the new schema but not in the old"]
        }

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_response
        manager.session = mock_session_instance

        schema = {"type": "record", "name": "TestRecord", "fields": []}
        result = manager.check_compatibility("test.subject", schema)

        assert result.is_compatible is False
        assert len(result.messages) > 0

    def test_validate_schema_valid(self) -> None:
        """Test valid schema validation."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": ["null", "string"], "default": None}
            ]
        }

        # Should not raise exception
        manager._validate_schema(schema)

    def test_validate_schema_invalid(self) -> None:
        """Test invalid schema validation."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Invalid schema - missing required fields
        invalid_schema = {
            "type": "record",
            "name": "TestRecord"
            # Missing fields
        }

        with pytest.raises(SubjectRegistrationError):
            manager._validate_schema(invalid_schema)

    def test_get_registry_status(self) -> None:
        """Test registry status retrieval."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock successful status response
        with patch.object(manager.session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "total_subjects": 10,
                "total_schemas": 25
            }
            mock_get.return_value = mock_response

            status = manager.get_registry_status()

            assert status["total_subjects"] == 10
            assert status["total_schemas"] == 25
            assert "base_url" in status

    def test_get_registry_status_error(self) -> None:
        """Test registry status error handling."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock error response
        with patch.object(manager.session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.raise_for_status.side_effect = Exception("Server error")
            mock_get.return_value = mock_response

            status = manager.get_registry_status()

            assert "error" in status
            assert status["base_url"] == "http://localhost:8081"


class TestSchemaRegistryIntegration:
    def test_full_registration_workflow(self) -> None:
        """Test complete schema registration workflow."""
        config = SchemaRegistryConfig(
            base_url="http://localhost:8081",
            enforce_compatibility=True,
            fail_on_incompatible=True
        )
        manager = SchemaRegistryManager(config)

        # Mock all HTTP responses
        with patch.object(manager.session, 'post') as mock_post, \
             patch.object(manager.session, 'get') as mock_get, \
             patch.object(manager.session, 'put') as mock_put:

            # Mock schema registration
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"id": 1, "version": 1}

            # Mock compatibility check
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {"is_compatible": True}

            # Mock compatibility mode setting
            mock_put.return_value.status_code = 200

            schema = {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": ["null", "string"], "default": None}
                ]
            }

            # Register schema
            result = manager.register_subject("test.subject", schema)

            assert result.subject == "test.subject"
            assert result.version == 1
            assert result.schema_id == 1
            assert result.compatibility_mode == SchemaCompatibilityMode.BACKWARD

    def test_incompatible_schema_handling(self) -> None:
        """Test handling of incompatible schemas."""
        config = SchemaRegistryConfig(
            base_url="http://localhost:8081",
            enforce_compatibility=True,
            fail_on_incompatible=True
        )
        manager = SchemaRegistryManager(config)

        # Mock compatibility check failure
        with patch.object(manager.session, 'post') as mock_post, \
             patch.object(manager.session, 'get') as mock_get:

            # Mock incompatible compatibility check
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {
                "is_compatible": False,
                "messages": ["Breaking change detected"]
            }

            schema = {
                "type": "record",
                "name": "TestRecord",
                "fields": [{"name": "id", "type": "string"}]
            }

            # Should raise exception for incompatible schema
            with pytest.raises(SchemaCompatibilityError):
                manager.register_subject("test.subject", schema)

    def test_schema_caching(self) -> None:
        """Test schema caching behavior."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Mock schema retrieval
        with patch.object(manager.session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "subject": "test.subject",
                "version": 1,
                "id": 1,
                "schema": {"type": "record", "name": "TestRecord", "fields": []},
                "compatibility": "BACKWARD"
            }
            mock_get.return_value = mock_response

            # First call should make HTTP request
            result1 = manager.get_latest_schema("test.subject")
            assert mock_get.call_count == 1

            # Second call should use cache
            result2 = manager.get_latest_schema("test.subject")
            assert mock_get.call_count == 1  # Still 1

            # Results should be the same
            assert result1.subject == result2.subject
            assert result1.version == result2.version

    def test_error_handling(self) -> None:
        """Test error handling in various scenarios."""
        config = SchemaRegistryConfig(base_url="http://localhost:8081")
        manager = SchemaRegistryManager(config)

        # Test connection error
        with patch.object(manager.session, 'post') as mock_post:
            mock_post.side_effect = Exception("Connection failed")

            schema = {"type": "record", "name": "TestRecord", "fields": []}

            with pytest.raises(SchemaRegistryConnectionError):
                manager.register_subject("test.subject", schema)

        # Test invalid JSON response
        with patch.object(manager.session, 'post') as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
            mock_post.return_value = mock_response

            schema = {"type": "record", "name": "TestRecord", "fields": []}

            with pytest.raises(SubjectRegistrationError):
                manager.register_subject("test.subject", schema)


class TestSubjectContracts:
    """Validates the frozen subject contract catalog."""

    @pytest.fixture(scope="class")
    def contracts(self) -> SubjectContracts:
        contracts_path = REPO_ROOT / "kafka" / "schemas" / "contracts.yml"
        return SubjectContracts(contracts_path)

    def test_contract_catalog_loaded(self, contracts: SubjectContracts) -> None:
        subjects = contracts.list_subjects()
        assert subjects, "Expected at least one subject contract"

    def test_subject_lookup(self, contracts: SubjectContracts) -> None:
        contract = contracts.get("aurum.ref.fred.series.v1-value")
        assert contract.schema == "fred.series.v1.avsc"
        assert contract.topic == "aurum.ref.fred.series.v1"
        assert contract.compatibility == "BACKWARD"

    def test_subject_pattern_enforcement(self, contracts: SubjectContracts) -> None:
        contracts.validate_subject_name("aurum.ref.eia.series.v1-value")
        with pytest.raises(SubjectPatternError):
            contracts.validate_subject_name("aurum.invalidSubject")

        with pytest.raises(SubjectNotDefinedError):
            contracts.get("aurum.missing.subject.v1-value")

    def test_schema_indexing(self, contracts: SubjectContracts) -> None:
        subjects = contracts.subjects_for_schema("iso.lmp.v1.avsc")
        assert any(s.subject.endswith(".lmp.v1-value") for s in subjects)
        assert len(subjects) >= 3

    def test_schema_payload_consistency(self, contracts: SubjectContracts) -> None:
        schema = contracts.load_schema("aurum.ref.fred.series.v1-value")
        assert schema["name"] == "FredSeriesPoint"
        assert schema["type"] == "record"


class TestSchemaRegistryCI:
    def test_ci_script_creation(self) -> None:
        """Test CI script creation and configuration."""
        config = SchemaRegistryConfig(
            base_url="http://localhost:8081",
            default_compatibility_mode=SchemaCompatibilityMode.BACKWARD
        )

        from aurum.schema_registry.registry_manager import SchemaRegistryManager
        manager = SchemaRegistryManager(config)

        assert manager.config.base_url == "http://localhost:8081"
        assert manager.config.default_compatibility_mode == SchemaCompatibilityMode.BACKWARD

    def test_ci_workflow_validation(self) -> None:
        """Test CI workflow validation logic."""
        # This would test the CI workflow logic
        # For now, just verify the workflow file exists
        workflow_path = REPO_ROOT / ".github" / "workflows" / "schema-registry-ci.yml"
        assert workflow_path.exists()

        # Read and validate workflow content
        workflow_content = workflow_path.read_text()
        assert "schema-registry-ci" in workflow_content
        assert "register_schemas.py" in workflow_content
        assert "BACKWARD" in workflow_content
