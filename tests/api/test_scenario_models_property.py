"""Property-based tests for scenario models validation.

These tests use Hypothesis to generate a wide range of inputs to ensure
robust validation of scenario-related data structures.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest
from hypothesis import assume, given, settings, strategies as st

pytest.importorskip("hypothesis.extra.pydantic")
from hypothesis.extra.pydantic import pydantic_conforms_to

from aurum.api.scenario_models import (
    CreateScenarioRequest,
    ScenarioData,
    ScenarioRunOptions,
    ScenarioRunData,
    ScenarioOutputPoint,
    ScenarioOutputFilter,
    BulkScenarioRunRequest,
    BulkScenarioRunItem,
    ScenarioStatus,
    ScenarioRunStatus,
    ScenarioRunPriority,
)


class TestScenarioModelsProperty:
    """Property-based tests for scenario model validation."""

    @given(st.text(min_size=1, max_size=100))
    @settings(max_examples=100)
    def test_scenario_name_validation(self, name):
        """Test scenario name validation with various inputs."""
        # Filter out problematic characters that might cause issues
        assume('\x00' not in name)  # Null bytes
        assume(name.strip())  # Non-empty after stripping

        request = CreateScenarioRequest(
            tenant_id="test-tenant",
            name=name,
            description="Test scenario",
            assumptions=[],
            parameters={},
            tags=[]
        )

        assert request.name == name
        assert len(request.name) <= 255  # Max length check

    @given(st.dictionaries(
        keys=st.text(min_size=1, max_size=50),
        values=st.one_of(
            st.text(),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans(),
            st.lists(st.text()),
            st.dictionaries(st.text(min_size=1), st.text())
        ),
        min_size=0,
        max_size=20
    ))
    @settings(max_examples=50)
    def test_scenario_parameters_validation(self, parameters):
        """Test scenario parameters validation with complex nested structures."""
        request = CreateScenarioRequest(
            tenant_id="test-tenant",
            name="test-scenario",
            description="Test scenario",
            assumptions=[],
            parameters=parameters,
            tags=[]
        )

        assert request.parameters == parameters

    @given(st.lists(
        st.dictionaries(
            keys=st.text(min_size=1, max_size=30),
            values=st.one_of(
                st.text(),
                st.integers(),
                st.floats(allow_nan=False, allow_infinity=False),
                st.booleans(),
                st.lists(st.text()),
                st.dictionaries(st.text(min_size=1), st.text())
            ),
            min_size=1,
            max_size=10
        ),
        min_size=0,
        max_size=10
    ))
    @settings(max_examples=30)
    def test_scenario_assumptions_validation(self, assumptions):
        """Test scenario assumptions validation with complex structures."""
        request = CreateScenarioRequest(
            tenant_id="test-tenant",
            name="test-scenario",
            description="Test scenario",
            assumptions=assumptions,
            parameters={},
            tags=[]
        )

        assert len(request.assumptions) == len(assumptions)

    @given(st.lists(
        st.text(min_size=1, max_size=30),
        min_size=0,
        max_size=20
    ))
    @settings(max_examples=50)
    def test_scenario_tags_validation(self, tags):
        """Test scenario tags validation."""
        request = CreateScenarioRequest(
            tenant_id="test-tenant",
            name="test-scenario",
            description="Test scenario",
            assumptions=[],
            parameters={},
            tags=tags
        )

        assert request.tags == tags

    @given(st.text(min_size=1, max_size=100))
    @settings(max_examples=50)
    def test_scenario_description_validation(self, description):
        """Test scenario description validation."""
        request = CreateScenarioRequest(
            tenant_id="test-tenant",
            name="test-scenario",
            description=description,
            assumptions=[],
            parameters={},
            tags=[]
        )

        assert request.description == description

    @given(st.dictionaries(
        keys=st.text(min_size=1, max_size=50),
        values=st.text(),
        min_size=1,
        max_size=10
    ))
    @settings(max_examples=30)
    def test_scenario_run_options_validation(self, options_dict):
        """Test scenario run options validation."""
        # Convert dict to expected format
        options = ScenarioRunOptions(
            code_version=options_dict.get("code_version"),
            seed=options_dict.get("seed"),
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=60,
            max_memory_mb=1024,
            environment={},
            parameters={},
            max_retries=3,
            idempotency_key=options_dict.get("idempotency_key")
        )

        assert isinstance(options.priority, ScenarioRunPriority)
        assert 1 <= options.timeout_minutes <= 1440  # 1 minute to 24 hours
        assert 256 <= options.max_memory_mb <= 16384  # 256MB to 16GB

    @given(st.dictionaries(
        keys=st.text(min_size=1, max_size=50),
        values=st.text(),
        min_size=1,
        max_size=10
    ))
    @settings(max_examples=30)
    def test_scenario_run_environment_validation(self, environment):
        """Test scenario run environment variable validation."""
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=60,
            max_memory_mb=1024,
            environment=environment,
            parameters={},
            max_retries=3
        )

        assert options.environment == environment

    @given(st.floats(min_value=0.0, max_value=100.0))
    @settings(max_examples=20)
    def test_scenario_run_timeout_validation(self, timeout_minutes):
        """Test scenario run timeout validation."""
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=int(timeout_minutes),
            max_memory_mb=1024,
            environment={},
            parameters={},
            max_retries=3
        )

        assert 1 <= options.timeout_minutes <= 1440

    @given(st.integers(min_value=256, max_value=16384))
    @settings(max_examples=20)
    def test_scenario_run_memory_validation(self, memory_mb):
        """Test scenario run memory validation."""
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=60,
            max_memory_mb=memory_mb,
            environment={},
            parameters={},
            max_retries=3
        )

        assert 256 <= options.max_memory_mb <= 16384

    @given(st.integers(min_value=0, max_value=10))
    @settings(max_examples=20)
    def test_scenario_run_retries_validation(self, retries):
        """Test scenario run retries validation."""
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=60,
            max_memory_mb=1024,
            environment={},
            parameters={},
            max_retries=retries
        )

        assert 0 <= options.max_retries <= 10

    @given(st.text(min_size=1, max_size=10))
    @settings(max_examples=20)
    def test_scenario_run_priority_validation(self, priority_str):
        """Test scenario run priority validation."""
        try:
            priority = ScenarioRunPriority(priority_str.lower())
            assert priority in [ScenarioRunPriority.LOW, ScenarioRunPriority.NORMAL,
                              ScenarioRunPriority.HIGH, ScenarioRunPriority.CRITICAL]
        except ValueError:
            # Should raise ValueError for invalid priorities
            with pytest.raises(ValueError):
                ScenarioRunPriority(priority_str.lower())

    @given(st.text(min_size=1, max_size=20))
    @settings(max_examples=20)
    def test_scenario_status_validation(self, status_str):
        """Test scenario status validation."""
        try:
            status = ScenarioStatus(status_str.lower())
            assert status in [ScenarioStatus.CREATED, ScenarioStatus.ACTIVE,
                            ScenarioStatus.ARCHIVED, ScenarioStatus.DELETED]
        except ValueError:
            # Should raise ValueError for invalid statuses
            with pytest.raises(ValueError):
                ScenarioStatus(status_str.lower())

    @given(st.text(min_size=1, max_size=20))
    @settings(max_examples=20)
    def test_scenario_run_status_validation(self, status_str):
        """Test scenario run status validation."""
        try:
            status = ScenarioRunStatus(status_str.lower())
            assert status in [ScenarioRunStatus.QUEUED, ScenarioRunStatus.RUNNING,
                            ScenarioRunStatus.SUCCEEDED, ScenarioRunStatus.FAILED,
                            ScenarioRunStatus.CANCELLED, ScenarioRunStatus.TIMEOUT]
        except ValueError:
            # Should raise ValueError for invalid statuses
            with pytest.raises(ValueError):
                ScenarioRunStatus(status_str.lower())

    @given(st.dictionaries(
        keys=st.text(min_size=1, max_size=50),
        values=st.floats(allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=10
    ))
    @settings(max_examples=20)
    def test_scenario_output_point_validation(self, data):
        """Test scenario output point validation."""
        # Generate timestamp
        timestamp = datetime.now(timezone.utc)

        point = ScenarioOutputPoint(
            timestamp=timestamp,
            metric_name="test-metric",
            value=1.0,
            unit="MW",
            tags=data
        )

        assert point.metric_name == "test-metric"
        assert point.unit == "MW"
        assert point.tags == data

    @given(st.dictionaries(
        keys=st.text(min_size=1, max_size=30),
        values=st.text(),
        min_size=0,
        max_size=5
    ))
    @settings(max_examples=20)
    def test_scenario_output_filter_validation(self, filter_data):
        """Test scenario output filter validation."""
        filter_obj = ScenarioOutputFilter(
            start_time=filter_data.get("start_time"),
            end_time=filter_data.get("end_time"),
            metric_name=filter_data.get("metric_name"),
            min_value=filter_data.get("min_value"),
            max_value=filter_data.get("max_value"),
            tags=filter_data.get("tags")
        )

        # Verify all fields are properly set
        for key, value in filter_data.items():
            assert getattr(filter_obj, key) == value

    @given(st.lists(
        st.dictionaries(
            keys=st.text(min_size=1, max_size=30),
            values=st.one_of(st.text(), st.integers(), st.floats()),
            min_size=1,
            max_size=5
        ),
        min_size=1,
        max_size=5
    ))
    @settings(max_examples=20)
    def test_bulk_scenario_run_request_validation(self, run_items):
        """Test bulk scenario run request validation."""
        request = BulkScenarioRunRequest(
            scenario_id="scenario-123",
            runs=run_items
        )

        assert request.scenario_id == "scenario-123"
        assert len(request.runs) == len(run_items)

    @given(st.dictionaries(
        keys=st.text(min_size=1, max_size=50),
        values=st.one_of(st.text(), st.integers(), st.floats()),
        min_size=1,
        max_size=10
    ))
    @settings(max_examples=20)
    def test_bulk_scenario_run_item_validation(self, item_data):
        """Test bulk scenario run item validation."""
        item = BulkScenarioRunItem(
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=60,
            max_memory_mb=1024,
            environment={},
            parameters=item_data
        )

        assert item.parameters == item_data
        assert item.priority == ScenarioRunPriority.NORMAL

    def test_idempotency_key_validation(self):
        """Test idempotency key validation rules."""
        # Valid idempotency keys
        valid_keys = [
            "run-123",
            "forecast-q4-2025",
            "user_123:scenario_456",
            "test/scenario/1.0"
        ]

        for key in valid_keys:
            options = ScenarioRunOptions(
                code_version="v1.0",
                seed=42,
                priority=ScenarioRunPriority.NORMAL,
                timeout_minutes=60,
                max_memory_mb=1024,
                environment={},
                parameters={},
                max_retries=3,
                idempotency_key=key
            )
            assert options.idempotency_key == key

        # Invalid idempotency keys (too long)
        with pytest.raises(ValueError):
            ScenarioRunOptions(
                code_version="v1.0",
                seed=42,
                priority=ScenarioRunPriority.NORMAL,
                timeout_minutes=60,
                max_memory_mb=1024,
                environment={},
                parameters={},
                max_retries=3,
                idempotency_key="a" * 256  # Too long
            )

        # Invalid idempotency keys (empty)
        with pytest.raises(ValueError):
            ScenarioRunOptions(
                code_version="v1.0",
                seed=42,
                priority=ScenarioRunPriority.NORMAL,
                timeout_minutes=60,
                max_memory_mb=1024,
                environment={},
                parameters={},
                max_retries=3,
                idempotency_key=""
            )

    def test_timeout_validation_edge_cases(self):
        """Test timeout validation edge cases."""
        # Minimum timeout
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=1,
            max_memory_mb=1024,
            environment={},
            parameters={},
            max_retries=3
        )
        assert options.timeout_minutes == 1

        # Maximum timeout
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=1440,  # 24 hours
            max_memory_mb=1024,
            environment={},
            parameters={},
            max_retries=3
        )
        assert options.timeout_minutes == 1440

    def test_memory_validation_edge_cases(self):
        """Test memory validation edge cases."""
        # Minimum memory
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=60,
            max_memory_mb=256,
            environment={},
            parameters={},
            max_retries=3
        )
        assert options.max_memory_mb == 256

        # Maximum memory
        options = ScenarioRunOptions(
            code_version="v1.0",
            seed=42,
            priority=ScenarioRunPriority.NORMAL,
            timeout_minutes=60,
            max_memory_mb=16384,  # 16GB
            environment={},
            parameters={},
            max_retries=3
        )
        assert options.max_memory_mb == 16384
