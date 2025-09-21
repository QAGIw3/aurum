"""Property-based tests for cursor semantics and validation."""
from __future__ import annotations

import json
import time
from datetime import date
from typing import Any, Dict

import pytest
from hypothesis import given, strategies as st

from aurum.core.pagination import Cursor


def test_cursor_signature_integrity():
    """Test that cursor signatures prevent tampering."""
    # Create a valid cursor
    cursor = Cursor(
        offset=0,
        limit=100,
        timestamp=time.time(),
        filters={"asof": "2024-01-01"}
    )

    cursor_str = cursor.to_string()

    # Try to modify the cursor string
    modified_str = cursor_str.replace("100", "999")

    # Should fail to parse due to signature mismatch
    try:
        Cursor.from_string(modified_str)
        assert False, "Expected signature validation to fail"
    except ValueError as e:
        assert "signature mismatch" in str(e).lower()


def test_cursor_expiration():
    """Test cursor expiration functionality."""
    # Create cursor with old timestamp
    old_timestamp = time.time() - 7200  # 2 hours ago
    cursor = Cursor(
        offset=0,
        limit=100,
        timestamp=old_timestamp,
        filters={"asof": "2024-01-01"}
    )

    # Should be expired after 1 hour
    assert cursor.is_expired(max_age_seconds=3600)

    # Should not be expired after 3 hours
    assert not cursor.is_expired(max_age_seconds=7200)


def test_cursor_filter_consistency():
    """Test that cursor filters must match query filters."""
    cursor = Cursor(
        offset=0,
        limit=100,
        timestamp=time.time(),
        filters={"asof": "2024-01-01", "iso": "MISO"}
    )

    # Same filters should match
    assert cursor.matches_filters({"asof": "2024-01-01", "iso": "MISO"})

    # Different filters should not match
    assert not cursor.matches_filters({"asof": "2024-01-02", "iso": "MISO"})

    # None values normalized for comparison
    assert cursor.matches_filters({"asof": "2024-01-01", "iso": "MISO", "location": None})


def test_cursor_validation():
    """Test cursor parameter validation."""
    # Valid cursor
    valid_cursor = Cursor(
        offset=0,
        limit=100,
        timestamp=time.time(),
        filters={}
    )
    assert valid_cursor.offset >= 0
    assert valid_cursor.limit > 0
    assert valid_cursor.timestamp > 0

    # Invalid offset
    try:
        Cursor(offset=-1, limit=100, timestamp=time.time(), filters={})
        assert False, "Should reject negative offset"
    except ValueError:
        pass

    # Invalid limit
    try:
        Cursor(offset=0, limit=0, timestamp=time.time(), filters={})
        assert False, "Should reject zero limit"
    except ValueError:
        pass

    # Invalid timestamp
    try:
        Cursor(offset=0, limit=100, timestamp=-1, filters={})
        assert False, "Should reject negative timestamp"
    except ValueError:
        pass


def test_cursor_edge_cases():
    """Test cursor edge cases and boundary conditions."""
    # Test with empty filters
    empty_filter_cursor = Cursor(
        offset=0,
        limit=1,
        timestamp=time.time(),
        filters={}
    )
    assert empty_filter_cursor.to_string()
    assert Cursor.from_string(empty_filter_cursor.to_string()).filters == {}

    # Test with complex filters
    complex_filters = {
        "asof": "2024-01-01",
        "iso": "MISO",
        "location": None,
        "market": "DA",
        "product": "NG"
    }
    complex_cursor = Cursor(
        offset=999,
        limit=500,
        timestamp=time.time(),
        filters=complex_filters
    )
    reconstructed = Cursor.from_string(complex_cursor.to_string())
    assert reconstructed.offset == 999
    assert reconstructed.limit == 500
    assert reconstructed.filters == complex_filters


def test_cursor_roundtrip_consistency():
    """Test that cursor serialization/deserialization is consistent."""
    base_time = time.time()
    test_cases = [
        {"offset": 0, "limit": 1, "filters": {}},
        {"offset": 100, "limit": 50, "filters": {"asof": "2024-01-01"}},
        {"offset": 1000, "limit": 500, "filters": {"asof": None, "iso": "MISO"}},
        {"offset": 99999, "limit": 1000, "filters": {"complex": {"nested": "value"}}},
    ]

    for case in test_cases:
        cursor = Cursor(
            offset=case["offset"],
            limit=case["limit"],
            timestamp=base_time,
            filters=case["filters"]
        )
        cursor_str = cursor.to_string()
        reconstructed = Cursor.from_string(cursor_str)

        assert reconstructed.offset == case["offset"]
        assert reconstructed.limit == case["limit"]
        assert reconstructed.filters == case["filters"]


@st.composite
def cursor_strategy(draw):
    """Hypothesis strategy for generating valid cursors."""
    offset = draw(st.integers(min_value=0, max_value=100000))
    limit = draw(st.integers(min_value=1, max_value=1000))
    timestamp = draw(st.floats(min_value=1000000000, max_value=time.time()))  # After 2001

    # Generate filter dictionaries
    filters = draw(st.dictionaries(
        keys=st.text(min_size=1, max_size=20),
        values=st.one_of(st.text(), st.none()),
        min_size=0,
        max_size=10
    ))

    return Cursor(
        offset=offset,
        limit=limit,
        timestamp=timestamp,
        filters=filters
    )


@given(cursor_strategy())
def test_cursor_property_based_roundtrip(cursor):
    """Property-based test: cursor roundtrip should be consistent."""
    cursor_str = cursor.to_string()
    reconstructed = Cursor.from_string(cursor_str)

    assert reconstructed.offset == cursor.offset
    assert reconstructed.limit == cursor.limit
    assert reconstructed.filters == cursor.filters
    assert abs(reconstructed.timestamp - cursor.timestamp) < 0.001  # Allow tiny float differences


@given(cursor_strategy())
def test_cursor_property_based_signature(cursor):
    """Property-based test: cursor signature should prevent tampering."""
    cursor_str = cursor.to_string()

    # Tamper with the string
    tampered_str = cursor_str.replace(str(cursor.offset), str(cursor.offset + 1))

    # Should fail to parse due to signature mismatch
    try:
        Cursor.from_string(tampered_str)
        assert False, "Tampered cursor should fail signature validation"
    except ValueError as e:
        assert "signature mismatch" in str(e).lower()


@st.composite
def filter_strategy(draw):
    """Strategy for generating filter dictionaries."""
    return draw(st.dictionaries(
        keys=st.text(min_size=1, max_size=20),
        values=st.one_of(st.text(), st.none()),
        min_size=0,
        max_size=10
    ))


@given(filter_strategy())
def test_cursor_filter_consistency_property(filters):
    """Property-based test: filter matching should be symmetric."""
    cursor = Cursor(
        offset=0,
        limit=100,
        timestamp=time.time(),
        filters=filters
    )

    assert cursor.matches_filters(filters)

    # Test with None normalization
    normalized_filters = {k: (v if v is not None else "") for k, v in filters.items()}
    assert cursor.matches_filters(normalized_filters)


def test_cursor_backward_compatibility():
    """Test that old cursor format still works."""
    # Old cursor format (without signature)
    old_data = {
        "offset": 100,
        "limit": 50,
        "timestamp": time.time(),
        "filters": {"asof": "2024-01-01"}
    }
    json_str = json.dumps(old_data, default=str)
    import base64
    old_cursor_str = base64.b64encode(json_str.encode()).decode()

    # Should still parse (for backward compatibility)
    cursor = Cursor.from_string(old_cursor_str)
    assert cursor.offset == 100
    assert cursor.limit == 50


def test_cursor_malformed_input():
    """Test cursor parsing with various malformed inputs."""
    test_cases = [
        "",  # Empty string
        "not-base64",  # Not base64
        "dGVzdA==",  # Base64 but not JSON
        "eyJpbnZhbGlkIjoianNvbiJ9",  # Invalid JSON
        "eyJ0ZXN0IjoidmFsdWUifQ==",  # Valid JSON but wrong structure
    ]

    for malformed in test_cases:
        try:
            Cursor.from_string(malformed)
            assert False, f"Should reject malformed cursor: {malformed}"
        except ValueError:
            pass  # Expected


def test_cursor_large_payloads():
    """Test cursor handling with large filter payloads."""
    large_filters = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
    cursor = Cursor(
        offset=0,
        limit=1,
        timestamp=time.time(),
        filters=large_filters
    )

    cursor_str = cursor.to_string()
    reconstructed = Cursor.from_string(cursor_str)

    assert reconstructed.filters == large_filters


def test_cursor_unicode_handling():
    """Test cursor handling with unicode characters."""
    unicode_filters = {
        "asof": "2024-01-01",
        "location": "München",
        "市场": "市场数据",
        "📊": "📈📉"
    }

    cursor = Cursor(
        offset=0,
        limit=100,
        timestamp=time.time(),
        filters=unicode_filters
    )

    cursor_str = cursor.to_string()
    reconstructed = Cursor.from_string(cursor_str)

    assert reconstructed.filters == unicode_filters
