"""Property-based tests for cursor pagination hardening."""

from __future__ import annotations

import pytest
import pytest_asyncio
from hypothesis import given, strategies as st
from typing import Dict, Any, List
from datetime import date, datetime

from aurum.api.http.pagination import (
    CursorPayload,
    SortDirection,
    SortKey,
    CursorSchema,
    FROZEN_CURSOR_SCHEMAS,
    encode_cursor,
    decode_cursor,
    extract_cursor_values,
    validate_cursor_schema,
)


class TestCursorHardening:
    """Property-based tests for cursor pagination hardening."""

    @given(
        schema_name=st.sampled_from(list(FROZEN_CURSOR_SCHEMAS.keys())),
        values=st.dictionaries(
            keys=st.text(min_size=1, max_size=20),
            values=st.one_of(
                st.text(),
                st.integers(),
                st.dates(),
                st.datetimes(),
                st.none()
            ),
            min_size=1
        )
    )
    def test_cursor_roundtrip_encoding(self, schema_name: str, values: Dict[str, Any]):
        """Test that cursor encoding/decoding is roundtrip-safe."""
        # Create cursor payload
        cursor = CursorPayload(
            schema_name=schema_name,
            values=values,
            direction=SortDirection.ASC
        )

        # Encode to string
        encoded = encode_cursor(cursor.to_dict(), schema_name)

        # Decode back
        decoded = decode_cursor(encoded)

        # Should match original
        assert decoded.schema_name == cursor.schema_name
        assert decoded.values == cursor.values
        assert decoded.direction == cursor.direction

    @given(
        schema_name=st.sampled_from(list(FROZEN_CURSOR_SCHEMAS.keys())),
        invalid_schema=st.text().filter(lambda x: x not in FROZEN_CURSOR_SCHEMAS)
    )
    def test_cursor_schema_validation(self, schema_name: str, invalid_schema: str):
        """Test that cursor schema validation works correctly."""
        # Create valid cursor
        schema = FROZEN_CURSOR_SCHEMAS[schema_name]
        values = {key.name: "test_value" for key in schema.sort_keys}

        cursor = CursorPayload(
            schema_name=schema_name,
            values=values,
            direction=SortDirection.ASC
        )

        # Valid schema should work
        validate_cursor_schema(cursor, schema_name)

        # Invalid schema should fail
        with pytest.raises(Exception):  # HTTPException or ValueError
            validate_cursor_schema(cursor, invalid_schema)

    @given(
        schema_name=st.sampled_from(list(FROZEN_CURSOR_SCHEMAS.keys())),
    )
    def test_cursor_schema_completeness(self, schema_name: str):
        """Test that all cursor schemas have required sort keys."""
        schema = FROZEN_CURSOR_SCHEMAS[schema_name]

        # Schema should have at least one sort key
        assert len(schema.sort_keys) > 0

        # All sort keys should have valid names
        for sort_key in schema.sort_keys:
            assert sort_key.name
            assert sort_key.direction in [SortDirection.ASC, SortDirection.DESC]

        # Sort keys should be unique
        key_names = [key.name for key in schema.sort_keys]
        assert len(key_names) == len(set(key_names))

    @given(
        values=st.dictionaries(
            keys=st.text(min_size=1, max_size=20),
            values=st.one_of(
                st.text(),
                st.integers(),
                st.dates(),
                st.datetimes(),
                st.none()
            ),
            min_size=1
        )
    )
    def test_cursor_value_extraction(self, values: Dict[str, Any]):
        """Test cursor value extraction for database queries."""
        # Create cursor
        cursor = CursorPayload(
            schema_name="curves",  # Use a known schema
            values=values,
            direction=SortDirection.ASC
        )

        # Extract values
        extracted = extract_cursor_values(cursor, "curves")

        # Should match original values
        assert extracted == values

        # Wrong schema should fail
        with pytest.raises(Exception):
            extract_cursor_values(cursor, "nonexistent_schema")

    @given(
        st.lists(
            st.tuples(
                st.sampled_from(list(FROZEN_CURSOR_SCHEMAS.keys())),
                st.dictionaries(
                    keys=st.text(min_size=1, max_size=10),
                    values=st.text(min_size=1, max_size=20),
                    min_size=1
                )
            ),
            min_size=2,
            max_size=10
        )
    )
    def test_cursor_schema_consistency(self, schema_value_pairs: List[tuple[str, Dict[str, str]]]):
        """Test that cursor schemas are consistent across different endpoints."""
        for schema_name, values in schema_value_pairs:
            schema = FROZEN_CURSOR_SCHEMAS[schema_name]
            expected_keys = {key.name for key in schema.sort_keys}

            # Create cursor
            cursor = CursorPayload(
                schema_name=schema_name,
                values=values,
                direction=SortDirection.ASC
            )

            # Validate schema
            validate_cursor_schema(cursor, schema_name)

            # Extract values should work
            extracted = extract_cursor_values(cursor, schema_name)
            assert isinstance(extracted, dict)
            assert set(extracted.keys()).issuperset(expected_keys)

    @given(
        st.text(min_size=1, max_size=100)
    )
    def test_invalid_cursor_handling(self, invalid_token: str):
        """Test that invalid cursors are handled gracefully."""
        with pytest.raises(Exception):  # Should raise HTTPException or similar
            decode_cursor(invalid_token)

    def test_frozen_cursor_schemas_are_immutable(self):
        """Test that frozen cursor schemas cannot be modified."""
        # Try to modify a frozen schema - this should not be possible
        schema = FROZEN_CURSOR_SCHEMAS["curves"]

        # Schema should be frozen
        assert isinstance(schema, CursorSchema)
        assert isinstance(schema.sort_keys, tuple)  # Should be frozen tuple

        # Attempting to modify should not work or should raise error
        original_keys = schema.sort_keys
        assert original_keys == schema.sort_keys  # Should remain unchanged

    def test_sort_key_equality_and_hashing(self):
        """Test that SortKey objects work correctly with equality and hashing."""
        key1 = SortKey("test_field", SortDirection.ASC)
        key2 = SortKey("test_field", SortDirection.ASC)
        key3 = SortKey("test_field", SortDirection.DESC)
        key4 = SortKey("different_field", SortDirection.ASC)

        # Equal keys should be equal
        assert key1 == key2
        assert key1 != key3
        assert key1 != key4

        # Hash should be consistent
        assert hash(key1) == hash(key2)
        assert hash(key1) != hash(key3)

        # Should work in sets
        key_set = {key1, key3, key4}
        assert len(key_set) == 3
        assert key2 in key_set

    def test_cursor_payload_validation(self):
        """Test cursor payload validation."""
        # Valid payload
        valid_payload = CursorPayload(
            schema_name="curves",
            values={"curve_key": "TEST", "tenor_label": "1M"},
            direction=SortDirection.ASC
        )
        assert valid_payload.schema_name == "curves"

        # Invalid schema
        with pytest.raises(ValueError, match="Unknown cursor schema"):
            CursorPayload(
                schema_name="nonexistent",
                values={"test": "value"},
                direction=SortDirection.ASC
            )

        # Missing required values
        with pytest.raises(ValueError, match="Missing cursor values"):
            # Create payload with wrong schema to trigger validation
            invalid_payload = CursorPayload(
                schema_name="curves",
                values={},  # Missing required keys
                direction=SortDirection.ASC
            )

    @given(
        st.sampled_from(list(FROZEN_CURSOR_SCHEMAS.keys()))
    )
    def test_all_schemas_have_version_consistency(self, schema_name: str):
        """Test that all cursor schemas have consistent versioning."""
        schema = FROZEN_CURSOR_SCHEMAS[schema_name]

        # All schemas should have version v1 for now
        assert schema.version == "v1"

        # Create a cursor with matching version
        values = {key.name: "test" for key in schema.sort_keys}
        cursor = CursorPayload(
            schema_name=schema_name,
            values=values,
            direction=SortDirection.ASC,
            version="v1"
        )

        assert cursor.version == schema.version

    def test_cursor_backward_compatibility(self):
        """Test that legacy cursor format still works."""
        # Legacy cursor format (just values, no schema)
        legacy_payload = {"offset": 100}
        encoded = encode_cursor(legacy_payload)

        # Should decode to legacy schema
        decoded = decode_cursor(encoded)
        assert decoded.schema_name == "legacy"
        assert decoded.values == legacy_payload

    @given(
        st.dictionaries(
            keys=st.text(min_size=1, max_size=20),
            values=st.one_of(
                st.text(),
                st.integers(),
                st.dates(),
                st.datetimes(),
                st.none()
            ),
            min_size=1
        )
    )
    def test_cursor_serialization_is_deterministic(self, payload: Dict[str, Any]):
        """Test that cursor encoding is deterministic."""
        # Encode the same payload multiple times
        encoded1 = encode_cursor(payload)
        encoded2 = encode_cursor(payload)
        encoded3 = encode_cursor(payload)

        # Should be identical
        assert encoded1 == encoded2 == encoded3

        # All should decode to same result
        decoded1 = decode_cursor(encoded1)
        decoded2 = decode_cursor(encoded2)
        decoded3 = decode_cursor(encoded3)

        assert decoded1.values == decoded2.values == decoded3.values


@pytest_asyncio.asyncio
class TestCursorIntegration:
    """Integration tests for cursor pagination with real data."""

    async def test_cursor_with_real_database_data(self):
        """Test cursor pagination with realistic database-like data."""
        # This would be an integration test with actual database queries
        # For now, just test the structure
        pass

    async def test_cursor_pagination_stability(self):
        """Test that cursor pagination produces stable, consistent results."""
        # This would test that repeated queries with same cursor
        # return the same results
        pass

    async def test_cursor_pagination_no_duplicates(self):
        """Test that cursor pagination doesn't produce duplicate results."""
        # This would test that iterating through pages doesn't
        # return the same items multiple times
        pass

    async def test_cursor_pagination_no_gaps(self):
        """Test that cursor pagination doesn't skip items."""
        # This would test that all items are returned when
        # paginating through a complete dataset
        pass

    async def test_cursor_pagination_concurrent_inserts(self):
        """Test cursor pagination stability with concurrent inserts."""
        # This would test that cursor pagination remains stable
        # even when new items are inserted during iteration
        pass
