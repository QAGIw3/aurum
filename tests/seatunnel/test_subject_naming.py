"""Tests for SeaTunnel subject naming and key generation utilities."""

from __future__ import annotations

import pytest

from aurum.seatunnel.subject_naming import (
    SubjectNamingError,
    enforce_subject_naming_pattern,
    generate_key_fields,
    get_default_subject_naming_config,
    suggest_key_schema_name,
    validate_topic_naming,
)


class TestSubjectNaming:
    def test_enforce_subject_naming_pattern_with_override(self) -> None:
        """Test subject naming with custom override."""
        topic = "aurum.ref.eia.series.v1"
        custom_subject = "custom.eia.subject"

        result = enforce_subject_naming_pattern(topic, custom_subject)
        assert result == custom_subject

    def test_enforce_subject_naming_pattern_from_topic(self) -> None:
        """Test subject naming generated from topic."""
        topic = "aurum.ref.eia.series.v1"

        result = enforce_subject_naming_pattern(topic)
        assert result == "aurum.ref.eia.series.v1-value"

        # Test with key entity type
        result = enforce_subject_naming_pattern(topic, entity_type="key")
        assert result == "aurum.ref.eia.series.v1-key"

    def test_enforce_subject_naming_pattern_invalid_override(self) -> None:
        """Test subject naming with invalid override."""
        topic = "aurum.ref.eia.series.v1"
        invalid_subject = "invalid subject with spaces!"

        with pytest.raises(SubjectNamingError, match="Invalid subject name"):
            enforce_subject_naming_pattern(topic, invalid_subject)

    def test_validate_topic_naming(self) -> None:
        """Test topic naming validation."""
        # Valid topics
        assert validate_topic_naming("aurum.ref.eia.series.v1") is True
        assert validate_topic_naming("aurum.iso.pjm.lmp.v1") is True
        assert validate_topic_naming("aurum.ref.fred.series.v1") is True

        # Invalid topics
        assert validate_topic_naming("invalid.topic") is False
        assert validate_topic_naming("aurum.ref.eia.series") is False  # Missing version
        assert validate_topic_naming("aurum.ref.eia.series.v") is False  # Invalid version
        assert validate_topic_naming("aurum.ref.eia series.v1") is False  # Spaces

    def test_suggest_key_schema_name(self) -> None:
        """Test key schema name suggestion."""
        # Valid topic
        topic = "aurum.ref.eia.series.v1"
        result = suggest_key_schema_name(topic)
        assert result == "aurum.ref.eia.Key"  # Note: uses generic Key name

        # Invalid topic
        invalid_topic = "invalid.topic"
        result = suggest_key_schema_name(invalid_topic)
        assert result == "invalid.topic-key"

    def test_generate_key_fields(self) -> None:
        """Test key field generation for different source types."""
        # EIA series
        key_expr, key_format = generate_key_fields("eia", {})
        assert "series_id" in key_expr
        assert "period" in key_expr
        assert key_format == "string"

        # FRED series
        key_expr, key_format = generate_key_fields("fred", {})
        assert key_expr == "series_id"
        assert key_format == "string"

        # CPI series
        key_expr, key_format = generate_key_fields("cpi", {"area": "US"})
        assert "series_id" in key_expr
        assert "area" in key_expr
        assert key_format == "string"

        # NOAA weather
        key_expr, key_format = generate_key_fields("noaa", {})
        assert "station" in key_expr
        assert "datatype" in key_expr
        assert "date" in key_expr
        assert key_format == "string"

        # ISO data
        key_expr, key_format = generate_key_fields("iso", {})
        assert "iso_code" in key_expr
        assert "location_id" in key_expr
        assert "interval_start" in key_expr
        assert key_format == "string"

        # Unknown source type
        key_expr, key_format = generate_key_fields("unknown", {})
        assert key_expr == "'default_key'"
        assert key_format == "string"

    def test_get_default_subject_naming_config(self) -> None:
        """Test default subject naming configuration."""
        config = get_default_subject_naming_config()

        # Check that expected subjects are present
        expected_subjects = [
            "EIA_SUBJECT", "EIA_KEY_SUBJECT",
            "FRED_SUBJECT", "FRED_KEY_SUBJECT",
            "CPI_SUBJECT", "CPI_KEY_SUBJECT",
            "NOAA_GHCND_SUBJECT", "NOAA_GHCND_KEY_SUBJECT",
            "ISO_LMP_SUBJECT", "ISO_LMP_KEY_SUBJECT",
            "ISO_LOAD_SUBJECT", "ISO_LOAD_KEY_SUBJECT",
            "ISO_GENMIX_SUBJECT", "ISO_GENMIX_KEY_SUBJECT",
        ]

        for subject in expected_subjects:
            assert subject in config
            assert config[subject]  # Should not be empty

        # Validate naming patterns
        for subject_name, subject_value in config.items():
            assert subject_value.startswith("aurum.")
            assert "-value" in subject_value or "-key" in subject_value


class TestSubjectNamingIntegration:
    def test_eia_subject_naming(self) -> None:
        """Test EIA-specific subject naming."""
        topic = "aurum.ref.eia.series.v1"

        # Value schema
        value_subject = enforce_subject_naming_pattern(topic, entity_type="value")
        assert value_subject == "aurum.ref.eia.series.v1-value"

        # Key schema
        key_subject = enforce_subject_naming_pattern(topic, entity_type="key")
        assert key_subject == "aurum.ref.eia.series.v1-key"

    def test_iso_subject_naming(self) -> None:
        """Test ISO-specific subject naming."""
        topic = "aurum.iso.pjm.lmp.v1"

        value_subject = enforce_subject_naming_pattern(topic, entity_type="value")
        assert value_subject == "aurum.iso.pjm.lmp.v1-value"

        key_subject = enforce_subject_naming_pattern(topic, entity_type="key")
        assert key_subject == "aurum.iso.pjm.lmp.v1-key"

    def test_custom_subject_validation(self) -> None:
        """Test custom subject validation."""
        # Valid custom subjects
        valid_subjects = [
            "custom.eia.subject",
            "my-custom-subject",
            "subject.with.dots",
            "subject123",
            "subject-with-hyphens",
        ]

        for subject in valid_subjects:
            result = enforce_subject_naming_pattern("any.topic", subject)
            assert result == subject

        # Invalid custom subjects
        invalid_subjects = [
            "invalid subject with spaces",
            "invalid@subject",
            "invalid(subject)",
            "invalid[subject]",
            "invalid{subject}",
        ]

        for subject in invalid_subjects:
            with pytest.raises(SubjectNamingError):
                enforce_subject_naming_pattern("any.topic", subject)

    def test_key_generation_with_template_vars(self) -> None:
        """Test key generation with actual template variables."""
        # EIA with template variables
        eia_vars = {
            "series_id": "ELEC.PRICE.NY-DAY-AHEAD",
            "period": "2024-01-01",
            "frequency": "DAILY"
        }

        key_expr, key_format = generate_key_fields("eia", eia_vars)
        assert "series_id" in key_expr
        assert "period" in key_expr
        assert key_format == "string"

        # FRED with template variables
        fred_vars = {
            "series_id": "DGS10",
            "frequency": "MONTHLY"
        }

        key_expr, key_format = generate_key_fields("fred", fred_vars)
        assert key_expr == "series_id"
        assert key_format == "string"
