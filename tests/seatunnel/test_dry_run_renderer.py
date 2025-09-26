from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from aurum.seatunnel.dry_run_renderer import (
    DryRunError,
    DryRunRenderer,
    _collect_placeholders,
    _resolve_template_name,
    render_template,
)


class TestDryRunRenderer:
    def test_collect_placeholders(self) -> None:
        """Test placeholder collection from templates."""
        template = "Hello ${NAME}! Your age is ${AGE}."
        placeholders = _collect_placeholders(template)
        assert placeholders == {"NAME", "AGE"}

    def test_get_sample_env(self) -> None:
        """Test sample environment generation."""
        renderer = DryRunRenderer()

        # Test EIA template
        env = renderer._get_sample_env("eia_series_to_kafka")
        assert "AURUM_KAFKA_BOOTSTRAP_SERVERS" in env
        assert "EIA_API_KEY" in env
        assert "EIA_SERIES_PATH" in env

        # Test FRED template
        env = renderer._get_sample_env("fred_series_to_kafka")
        assert "FRED_API_KEY" in env
        assert "FRED_SERIES_ID" in env

    def test_get_required_vars(self) -> None:
        """Test required variable extraction."""
        renderer = DryRunRenderer()

        # Test known templates
        eia_vars = renderer._get_required_vars("eia_series_to_kafka")
        assert "EIA_API_KEY" in eia_vars

        fred_vars = renderer._get_required_vars("fred_series_to_kafka")
        assert "FRED_API_KEY" in fred_vars

        # Test unknown template
        unknown_vars = renderer._get_required_vars("unknown_template")
        assert unknown_vars == []

    def test_template_hashing(self) -> None:
        """Test template hash computation."""
        renderer = DryRunRenderer()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".tmpl", delete=False) as f:
            f.write("Test template ${VAR}")
            template_path = Path(f.name)

        try:
            hash1 = renderer._get_template_hash(template_path)
            hash2 = renderer._get_template_hash(template_path)
            assert hash1 == hash2

            # Modify template
            template_path.write_text("Test template ${VAR} ${VAR2}")
            hash3 = renderer._get_template_hash(template_path)
            assert hash1 != hash3

        finally:
            template_path.unlink()

    def test_render_template_dry_run_success(self) -> None:
        """Test successful dry-run rendering."""
        renderer = DryRunRenderer()

        # Create a simple template
        template_content = """env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Http {
    url = "${TEST_URL}"
    method = "GET"
    retry = 3
  }
}

sink {
  Kafka {
    bootstrap.servers = "${AURUM_KAFKA_BOOTSTRAP_SERVERS}"
    topic = "${TEST_TOPIC}"
  }
}
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf.tmpl", delete=False) as f:
            f.write(template_content)
            template_path = Path(f.name)

        try:
            with patch.dict("os.environ", {
                "TEST_URL": "https://example.com",
                "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "TEST_TOPIC": "test.topic",
            }):
                result = renderer.render_template_dry_run(template_path)

            assert result["template"] == _resolve_template_name(template_path)
            assert result["rendered"] is True
            assert "content" in result
            assert result["content_length"] > 0
            assert result["line_count"] > 0

        finally:
            template_path.unlink()

    def test_render_template_dry_run_failure(self) -> None:
        """Test dry-run rendering failure."""
        renderer = DryRunRenderer()

        # Create a template with missing required variables
        template_content = """env {
  job.mode = "BATCH"
}

source {
  Http {
    url = "${MISSING_VAR}"
  }
}
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf.tmpl", delete=False) as f:
            f.write(template_content)
            template_path = Path(f.name)

        try:
            result = renderer.render_template_dry_run(template_path)

            assert result["template"] == _resolve_template_name(template_path)
            assert result["rendered"] is False
            assert "error" in result

        finally:
            template_path.unlink()

    def test_render_all_templates(self) -> None:
        """Test rendering all templates."""
        renderer = DryRunRenderer()

        # Create a temporary template directory
        with tempfile.TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir)

            # Create a simple template
            template_content = """env {
  job.mode = "BATCH"
}

source {
  Http {
    url = "${TEST_URL}"
  }
}

sink {
  Kafka {
    bootstrap.servers = "${AURUM_KAFKA_BOOTSTRAP_SERVERS}"
    topic = "${TEST_TOPIC}"
  }
}
"""

            template_path = template_dir / "test_template.conf.tmpl"
            template_path.write_text(template_content)

            with patch("aurum.seatunnel.dry_run_renderer.TEMPLATE_DIR", template_dir):
                scoped_renderer = DryRunRenderer(template_dirs=[template_dir])
                with patch.dict("os.environ", {
                    "TEST_URL": "https://example.com",
                    "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "TEST_TOPIC": "test.topic",
                }):
                    results = scoped_renderer.render_all_templates()

            assert len(results) == 1
            assert results[0]["template"] == "test_template"
            assert results[0]["rendered"] is True

    def test_generate_report(self) -> None:
        """Test report generation."""
        renderer = DryRunRenderer()

        # Sample results
        results = [
            {
                "template": "success_template",
                "rendered": True,
                "placeholders": ["VAR1", "VAR2"],
                "line_count": 10,
                "content_length": 200,
            },
            {
                "template": "failed_template",
                "rendered": False,
                "error": "Missing required variable",
            },
        ]

        report = renderer.generate_report(results)

        assert "# SeaTunnel Template Dry-Run Report" in report
        assert "## Summary" in report
        assert "## Failed Templates" in report
        assert "## Template Details" in report
        assert "success_template ✅" in report
        assert "failed_template ❌" in report

    def test_hash_persistence(self) -> None:
        """Test that hashes are properly saved and loaded."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            json.dump({"old_template": "old_hash"}, f)
            hash_file = Path(f.name)

        try:
            renderer = DryRunRenderer()
            renderer.template_hashes = {"test": "new_hash"}

            with patch("aurum.seatunnel.dry_run_renderer.HASH_FILE", hash_file):
                renderer._save_hashes()

            # Verify saved
            with hash_file.open(encoding="utf-8") as f:
                saved_hashes = json.load(f)

            assert saved_hashes["test"] == "new_hash"

            # Test loading
            renderer2 = DryRunRenderer()
            with patch("aurum.seatunnel.dry_run_renderer.HASH_FILE", hash_file):
                renderer2._load_hashes()

            assert renderer2.template_hashes["test"] == "new_hash"

        finally:
            hash_file.unlink()

    def test_error_handling(self) -> None:
        """Test error handling in various scenarios."""
        renderer = DryRunRenderer()

        # Test missing template directory
        with patch("aurum.seatunnel.dry_run_renderer.TEMPLATE_DIR", Path("/nonexistent")):
            scoped_renderer = DryRunRenderer(template_dirs=[Path("/nonexistent")])
            with pytest.raises(DryRunError, match="Template directory not found"):
                scoped_renderer.render_all_templates()

        # Test template not found
        nonexistent_path = Path("/tmp/nonexistent.conf.tmpl")
        with pytest.raises(DryRunError, match="Template not found"):
            renderer.render_template_dry_run(nonexistent_path)
