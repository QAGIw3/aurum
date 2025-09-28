from __future__ import annotations

from pathlib import Path

import pytest

from aurum.seatunnel.renderer import RendererError, render_template

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_DIR = REPO_ROOT / "seatunnel" / "jobs" / "templates"


def _set_common_timescale_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURUM_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("AURUM_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    monkeypatch.setenv(
        "AURUM_TIMESCALE_JDBC_URL", "jdbc:postgresql://timescale:5432/timeseries"
    )
    monkeypatch.setenv("AURUM_TIMESCALE_USER", "ts_user")
    monkeypatch.setenv("AURUM_TIMESCALE_PASSWORD", "ts_pass")


def _set_common_iceberg_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURUM_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("AURUM_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    monkeypatch.setenv("ICEBERG_CATALOG_NAME", "nessie")
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "nessie")
    monkeypatch.setenv("ICEBERG_URI", "http://nessie:19120/api/v1")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://aurum-curated/iceberg")
    monkeypatch.setenv("EXTERNAL_TENANT_ID", "aurum")
    monkeypatch.setenv("EXTERNAL_CHECKPOINT_INTERVAL_MS", "30000")
    monkeypatch.setenv("EXTERNAL_CHECKPOINT_TIMEOUT_MS", "60000")
    monkeypatch.setenv("EXTERNAL_INGEST_JOB_ID", "seatunnel.external.test")
    monkeypatch.setenv("EXTERNAL_INGEST_RUN_ID", "unit-test-run")
    monkeypatch.setenv("EXTERNAL_INGEST_BATCH_ID", "batch-1")


def test_render_noaa_weather_kafka_to_timescale(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_common_timescale_env(monkeypatch)
    monkeypatch.setenv("NOAA_TOPIC_PATTERN", "aurum.ref.noaa.weather.v1")
    monkeypatch.setenv("NOAA_TABLE", "noaa_weather_timeseries")
    monkeypatch.setenv("NOAA_SAVE_MODE", "append")

    output_path = tmp_path / "noaa.conf"
    render_template(
        job="noaa_weather_kafka_to_timescale",
        template_path=TEMPLATES_DIR / "noaa_weather_kafka_to_timescale.conf.tmpl",
        output_path=output_path,
        required_vars=[
            "AURUM_KAFKA_BOOTSTRAP_SERVERS",
            "AURUM_SCHEMA_REGISTRY_URL",
            "AURUM_TIMESCALE_JDBC_URL",
            "AURUM_TIMESCALE_USER",
            "AURUM_TIMESCALE_PASSWORD",
        ],
    )

    rendered = output_path.read_text(encoding="utf-8")
    assert "noaa_weather_timeseries" in rendered
    assert "bootstrap.servers = \"kafka:9092\"" in rendered
    assert "schema.registry.url = \"http://schema-registry:8081\"" in rendered


def test_render_external_timeseries_kafka_to_iceberg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_common_iceberg_env(monkeypatch)
    monkeypatch.setenv("EXTERNAL_TIMESERIES_TOPIC_PATTERN", "aurum\\.ext\\.timeseries\\.obs\\.v1")
    monkeypatch.setenv("EXTERNAL_TIMESERIES_GROUP_ID", "seatunnel.external.timeseries")
    monkeypatch.setenv("EXTERNAL_TIMESERIES_START_MODE", "latest")
    monkeypatch.setenv("EXTERNAL_TIMESERIES_TABLE", "timeseries_observation")
    monkeypatch.setenv("EXTERNAL_ICEBERG_DB", "external")
    monkeypatch.setenv("EXTERNAL_WRITE_DISTRIBUTION", "hash")
    monkeypatch.setenv("EXTERNAL_WRITE_FORMAT", "parquet")

    output_path = tmp_path / "external_timeseries.conf"
    render_template(
        job="external_timeseries_kafka_to_iceberg",
        template_path=TEMPLATES_DIR / "external_timeseries_kafka_to_iceberg.conf.tmpl",
        output_path=output_path,
        required_vars=[
            "AURUM_KAFKA_BOOTSTRAP_SERVERS",
            "AURUM_SCHEMA_REGISTRY_URL",
            "ICEBERG_CATALOG_NAME",
            "ICEBERG_CATALOG_TYPE",
            "ICEBERG_URI",
            "ICEBERG_WAREHOUSE",
            "EXTERNAL_TENANT_ID",
        ],
    )

    rendered = output_path.read_text(encoding="utf-8")
    assert "external_obs_enriched" in rendered
    assert "primary_keys = [\"tenant_id\", \"provider\", \"series_id\", \"ts\", \"asof_date\"]" in rendered
    assert "seatunnel.external.test" in rendered


def test_render_external_series_catalog_kafka_to_iceberg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_common_iceberg_env(monkeypatch)
    monkeypatch.setenv("EXTERNAL_SERIES_TOPIC_PATTERN", "aurum\\.ext\\.series_catalog\\.upsert\\.v1")
    monkeypatch.setenv("EXTERNAL_SERIES_GROUP_ID", "seatunnel.external.series")
    monkeypatch.setenv("EXTERNAL_SERIES_START_MODE", "latest")
    monkeypatch.setenv("EXTERNAL_SERIES_TABLE", "series_catalog")
    monkeypatch.setenv("EXTERNAL_SERIES_WRITE_DISTRIBUTION", "hash")
    monkeypatch.setenv("EXTERNAL_SERIES_WRITE_FORMAT", "parquet")
    monkeypatch.setenv("EXTERNAL_ICEBERG_DB", "external")

    output_path = tmp_path / "external_series.conf"
    render_template(
        job="external_series_catalog_kafka_to_iceberg",
        template_path=TEMPLATES_DIR / "external_series_catalog_kafka_to_iceberg.conf.tmpl",
        output_path=output_path,
        required_vars=[
            "AURUM_KAFKA_BOOTSTRAP_SERVERS",
            "AURUM_SCHEMA_REGISTRY_URL",
            "ICEBERG_CATALOG_NAME",
            "ICEBERG_CATALOG_TYPE",
            "ICEBERG_URI",
            "ICEBERG_WAREHOUSE",
            "EXTERNAL_TENANT_ID",
        ],
    )

    rendered = output_path.read_text(encoding="utf-8")
    assert "external_series_enriched" in rendered
    assert "primary_keys = [\"tenant_id\", \"provider\", \"series_id\"]" in rendered
    assert "seatunnel.external.test" in rendered


def test_render_pjm_lmp_to_kafka(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PJM_API_KEY", "token-123")
    monkeypatch.setenv("PJM_TOPIC", "aurum.iso.pjm.lmp.v1")
    monkeypatch.setenv("PJM_INTERVAL_START", "2024-01-01T00:00:00-05:00")
    monkeypatch.setenv("PJM_INTERVAL_END", "2024-01-02T00:00:00-05:00")
    monkeypatch.setenv("PJM_ENDPOINT", "https://api.pjm.com/api/v1/da_hrl_lmps")
    monkeypatch.setenv("PJM_ROW_LIMIT", "10000")
    monkeypatch.setenv("PJM_MARKET", "DAY_AHEAD")
    monkeypatch.setenv("PJM_LOCATION_TYPE", "NODE")
    monkeypatch.setenv("PJM_SUBJECT", "aurum.iso.pjm.lmp.v1-value")
    monkeypatch.setenv("ISO_LMP_SCHEMA", '{"type":"record","name":"IsoLmp"}')
    monkeypatch.setenv("ISO_LOCATION_REGISTRY", str(REPO_ROOT / "config" / "iso_nodes.csv"))
    monkeypatch.setenv("ISO_TIMEZONE_REGISTRY", "UTC")

    monkeypatch.setenv("AURUM_KAFKA_BOOTSTRAP_SERVERS", "legacy:9092")
    monkeypatch.setenv("AURUM_SCHEMA_REGISTRY_URL", "http://registry:8081")

    output_path = tmp_path / "pjm.conf"
    render_template(
        job="pjm_lmp_to_kafka",
        template_path=TEMPLATES_DIR / "pjm_lmp_to_kafka.conf.tmpl",
        output_path=output_path,
        required_vars=[
            "PJM_API_KEY",
            "PJM_TOPIC",
            "PJM_INTERVAL_START",
            "PJM_INTERVAL_END",
            "AURUM_KAFKA_BOOTSTRAP_SERVERS",
            "AURUM_SCHEMA_REGISTRY_URL",
        ],
    )

    rendered = output_path.read_text(encoding="utf-8")
    assert "result_table_name = \"pjm_normalized\"" in rendered
    assert "bootstrap.servers = \"legacy:9092\"" in rendered
    assert "schema.registry.url = \"http://registry:8081\"" in rendered
    assert "format = \"avro\"" in rendered


def test_alias_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("AURUM_KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "alias:29092")
    monkeypatch.setenv("AURUM_SCHEMA_REGISTRY_URL", "http://schema:8081")
    monkeypatch.setenv("AURUM_TIMESCALE_JDBC_URL", "jdbc:postgresql://timescale:5432/timeseries")
    monkeypatch.setenv("AURUM_TIMESCALE_USER", "ts")
    monkeypatch.setenv("AURUM_TIMESCALE_PASSWORD", "ts")
    monkeypatch.setenv("NOAA_TOPIC_PATTERN", "aurum.ref.noaa.weather.v1")
    monkeypatch.setenv("NOAA_TABLE", "noaa_weather_timeseries")
    monkeypatch.setenv("NOAA_SAVE_MODE", "append")

    output_path = tmp_path / "alias.conf"
    render_template(
        job="noaa_weather_kafka_to_timescale",
        template_path=TEMPLATES_DIR / "noaa_weather_kafka_to_timescale.conf.tmpl",
        output_path=output_path,
        required_vars=[
            "AURUM_KAFKA_BOOTSTRAP_SERVERS",
            "AURUM_SCHEMA_REGISTRY_URL",
            "AURUM_TIMESCALE_JDBC_URL",
            "AURUM_TIMESCALE_USER",
            "AURUM_TIMESCALE_PASSWORD",
        ],
    )

    rendered = output_path.read_text(encoding="utf-8")
    assert "bootstrap.servers = \"alias:29092\"" in rendered
    stderr = capsys.readouterr().err
    assert "legacy environment variable" in stderr


def test_missing_required_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("NOAA_TOPIC_PATTERN", "aurum.ref.noaa.weather.v1")
    monkeypatch.setenv("NOAA_TABLE", "noaa_weather_timeseries")
    monkeypatch.setenv("NOAA_SAVE_MODE", "append")

    with pytest.raises(RendererError) as exc:
        render_template(
            job="noaa_weather_kafka_to_timescale",
            template_path=TEMPLATES_DIR / "noaa_weather_kafka_to_timescale.conf.tmpl",
            output_path=tmp_path / "noaa.conf",
            required_vars=[
                "AURUM_KAFKA_BOOTSTRAP_SERVERS",
                "AURUM_SCHEMA_REGISTRY_URL",
                "AURUM_TIMESCALE_JDBC_URL",
                "AURUM_TIMESCALE_USER",
                "AURUM_TIMESCALE_PASSWORD",
            ],
        )

    assert "Missing required environment variables" in str(exc.value)

def test_render_caiso_lmp_to_kafka(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sample_path = REPO_ROOT / "seatunnel" / "jobs" / "samples" / "caiso_lmp_input.json"
    monkeypatch.setenv("CAISO_INPUT_JSON", str(sample_path))
    monkeypatch.setenv("CAISO_TOPIC", "aurum.iso.caiso.lmp.v1")
    monkeypatch.setenv("CAISO_SUBJECT", "aurum.iso.caiso.lmp.v1-value")
    monkeypatch.setenv("CAISO_CURRENCY", "USD")
    monkeypatch.setenv("CAISO_UOM", "MWh")
    monkeypatch.setenv("ISO_LMP_SCHEMA", '{"type":"record","name":"IsoLmp"}')
    monkeypatch.setenv("ISO_LOCATION_REGISTRY", str(REPO_ROOT / "config" / "iso_nodes.csv"))
    monkeypatch.setenv("AURUM_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("AURUM_SCHEMA_REGISTRY_URL", "http://schema:8081")

    output_path = tmp_path / "caiso.conf"
    render_template(
        job="caiso_lmp_to_kafka",
        template_path=TEMPLATES_DIR / "caiso_lmp_to_kafka.conf.tmpl",
        output_path=output_path,
        required_vars=[
            "CAISO_INPUT_JSON",
            "AURUM_KAFKA_BOOTSTRAP_SERVERS",
            "AURUM_SCHEMA_REGISTRY_URL",
        ],
    )

    rendered = output_path.read_text(encoding="utf-8")
    assert str(sample_path) in rendered
    assert "schema.registry.url = \"http://schema:8081\"" in rendered
