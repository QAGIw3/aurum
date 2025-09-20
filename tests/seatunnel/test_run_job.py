from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "seatunnel" / "run_job.sh"
ISO_LMP_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "iso.lmp.v1.avsc"
NOAA_GHCND_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "noaa.weather.v1.avsc"
EIA_SERIES_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "eia.series.v1.avsc"
FRED_SERIES_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "fred.series.v1.avsc"
FUEL_CURVE_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "fuel.curve.v1.avsc"


def extract_value_schema(config_path: Path) -> dict:
    contents = config_path.read_text(encoding="utf-8")
    marker = 'value.schema = """'
    start = contents.index(marker) + len(marker)
    end = contents.index('"""', start)
    schema_str = contents[start:end]
    return json.loads(schema_str)


@pytest.mark.parametrize(
    "job_name",
    [
        (
            "noaa_ghcnd_to_kafka",
            {
                "NOAA_GHCND_TOKEN": "token",
                "NOAA_GHCND_START_DATE": "2024-01-01",
                "NOAA_GHCND_END_DATE": "2024-01-02",
                "NOAA_GHCND_TOPIC": "aurum.ref.noaa.weather.v1",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "NOAA_GHCND_TOKEN",
        ),
        (
            "eia_series_to_kafka",
            {
                "EIA_API_KEY": "key",
                "EIA_SERIES_PATH": "electricity/wholesale/prices/data",
                "EIA_SERIES_ID": "EBA.ALL.D.H",
                "EIA_FREQUENCY": "HOURLY",
                "EIA_TOPIC": "aurum.ref.eia.series.v1",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "EIA_API_KEY",
        ),
        (
            "fred_series_to_kafka",
            {
                "FRED_API_KEY": "fredkey",
                "FRED_SERIES_ID": "DGS10",
                "FRED_FREQUENCY": "DAILY",
                "FRED_SEASONAL_ADJ": "NSA",
                "FRED_TOPIC": "aurum.ref.fred.series.v1",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "FRED_API_KEY",
        ),
        (
            "pjm_lmp_to_kafka",
            {
                "PJM_API_KEY": "pjmtoken",
                "PJM_TOPIC": "aurum.iso.pjm.lmp.v1",
                "PJM_INTERVAL_START": "2024-01-01T00:00:00-05:00",
                "PJM_INTERVAL_END": "2024-01-01T01:00:00-05:00",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "PJM_API_KEY",
        ),
        (
            "iso_lmp_kafka_to_timescale",
            {
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
                "TIMESCALE_JDBC_URL": "jdbc:postgresql://timescale:5432/timeseries",
                "TIMESCALE_USER": "ts",
                "TIMESCALE_PASSWORD": "ts",
            },
            "KAFKA_BOOTSTRAP_SERVERS",
        ),
        (
            "nyiso_lmp_to_kafka",
            {
                "NYISO_URL": "https://example.com/nyiso.csv",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
                "SEATUNNEL_OUTPUT_DIR": str(Path("/tmp")),
            },
            "NYISO_URL",
        ),
        (
            "eia_fuel_curve_to_kafka",
            {
                "EIA_API_KEY": "key",
                "FUEL_EIA_PATH": "natural-gas/pri/fut/wfut/data",
                "FUEL_SERIES_ID": "NG.HUB.PRICE.D",
                "FUEL_FUEL_TYPE": "NATURAL_GAS",
                "FUEL_FREQUENCY": "DAILY",
                "FUEL_TOPIC": "aurum.ref.fuel.natural_gas.v1",
                "FUEL_UNITS": "USD/MMBtu",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "FUEL_SERIES_ID",
        ),
    ],
)
def test_run_job_requires_env_vars(job_name: tuple[str, dict[str, str], str], tmp_path: Path) -> None:
    name, base_env, missing_var = job_name
    env = os.environ.copy()
    env.update(base_env)
    env["SEATUNNEL_OUTPUT_DIR"] = str(tmp_path)
    env.pop(missing_var, None)

    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), name, "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert f"Missing required env var {missing_var}" in result.stderr


def test_run_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "NOAA_GHCND_TOKEN": "token",
            "NOAA_GHCND_START_DATE": "2024-01-01",
            "NOAA_GHCND_END_DATE": "2024-01-02",
            "NOAA_GHCND_TOPIC": "aurum.ref.noaa.weather.v1",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
            "NOAA_GHCND_UNIT_CODE": "degC",
        }
    )

    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), "noaa_ghcnd_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "noaa_ghcnd_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'format = "avro"' in rendered
    assert 'value.schema.subject' in rendered
    assert '${NOAA_GHCND_DATASET}' not in rendered
    assert 'degC' in rendered
    assert 'noaa_stations' in rendered
    assert "aurum.ref.noaa.weather.v1" in result.stdout

    rendered_schema = extract_value_schema(rendered_path)
    expected_schema = json.loads(NOAA_GHCND_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert rendered_schema == expected_schema


def test_list_jobs() -> None:
    env = os.environ.copy()
    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), "--list"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    lines = [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]
    assert "noaa_ghcnd_to_kafka" in lines
    assert any(ln.endswith("_to_kafka") or ln.endswith("_kafka_to_timescale") for ln in lines)


def test_describe_job() -> None:
    env = os.environ.copy()
    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), "--describe", "noaa_ghcnd_to_kafka"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    out = result.stdout
    assert "Required vars:" in out
    assert "NOAA_GHCND_TOKEN" in out
    assert "NOAA_GHCND_START_DATE" in out
    assert "KAFKA_BOOTSTRAP_SERVERS" in out


def test_eia_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "EIA_API_KEY": "key",
            "EIA_SERIES_PATH": "electricity/wholesale/prices/data",
            "EIA_SERIES_ID": "EBA.ALL.D.H",
            "EIA_FREQUENCY": "HOURLY",
            "EIA_TOPIC": "aurum.ref.eia.series.v1",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
            "EIA_UNITS": "USD/MWh",
            "EIA_SEASONAL_ADJUSTMENT": "UNKNOWN",
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "eia_series_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "eia_series_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'aurum.ref.eia' in rendered
    assert '${EIA_SERIES_ID}' not in rendered
    assert 'USD/MWh' in rendered
    assert "COALESCE(source, 'EIA')" in rendered
    assert 'WHERE (TRUE)' in rendered

    rendered_schema = extract_value_schema(rendered_path)
    expected_schema = json.loads(EIA_SERIES_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert rendered_schema == expected_schema


def test_eia_job_supports_series_expr(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "EIA_API_KEY": "key",
            "EIA_SERIES_PATH": "natural-gas/stor/wkly/data",
            "EIA_SERIES_ID": "PLACEHOLDER",
            "EIA_SERIES_ID_EXPR": "series",
            "EIA_FREQUENCY": "WEEKLY",
            "EIA_TOPIC": "aurum.ref.eia.ng.storage.v1",
            "EIA_FILTER_EXPR": "series LIKE 'NW2%'",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "eia_series_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "eia_series_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert "series                                      AS series_id" in rendered
    assert "WHERE (series LIKE 'NW2%')" in rendered


def test_eia_job_renders_param_overrides(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "EIA_API_KEY": "key",
            "EIA_SERIES_PATH": "natural-gas/stor/wkly/data",
            "EIA_SERIES_ID": "PLACEHOLDER",
            "EIA_TOPIC": "aurum.ref.eia.ng.storage.v1",
            "EIA_PARAM_OVERRIDES_JSON": '[{"facets[duoarea][]":"R31"},{"data[0]":"value"}]',
            "EIA_FREQUENCY": "WEEKLY",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "eia_series_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "eia_series_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'facets[duoarea][] = "R31"' in rendered
    assert 'data[0] = "value"' in rendered


def test_fuel_curve_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "EIA_API_KEY": "key",
            "FUEL_EIA_PATH": "natural-gas/pri/fut/wfut/data",
            "FUEL_SERIES_ID": "NG.HUB.PRICE.D",
            "FUEL_FUEL_TYPE": "NATURAL_GAS",
            "FUEL_FREQUENCY": "DAILY",
            "FUEL_TOPIC": "aurum.ref.fuel.natural_gas.v1",
            "FUEL_UNITS": "USD/MMBtu",
            "FUEL_BENCHMARK_EXPR": "'Henry Hub'",
            "FUEL_REGION_EXPR": "'US_Gulf'",
            "FUEL_CURRENCY": "USD",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "eia_fuel_curve_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "eia_fuel_curve_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert "aurum.ref.fuel.natural_gas.v1" in rendered
    assert "Henry Hub" in rendered
    assert "NATURAL_GAS" in rendered
    assert "AND (TRUE)" in rendered

    rendered_schema = extract_value_schema(rendered_path)
    expected_schema = json.loads(FUEL_CURVE_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert rendered_schema == expected_schema


def test_fred_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "FRED_API_KEY": "fredkey",
            "FRED_SERIES_ID": "DGS10",
            "FRED_FREQUENCY": "DAILY",
            "FRED_SEASONAL_ADJ": "NSA",
            "FRED_TOPIC": "aurum.ref.fred.series.v1",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "FRED_UNITS": "Percent",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "fred_series_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "fred_series_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'aurum.ref.fred' in rendered
    assert '${FRED_SERIES_ID}' not in rendered
    assert 'Percent' in rendered

    rendered_schema = extract_value_schema(rendered_path)
    expected_schema = json.loads(FRED_SERIES_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert rendered_schema == expected_schema


def test_pjm_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "PJM_API_KEY": "pjmtoken",
            "PJM_TOPIC": "aurum.iso.pjm.lmp.v1",
            "PJM_INTERVAL_START": "2024-01-01T00:00:00-05:00",
            "PJM_INTERVAL_END": "2024-01-01T01:00:00-05:00",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "pjm_lmp_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "pjm_lmp_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'aurum.iso' in rendered
    assert '${PJM_TOPIC}' not in rendered


def test_iso_lmp_timescale_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "TIMESCALE_JDBC_URL": "jdbc:postgresql://timescale:5432/timeseries",
            "TIMESCALE_USER": "ts",
            "TIMESCALE_PASSWORD": "ts",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "iso_lmp_kafka_to_timescale", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "iso_lmp_kafka_to_timescale.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'lmp_enriched' in rendered
    assert 'jdbc:postgresql://timescale:5432/timeseries' in rendered


def test_nyiso_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "NYISO_URL": "https://example.com/nyiso.csv",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "nyiso_lmp_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "nyiso_lmp_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")
    assert 'aurum.iso.nyiso.lmp.v1' in rendered


def test_eia_series_timescale_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "TIMESCALE_JDBC_URL": "jdbc:postgresql://timescale:5432/timeseries",
            "TIMESCALE_USER": "ts",
            "TIMESCALE_PASSWORD": "ts",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "eia_series_kafka_to_timescale", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "eia_series_kafka_to_timescale.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'eia_enriched' in rendered
    assert 'jdbc:postgresql://timescale:5432/timeseries' in rendered


def test_noaa_weather_timescale_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "TIMESCALE_JDBC_URL": "jdbc:postgresql://timescale:5432/timeseries",
            "TIMESCALE_USER": "ts",
            "TIMESCALE_PASSWORD": "ts",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "noaa_weather_kafka_to_timescale", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "noaa_weather_kafka_to_timescale.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'noaa_enriched' in rendered
    assert 'jdbc:postgresql://timescale:5432/timeseries' in rendered


def test_iso_jobs_render_canonical_schema(tmp_path: Path) -> None:
    iso_schema = json.loads(ISO_LMP_SCHEMA_PATH.read_text(encoding="utf-8"))

    base_env = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SCHEMA_REGISTRY_URL": "http://localhost:8081",
        "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
    }

    jobs: list[tuple[str, dict[str, str]]] = [
        (
            "pjm_lmp_to_kafka",
            {
                "PJM_API_KEY": "pjmtoken",
                "PJM_TOPIC": "aurum.iso.pjm.lmp.v1",
                "PJM_INTERVAL_START": "2024-01-01T00:00:00-05:00",
                "PJM_INTERVAL_END": "2024-01-01T01:00:00-05:00",
            },
        ),
        (
            "nyiso_lmp_to_kafka",
            {
                "NYISO_URL": "https://example.com/nyiso.csv",
            },
        ),
    ]

    for job_name, extra_env in jobs:
        env = os.environ.copy()
        env.update(base_env)
        env.update(extra_env)

        subprocess.run(
            ["bash", str(SCRIPT_PATH), job_name, "--render-only"],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )

        config_path = tmp_path / f"{job_name}.conf"
        rendered_schema = extract_value_schema(config_path)
    assert rendered_schema == iso_schema


def _render(job: str, env: dict[str, str], tmp_path: Path) -> Path:
    env2 = os.environ.copy()
    env2.update(env)
    env2["SEATUNNEL_OUTPUT_DIR"] = str(tmp_path)
    subprocess.run(["bash", str(SCRIPT_PATH), job, "--render-only"], cwd=REPO_ROOT, env=env2, capture_output=True, text=True, check=True)
    return tmp_path / f"{job}.conf"


def test_localfile_jobs_have_sink_plugin_input(tmp_path: Path) -> None:
    # Prepare minimal JSON arrays for CAISO/ERCOt/SPP
    sample = tmp_path / "sample.json"
    sample.write_text("[{\"x\":1}]", encoding="utf-8")
    base_env = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SCHEMA_REGISTRY_URL": "http://localhost:8081",
    }
    jobs: list[tuple[str, dict[str, str]]] = [
        ("caiso_lmp_to_kafka", {"CAISO_INPUT_JSON": str(sample)}),
        ("ercot_lmp_to_kafka", {"ERCOT_INPUT_JSON": str(sample)}),
        ("spp_lmp_to_kafka", {"SPP_INPUT_JSON": str(sample)}),
    ]
    for job, extra in jobs:
        path = _render(job, {**base_env, **extra}, tmp_path)
        text = path.read_text(encoding="utf-8")
        assert "plugin_input" in text
        # Ensure schema/jsonpath present for LocalFile JSON sources
        assert "schema" in text
        assert "jsonpath" in text


def test_http_jobs_have_schema_jsonpath_and_sink_plugin_input(tmp_path: Path) -> None:
    base_env = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SCHEMA_REGISTRY_URL": "http://localhost:8081",
    }
    cases: list[tuple[str, dict[str, str]]] = [
        (
            "eia_series_to_kafka",
            {
                "EIA_API_KEY": "key",
                "EIA_SERIES_PATH": "electricity/wholesale/prices/data",
                "EIA_SERIES_ID": "EBA.ALL.D.H",
                "EIA_FREQUENCY": "HOURLY",
                "EIA_TOPIC": "aurum.ref.eia.series.v1",
            },
        ),
        (
            "fred_series_to_kafka",
            {
                "FRED_API_KEY": "fredkey",
                "FRED_SERIES_ID": "DGS10",
                "FRED_FREQUENCY": "DAILY",
                "FRED_SEASONAL_ADJ": "NSA",
                "FRED_TOPIC": "aurum.ref.fred.series.v1",
            },
        ),
        (
            "pjm_lmp_to_kafka",
            {
                "PJM_API_KEY": "pjmtoken",
                "PJM_TOPIC": "aurum.iso.pjm.lmp.v1",
                "PJM_INTERVAL_START": "2024-01-01T00:00:00-05:00",
                "PJM_INTERVAL_END": "2024-01-01T01:00:00-05:00",
            },
        ),
        (
            "isone_lmp_to_kafka",
            {
                "ISONE_URL": "https://example.com/api",
                "ISONE_START": "2024-01-01T00:00:00Z",
                "ISONE_END": "2024-01-01T01:00:00Z",
                "ISONE_MARKET": "DA",
                "ISONE_AUTH_HEADER": "",
                "ISONE_USERNAME": "",
                "ISONE_PASSWORD": "",
            },
        ),
    ]
    for job, extra in cases:
        path = _render(job, {**base_env, **extra}, tmp_path)
        text = path.read_text(encoding="utf-8")
        assert "plugin_input" in text
        # Ensure JSON schema and jsonpath are present for Http sources
        assert "schema" in text
        assert "jsonpath" in text


def test_pjm_jobs_render_configs(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update({
        "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SCHEMA_REGISTRY_URL": "http://localhost:8081",
        "PJM_API_KEY": "token",
        "PJM_ROW_LIMIT": "1000",
    })

    # Load
    env_load = env.copy()
    env_load.update({
        "PJM_LOAD_ENDPOINT": "https://api.pjm.com/api/v1/inst_load",
        "PJM_INTERVAL_START": "2024-01-01T00:00:00Z",
        "PJM_INTERVAL_END": "2024-01-01T01:00:00Z",
        "PJM_LOAD_TOPIC": "aurum.iso.pjm.load.v1",
    })
    subprocess.run(["bash", str(SCRIPT_PATH), "pjm_load_to_kafka", "--render-only"], cwd=REPO_ROOT, env=env_load, check=True)
    rendered = (tmp_path / "pjm_load_to_kafka.conf").read_text(encoding="utf-8")
    assert "aurum.iso.pjm.load.v1" in rendered

    # Genmix
    env_gm = env.copy()
    env_gm.update({
        "PJM_GENMIX_ENDPOINT": "https://api.pjm.com/api/v1/gen_by_fuel",
        "PJM_INTERVAL_START": "2024-01-01T00:00:00Z",
        "PJM_INTERVAL_END": "2024-01-01T01:00:00Z",
        "PJM_GENMIX_TOPIC": "aurum.iso.pjm.genmix.v1",
    })
    subprocess.run(["bash", str(SCRIPT_PATH), "pjm_genmix_to_kafka", "--render-only"], cwd=REPO_ROOT, env=env_gm, check=True)
    rendered = (tmp_path / "pjm_genmix_to_kafka.conf").read_text(encoding="utf-8")
    assert "aurum.iso.pjm.genmix.v1" in rendered

    # Pnodes
    env_pn = env.copy()
    env_pn.update({
        "PJM_PNODES_ENDPOINT": "https://api.pjm.com/api/v1/pnodes",
        "PJM_PNODES_TOPIC": "aurum.iso.pjm.pnode.v1",
        "PJM_PNODES_EFFECTIVE_START": "2024-01-01",
    })
    subprocess.run(["bash", str(SCRIPT_PATH), "pjm_pnodes_to_kafka", "--render-only"], cwd=REPO_ROOT, env=env_pn, check=True)
    rendered = (tmp_path / "pjm_pnodes_to_kafka.conf").read_text(encoding="utf-8")
    assert "aurum.iso.pjm.pnode.v1" in rendered
