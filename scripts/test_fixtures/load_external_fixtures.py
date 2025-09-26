#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Iterable

try:  # pragma: no cover - runtime optional dependency
    from trino import dbapi
except ImportError:  # pragma: no cover - fallback for test environments
    class _StubDbapi:
        def connect(self, *args, **kwargs):
            raise RuntimeError("trino client not available; install the 'trino' package to use this script")

    dbapi = _StubDbapi()

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO_ROOT / "testdata" / "external"
MERGE_DIR = REPO_ROOT / "sql" / "merge"


SERIES_CATALOG_COLUMNS = [
    "provider",
    "series_id",
    "dataset_code",
    "title",
    "description",
    "unit_code",
    "frequency_code",
    "geo_id",
    "status",
    "category",
    "source_url",
    "notes",
    "start_ts",
    "end_ts",
    "last_observation_ts",
    "asof_date",
    "tags",
    "metadata",
    "version",
    "updated_at",
    "created_at",
    "ingest_ts",
]

SERIES_CATALOG_TYPES = {
    "start_ts": "ts",
    "end_ts": "ts",
    "last_observation_ts": "ts",
    "asof_date": "date",
    "tags": "array_str",
    "metadata": "json",
    "version": "bigint",
    "updated_at": "ts",
    "created_at": "ts",
    "ingest_ts": "ts",
}

TIMESERIES_COLUMNS = [
    "provider",
    "series_id",
    "ts",
    "asof_date",
    "value",
    "value_raw",
    "unit_code",
    "geo_id",
    "dataset_code",
    "frequency_code",
    "status",
    "quality_flag",
    "ingest_ts",
    "source_event_id",
    "metadata",
    "updated_at",
]

TIMESERIES_TYPES = {
    "ts": "ts",
    "asof_date": "date",
    "value": "double",
    "ingest_ts": "ts",
    "metadata": "json",
    "updated_at": "ts",
}


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load sample external fixtures into Iceberg via Trino")
    parser.add_argument("--host", default="localhost", help="Trino coordinator hostname")
    parser.add_argument("--port", type=int, default=8080, help="Trino coordinator port")
    parser.add_argument("--user", default="aurum-dev", help="Trino username")
    parser.add_argument("--password", default=None, help="Optional password for basic auth")
    parser.add_argument("--catalog", default="iceberg", help="Target catalog for canonical tables")
    parser.add_argument("--schema", default="external", help="Target schema for canonical tables")
    parser.add_argument("--http-scheme", dest="http_scheme", default="http", choices=("http", "https"))
    return parser.parse_args(list(argv) if argv is not None else None)


def load_series_catalog_fixture(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def load_timeseries_fixture(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row: dict[str, Any] = {}
            for key, value in raw_row.items():
                if value is None or value == "":
                    row[key] = None
                    continue
                if key == "value":
                    row[key] = float(value)
                elif key == "metadata":
                    row[key] = json.loads(value)
                else:
                    row[key] = value
            rows.append(row)
    return rows


def sql_literal(value: Any, value_type: str | None = None) -> str:
    if value is None:
        return "NULL"
    if value_type == "ts":
        return f"from_iso8601_timestamp('{value}')"
    if value_type == "date":
        return f"DATE '{value}'"
    if value_type == "double":
        return str(float(value))
    if value_type == "json":
        payload = json.dumps(value)
        escaped = payload.replace("'", "''")
        return f"JSON '{escaped}'"
    if value_type == "array_str":
        if not value:
            return "CAST(ARRAY[] AS ARRAY(VARCHAR))"
        items = ", ".join(_quote_string(item) for item in value)
        return f"ARRAY[{items}]"
    if value_type == "bigint":
        return str(int(value))
    if isinstance(value, (int, float)):
        return str(value)
    return _quote_string(str(value))


def _quote_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def build_values_clause(rows: list[dict[str, Any]], columns: list[str], type_map: dict[str, str]) -> str:
    lines: list[str] = []
    for row in rows:
        formatted = [sql_literal(row.get(column), type_map.get(column)) for column in columns]
        lines.append("        (" + ", ".join(formatted) + ")")
    return ",\n".join(lines)


def replace_source_block(sql_text: str, placeholder: str, columns: list[str], values_clause: str) -> str:
    source_block = (
        f"USING (\n    VALUES\n{values_clause}\n) AS source (" + ", ".join(columns) + ")"
    )
    return sql_text.replace(placeholder, source_block)


def render_merge_sql(filename: str, placeholder: str, columns: list[str], type_map: dict[str, str], rows: list[dict[str, Any]], catalog: str, schema: str) -> str:
    base_sql = (MERGE_DIR / filename).read_text(encoding="utf-8")
    values_clause = build_values_clause(rows, columns, type_map)
    merged_sql = replace_source_block(base_sql, placeholder, columns, values_clause)
    target_prefix = f"{catalog}.{schema}."
    return merged_sql.replace("iceberg.external.", target_prefix)


def run_queries(cursor: Any, queries: dict[str, str]) -> None:
    for label, query in queries.items():
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"{label} -> {rows}")


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    series_catalog_rows = load_series_catalog_fixture(FIXTURE_DIR / "series_catalog.jsonl")
    timeseries_rows = load_timeseries_fixture(FIXTURE_DIR / "timeseries_observation.csv")

    statements: list[tuple[str, str]] = []
    if series_catalog_rows:
        sql_text = render_merge_sql(
            "catalog_merge.sql",
            "USING staging_external_series_catalog AS source",
            SERIES_CATALOG_COLUMNS,
            SERIES_CATALOG_TYPES,
            series_catalog_rows,
            args.catalog,
            args.schema,
        )
        statements.append(("series_catalog", sql_text))
    if timeseries_rows:
        sql_text = render_merge_sql(
            "obs_merge.sql",
            "USING staging_external_timeseries_observation AS source",
            TIMESERIES_COLUMNS,
            TIMESERIES_TYPES,
            timeseries_rows,
            args.catalog,
            args.schema,
        )
        statements.append(("timeseries_observation", sql_text))

    if not statements:
        print("No fixtures found. Nothing to load.")
        return 0

    auth = None
    if args.password:
        from trino.auth import BasicAuthentication

        auth = BasicAuthentication(args.user, args.password)

    conn = dbapi.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        catalog=args.catalog,
        schema=args.schema,
        http_scheme=args.http_scheme,
        auth=auth,
    )
    conn.autocommit = True

    with conn.cursor() as cursor:
        for label, statement in statements:
            print(f"Applying merge for {label} ...")
            cursor.execute(statement)
        run_queries(
            cursor,
            {
                "timeseries_counts": f"SELECT provider, series_id, COUNT(*) AS row_count FROM {args.catalog}.{args.schema}.timeseries_observation GROUP BY provider, series_id ORDER BY provider, series_id",
                "catalog_titles": f"SELECT provider, series_id, title FROM {args.catalog}.{args.schema}.series_catalog ORDER BY provider, series_id",
            },
        )

    print("Fixture load complete.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
