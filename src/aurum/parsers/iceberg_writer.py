"""Utilities for writing canonical data into Iceberg tables backed by Nessie."""
from __future__ import annotations

import importlib
import json
import math
import os
import time
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Optional

import pandas as pd


LOGGER = logging.getLogger(__name__)

_PPA_DECIMAL_QUANTIZER = Decimal("0.000001")


def _to_quantized_decimal(value: Any) -> Optional[Decimal]:
    """Return a 6-decimal-place Decimal or None for missing inputs."""

    if value is None:
        return None
    if isinstance(value, Decimal):
        if value.is_nan():  # type: ignore[no-untyped-call]
            return None
        try:
            return value.quantize(_PPA_DECIMAL_QUANTIZER, rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
    try:
        as_decimal = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    try:
        return as_decimal.quantize(_PPA_DECIMAL_QUANTIZER, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None


def _require_pyiceberg():
    try:
        catalog_module = importlib.import_module("pyiceberg.catalog")
        schema_module = importlib.import_module("pyiceberg.schema")
        types_module = importlib.import_module("pyiceberg.types")
        partitioning_module = importlib.import_module("pyiceberg.partitioning")
        transforms_module = importlib.import_module("pyiceberg.transforms")
        exceptions_module = importlib.import_module("pyiceberg.exceptions")
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "pyiceberg is required for Iceberg integration. Install with 'pip install aurum[iceberg]'"
        ) from exc
    return (
        catalog_module,
        schema_module,
        types_module,
        partitioning_module,
        transforms_module,
        exceptions_module,
    )


def _build_schema():
    _, schema_module, types_module, partitioning_module, transforms_module, _ = _require_pyiceberg()
    Schema = schema_module.Schema
    NestedField = schema_module.NestedField
    (StringType, DateType, DoubleType, TimestampType) = (
        types_module.StringType,
        types_module.DateType,
        types_module.DoubleType,
        types_module.TimestampType,
    )
    spec = None  # placeholder to appease linter
    schema = Schema(
        NestedField(1, "asof_date", DateType(), required=True),
        NestedField(2, "source_file", StringType()),
        NestedField(3, "sheet_name", StringType()),
        NestedField(4, "asset_class", StringType()),
        NestedField(5, "region", StringType()),
        NestedField(6, "iso", StringType()),
        NestedField(7, "location", StringType()),
        NestedField(8, "market", StringType()),
        NestedField(9, "product", StringType()),
        NestedField(10, "block", StringType()),
        NestedField(11, "spark_location", StringType()),
        NestedField(12, "price_type", StringType()),
        NestedField(13, "units_raw", StringType()),
        NestedField(14, "currency", StringType()),
        NestedField(15, "per_unit", StringType()),
        NestedField(16, "tenor_type", StringType()),
        NestedField(17, "contract_month", DateType()),
        NestedField(18, "tenor_label", StringType()),
        NestedField(19, "value", DoubleType()),
        NestedField(20, "bid", DoubleType()),
        NestedField(21, "ask", DoubleType()),
        NestedField(22, "mid", DoubleType()),
        NestedField(23, "curve_key", StringType(), required=True),
        NestedField(24, "version_hash", StringType()),
        NestedField(25, "_ingest_ts", TimestampType()),
    )
    return schema


def _build_partition_spec(schema):
    _, _, _, partitioning_module, transforms_module, _ = _require_pyiceberg()
    PartitionSpec = partitioning_module.PartitionSpec
    PartitionField = partitioning_module.PartitionField
    YearTransform = transforms_module.YearTransform
    MonthTransform = transforms_module.MonthTransform

    asof_id = schema.find_field("asof_date").field_id
    return PartitionSpec(
        PartitionField(source_id=asof_id, transform=YearTransform(), name="asof_year"),
        PartitionField(source_id=asof_id, transform=MonthTransform(), name="asof_month"),
    )


def _build_scenario_schema():
    _, schema_module, types_module, _, _, _ = _require_pyiceberg()
    Schema = schema_module.Schema
    NestedField = schema_module.NestedField
    StringType = types_module.StringType
    DateType = types_module.DateType
    DoubleType = types_module.DoubleType
    TimestampType = types_module.TimestampType

    return Schema(
        NestedField(1, "asof_date", DateType(), required=True),
        NestedField(2, "scenario_id", StringType(), required=True),
        NestedField(3, "tenant_id", StringType(), required=True),
        NestedField(4, "curve_key", StringType(), required=True),
        NestedField(5, "tenor_type", StringType()),
        NestedField(6, "contract_month", DateType()),
        NestedField(7, "tenor_label", StringType()),
        NestedField(8, "metric", StringType(), required=True),
        NestedField(9, "value", DoubleType()),
        NestedField(10, "band_lower", DoubleType()),
        NestedField(11, "band_upper", DoubleType()),
        NestedField(12, "attribution", StringType()),
        NestedField(13, "version_hash", StringType()),
        NestedField(14, "computed_ts", TimestampType()),
        NestedField(15, "_ingest_ts", TimestampType()),
        NestedField(16, "run_id", StringType()),
    )


def _build_scenario_partition_spec(schema):
    _, _, _, partitioning_module, transforms_module, _ = _require_pyiceberg()
    PartitionSpec = partitioning_module.PartitionSpec
    PartitionField = partitioning_module.PartitionField
    IdentityTransform = transforms_module.IdentityTransform
    YearTransform = transforms_module.YearTransform

    scenario_id_field = schema.find_field("scenario_id").field_id
    asof_id = schema.find_field("asof_date").field_id

    return PartitionSpec(
        PartitionField(source_id=scenario_id_field, transform=IdentityTransform(), name="scenario_id"),
        PartitionField(source_id=asof_id, transform=YearTransform(), name="asof_year"),
    )


def _build_ppa_schema():
    _, schema_module, types_module, _, _, _ = _require_pyiceberg()
    Schema = schema_module.Schema
    NestedField = schema_module.NestedField
    StringType = types_module.StringType
    DateType = types_module.DateType
    DoubleType = types_module.DoubleType
    DecimalType = types_module.DecimalType
    TimestampType = types_module.TimestampType

    return Schema(
        NestedField(1, "asof_date", DateType()),
        NestedField(2, "ppa_contract_id", StringType(), required=True),
        NestedField(3, "scenario_id", StringType(), required=True),
        NestedField(4, "tenant_id", StringType(), required=True),
        NestedField(5, "curve_key", StringType()),
        NestedField(6, "period_start", DateType()),
        NestedField(7, "period_end", DateType()),
        NestedField(8, "cashflow", DecimalType(18, 6)),
        NestedField(9, "npv", DecimalType(18, 6)),
        NestedField(10, "irr", DoubleType()),
        NestedField(11, "metric", StringType(), required=True),
        NestedField(12, "value", DecimalType(18, 6)),
        NestedField(13, "version_hash", StringType()),
        NestedField(14, "_ingest_ts", TimestampType()),
    )


def _build_ppa_partition_spec(schema):
    _, _, _, partitioning_module, transforms_module, _ = _require_pyiceberg()
    PartitionSpec = partitioning_module.PartitionSpec
    PartitionField = partitioning_module.PartitionField
    IdentityTransform = transforms_module.IdentityTransform
    YearTransform = transforms_module.YearTransform

    contract_field = schema.find_field("ppa_contract_id").field_id
    tenant_field = schema.find_field("tenant_id").field_id
    asof_field = schema.find_field("asof_date").field_id

    return PartitionSpec(
        PartitionField(source_id=tenant_field, transform=IdentityTransform(), name="tenant_id"),
        PartitionField(source_id=contract_field, transform=IdentityTransform(), name="ppa_contract_id"),
        PartitionField(source_id=asof_field, transform=YearTransform(), name="asof_year"),
    )


def write_to_iceberg(
    df: pd.DataFrame,
    *,
    table: Optional[str] = None,
    branch: Optional[str] = None,
    catalog_name: Optional[str] = None,
    warehouse: Optional[str] = None,
    properties: Optional[dict[str, str]] = None,
) -> None:
    """Append the provided dataframe to an Iceberg table.

    Environment variables used when arguments are omitted:
    - ``AURUM_ICEBERG_TABLE``: fully qualified table name (default ``iceberg.market.curve_observation``).
    - ``AURUM_ICEBERG_BRANCH``: Nessie branch/ref (default ``main``).
    - ``AURUM_ICEBERG_CATALOG``: catalog name passed to ``pyiceberg.catalog.load_catalog`` (default ``nessie``).
    - ``AURUM_NESSIE_URI`` and ``AURUM_S3_WAREHOUSE`` for catalog configuration.
    """

    (
        catalog_module,
        schema_module,
        types_module,
        partitioning_module,
        transforms_module,
        exceptions_module,
    ) = _require_pyiceberg()

    load_catalog = catalog_module.load_catalog
    NoSuchTableError = exceptions_module.NoSuchTableError

    table_name: str = table if table is not None else os.getenv("AURUM_ICEBERG_TABLE", "iceberg.market.curve_observation")  # type: ignore[assignment]
    branch_name: str = branch if branch is not None else os.getenv("AURUM_ICEBERG_BRANCH", "main")  # type: ignore[assignment]
    catalog_name_val: str = catalog_name if catalog_name is not None else os.getenv("AURUM_ICEBERG_CATALOG", "nessie")  # type: ignore[assignment]

    uri: str = os.getenv("AURUM_NESSIE_URI", "http://nessie:19121/api/v1")
    warehouse_val: str = warehouse if warehouse is not None else os.getenv("AURUM_S3_WAREHOUSE", "s3://aurum/curated/iceberg")  # type: ignore[assignment]

    catalog_properties = {
        "uri": uri,
        "warehouse": warehouse,
        "s3.endpoint": os.getenv("AURUM_S3_ENDPOINT"),
        "s3.access-key-id": os.getenv("AURUM_S3_ACCESS_KEY"),
        "s3.secret-access-key": os.getenv("AURUM_S3_SECRET_KEY"),
        "nessie.ref": branch,
    }
    if properties:
        catalog_properties.update(properties)

    # Remove None values so pyiceberg does not choke on them
    catalog_properties = {k: v for k, v in catalog_properties.items() if v is not None}

    catalog = load_catalog(catalog_name_val, **catalog_properties)

    schema = _build_schema()
    spec = _build_partition_spec(schema)

    expected_columns = [field.name for field in schema]
    for column in expected_columns:
        if column not in df.columns:
            df[column] = None
    df = df[expected_columns]

    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date
    df["contract_month"] = pd.to_datetime(df["contract_month"], errors="coerce").dt.date
    if "_ingest_ts" in df.columns:
        df["_ingest_ts"] = pd.to_datetime(df["_ingest_ts"], utc=True)

    table_identifier = f"{table_name}@{branch_name}" if "@" not in table_name else table_name

    try:
        iceberg_table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        iceberg_table = catalog.create_table(
            table_identifier,
            schema=schema,
            partition_spec=spec,
            properties={"write.format.default": "parquet"},
        )

    arrow_table = df.to_arrow()
    max_attempts = int(os.getenv("AURUM_ICEBERG_WRITE_RETRIES", "3"))
    _append_with_retry(iceberg_table, arrow_table, max_attempts=max_attempts)


def _delete_existing_scenario_rows(table, frame: pd.DataFrame) -> None:
    try:
        from pyiceberg.expressions import And, EqualTo  # type: ignore
    except ModuleNotFoundError:
        return
    key_cols = ["tenant_id", "scenario_id", "run_id", "metric", "tenor_label"]
    if any(col not in frame.columns for col in key_cols):
        return
    unique_keys = frame[key_cols].dropna().drop_duplicates()
    for _, row in unique_keys.iterrows():
        if any(pd.isna(row[col]) for col in key_cols):
            continue
        expr = EqualTo(key_cols[0], row[key_cols[0]])
        for col in key_cols[1:]:
            expr = And(expr, EqualTo(col, row[col]))
        try:
            table.delete(expr)
        except AttributeError:
            LOGGER.debug("Iceberg table.delete not available; skipping dedup")
            return
        except Exception as exc:  # pragma: no cover - delete failures
            LOGGER.warning("Failed to delete existing scenario rows for %s: %s", row.to_dict(), exc)


def _delete_existing_ppa_rows(table, frame: pd.DataFrame) -> None:
    try:
        from pyiceberg.expressions import And, EqualTo  # type: ignore
    except ModuleNotFoundError:
        return
    key_cols = ["tenant_id", "ppa_contract_id", "scenario_id", "period_start", "metric"]
    if any(col not in frame.columns for col in key_cols):
        return
    unique_keys = frame[key_cols].dropna().drop_duplicates()
    for _, row in unique_keys.iterrows():
        expr = EqualTo(key_cols[0], row[key_cols[0]])
        for col in key_cols[1:]:
            expr = And(expr, EqualTo(col, row[col]))
        try:
            table.delete(expr)
        except AttributeError:
            LOGGER.debug("Iceberg table.delete not available; skipping PPA dedup")
            return
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Failed to delete PPA rows for %s: %s", row.to_dict(), exc)


def _append_with_retry(table, arrow_table, *, max_attempts: int = 3, sleep_seconds: float = 1.0) -> None:
    attempt = 0
    while True:
        attempt += 1
        try:
            table.append(arrow_table)
            return
        except Exception as exc:  # pragma: no cover
            if attempt >= max_attempts:
                raise
            LOGGER.warning("Iceberg append failed (attempt %s/%s): %s", attempt, max_attempts, exc)
            time.sleep(sleep_seconds * attempt)


def write_ppa_valuation(
    df: pd.DataFrame,
    *,
    table: Optional[str] = None,
    branch: Optional[str] = None,
    catalog_name: Optional[str] = None,
    warehouse: Optional[str] = None,
    properties: Optional[dict[str, str]] = None,
) -> None:
    (
        catalog_module,
        _,
        _,
        _,
        _,
        exceptions_module,
    ) = _require_pyiceberg()

    load_catalog = catalog_module.load_catalog
    NoSuchTableError = exceptions_module.NoSuchTableError

    table_name: str = table if table is not None else os.getenv("AURUM_PPA_ICEBERG_TABLE", "iceberg.market.ppa_valuation")  # type: ignore[assignment]
    branch_name: str = branch if branch is not None else os.getenv("AURUM_ICEBERG_BRANCH", "main")  # type: ignore[assignment]
    catalog_name_val: str = catalog_name if catalog_name is not None else os.getenv("AURUM_ICEBERG_CATALOG", "nessie")  # type: ignore[assignment]

    uri: str = os.getenv("AURUM_NESSIE_URI", "http://nessie:19121/api/v1")
    warehouse_val: str = warehouse if warehouse is not None else os.getenv("AURUM_S3_WAREHOUSE", "s3://aurum/curated/iceberg")  # type: ignore[assignment]

    catalog_properties = {
        "uri": uri,
        "warehouse": warehouse_val,
        "s3.endpoint": os.getenv("AURUM_S3_ENDPOINT"),
        "s3.access-key-id": os.getenv("AURUM_S3_ACCESS_KEY"),
        "s3.secret-access-key": os.getenv("AURUM_S3_SECRET_KEY"),
        "nessie.ref": branch_name,
    }
    if properties:
        catalog_properties.update(properties)
    catalog_properties = {k: v for k, v in catalog_properties.items() if v is not None}

    catalog = load_catalog(catalog_name_val, **catalog_properties)

    schema = _build_ppa_schema()
    spec = _build_ppa_partition_spec(schema)

    expected_columns = [field.name for field in schema]
    frame = df.copy()
    for column in expected_columns:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[expected_columns]

    decimal_columns = ("cashflow", "npv", "value")
    for column in decimal_columns:
        frame[column] = frame[column].apply(_to_quantized_decimal)

    if "tenant_id" in frame.columns:
        frame["tenant_id"] = frame["tenant_id"].apply(
            lambda value: str(value) if value is not None and str(value).strip() else None
        )
        if frame["tenant_id"].isna().any():
            raise ValueError("tenant_id is required for PPA valuations")

    frame["irr"] = frame["irr"].apply(
        lambda value: float(value) if value is not None and not pd.isna(value) else None
    )

    for date_col in ("asof_date", "period_start", "period_end"):
        frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce").dt.date
    frame["_ingest_ts"] = pd.to_datetime(frame.get("_ingest_ts"), utc=True, errors="coerce")
    if frame["_ingest_ts"].isna().all():
        frame["_ingest_ts"] = pd.Timestamp.utcnow().tz_localize("UTC")
    else:
        fallback_ts = pd.Timestamp.utcnow().tz_localize("UTC")
        frame.loc[frame["_ingest_ts"].isna(), "_ingest_ts"] = fallback_ts

    table_identifier = f"{table_name}@{branch_name}" if "@" not in table_name else table_name

    try:
        iceberg_table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        iceberg_table = catalog.create_table(
            table_identifier,
            schema=schema,
            partition_spec=spec,
            properties={"write.format.default": "parquet"},
        )

    arrow_table = frame.to_arrow()
    _delete_existing_ppa_rows(iceberg_table, frame)
    max_attempts = int(os.getenv("AURUM_ICEBERG_WRITE_RETRIES", "3"))
    _append_with_retry(iceberg_table, arrow_table, max_attempts=max_attempts)


def write_scenario_output(
    df: pd.DataFrame,
    *,
    table: Optional[str] = None,
    branch: Optional[str] = None,
    catalog_name: Optional[str] = None,
    warehouse: Optional[str] = None,
    properties: Optional[dict[str, str]] = None,
) -> None:
    (
        catalog_module,
        _,
        _,
        _,
        _,
        exceptions_module,
    ) = _require_pyiceberg()

    load_catalog = catalog_module.load_catalog
    NoSuchTableError = exceptions_module.NoSuchTableError

    table_name: str = table if table is not None else os.getenv("AURUM_SCENARIO_ICEBERG_TABLE", "iceberg.market.scenario_output")  # type: ignore[assignment]
    branch_name: str = branch if branch is not None else os.getenv("AURUM_ICEBERG_BRANCH", "main")  # type: ignore[assignment]
    catalog_name_val: str = catalog_name if catalog_name is not None else os.getenv("AURUM_ICEBERG_CATALOG", "nessie")  # type: ignore[assignment]

    uri: str = os.getenv("AURUM_NESSIE_URI", "http://nessie:19121/api/v1")
    warehouse_val: str = warehouse if warehouse is not None else os.getenv("AURUM_S3_WAREHOUSE", "s3://aurum/curated/iceberg")  # type: ignore[assignment]

    catalog_properties = {
        "uri": uri,
        "warehouse": warehouse_val,
        "s3.endpoint": os.getenv("AURUM_S3_ENDPOINT"),
        "s3.access-key-id": os.getenv("AURUM_S3_ACCESS_KEY"),
        "s3.secret-access-key": os.getenv("AURUM_S3_SECRET_KEY"),
        "nessie.ref": branch_name,
    }
    if properties:
        catalog_properties.update(properties)
    catalog_properties = {k: v for k, v in catalog_properties.items() if v is not None}

    catalog = load_catalog(catalog_name_val, **catalog_properties)

    schema = _build_scenario_schema()
    spec = _build_scenario_partition_spec(schema)

    expected_columns = [field.name for field in schema]
    frame = df.copy()
    for column in expected_columns:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[expected_columns]

    if "run_id" in frame.columns:
        frame["run_id"] = frame["run_id"].apply(lambda value: str(value) if pd.notna(value) else None)

    frame["asof_date"] = pd.to_datetime(frame["asof_date"]).dt.date
    frame["contract_month"] = pd.to_datetime(frame["contract_month"], errors="coerce").dt.date
    frame["computed_ts"] = pd.to_datetime(frame["computed_ts"], utc=True, errors="coerce")
    if frame["computed_ts"].isna().any():
        fallback_ts = pd.Timestamp.utcnow().tz_localize("UTC")
        frame.loc[frame["computed_ts"].isna(), "computed_ts"] = fallback_ts
    if "_ingest_ts" in frame.columns:
        frame["_ingest_ts"] = pd.to_datetime(frame["_ingest_ts"], utc=True, errors="coerce")
        if frame["_ingest_ts"].isna().any():
            fallback_ingest = pd.Timestamp.utcnow().tz_localize("UTC")
            frame.loc[frame["_ingest_ts"].isna(), "_ingest_ts"] = fallback_ingest
    else:
        frame["_ingest_ts"] = pd.Timestamp.utcnow().tz_localize("UTC")

    if "attribution" in frame.columns:
        frame["attribution"] = frame["attribution"].apply(
            lambda value: json.dumps(value) if isinstance(value, (dict, list)) else value
        )

    table_identifier = f"{table_name}@{branch_name}" if "@" not in table_name else table_name

    try:
        iceberg_table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        iceberg_table = catalog.create_table(
            table_identifier,
            schema=schema,
            partition_spec=spec,
            properties={"write.format.default": "parquet"},
        )

    arrow_table = frame.to_arrow()
    _delete_existing_scenario_rows(iceberg_table, frame)
    max_attempts = int(os.getenv("AURUM_ICEBERG_WRITE_RETRIES", "3"))
    _append_with_retry(iceberg_table, arrow_table, max_attempts=max_attempts)
