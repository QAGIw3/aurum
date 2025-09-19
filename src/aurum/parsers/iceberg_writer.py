"""Utilities for writing canonical data into Iceberg tables backed by Nessie."""
from __future__ import annotations

import importlib
import os
from typing import Optional

import pandas as pd


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

    table = table or os.getenv("AURUM_ICEBERG_TABLE", "iceberg.market.curve_observation")
    branch = branch or os.getenv("AURUM_ICEBERG_BRANCH", "main")
    catalog_name = catalog_name or os.getenv("AURUM_ICEBERG_CATALOG", "nessie")

    uri = os.getenv("AURUM_NESSIE_URI", "http://nessie:19121/api/v1")
    warehouse = warehouse or os.getenv("AURUM_S3_WAREHOUSE", "s3://aurum/curated/iceberg")

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

    catalog = load_catalog(catalog_name, **catalog_properties)

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

    table_identifier = f"{table}@{branch}" if "@" not in table else table

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
    iceberg_table.append(arrow_table)
