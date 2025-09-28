from types import SimpleNamespace

import pytest

from aurum.api.services.dbt_management_service import (
    DBTManagementService,
    IcebergColumnDefinition,
    IcebergPartitionField,
    IcebergTableDefinition,
    TrinoQueryExpectation,
)


class FakeTrinoDao:
    """Simple async stub for Trino DAO calls."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.queries = []

    async def execute_query(self, query, params=None):
        self.queries.append(query)
        if self._responses:
            response = self._responses.pop(0)
            if isinstance(response, Exception):
                raise response
            if callable(response):
                return response(query)
            return response
        return []


@pytest.fixture()
def dbt_service(monkeypatch):
    fake_cache = SimpleNamespace(
        set=lambda *args, **kwargs: None,
        get=lambda *args, **kwargs: None,
        delete=lambda *args, **kwargs: None,
    )
    fake_telemetry = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
    )

    monkeypatch.setattr(DBTManagementService, "_load_dbt_project", lambda self: None)
    monkeypatch.setattr(
        "aurum.api.services.dbt_management_service.get_unified_cache_manager",
        lambda: fake_cache,
    )
    monkeypatch.setattr(
        "aurum.api.services.dbt_management_service.get_telemetry_facade",
        lambda: fake_telemetry,
    )

    service = DBTManagementService()
    return service


@pytest.mark.asyncio
async def test_provision_iceberg_tables_success(dbt_service):
    table_def = IcebergTableDefinition(
        catalog="iceberg",
        schema="market",
        table_name="demand_features",
        columns=[
            IcebergColumnDefinition(name="asof_date", data_type="date", nullable=False),
            IcebergColumnDefinition(name="tenant_id", data_type="varchar", nullable=False),
            IcebergColumnDefinition(name="load_mw", data_type="double"),
        ],
        partition_fields=[IcebergPartitionField(column="asof_date", transform="day")],
        table_properties={"write_compression": "ZSTD"},
    )

    describe_rows = [
        {"Column": "asof_date", "Type": "date"},
        {"Column": "tenant_id", "Type": "varchar"},
        {"Column": "load_mw", "Type": "double"},
    ]
    show_create_rows = [{"Create Table": "CREATE TABLE iceberg.market.demand_features (\n    asof_date date,\n    tenant_id varchar,\n    load_mw double\n)\nWITH (\n    format = 'ICEBERG',\n    partitioning = ARRAY['day(asof_date)']\n)"}]
    validation_rows = [{"tenant_id": "tenant-1", "load_mw": 42.0}]

    fake_dao = FakeTrinoDao([
        [],
        describe_rows,
        show_create_rows,
        validation_rows,
    ])

    dbt_service._trino_dao_factory = lambda: fake_dao

    expectation = TrinoQueryExpectation(
        query="SELECT tenant_id, load_mw FROM iceberg.market.demand_features",
        expected_rows=validation_rows,
    )

    result = await dbt_service.provision_iceberg_tables(
        [table_def],
        validation_queries=[expectation],
    )

    assert result["status"] == "success"
    assert result["validation"]["success"] is True
    assert result["tables"][0]["status"] == "success"
    assert any(query.startswith("CREATE TABLE") for query in fake_dao.queries)


@pytest.mark.asyncio
async def test_provision_iceberg_tables_detects_partition_issue(dbt_service):
    table_def = IcebergTableDefinition(
        catalog="iceberg",
        schema="analytics",
        table_name="load_hourly",
        columns=[
            IcebergColumnDefinition(name="asof_date", data_type="date"),
            IcebergColumnDefinition(name="hour", data_type="integer"),
        ],
        partition_fields=[IcebergPartitionField(column="asof_date", transform="month")],
    )

    describe_rows = [
        {"Column": "asof_date", "Type": "date"},
        {"Column": "hour", "Type": "integer"},
    ]
    show_create_rows = [{"Create Table": "CREATE TABLE iceberg.analytics.load_hourly (\n    asof_date date,\n    hour integer\n)\nWITH (\n    format = 'ICEBERG'\n)"}]

    fake_dao = FakeTrinoDao([
        [],
        describe_rows,
        show_create_rows,
    ])

    dbt_service._trino_dao_factory = lambda: fake_dao

    result = await dbt_service.provision_iceberg_tables([table_def])

    assert result["status"] == "issues"
    table_result = result["tables"][0]
    assert table_result["status"] == "issues"
    assert table_result["partition_validation"]["missing_partition_clauses"] == ["'month(asof_date)'"]
