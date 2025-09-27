#!/usr/bin/env python3
"""Mock demonstration of async DAO pattern with connection pooling.

This example shows the async DAO pattern working with mocked backends,
demonstrating the API and patterns without requiring real database connections.
"""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

# Import our async DAO classes
from aurum.api.dao import EiaAsyncDao, TrinoAsyncDao, ClickHouseAsyncDao, TimescaleAsyncDao
from aurum.data import QueryResult


async def demo_eia_async_dao():
    """Demonstrate EIA async DAO with mocked backend."""
    print("\n=== EIA Async DAO Demo ===")
    
    # Create mock settings
    settings = MagicMock()
    
    # Create DAO with mocked backend
    dao = EiaAsyncDao(settings)
    
    # Mock the backend adapter
    mock_adapter = AsyncMock()
    mock_backend = AsyncMock()
    mock_backend.name = "timescale"
    
    # Mock query result
    mock_backend.execute_query.return_value = QueryResult(
        columns=["series_id", "timestamp_utc", "value", "unit"],
        rows=[
            ("ELEC.GEN.ALL-US-99.M", datetime(2024, 1, 1), 4500.0, "MWh"),
            ("ELEC.GEN.ALL-US-99.M", datetime(2024, 2, 1), 4200.0, "MWh"),
            ("ELEC.GEN.ALL-US-99.M", datetime(2024, 3, 1), 4800.0, "MWh"),
        ],
        metadata={"backend": "timescale", "rows_affected": 3}
    )
    
    mock_adapter.get_backend.return_value = mock_backend
    dao._backend_adapter = mock_adapter
    
    try:
        # Query EIA series data
        print("📊 Querying EIA series data...")
        series_data = await dao.query_eia_series(
            series_id="ELEC.GEN.ALL-US-99.M",
            start_date="2024-01-01",
            end_date="2024-03-31"
        )
        
        print(f"✅ Retrieved {len(series_data)} records:")
        for record in series_data:
            print(f"   {record['timestamp_utc']}: {record['value']} {record['unit']}")
        
        # Mock dimensions query
        mock_backend.execute_query.return_value = QueryResult(
            columns=["frequency", "area", "sector"],
            rows=[
                ("monthly", "US", "electricity"),
                ("daily", "TX", "electricity"),
                ("hourly", "CA", "electricity"),
            ],
            metadata={}
        )
        
        print("\n📈 Querying available dimensions...")
        dimensions = await dao.query_eia_series_dimensions(dataset="electricity")
        print(f"✅ Available frequencies: {dimensions['frequency']}")
        print(f"✅ Available areas: {dimensions['area']}")
        print(f"✅ Available sectors: {dimensions['sector']}")
        
        # Mock count query
        mock_backend.execute_query.return_value = QueryResult(
            columns=["total"],
            rows=[(1250,)],
            metadata={}
        )
        
        print("\n🔢 Getting total record count...")
        total = await dao.get_eia_series_count(frequency="monthly")
        print(f"✅ Total monthly records: {total}")
        
    finally:
        await dao.close()
        print("✅ DAO closed successfully")


async def demo_trino_async_dao():
    """Demonstrate Trino async DAO with mocked backend."""
    print("\n=== Trino Async DAO Demo ===")
    
    settings = MagicMock()
    dao = TrinoAsyncDao(settings)
    
    # Mock backend
    mock_adapter = AsyncMock()
    mock_backend = AsyncMock()
    mock_backend.name = "trino"
    
    # Mock Iceberg tables query
    mock_backend.execute_query.return_value = QueryResult(
        columns=["table_catalog", "table_schema", "table_name", "table_type"],
        rows=[
            ("iceberg", "mart", "energy_prices", "BASE TABLE"),
            ("iceberg", "mart", "energy_volumes", "BASE TABLE"),
            ("iceberg", "raw", "energy_trades", "BASE TABLE"),
        ],
        metadata={}
    )
    
    mock_adapter.get_backend.return_value = mock_backend
    dao._backend_adapter = mock_adapter
    
    try:
        print("🏗️ Querying Iceberg tables...")
        tables = await dao.query_iceberg_tables(
            catalog="iceberg",
            schema="mart",
            table_pattern="energy%"
        )
        
        print(f"✅ Found {len(tables)} energy tables:")
        for table in tables:
            print(f"   {table['table_catalog']}.{table['table_schema']}.{table['table_name']}")
        
        # Mock federated query
        mock_backend.execute_query.return_value = QueryResult(
            columns=["region", "avg_price", "total_volume"],
            rows=[
                ("US-TX", 45.67, 12500.0),
                ("US-CA", 52.33, 8900.0),
                ("US-NY", 48.12, 15600.0),
            ],
            metadata={"query_id": "20240101_123456_00001_abcde"}
        )
        
        print("\n🔗 Executing federated query...")
        results = await dao.execute_federated_query(
            """
            SELECT region, avg(price) as avg_price, sum(volume) as total_volume
            FROM iceberg.mart.energy_trades
            WHERE date >= DATE '2024-01-01'
            GROUP BY region
            """,
            catalogs=["iceberg"]
        )
        
        print(f"✅ Federated query results ({len(results)} regions):")
        for result in results:
            print(f"   {result['region']}: ${result['avg_price']:.2f} avg, {result['total_volume']} total")
        
    finally:
        await dao.close()
        print("✅ DAO closed successfully")


async def demo_clickhouse_async_dao():
    """Demonstrate ClickHouse async DAO with mocked backend."""
    print("\n=== ClickHouse Async DAO Demo ===")
    
    settings = MagicMock()
    dao = ClickHouseAsyncDao(settings)
    
    # Mock backend
    mock_adapter = AsyncMock()
    mock_backend = AsyncMock()
    mock_backend.name = "clickhouse"
    
    # Mock time series query
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    mock_backend.execute_query.return_value = QueryResult(
        columns=["time_bucket", "price_avg", "volume_avg"],
        rows=[
            (base_time, 45.50, 1250.0),
            (base_time + timedelta(hours=1), 46.20, 1180.0),
            (base_time + timedelta(hours=2), 44.80, 1340.0),
            (base_time + timedelta(hours=3), 47.10, 1420.0),
        ],
        metadata={}
    )
    
    mock_adapter.get_backend.return_value = mock_backend
    dao._backend_adapter = mock_adapter
    
    try:
        print("📊 Querying time series data with aggregation...")
        time_series = await dao.query_time_series_data(
            table="energy_prices",
            timestamp_column="timestamp",
            value_columns=["price", "volume"],
            start_time=base_time,
            end_time=base_time + timedelta(hours=6),
            group_by_interval="1 HOUR",
            aggregation="avg"
        )
        
        print(f"✅ Retrieved {len(time_series)} hourly aggregates:")
        for ts in time_series:
            print(f"   {ts['time_bucket']}: ${ts['price_avg']:.2f}, {ts['volume_avg']} vol")
        
        # Mock aggregated metrics
        mock_backend.execute_query.return_value = QueryResult(
            columns=["region", "product", "volume_sum", "price_avg"],
            rows=[
                ("TX", "crude_oil", 15000.0, 72.50),
                ("TX", "natural_gas", 8500.0, 3.25),
                ("CA", "crude_oil", 12000.0, 74.20),
                ("CA", "natural_gas", 6800.0, 3.45),
            ],
            metadata={}
        )
        
        print("\n📈 Querying aggregated metrics...")
        metrics = await dao.query_aggregated_metrics(
            table="energy_trades",
            metrics={"volume": "sum", "price": "avg"},
            dimensions=["region", "product"],
            filters={"active": True}
        )
        
        print(f"✅ Retrieved {len(metrics)} metric combinations:")
        for metric in metrics:
            print(f"   {metric['region']} {metric['product']}: {metric['volume_sum']} vol, ${metric['price_avg']:.2f}")
        
    finally:
        await dao.close()
        print("✅ DAO closed successfully")


async def demo_timescale_async_dao():
    """Demonstrate TimescaleDB async DAO with mocked backend."""
    print("\n=== TimescaleDB Async DAO Demo ===")
    
    settings = MagicMock()
    dao = TimescaleAsyncDao(settings)
    
    # Mock backend
    mock_adapter = AsyncMock()
    mock_backend = AsyncMock()
    mock_backend.name = "timescale"
    
    # Mock time bucket query
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    mock_backend.execute_query.return_value = QueryResult(
        columns=["time_bucket", "temperature_avg", "humidity_avg"],
        rows=[
            (base_time, 22.5, 65.2),
            (base_time + timedelta(hours=1), 23.1, 64.8),
            (base_time + timedelta(hours=2), 23.8, 63.9),
            (base_time + timedelta(hours=3), 24.2, 62.5),
        ],
        metadata={}
    )
    
    mock_adapter.get_backend.return_value = mock_backend
    dao._backend_adapter = mock_adapter
    
    try:
        print("🌡️ Querying time-bucketed sensor data...")
        bucketed_data = await dao.query_time_bucket_data(
            table="sensor_readings",
            timestamp_column="timestamp",
            value_columns=["temperature", "humidity"],
            bucket_width="1 hour",
            start_time=base_time,
            aggregation="avg"
        )
        
        print(f"✅ Retrieved {len(bucketed_data)} hourly buckets:")
        for data in bucketed_data:
            print(f"   {data['time_bucket']}: {data['temperature_avg']:.1f}°C, {data['humidity_avg']:.1f}%")
        
        # Mock hypertable info
        mock_backend.execute_query.return_value = QueryResult(
            columns=["hypertable_name", "num_chunks", "compression_enabled"],
            rows=[("sensor_readings", 48, True)],
            metadata={}
        )
        
        print("\n📋 Getting hypertable information...")
        table_info = await dao.get_hypertable_info("sensor_readings")
        print(f"✅ Hypertable: {table_info['hypertable_name']}")
        print(f"✅ Chunks: {table_info['num_chunks']}")
        print(f"✅ Compression: {'Enabled' if table_info['compression_enabled'] else 'Disabled'}")
        
    finally:
        await dao.close()
        print("✅ DAO closed successfully")


async def demo_concurrent_operations():
    """Demonstrate concurrent operations across multiple DAOs."""
    print("\n=== Concurrent Operations Demo ===")
    
    settings = MagicMock()
    
    # Create multiple DAOs
    eia_dao = EiaAsyncDao(settings)
    trino_dao = TrinoAsyncDao(settings)
    
    # Mock adapters and backends
    for dao, backend_name in [(eia_dao, "timescale"), (trino_dao, "trino")]:
        mock_adapter = AsyncMock()
        mock_backend = AsyncMock()
        mock_backend.name = backend_name
        mock_backend.execute_query.return_value = QueryResult(
            columns=["result"],
            rows=[("success",)],
            metadata={}
        )
        mock_adapter.get_backend.return_value = mock_backend
        dao._backend_adapter = mock_adapter
    
    try:
        print("🔄 Running concurrent operations...")
        
        # Start operations concurrently
        start_time = asyncio.get_event_loop().time()
        
        results = await asyncio.gather(
            eia_dao.query_eia_series_dimensions(dataset="electricity"),
            trino_dao.query_catalog_schemas("iceberg"),
            return_exceptions=True
        )
        
        end_time = asyncio.get_event_loop().time()
        
        print(f"✅ Completed {len(results)} operations in {end_time - start_time:.3f} seconds")
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ Operation {i} failed: {result}")
            else:
                print(f"✅ Operation {i} completed successfully")
        
    finally:
        # Close all DAOs concurrently
        await asyncio.gather(
            eia_dao.close(),
            trino_dao.close(),
            return_exceptions=True
        )
        print("✅ All DAOs closed successfully")


async def main():
    """Main demo function."""
    print("🚀 Async DAO Pattern Mock Demonstration")
    print("=" * 60)
    print("This demo shows the async DAO pattern with mocked backends")
    print("demonstrating the API and patterns without real database connections.")
    
    await demo_eia_async_dao()
    await demo_trino_async_dao()
    await demo_clickhouse_async_dao()
    await demo_timescale_async_dao()
    await demo_concurrent_operations()
    
    print("\n" + "=" * 60)
    print("✅ All demonstrations completed successfully!")
    print("\n🎯 Key Features Demonstrated:")
    print("- Async/await patterns for non-blocking operations")
    print("- Connection pooling through backend adapters")
    print("- Backend-specific optimizations and methods")
    print("- Proper resource cleanup with close() methods")
    print("- Concurrent operations with asyncio.gather()")
    print("- Type-safe results with comprehensive error handling")


if __name__ == "__main__":
    asyncio.run(main())