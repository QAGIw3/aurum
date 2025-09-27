#!/usr/bin/env python3
"""Example usage of async DAO pattern with connection pooling.

This example demonstrates how to use the new async DAO classes for
database operations with automatic connection pooling and resource management.
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any

from aurum.core import AurumSettings
from aurum.api.dao import (
    EiaAsyncDao,
    TrinoAsyncDao,
    ClickHouseAsyncDao,
    TimescaleAsyncDao
)


async def example_eia_operations():
    """Example EIA data operations using async DAO."""
    print("\n=== EIA Async DAO Example ===")
    
    # Initialize with settings (would use real settings in production)
    settings = AurumSettings()
    dao = EiaAsyncDao(settings)
    
    try:
        # Query EIA series data
        series_data = await dao.query_eia_series(
            series_id="ELEC.GEN.ALL-US-99.M",
            start_date="2024-01-01",
            end_date="2024-12-31",
            limit=100
        )
        print(f"✅ Retrieved {len(series_data)} EIA series records")
        
        # Get dimensions for filtering
        dimensions = await dao.query_eia_series_dimensions(
            dataset="electricity"
        )
        print(f"✅ Available frequencies: {dimensions.get('frequency', [])[:5]}")
        
        # Get total count for pagination
        total_count = await dao.get_eia_series_count(
            dataset="electricity",
            frequency="monthly"
        )
        print(f"✅ Total monthly electricity records: {total_count}")
        
    except Exception as e:
        print(f"❌ EIA operations failed: {e}")
    finally:
        await dao.close()


async def example_trino_operations():
    """Example Trino federated query operations using async DAO."""
    print("\n=== Trino Async DAO Example ===")
    
    settings = AurumSettings()
    dao = TrinoAsyncDao(settings)
    
    try:
        # Query Iceberg tables
        tables = await dao.query_iceberg_tables(
            catalog="iceberg",
            schema="mart",
            table_pattern="energy%"
        )
        print(f"✅ Found {len(tables)} energy-related tables")
        
        # Get catalog schemas
        schemas = await dao.query_catalog_schemas(
            catalog="iceberg",
            schema_pattern="m%"
        )
        print(f"✅ Found {len(schemas)} schemas starting with 'm'")
        
        # Execute federated query
        result = await dao.execute_federated_query(
            "SELECT COUNT(*) as total FROM iceberg.information_schema.tables",
            catalogs=["iceberg"]
        )
        print(f"✅ Federated query result: {result}")
        
        # Validate query syntax
        validation = await dao.validate_query_syntax(
            "SELECT table_name FROM iceberg.information_schema.tables LIMIT 10"
        )
        print(f"✅ Query validation: {'Valid' if validation['valid'] else 'Invalid'}")
        
    except Exception as e:
        print(f"❌ Trino operations failed: {e}")
    finally:
        await dao.close()


async def example_clickhouse_operations():
    """Example ClickHouse OLAP operations using async DAO."""
    print("\n=== ClickHouse Async DAO Example ===")
    
    settings = AurumSettings()
    dao = ClickHouseAsyncDao(settings)
    
    try:
        # Query time series data with aggregation
        time_series = await dao.query_time_series_data(
            table="energy_prices",
            timestamp_column="timestamp",
            value_columns=["price", "volume"],
            start_time=datetime.now() - timedelta(days=30),
            end_time=datetime.now(),
            group_by_interval="1 DAY",
            aggregation="avg"
        )
        print(f"✅ Retrieved {len(time_series)} time series data points")
        
        # Query aggregated metrics
        metrics = await dao.query_aggregated_metrics(
            table="energy_trades",
            metrics={
                "volume": "sum",
                "price": "avg",
                "trades": "count"
            },
            dimensions=["region", "product"],
            filters={"region": ["TX", "CA"], "active": True}
        )
        print(f"✅ Retrieved {len(metrics)} aggregated metrics")
        
        # Get table partitions
        partitions = await dao.get_table_partitions("energy_prices")
        print(f"✅ Found {len(partitions)} table partitions")
        
        # Optimize table
        result = await dao.optimize_table("energy_prices")
        print(f"✅ Table optimization: {'Success' if result['success'] else 'Failed'}")
        
    except Exception as e:
        print(f"❌ ClickHouse operations failed: {e}")
    finally:
        await dao.close()


async def example_timescale_operations():
    """Example TimescaleDB time-series operations using async DAO."""
    print("\n=== TimescaleDB Async DAO Example ===")
    
    settings = AurumSettings()
    dao = TimescaleAsyncDao(settings)
    
    try:
        # Query time-bucketed data
        bucketed_data = await dao.query_time_bucket_data(
            table="sensor_readings",
            timestamp_column="timestamp",
            value_columns=["temperature", "humidity"],
            bucket_width="1 hour",
            start_time=datetime.now() - timedelta(days=7),
            aggregation="avg"
        )
        print(f"✅ Retrieved {len(bucketed_data)} time-bucketed records")
        
        # Query continuous aggregate
        cagg_data = await dao.query_continuous_aggregate(
            cagg_name="hourly_energy_summary",
            start_time=datetime.now() - timedelta(days=1),
            dimensions=["region"]
        )
        print(f"✅ Retrieved {len(cagg_data)} continuous aggregate records")
        
        # Get hypertable info
        table_info = await dao.get_hypertable_info("sensor_readings")
        print(f"✅ Hypertable info: {table_info.get('num_chunks', 0)} chunks")
        
        # Get chunk information
        chunks = await dao.get_chunk_info(
            table="sensor_readings",
            start_time=datetime.now() - timedelta(days=30)
        )
        print(f"✅ Found {len(chunks)} chunks in the last 30 days")
        
        # Compress old chunks
        compression_result = await dao.compress_chunks(
            table="sensor_readings",
            older_than=timedelta(days=7)
        )
        print(f"✅ Compressed {compression_result.get('chunks_compressed', 0)} chunks")
        
    except Exception as e:
        print(f"❌ TimescaleDB operations failed: {e}")
    finally:
        await dao.close()


async def example_concurrent_operations():
    """Example of concurrent operations across multiple DAOs."""
    print("\n=== Concurrent Operations Example ===")
    
    settings = AurumSettings()
    
    # Create multiple DAOs
    eia_dao = EiaAsyncDao(settings)
    trino_dao = TrinoAsyncDao(settings)
    
    try:
        # Run operations concurrently
        results = await asyncio.gather(
            eia_dao.query_eia_series_dimensions(dataset="electricity"),
            trino_dao.query_catalog_schemas("iceberg"),
            return_exceptions=True
        )
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ Operation {i} failed: {result}")
            else:
                print(f"✅ Operation {i} completed successfully")
        
    finally:
        # Close all DAOs
        await asyncio.gather(
            eia_dao.close(),
            trino_dao.close(),
            return_exceptions=True
        )


async def main():
    """Main example function."""
    print("🚀 Async DAO Pattern Examples")
    print("=" * 50)
    
    # Note: These examples would work with real database connections
    # For demo purposes, they'll show the patterns even if connections fail
    
    await example_eia_operations()
    await example_trino_operations() 
    await example_clickhouse_operations()
    await example_timescale_operations()
    await example_concurrent_operations()
    
    print("\n✅ All examples completed!")
    print("\nKey Benefits of Async DAO Pattern:")
    print("- Connection pooling for efficient resource usage")
    print("- Async/await for non-blocking database operations")
    print("- Backend-specific optimizations")
    print("- Proper resource cleanup")
    print("- Type-safe operations with comprehensive error handling")


if __name__ == "__main__":
    asyncio.run(main())