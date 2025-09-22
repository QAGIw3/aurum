{#
    Iceberg table configuration macros for optimal partitioning, compaction, and performance.

    These macros provide standardized table properties for different types of data:
    - Hot path time series (high frequency updates, time-based access)
    - Cold path reference data (infrequent updates, dimension lookups)
    - External observations (mixed access patterns)
    - Fact tables (analytical workloads)

    Each macro returns appropriate table properties including:
    - Partitioning strategy optimized for query patterns
    - Compaction settings for read performance
    - Compression and encoding settings
    - Metadata optimization parameters
#}

{#-
    Configuration for hot path time series tables like curve observations,
    ISO LMP data, and other frequently updated time-based data.

    Key characteristics:
    - High write frequency
    - Time-based query patterns
    - Need for fast point-in-time queries
    - Rolling window retention
-#}
{%- macro iceberg_config_timeseries(
    partition_fields=['asof_date'],
    partition_granularity='day',
    target_file_size_mb=128,
    write_compression='ZSTD',
    sort_order=None,
    compaction_strategy='sort'
) -%}
{{
    config(
        table_properties={
            'format': 'ICEBERG',
            'partitioning': "ARRAY['" + partition_granularity + "s(" + partition_fields[0] + ")'" +
                (", '" + "', '".join(partition_fields[1:]) + "'" if partition_fields[1:] else '') + "]",
            'write_compression': write_compression,
            'write_target_file_size_bytes': (target_file_size_mb * 1024 * 1024) | string,
            'write_parquet_compression_codec': write_compression,
            'write_parquet_compression_level': '9',
            'commit_num_retries': '3',
            'commit_retry_num_threads': '4',
            'write_metadata_delete_after_commit_enabled': 'true',
            'write_metadata_previous_versions_max': '10',
            'format_version': '2',
            'engine_hive_enabled': 'false',
            'write_parquet_bloom_filter_columns': "ARRAY['" + "', '".join([
                'tenant_id', 'provider', 'series_id', 'geo_id', 'dataset_code'
            ]) + "']" if sort_order else "ARRAY[]",
            'write_parquet_bloom_filter_enabled': 'true',
            'write_parquet_page_size_bytes': '1048576',
            'write_parquet_row_group_size_bytes': '134217728',
            'write_parquet_dict_encoding_enabled': 'true',
            'write_parquet_plain_encoding_enabled': 'false',
            'write_parquet_rle_encoding_enabled': 'true',
            'write_parquet_bloom_filter_fpp': '0.01'
        }
    )
}}
{%- endmacro -%}

{#-
    Configuration for reference/dimension tables that are infrequently updated
    but frequently joined against in analytical queries.

    Key characteristics:
    - Low write frequency
    - High read frequency for joins
    - Small to medium size
    - Need for fast lookups
-#}
{%- macro iceberg_config_dimension(
    sort_columns=['tenant_id', 'provider', 'series_id'],
    target_file_size_mb=64,
    write_compression='ZSTD',
    bloom_filter_columns=None
) -%}
{{
    config(
        table_properties={
            'format': 'ICEBERG',
            'write_compression': write_compression,
            'write_target_file_size_bytes': (target_file_size_mb * 1024 * 1024) | string,
            'write_parquet_compression_codec': write_compression,
            'write_parquet_compression_level': '9',
            'commit_num_retries': '3',
            'commit_retry_num_threads': '4',
            'write_metadata_delete_after_commit_enabled': 'true',
            'write_metadata_previous_versions_max': '5',
            'format_version': '2',
            'engine_hive_enabled': 'false',
            'write_parquet_bloom_filter_columns': "ARRAY['" + "', '".join(
                bloom_filter_columns or sort_columns[:3]
            ) + "']",
            'write_parquet_bloom_filter_enabled': 'true',
            'write_parquet_page_size_bytes': '1048576',
            'write_parquet_row_group_size_bytes': '67108864',
            'write_parquet_dict_encoding_enabled': 'true',
            'write_parquet_plain_encoding_enabled': 'false',
            'write_parquet_rle_encoding_enabled': 'true',
            'write_parquet_bloom_filter_fpp': '0.005'
        }
    )
}}
{%- endmacro -%}

{#-
    Configuration for external observation data that has mixed access patterns
    with time-based queries and catalog lookups.

    Key characteristics:
    - Medium write frequency
    - Mixed query patterns (time + catalog)
    - Need for efficient catalog joins
    - Variable data volumes
-#}
{%- macro iceberg_config_external_observations(
    partition_fields=['asof_date', 'provider'],
    partition_granularity='day',
    sort_columns=['provider', 'series_id', 'asof_date'],
    target_file_size_mb=96,
    write_compression='ZSTD'
) -%}
{{
    config(
        table_properties={
            'format': 'ICEBERG',
            'partitioning': "ARRAY['" + partition_granularity + "s(" + partition_fields[0] + ")', '" + partition_fields[1] + "']",
            'write_compression': write_compression,
            'write_target_file_size_bytes': (target_file_size_mb * 1024 * 1024) | string,
            'write_parquet_compression_codec': write_compression,
            'write_parquet_compression_level': '9',
            'commit_num_retries': '3',
            'commit_retry_num_threads': '4',
            'write_metadata_delete_after_commit_enabled': 'true',
            'write_metadata_previous_versions_max': '15',
            'format_version': '2',
            'engine_hive_enabled': 'false',
            'write_parquet_bloom_filter_columns': "ARRAY['provider', 'series_id', 'geo_id']",
            'write_parquet_bloom_filter_enabled': 'true',
            'write_parquet_page_size_bytes': '1048576',
            'write_parquet_row_group_size_bytes': '100663296',
            'write_parquet_dict_encoding_enabled': 'true',
            'write_parquet_plain_encoding_enabled': 'false',
            'write_parquet_rle_encoding_enabled': 'true',
            'write_parquet_bloom_filter_fpp': '0.01',
            'write_parquet_statistics_enabled': 'true'
        }
    )
}}
{%- endmacro -%}

{#-
    Configuration for fact tables used in analytical workloads with complex joins
    and aggregations.

    Key characteristics:
    - High read frequency for analytics
    - Complex query patterns with multiple joins
    - Large data volumes
    - Need for fast aggregations
-#}
{%- macro iceberg_config_fact_table(
    partition_fields=['asof_date'],
    partition_granularity='day',
    sort_columns=['tenant_id', 'iso_code', 'market_code', 'asof_date'],
    target_file_size_mb=256,
    write_compression='ZSTD'
) -%}
{{
    config(
        table_properties={
            'format': 'ICEBERG',
            'partitioning': "ARRAY['" + partition_granularity + "s(" + partition_fields[0] + ")'" +
                (", '" + "', '".join(partition_fields[1:]) + "'" if partition_fields[1:] else '') + "]",
            'write_compression': write_compression,
            'write_target_file_size_bytes': (target_file_size_mb * 1024 * 1024) | string,
            'write_parquet_compression_codec': write_compression,
            'write_parquet_compression_level': '9',
            'commit_num_retries': '3',
            'commit_retry_num_threads': '4',
            'write_metadata_delete_after_commit_enabled': 'true',
            'write_metadata_previous_versions_max': '20',
            'format_version': '2',
            'engine_hive_enabled': 'false',
            'write_parquet_bloom_filter_columns': "ARRAY['tenant_id', 'iso_code', 'market_code', 'product_code']",
            'write_parquet_bloom_filter_enabled': 'true',
            'write_parquet_page_size_bytes': '2097152',
            'write_parquet_row_group_size_bytes': '268435456',
            'write_parquet_dict_encoding_enabled': 'true',
            'write_parquet_plain_encoding_enabled': 'false',
            'write_parquet_rle_encoding_enabled': 'true',
            'write_parquet_bloom_filter_fpp': '0.005',
            'write_parquet_statistics_enabled': 'true',
            'write_parquet_statistics_columns': "ARRAY['" + "', '".join([
                'mid', 'bid', 'ask', 'price_total', 'price_energy', 'mw', 'value'
            ]) + "']"
        }
    )
}}
{%- endmacro -%}

{#-
    Configuration for raw/staging tables that need high write throughput
    and minimal processing overhead.

    Key characteristics:
    - High write frequency
    - Minimal read optimization needed
    - Temporary/transient data
    - ETL processing tables
-#}
{%- macro iceberg_config_staging(
    target_file_size_mb=64,
    write_compression='ZSTD'
) -%}
{{
    config(
        table_properties={
            'format': 'ICEBERG',
            'write_compression': write_compression,
            'write_target_file_size_bytes': (target_file_size_mb * 1024 * 1024) | string,
            'write_parquet_compression_codec': write_compression,
            'write_parquet_compression_level': '6',
            'commit_num_retries': '3',
            'commit_retry_num_threads': '2',
            'write_metadata_delete_after_commit_enabled': 'true',
            'write_metadata_previous_versions_max': '3',
            'format_version': '2',
            'engine_hive_enabled': 'false',
            'write_parquet_bloom_filter_enabled': 'false',
            'write_parquet_page_size_bytes': '1048576',
            'write_parquet_row_group_size_bytes': '33554432',
            'write_parquet_dict_encoding_enabled': 'true'
        }
    )
}}
{%- endmacro -%}

{#-
    Helper macro to generate optimized table properties based on data characteristics.
    This provides a unified interface for applying the appropriate configuration.
-#}
{%- macro iceberg_config_auto(
    table_type='timeseries',
    partition_fields=None,
    sort_columns=None,
    **kwargs
) -%}
{%- if table_type == 'timeseries' -%}
{{ iceberg_config_timeseries(partition_fields or ['asof_date'], **kwargs) }}
{%- elif table_type == 'dimension' -%}
{{ iceberg_config_dimension(sort_columns or ['tenant_id', 'provider', 'series_id'], **kwargs) }}
{%- elif table_type == 'external' -%}
{{ iceberg_config_external_observations(partition_fields or ['asof_date', 'provider'], **kwargs) }}
{%- elif table_type == 'fact' -%}
{{ iceberg_config_fact_table(partition_fields or ['asof_date'], **kwargs) }}
{%- elif table_type == 'staging' -%}
{{ iceberg_config_staging(**kwargs) }}
{%- else -%}
{{ iceberg_config_timeseries(partition_fields or ['asof_date'], **kwargs) }}
{%- endif -%}
{%- endmacro -%}
