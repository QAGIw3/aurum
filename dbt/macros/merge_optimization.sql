{#
    Optimized MERGE operation macros for Iceberg tables.

    These macros provide optimized MERGE operations for different scenarios:
    - Time-based incremental updates
    - Upsert operations with conflict resolution
    - Deduplication with row ranking
    - Late-arriving data handling

    Each macro handles the complexity of Iceberg MERGE operations while
    maintaining performance and consistency.
#}

{#-
    Macro for time-based incremental MERGE operations.
    Used for tables where data arrives in chronological order and we need
    to merge based on time windows with late-arriving data handling.
-#}
{%- macro merge_incremental_time_based(
    source_table,
    target_table,
    unique_key,
    time_column='ingest_ts',
    late_arriving_hours=6,
    partition_column='asof_date'
) -%}
{%- set source_relation = ref(source_table) -%}
{%- set target_relation = ref(target_table) -%}
{%- set time_filter = time_column + " >= (select coalesce(max(" + time_column + "), '1970-01-01 00:00:00') from " + target_relation + ")" %}

MERGE INTO {{ target_relation }} AS target
USING (
    SELECT *
    FROM {{ source_relation }}
    WHERE {{ time_filter }}
) AS source
ON {{ unique_key | join(' AND target.') | replace('target.', 'target.') }}
WHEN MATCHED AND source.{{ time_column }} > target.{{ time_column }}
    THEN UPDATE SET *
WHEN NOT MATCHED
    THEN INSERT *
{%- endmacro -%}

{#-
    Macro for upsert MERGE operations with conflict resolution.
    Used for reference tables where we need to handle updates and inserts
    with proper conflict resolution strategies.
-#}
{%- macro merge_upsert_with_conflict_resolution(
    source_table,
    target_table,
    unique_key,
    conflict_columns=[],
    update_strategy='latest_wins'
) -%}
{%- set source_relation = ref(source_table) -%}
{%- set target_relation = ref(target_table) -%}
{%- set conflict_condition = conflict_columns | join(' AND source.') | replace('source.', 'source.') if conflict_columns else 'FALSE' %}

MERGE INTO {{ target_relation }} AS target
USING {{ source_relation }} AS source
ON {{ unique_key | join(' AND target.') | replace('target.', 'target.') }}
WHEN MATCHED THEN
    UPDATE SET
    {%- for column in adapter.get_columns_in_relation(source_relation) %}
        {%- if column.name not in unique_key %}
            {{ column.name }} = CASE
                WHEN {{ conflict_condition }} THEN source.{{ column.name }}
                ELSE {{ update_strategy }}(target.{{ column.name }}, source.{{ column.name }})
            END
            {%- if not loop.last %}, {% endif %}
        {%- endif %}
    {%- endfor %}
WHEN NOT MATCHED THEN
    INSERT *
{%- endmacro -%}

{#-
    Macro for deduplication MERGE operations.
    Used when we need to handle duplicate records by keeping the most recent
    or highest quality version.
-#}
{%- macro merge_deduplicate(
    source_table,
    target_table,
    unique_key,
    ranking_columns=['ingest_ts DESC', 'quality_score DESC'],
    partition_column=None
) -%}
{%- set source_relation = ref(source_table) -%}
{%- set target_relation = ref(target_table) -%}
{%- set ranking = ranking_columns | join(', ') %}

MERGE INTO {{ target_relation }} AS target
USING (
    WITH ranked_source AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ unique_key | join(', ') }}
                ORDER BY {{ ranking }}
            ) as rn
        FROM {{ source_relation }}
        {%- if partition_column %}
        WHERE {{ partition_column }} >= CURRENT_DATE - INTERVAL '7 days'
        {%- endif %}
    )
    SELECT * FROM ranked_source WHERE rn = 1
) AS source
ON {{ unique_key | join(' AND target.') | replace('target.', 'target.') }}
WHEN MATCHED AND source.rn = 1 THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
{%- endmacro -%}

{#-
    Macro for late-arriving data MERGE operations.
    Specialized for handling data that arrives out of order with time-based
    conflict resolution.
-#}
{%- macro merge_late_arriving_data(
    source_table,
    target_table,
    unique_key,
    time_column='asof_date',
    grace_period_days=3,
    update_priority='source_wins'
) -%}
{%- set source_relation = ref(source_table) -%}
{%- set target_relation = ref(target_table) -%}
{%- set time_window = time_column + " >= CURRENT_DATE - INTERVAL '" + grace_period_days|string + " days'" %}

MERGE INTO {{ target_relation }} AS target
USING (
    SELECT *
    FROM {{ source_relation }}
    WHERE {{ time_window }}
) AS source
ON {{ unique_key | join(' AND target.') | replace('target.', 'target.') }}
WHEN MATCHED THEN
    UPDATE SET
    {%- for column in adapter.get_columns_in_relation(source_relation) %}
        {%- if column.name not in unique_key %}
            {{ column.name }} = CASE
                WHEN source.{{ time_column }} > target.{{ time_column }} THEN source.{{ column.name }}
                WHEN source.{{ time_column }} = target.{{ time_column }} AND {{ update_priority }} = 'source_wins' THEN source.{{ column.name }}
                ELSE target.{{ column.name }}
            END
            {%- if not loop.last %}, {% endif %}
        {%- endif %}
    {%- endfor %}
WHEN NOT MATCHED THEN
    INSERT *
{%- endmacro -%}

{#-
    Macro for batch MERGE operations with error handling.
    Used for high-volume batch processing with rollback capabilities.
-#}
{%- macro merge_batch_with_error_handling(
    source_table,
    target_table,
    unique_key,
    batch_size=10000,
    error_table='error_log',
    enable_transaction=true
) -%}
{%- set source_relation = ref(source_table) -%}
{%- set target_relation = ref(target_table) -%}
{%- set error_relation = ref(error_table) if error_table else 'NULL' %}

{%- if enable_transaction %}
BEGIN TRANSACTION;
{%- endif %}

-- Log the merge operation
INSERT INTO {{ error_relation }} (operation_type, source_table, target_table, operation_start)
VALUES ('MERGE', '{{ source_table }}', '{{ target_table }}', CURRENT_TIMESTAMP);

-- Perform the merge in batches
{%- for batch in range(0, (source_relation | length // batch_size) + 1) %}
{%- set offset = batch * batch_size %}

MERGE INTO {{ target_relation }} AS target
USING (
    SELECT *
    FROM {{ source_relation }}
    ORDER BY {{ unique_key | first }}
    LIMIT {{ batch_size }} OFFSET {{ offset }}
) AS source
ON {{ unique_key | join(' AND target.') | replace('target.', 'target.') }}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

{%- endfor %}

-- Log successful completion
UPDATE {{ error_relation }}
SET operation_end = CURRENT_TIMESTAMP,
    status = 'SUCCESS',
    records_processed = (SELECT COUNT(*) FROM {{ source_relation }})
WHERE operation_type = 'MERGE'
  AND source_table = '{{ source_table }}'
  AND target_table = '{{ target_table }}'
  AND operation_end IS NULL;

{%- if enable_transaction %}
COMMIT;
{%- endif %}

{%- endmacro -%}

{#-
    Helper macro to generate optimized unique key conditions for MERGE operations.
    Handles different data types and NULL value scenarios.
-#}
{%- macro merge_unique_key_condition(keys) -%}
{%- for key in keys %}
    {%- if key.endswith('_id') or key.endswith('_key') %}
        COALESCE(target.{{ key }}, '') = COALESCE(source.{{ key }}, '')
    {%- else %}
        target.{{ key }} = source.{{ key }}
    {%- endif %}
    {%- if not loop.last %} AND {% endif %}
{%- endfor %}
{%- endmacro -%}
