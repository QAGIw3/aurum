-- Helper macros for external data conformance layer

-- Convert units to canonical units
{% macro convert_unit(value, from_unit, to_unit) %}
    CASE
        WHEN {{ from_unit }} = 'USD/MWh' AND {{ to_unit }} = 'USD/MWh' THEN {{ value }}
        WHEN {{ from_unit }} = 'USD/MMBtu' AND {{ to_unit }} = 'USD/MWh' THEN {{ value }} * 0.293297
        WHEN {{ from_unit }} = 'USD/Mcf' AND {{ to_unit }} = 'USD/MMBtu' THEN {{ value }} * 0.001028
        WHEN {{ from_unit }} = 'EUR/MWh' AND {{ to_unit }} = 'USD/MWh' THEN {{ value }} * 1.1  -- Example EUR to USD conversion
        WHEN {{ from_unit }} = 'CAD/MWh' AND {{ to_unit }} = 'USD/MWh' THEN {{ value }} * 0.75  -- Example CAD to USD conversion
        ELSE {{ value }}  -- No conversion available, return as-is
    END
{% endmacro %}

-- Normalize frequency codes to canonical format
{% macro normalize_frequency(frequency_code) %}
    CASE
        WHEN {{ frequency_code }} IN ('A', 'ANNUAL', 'YEARLY') THEN 'ANNUAL'
        WHEN {{ frequency_code }} IN ('Q', 'QUARTERLY', 'QUARTER') THEN 'QUARTERLY'
        WHEN {{ frequency_code }} IN ('M', 'MONTHLY', 'MONTH') THEN 'MONTHLY'
        WHEN {{ frequency_code }} IN ('W', 'WEEKLY', 'WEEK') THEN 'WEEKLY'
        WHEN {{ frequency_code }} IN ('D', 'DAILY', 'DAY') THEN 'DAILY'
        WHEN {{ frequency_code }} IN ('H', 'HOURLY', 'HOUR') THEN 'HOURLY'
        ELSE 'OTHER'
    END
{% endmacro %}

-- Normalize timezone to UTC
{% macro normalize_timezone(timestamp_field, source_tz) %}
    CASE
        WHEN {{ source_tz }} = 'UTC' THEN {{ timestamp_field }}
        WHEN {{ source_tz }} = 'EST' THEN {{ timestamp_field }} + INTERVAL '5 hours'
        WHEN {{ source_tz }} = 'CST' THEN {{ timestamp_field }} + INTERVAL '6 hours'
        WHEN {{ source_tz }} = 'PST' THEN {{ timestamp_field }} + INTERVAL '8 hours'
        WHEN {{ source_tz }} = 'GMT' THEN {{ timestamp_field }}
        ELSE {{ timestamp_field }}  -- Assume UTC if unknown
    END
{% endmacro %}

-- Validate that values are within plausible ranges by provider
{% macro validate_value_range(value, provider, unit_code) %}
    CASE
        WHEN {{ provider }} = 'EIA' AND {{ unit_code }} = 'USD/MWh' AND ({{ value }} < 0 OR {{ value }} > 10000) THEN NULL
        WHEN {{ provider }} = 'FRED' AND {{ unit_code }} = 'USD/MWh' AND ({{ value }} < -1000 OR {{ value }} > 10000) THEN NULL
        WHEN {{ provider }} = 'EIA' AND {{ unit_code }} = 'USD/MMBtu' AND ({{ value }} < 0 OR {{ value }} > 100) THEN NULL
        WHEN {{ provider }} = 'FRED' AND {{ unit_code }} = 'USD/MMBtu' AND ({{ value }} < 0 OR {{ value }} > 100) THEN NULL
        ELSE {{ value }}
    END
{% endmacro %}
