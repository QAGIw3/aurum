-- Seed canonical dimension tables for external lakehouse integration.

MERGE INTO iceberg.external.unit AS target
USING (
    VALUES
        ('PCT', 'Percent', '%', 'dimensionless', 'Percentage expressed as a decimal fraction.', 0.01, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('USD', 'US Dollar', '$', 'currency', 'United States dollar.', NULL, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('MW', 'Megawatt', 'MW', 'power', 'One million watts.', 1000000.0, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('MWH', 'Megawatt hour', 'MWh', 'energy', 'Energy produced by one megawatt for one hour.', 3600000000.0, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('MMBTU', 'Million British thermal units', 'MMBtu', 'energy', 'One million BTUs.', 1055055852.62, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('DEGC', 'Degree Celsius', 'degC', 'temperature', 'Temperature in degrees Celsius.', 1.0, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')
) AS source (unit_code, unit_name, unit_symbol, quantity, description, si_conversion_factor, created_at, updated_at)
    ON target.unit_code = source.unit_code
WHEN MATCHED AND (
       target.unit_name IS DISTINCT FROM source.unit_name
    OR target.unit_symbol IS DISTINCT FROM source.unit_symbol
    OR target.quantity IS DISTINCT FROM source.quantity
    OR target.description IS DISTINCT FROM source.description
    OR target.si_conversion_factor IS DISTINCT FROM source.si_conversion_factor
    OR target.updated_at IS DISTINCT FROM source.updated_at
)
THEN UPDATE SET
    unit_name = source.unit_name,
    unit_symbol = source.unit_symbol,
    quantity = source.quantity,
    description = source.description,
    si_conversion_factor = source.si_conversion_factor,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    unit_code,
    unit_name,
    unit_symbol,
    quantity,
    description,
    si_conversion_factor,
    created_at,
    updated_at
) VALUES (
    source.unit_code,
    source.unit_name,
    source.unit_symbol,
    source.quantity,
    source.description,
    source.si_conversion_factor,
    source.created_at,
    source.updated_at
);

-- Frequencies
MERGE INTO iceberg.external.frequency AS target
USING (
    VALUES
        ('DAILY', '1 day', 86400, 'Daily published series.', 'calendar', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('WEEKLY', '7 days', 604800, 'Weekly published series.', 'calendar', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('MONTHLY', '1 month', 2592000, 'Monthly published series.', 'calendar', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('QUARTERLY', '3 months', 7776000, 'Quarterly published series.', 'calendar', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('ANNUAL', '1 year', 31536000, 'Annual published series.', 'calendar', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')
) AS source (frequency_code, interval_label, interval_seconds, description, calendar_alignment, created_at, updated_at)
    ON target.frequency_code = source.frequency_code
WHEN MATCHED AND (
       target.interval_label IS DISTINCT FROM source.interval_label
    OR target.interval_seconds IS DISTINCT FROM source.interval_seconds
    OR target.description IS DISTINCT FROM source.description
    OR target.calendar_alignment IS DISTINCT FROM source.calendar_alignment
    OR target.updated_at IS DISTINCT FROM source.updated_at
)
THEN UPDATE SET
    interval_label = source.interval_label,
    interval_seconds = source.interval_seconds,
    description = source.description,
    calendar_alignment = source.calendar_alignment,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    frequency_code,
    interval_label,
    interval_seconds,
    description,
    calendar_alignment,
    created_at,
    updated_at
) VALUES (
    source.frequency_code,
    source.interval_label,
    source.interval_seconds,
    source.description,
    source.calendar_alignment,
    source.created_at,
    source.updated_at
);

-- Geography seeds
MERGE INTO iceberg.external.geo AS target
USING (
    VALUES
        ('US', 'COUNTRY', 'UN', 'US', 'USA', NULL, 'United States', NULL, 37.0902, -95.7129, 'America/New_York', JSON '{"iso_level":"country"}', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('CA', 'COUNTRY', 'UN', 'CA', 'CAN', NULL, 'Canada', NULL, 56.1304, -106.3468, 'America/Toronto', JSON '{"iso_level":"country"}', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('MX', 'COUNTRY', 'UN', 'MX', 'MEX', NULL, 'Mexico', NULL, 23.6345, -102.5528, 'America/Mexico_City', JSON '{"iso_level":"country"}', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')
) AS source (geo_id, geo_type, provider, iso_3166_1_alpha2, iso_3166_1_alpha3, iso_3166_2, name, parent_geo_id, latitude, longitude, timezone, metadata, created_at, updated_at)
    ON target.geo_id = source.geo_id
WHEN MATCHED AND (
       target.geo_type IS DISTINCT FROM source.geo_type
    OR target.provider IS DISTINCT FROM source.provider
    OR target.iso_3166_1_alpha2 IS DISTINCT FROM source.iso_3166_1_alpha2
    OR target.iso_3166_1_alpha3 IS DISTINCT FROM source.iso_3166_1_alpha3
    OR target.iso_3166_2 IS DISTINCT FROM source.iso_3166_2
    OR target.name IS DISTINCT FROM source.name
    OR target.parent_geo_id IS DISTINCT FROM source.parent_geo_id
    OR target.latitude IS DISTINCT FROM source.latitude
    OR target.longitude IS DISTINCT FROM source.longitude
    OR target.timezone IS DISTINCT FROM source.timezone
    OR target.metadata IS DISTINCT FROM source.metadata
    OR target.updated_at IS DISTINCT FROM source.updated_at
)
THEN UPDATE SET
    geo_type = source.geo_type,
    provider = source.provider,
    iso_3166_1_alpha2 = source.iso_3166_1_alpha2,
    iso_3166_1_alpha3 = source.iso_3166_1_alpha3,
    iso_3166_2 = source.iso_3166_2,
    name = source.name,
    parent_geo_id = source.parent_geo_id,
    latitude = source.latitude,
    longitude = source.longitude,
    timezone = source.timezone,
    metadata = source.metadata,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    geo_id,
    geo_type,
    provider,
    iso_3166_1_alpha2,
    iso_3166_1_alpha3,
    iso_3166_2,
    name,
    parent_geo_id,
    latitude,
    longitude,
    timezone,
    metadata,
    created_at,
    updated_at
) VALUES (
    source.geo_id,
    source.geo_type,
    source.provider,
    source.iso_3166_1_alpha2,
    source.iso_3166_1_alpha3,
    source.iso_3166_2,
    source.name,
    source.parent_geo_id,
    source.latitude,
    source.longitude,
    source.timezone,
    source.metadata,
    source.created_at,
    source.updated_at
);

-- Datasets
MERGE INTO iceberg.external.dataset AS target
USING (
    VALUES
        ('H15', 'FRED', 'H.15 Selected Interest Rates', 'Federal Reserve statistical release covering selected interest rates.', 'macro', 'DAILY', 'PCT', 'https://fred.stlouisfed.org/release?rid=119', 'https://www.federalreserve.gov/releases/h15.htm', 'Public Domain', JSON '{"category":"interest_rates"}', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('STEO', 'EIA', 'Short-Term Energy Outlook', 'EIA short-term energy outlook series.', 'energy', 'MONTHLY', 'USD', 'https://www.eia.gov/outlooks/steo/', 'https://www.eia.gov/outlooks/steo/documentation/', 'Public Domain', JSON '{"category":"energy_outlook"}', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
        ('GHCND', 'NOAA', 'Global Historical Climatology Network Daily', 'Daily weather observations published by NOAA.', 'weather', 'DAILY', 'DEGC', 'https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily', 'https://www.ncdc.noaa.gov/ghcnd-data-access', 'Public Domain', JSON '{"category":"climate"}', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')
) AS source (dataset_code, provider, name, description, topic, default_frequency_code, default_unit_code, source_url, documentation_url, license, metadata, created_at, updated_at)
    ON target.dataset_code = source.dataset_code
WHEN MATCHED AND (
       target.provider IS DISTINCT FROM source.provider
    OR target.name IS DISTINCT FROM source.name
    OR target.description IS DISTINCT FROM source.description
    OR target.topic IS DISTINCT FROM source.topic
    OR target.default_frequency_code IS DISTINCT FROM source.default_frequency_code
    OR target.default_unit_code IS DISTINCT FROM source.default_unit_code
    OR target.source_url IS DISTINCT FROM source.source_url
    OR target.documentation_url IS DISTINCT FROM source.documentation_url
    OR target.license IS DISTINCT FROM source.license
    OR target.metadata IS DISTINCT FROM source.metadata
    OR target.updated_at IS DISTINCT FROM source.updated_at
)
THEN UPDATE SET
    provider = source.provider,
    name = source.name,
    description = source.description,
    topic = source.topic,
    default_frequency_code = source.default_frequency_code,
    default_unit_code = source.default_unit_code,
    source_url = source.source_url,
    documentation_url = source.documentation_url,
    license = source.license,
    metadata = source.metadata,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    dataset_code,
    provider,
    name,
    description,
    topic,
    default_frequency_code,
    default_unit_code,
    source_url,
    documentation_url,
    license,
    metadata,
    created_at,
    updated_at
) VALUES (
    source.dataset_code,
    source.provider,
    source.name,
    source.description,
    source.topic,
    source.default_frequency_code,
    source.default_unit_code,
    source.source_url,
    source.documentation_url,
    source.license,
    source.metadata,
    source.created_at,
    source.updated_at
);

-- Sanity checks
SELECT 'unit' AS table_name, COUNT(*) AS row_count FROM iceberg.external.unit;
SELECT 'frequency' AS table_name, COUNT(*) AS row_count FROM iceberg.external.frequency;
SELECT 'geo' AS table_name, COUNT(*) AS row_count FROM iceberg.external.geo;
SELECT 'dataset' AS table_name, COUNT(*) AS row_count FROM iceberg.external.dataset;
