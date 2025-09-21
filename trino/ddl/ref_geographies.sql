CREATE SCHEMA IF NOT EXISTS iceberg.ref;

CREATE TABLE IF NOT EXISTS iceberg.ref.geographies (
    region_type VARCHAR,
    region_id VARCHAR,
    region_name VARCHAR,
    parent_region_id VARCHAR,
    geometry GEOMETRY,
    centroid GEOMETRY,
    area_sq_km DOUBLE,
    crs VARCHAR,
    metadata MAP<VARCHAR, VARCHAR>,
    updated_at TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['region_type']
);


CREATE OR REPLACE VIEW iceberg.ref.geographies_state AS
SELECT * FROM iceberg.ref.geographies WHERE region_type = 'STATE';

CREATE OR REPLACE VIEW iceberg.ref.geographies_county AS
SELECT * FROM iceberg.ref.geographies WHERE region_type = 'COUNTY';

CREATE OR REPLACE VIEW iceberg.ref.geographies_iso_zone AS
SELECT * FROM iceberg.ref.geographies WHERE region_type = 'ISO_ZONE';

CREATE OR REPLACE VIEW iceberg.ref.geographies_iso_node AS
SELECT * FROM iceberg.ref.geographies WHERE region_type = 'ISO_NODE';

CREATE OR REPLACE VIEW iceberg.ref.geographies_huc2 AS
SELECT * FROM iceberg.ref.geographies WHERE region_type = 'HUC2';

CREATE OR REPLACE VIEW iceberg.ref.geographies_huc8 AS
SELECT * FROM iceberg.ref.geographies WHERE region_type = 'HUC8';
