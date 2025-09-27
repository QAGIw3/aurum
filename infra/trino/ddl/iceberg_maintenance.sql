-- Iceberg maintenance: compaction + snapshot retention for hot curve tables.
ALTER TABLE iceberg.market.curve_observation EXECUTE optimize;
ALTER TABLE iceberg.market.curve_observation EXECUTE expire_snapshots(retain_last => 30);

ALTER TABLE iceberg.fact.fct_curve_observation EXECUTE optimize;
ALTER TABLE iceberg.fact.fct_curve_observation EXECUTE expire_snapshots(retain_last => 30);

-- External contracts maintenance.
ALTER TABLE iceberg.external.timeseries_observation EXECUTE optimize;
ALTER TABLE iceberg.external.timeseries_observation
    EXECUTE rewrite_data_files(
        strategy => 'sort',
        sort_order => ARRAY['provider', 'series_id', 'ts']
    );
ALTER TABLE iceberg.external.series_catalog EXECUTE optimize;
