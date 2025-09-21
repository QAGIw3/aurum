-- Iceberg maintenance: compaction + snapshot retention for hot curve tables.
ALTER TABLE iceberg.market.curve_observation EXECUTE optimize;
ALTER TABLE iceberg.market.curve_observation EXECUTE expire_snapshots(retain_last => 30);

ALTER TABLE iceberg.fact.fct_curve_observation EXECUTE optimize;
ALTER TABLE iceberg.fact.fct_curve_observation EXECUTE expire_snapshots(retain_last => 30);
