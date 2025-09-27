CREATE OR REPLACE VIEW iceberg.market.iso_asm_unified AS
SELECT
  interval_start,
  interval_end,
  iso_code,
  market,
  product,
  zone,
  preliminary_final,
  price_mcp,
  currency,
  uom,
  ingest_ts,
  record_hash,
  metadata
FROM timescale.public.iso_asm_timeseries;

CREATE OR REPLACE VIEW iceberg.market.iso_pnode_registry AS
SELECT
  iso_code,
  pnode_id,
  pnode_name,
  type,
  zone,
  hub,
  effective_start,
  effective_end,
  ingest_ts
FROM timescale.public.iso_pnode_registry;

