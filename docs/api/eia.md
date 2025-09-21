# EIA API Examples

The Aurum API exposes enriched EIA v2 observations and metadata. All responses are cached in-memory using `AURUM_API_INMEMORY_TTL` and mirrored into Redis when configured.

## Catalog lookup

```bash
curl -s "http://localhost:8000/v1/metadata/eia/datasets?prefix=natural-gas/" | jq '.data[:3]'
```

## Series observations

Fetch the latest observations for a balancing-authority interchange series with canonical units and pagination:

```bash
curl -s \
  "http://localhost:8000/v1/ref/eia/series?series_id=EBA.CAL-NEVP.ID.H&limit=100" \
  | jq '.data[0]'
```

## Facet metadata

Return the available dimensions for filtering (dataset, area, sector, units, source):

```bash
curl -s "http://localhost:8000/v1/ref/eia/series/dimensions?frequency=HOURLY" | jq '.data'
```

## Units mapping

List canonical currency/unit pairs and raw-to-normalized mappings:

```bash
curl -s "http://localhost:8000/v1/metadata/units/mapping?prefix=USD/" | jq '.data[:5]'
```

## Latest-mart view via Trino

Query the curated mart created by dbt for the latest value per series:

```sql
SELECT series_id, period_start, value, currency_normalized, unit_normalized
FROM mart.mart_eia_series_latest
WHERE dataset = 'electricity/rto/daily-region-data'
ORDER BY period_start DESC
LIMIT 10;
```

Use the workflow outlined in `README.md` to run `make trino-apply-eia` and `dbt run` before executing the Trino SQL.
