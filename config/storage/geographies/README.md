# Geography Sources

Place canonical geometry exports (GeoJSON, Parquet, or Geopackage) for drought ingestion under this directory.  Airflow backfills expect matching filenames:

- `states.geojson` – US states (FIPS codes, EPSG:4326)
- `counties.geojson` – US counties (FIPS codes, EPSG:4326)
- `iso_zones.geojson` – ISO market zones keyed by Aurum `iso_zone`
- `iso_nodes.geojson` – ISO nodes keyed by Aurum `iso_node`
- `huc2.geojson` – USGS HUC2 polygons (EPSG:4269)
- `huc8.geojson` – USGS HUC8 polygons (EPSG:4269)

During ingestion these files are projected to EPSG:5070 for area calculations and written to `iceberg.ref.geographies`.
