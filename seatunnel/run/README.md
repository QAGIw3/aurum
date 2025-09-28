# SeaTunnel Run Manifests

Use this directory to stage rendered SeaTunnel configs that will be executed in dev/prod. Render the new external sync jobs with:

```bash
scripts/seatunnel/run_job.sh external_series_catalog_kafka_to_iceberg --render-only \
  > seatunnel/run/external_series_catalog_kafka_to_iceberg.conf
scripts/seatunnel/run_job.sh external_timeseries_kafka_to_iceberg --render-only \
  > seatunnel/run/external_timeseries_kafka_to_iceberg.conf
```

Before rendering, export the required Iceberg, Kafka, and checkpoint environment variables documented in `seatunnel/README.md`.

Pair each rendered config with the checkpoint profile in `seatunnel/checkpoints/external_lake_checkpoint.properties` (copy and adjust values per environment) so the jobs resume from incremental offsets between restarts.
