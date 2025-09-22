#!/usr/bin/env python
"""Generate SeaTunnel configuration for ISO-NE data types and execute ingestion."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any


def get_lmp_avro_schema() -> str:
    """Get Avro schema for LMP records."""
    return json.dumps({
        "type": "record",
        "name": "IsoLmpRecord",
        "namespace": "aurum.iso",
        "fields": [
            {"name": "iso_code", "type": "string"},
            {"name": "market", "type": "string"},
            {"name": "delivery_date", "type": "int"},
            {"name": "interval_start", "type": "long"},
            {"name": "interval_end", "type": "long"},
            {"name": "interval_minutes", "type": "int"},
            {"name": "location_id", "type": "string"},
            {"name": "location_name", "type": "string"},
            {"name": "location_type", "type": "string"},
            {"name": "price_total", "type": "double"},
            {"name": "price_energy", "type": ["null", "double"], "default": None},
            {"name": "price_congestion", "type": ["null", "double"], "default": None},
            {"name": "price_loss", "type": ["null", "double"], "default": None},
            {"name": "currency", "type": "string"},
            {"name": "uom", "type": "string"},
            {"name": "settlement_point", "type": "string"},
            {"name": "source_run_id", "type": ["null", "string"], "default": None},
            {"name": "ingest_ts", "type": "long"},
            {"name": "record_hash", "type": "string"},
            {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
        ]
    })


def get_load_avro_schema() -> str:
    """Get Avro schema for load records."""
    return json.dumps({
        "type": "record",
        "name": "IsoLoadRecord",
        "namespace": "aurum.iso",
        "fields": [
            {"name": "iso_code", "type": "string"},
            {"name": "area", "type": ["null", "string"], "default": None},
            {"name": "interval_start", "type": "long"},
            {"name": "interval_end", "type": ["null", "long"], "default": None},
            {"name": "interval_minutes", "type": ["null", "int"], "default": None},
            {"name": "mw", "type": "double"},
            {"name": "ingest_ts", "type": "long"},
            {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
        ]
    })


def get_genmix_avro_schema() -> str:
    """Get Avro schema for generation mix records."""
    return json.dumps({
        "type": "record",
        "name": "IsoGenerationMixRecord",
        "namespace": "aurum.iso",
        "fields": [
            {"name": "iso_code", "type": "string"},
            {"name": "asof_time", "type": "long"},
            {"name": "fuel_type", "type": "string"},
            {"name": "mw", "type": "double"},
            {"name": "unit", "type": "string"},
            {"name": "ingest_ts", "type": "long"},
            {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
        ]
    })


def get_asm_avro_schema() -> str:
    """Get Avro schema for ancillary services records."""
    return json.dumps({
        "type": "record",
        "name": "IsoAsmRecord",
        "namespace": "aurum.iso",
        "fields": [
            {"name": "iso_code", "type": "string"},
            {"name": "market", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "zone", "type": "string"},
            {"name": "preliminary_final", "type": ["null", "string"], "default": None},
            {"name": "interval_start", "type": "long"},
            {"name": "interval_end", "type": ["null", "long"], "default": None},
            {"name": "interval_minutes", "type": ["null", "int"], "default": None},
            {"name": "price_mcp", "type": ["null", "double"], "default": None},
            {"name": "currency", "type": "string"},
            {"name": "uom", "type": "string"},
            {"name": "ingest_ts", "type": "long"},
            {"name": "record_hash", "type": "string"},
            {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
        ]
    })


def get_data_type_config(data_type: str, market: str) -> Dict[str, Any]:
    """Get configuration for specific data type."""
    configs = {
        "lmp": {
            "topic": "aurum.iso.isone.lmp.v1",
            "avro_schema": get_lmp_avro_schema()
        },
        "load": {
            "topic": "aurum.iso.isone.load.v1",
            "avro_schema": get_load_avro_schema()
        },
        "generation_mix": {
            "topic": "aurum.iso.isone.genmix.v1",
            "avro_schema": get_genmix_avro_schema()
        },
        "ancillary_services": {
            "topic": "aurum.iso.isone.asm.v1",
            "avro_schema": get_asm_avro_schema()
        }
    }
    return configs.get(data_type, {})


def generate_seatunnel_config(args: argparse.Namespace) -> str:
    """Generate SeaTunnel configuration based on arguments."""
    # Determine data type from source name
    data_type_map = {
        "lmp": ["lmp_rtm", "lmp_dam"],
        "load": ["load_rtm"],
        "generation_mix": ["generation_mix"],
        "ancillary_services": ["ancillary_services"]
    }

    data_type = None
    for dt, sources in data_type_map.items():
        for source in sources:
            if source in args.source:
                data_type = dt
                break
        if data_type:
            break

    if not data_type:
        raise ValueError(f"Unknown data type for source: {args.source}")

    # Get configuration for data type
    config = get_data_type_config(data_type, args.market or "ALL")

    # Generate template
    template = """
# ISO-NE {{ data_type }} → Kafka (Avro)
#
# Generated configuration for {{ source_name }}
# Required environment variables:
#   ISONE_URL               - ISO-NE web service endpoint
#   ISONE_START             - Start datetime
#   ISONE_END               - End datetime
#   ISONE_MARKET            - Market type (DAM/RTM)
#   AURUM_KAFKA_BOOTSTRAP_SERVERS - Kafka brokers
#   AURUM_SCHEMA_REGISTRY_URL     - Schema Registry endpoint

env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Http {
    url = "{{ url }}"
    method = "GET"
    params {
      start = "{{ start }}"
      end = "{{ end }}"
      market = "{{ market }}"
    }
    connection_timeout_ms = 20000
    retry = 10
    format = "json"
    schema = { fields {
      begin = string
      end = string
      ptid = string
      location_id = string
      name = string
      location_name = string
      lmp = string
      energy = string
      congestion = string
      loss = string
      load = string
      fuel_type = string
      generation = string
      service_type = string
      price = string
      mw = string
    } }
    jsonpath = "$.items[*]"
    headers {
      Accept = "application/json"
      "X-API-Key" = "{{ api_key }}"
    }
    result_table_name = "isone_raw"
  }
}

transform {
  Sql {
    source_table_name = "isone_raw"
    result_table_name = "isone_normalized"
    query = """
      WITH prepared AS (
        SELECT
          COALESCE(begin, startTime, start_time) AS begin_raw,
          COALESCE(end, endTime, end_time) AS end_raw,
          COALESCE(ptid, location_id) AS node_id,
          COALESCE(name, location_name) AS node_name,
          CAST(lmp AS DOUBLE) AS price_total,
          CAST(energy AS DOUBLE) AS price_energy,
          CAST(congestion AS DOUBLE) AS price_congestion,
          CAST(loss AS DOUBLE) AS price_loss,
          CAST(load AS DOUBLE) AS load_mw,
          fuel_type,
          CAST(generation AS DOUBLE) AS generation_mw,
          service_type,
          CAST(price AS DOUBLE) AS service_price,
          CAST(mw AS DOUBLE) AS service_mw
        FROM isone_raw
      ), converted AS (
        SELECT
          begin_raw,
          end_raw,
          node_id,
          node_name,
          price_total,
          price_energy,
          price_congestion,
          price_loss,
          load_mw,
          fuel_type,
          generation_mw,
          service_type,
          service_price,
          service_mw,
          CAST(UNIX_TIMESTAMP(SUBSTRING(begin_raw, 1, 19), 'yyyy-MM-dd''T''HH:mm:ss') * 1000000 AS BIGINT) AS interval_start,
          CAST(UNIX_TIMESTAMP(SUBSTRING(end_raw, 1, 19), 'yyyy-MM-dd''T''HH:mm:ss') * 1000000 AS BIGINT) AS interval_end
        FROM prepared
      )
      SELECT
        {% if data_type == 'lmp' %}
        'ISONE' AS iso_code,
        CASE
          WHEN upper('{{ market }}') LIKE 'DA%' THEN 'DAY_AHEAD'
          ELSE 'REAL_TIME'
        END AS market,
        DATEDIFF(TO_DATE(SUBSTRING(c.begin_raw, 1, 10)), TO_DATE('1970-01-01')) AS delivery_date,
        interval_start,
        interval_end,
        CAST((interval_end - interval_start) / 60000000 AS INT) AS interval_minutes,
        CAST(c.node_id AS STRING) AS location_id,
        CAST(c.node_name AS STRING) AS location_name,
        'NODE' AS location_type,
        COALESCE(c.price_total, 0.0) AS price_total,
        c.price_energy AS price_energy,
        c.price_congestion AS price_congestion,
        c.price_loss AS price_loss,
        'USD' AS currency,
        'MWh' AS uom,
        CAST(c.node_name AS STRING) AS settlement_point,
        NULL AS source_run_id,
        CAST(UNIX_TIMESTAMP() * 1000000 AS BIGINT) AS ingest_ts,
        SHA2(CONCAT_WS('|', c.begin_raw, CAST(c.node_id AS STRING), CAST(c.price_total AS STRING)), 256) AS record_hash,
        NULL AS metadata
        {% elif data_type == 'load' %}
        'ISONE' AS iso_code,
        'ALL' AS area,
        interval_start,
        interval_end,
        CAST((interval_end - interval_start) / 60000000 AS INT) AS interval_minutes,
        COALESCE(c.load_mw, 0.0) AS mw,
        CAST(UNIX_TIMESTAMP() * 1000000 AS BIGINT) AS ingest_ts,
        NULL AS metadata
        {% elif data_type == 'generation_mix' %}
        'ISONE' AS iso_code,
        CAST(UNIX_TIMESTAMP(SUBSTRING(c.begin_raw, 1, 19), 'yyyy-MM-dd''T''HH:mm:ss') * 1000000 AS BIGINT) AS asof_time,
        CAST(c.fuel_type AS STRING) AS fuel_type,
        COALESCE(c.generation_mw, 0.0) AS mw,
        'MW' AS unit,
        CAST(UNIX_TIMESTAMP() * 1000000 AS BIGINT) AS ingest_ts,
        NULL AS metadata
        {% elif data_type == 'ancillary_services' %}
        'ISONE' AS iso_code,
        '{{ market }}' AS market,
        CAST(c.service_type AS STRING) AS product,
        CAST(c.node_id AS STRING) AS zone,
        NULL AS preliminary_final,
        interval_start,
        interval_end,
        CAST((interval_end - interval_start) / 60000000 AS INT) AS interval_minutes,
        COALESCE(c.service_price, 0.0) AS price_mcp,
        'USD' AS currency,
        'MWh' AS uom,
        CAST(UNIX_TIMESTAMP() * 1000000 AS BIGINT) AS ingest_ts,
        SHA2(CONCAT_WS('|', c.begin_raw, CAST(c.node_id AS STRING), CAST(c.service_type AS STRING)), 256) AS record_hash,
        NULL AS metadata
        {% endif %}
      FROM converted c
      WHERE interval_start IS NOT NULL
    """
  }
}

sink {
  Kafka {
    plugin_input = "isone_normalized"
    bootstrap.servers = "{{ kafka_bootstrap }}"
    topic = "{{ topic }}"
    semantic = "AT_LEAST_ONCE"
    format = "avro"
    avro {
      use.schema.registry = true
      schema.registry.url = "{{ schema_registry }}"
      value.schema.subject = "{{ subject }}"
      value.schema = """{{ avro_schema }}"""
    }
    producer {
      request.timeout.ms = 30000
      delivery.timeout.ms = 120000
      retry.backoff.max.ms = 10000
      retry.backoff.ms = 100
      acks = "all"
      enable.idempotence = true
      max.in.flight.requests.per.connection = 5
      linger.ms = 500
      batch.size = 32768
      retries = 5
    }
  }
}
"""

    return template.format(
        data_type=data_type,
        source_name=args.source,
        url=args.url,
        start=args.start,
        end=args.end,
        market=args.market or "ALL",
        kafka_bootstrap=args.kafka_bootstrap,
        schema_registry=args.schema_registry,
        topic=config["topic"],
        subject=f"{config['topic']}-value",
        api_key=args.api_key or "",
        avro_schema=config["avro_schema"]
    )


def main():
    parser = argparse.ArgumentParser(description="Generate ISO-NE SeaTunnel configuration")
    parser.add_argument("--url", required=True, help="ISO-NE endpoint URL")
    parser.add_argument("--start", required=True, help="Start datetime")
    parser.add_argument("--end", required=True, help="End datetime")
    parser.add_argument("--source", required=True, help="Source name")
    parser.add_argument("--market", default="ALL", help="Market identifier")
    parser.add_argument("--kafka-bootstrap", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--schema-registry", required=True, help="Schema registry URL")
    parser.add_argument("--api-key", help="ISO-NE API key")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--execute-seatunnel", action="store_true", help="Execute SeaTunnel job")

    args = parser.parse_args()

    # Generate configuration
    config = generate_seatunnel_config(args)

    # Write to file or stdout
    if args.output:
        with open(args.output, "w") as f:
            f.write(config)
        print(f"Generated SeaTunnel configuration: {args.output}")
    else:
        print(config)

    # Execute SeaTunnel if requested
    if args.execute_seatunnel:
        import subprocess
        import tempfile

        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(config)
            temp_file = f.name

        try:
            # Execute SeaTunnel
            cmd = ["seatunnel", "--config", temp_file]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                print("SeaTunnel job completed successfully")
                print(result.stdout)
            else:
                print("SeaTunnel job failed")
                print(result.stderr)
                exit(1)
        finally:
            import os
            os.unlink(temp_file)


if __name__ == "__main__":
    main()