from __future__ import annotations

"""Shared helpers for drought ingestion pipelines."""

import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import geopandas as gpd
import requests
from shapely import wkt

from .catalog import DroughtCatalog, RasterIndexDataset, load_catalog
from .fetcher import AssetFetcher, HttpAsset
from .zonal import ZonalStatisticsEngine

from aurum.api.config import TrinoConfig

import logging

logger = logging.getLogger(__name__)

try:  # Optional dependency
    import trino
except ImportError:  # pragma: no cover - optional
    trino = None  # type: ignore[assignment]

try:  # Optional dependency
    from confluent_kafka import avro
    from confluent_kafka.avro import AvroProducer
except Exception:  # pragma: no cover - optional
    avro = None  # type: ignore[assignment]
    AvroProducer = None  # type: ignore[assignment]


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    schema_registry_url: str
    topic: str
    schema_file: Path


def discover_raster_workload(
    logical_date: date,
    catalog_path: Path,
    datasets: Optional[Iterable[str]] = None,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Determine which raster assets to process for a run."""

    catalog = load_catalog(catalog_path)
    selected = {name.lower() for name in datasets} if datasets else None
    session = session or requests.Session()

    workload: List[Dict[str, Any]] = []
    for dataset in catalog.raster_indices:
        if selected and dataset.dataset.lower() not in selected:
            continue
        for index in dataset.indices:
            for timescale in dataset.timescales:
                resolved = _resolve_latest_asset(dataset, index, timescale, logical_date, session)
                if not resolved:
                    logger.warning(
                        "No asset available within latency window",
                        extra={
                            "dataset": dataset.dataset,
                            "index": index,
                            "timescale": timescale,
                            "logical_date": logical_date.isoformat(),
                        },
                    )
                    continue
                workload.append(resolved)
    logger.info("Discovered %s raster tasks", len(workload))
    return workload


def process_raster_asset(
    task: Mapping[str, Any],
    catalog_path: Path,
    trino_config: TrinoConfig,
    kafka_config: KafkaConfig,
    workdir: Path,
    job_id: str,
    tenant_id: str = "aurum",
    schema_version: str = "1",
) -> Dict[str, Any]:
    """Download a raster, compute zonal statistics, and publish to Kafka."""

    if AvroProducer is None or avro is None:  # pragma: no cover - runtime guard
        raise RuntimeError("confluent-kafka[avro] is required for drought ingestion")
    if trino is None:
        raise RuntimeError("trino python client is required for drought ingestion")

    catalog = load_catalog(catalog_path)
    dataset = catalog.raster_by_name(task["dataset"])
    valid_date = date.fromisoformat(task["valid_date"])
    index = task["index"]
    timescale = task["timescale"]
    info_url = task["info_url"]
    geotiff_url = task["geotiff_url"]

    info_payload = _fetch_json(info_url)

    workdir.mkdir(parents=True, exist_ok=True)
    fetcher = AssetFetcher(workdir)
    asset_path = workdir / f"{dataset.dataset}_{index}_{timescale}_{valid_date}.tif"
    fetch_result = fetcher.fetch(HttpAsset(url=geotiff_url, destination=asset_path))
    logger.info(
        "Downloaded raster",
        extra={
            "dataset": dataset.dataset,
            "index": index,
            "timescale": timescale,
            "path": str(fetch_result.path),
            "cache": fetch_result.from_cache,
            "bytes": fetch_result.bytes_downloaded,
        },
    )

    geodf = _load_geographies(trino_config, [region.region_type for region in catalog.region_sets])
    engine = ZonalStatisticsEngine()
    stats = engine.compute(fetch_result.path, geodf)

    records: List[Dict[str, Any]] = []
    ingest_ts = _timestamp_micros(datetime.now(timezone.utc))
    as_of_ts = _extract_timestamp(info_payload) or ingest_ts

    for row in stats:
        series_id = _series_id(dataset.dataset, index, timescale, row["region_type"], row["region_id"])
        metadata = {
            "info_url": info_url,
            "geotiff_url": geotiff_url,
            "valid_fraction": f"{row.get('valid_fraction', 0):.6f}",
        }
        metadata.update({k: str(v) for k, v in task.get("metadata", {}).items()})

        records.append(
            {
                "tenant_id": tenant_id,
                "schema_version": schema_version,
                "ingest_ts": ingest_ts,
                "ingest_job_id": job_id,
                "series_id": series_id,
                "region_type": row["region_type"],
                "region_id": row["region_id"],
                "dataset": dataset.dataset,
                "index": index,
                "timescale": timescale,
                "valid_date": valid_date,
                "as_of": as_of_ts,
                "value": row.get("value"),
                "unit": _unit_for_index(index),
                "poc": dataset.provider,
                "source_url": geotiff_url,
                "metadata": metadata,
            }
        )

    if not records:
        logger.warning(
            "No raster records computed",
            extra={"dataset": dataset.dataset, "index": index, "timescale": timescale, "valid_date": valid_date.isoformat()},
        )
        return {"records": 0, "bytes_downloaded": fetch_result.bytes_downloaded}

    producer = AvroProducer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "schema.registry.url": kafka_config.schema_registry_url,
        },
        default_value_schema=avro.load(str(kafka_config.schema_file)),
    )

    for record in records:
        cleaned = _encode_metadata(record)
        producer.produce(topic=kafka_config.topic, value=cleaned)
    producer.flush()

    logger.info(
        "Published drought index records",
        extra={
            "dataset": dataset.dataset,
            "index": index,
            "timescale": timescale,
            "count": len(records),
        },
    )
    return {"records": len(records), "bytes_downloaded": fetch_result.bytes_downloaded}


def _resolve_latest_asset(
    dataset: RasterIndexDataset,
    index: str,
    timescale: str,
    logical_date: date,
    session: requests.Session,
) -> Optional[Dict[str, Any]]:
    max_window = dataset.latency.maximum_days or 0
    for offset in range(max_window + 1):
        candidate = logical_date - timedelta(days=offset)
        slug = candidate.strftime("%Y-%m-%d")
        info_url = dataset.info_url(index, timescale, slug)
        geotiff_url = dataset.geotiff_url(index, timescale, slug)
        if _head_ok(session, info_url):
            try:
                payload = _fetch_json(info_url, session=session)
            except Exception:
                logger.debug("Failed to fetch info.json", exc_info=True)
                continue
            valid_date = _extract_valid_date(payload) or candidate
            return {
                "dataset": dataset.dataset,
                "index": index,
                "timescale": timescale,
                "valid_date": valid_date.isoformat(),
                "info_url": info_url,
                "geotiff_url": geotiff_url,
                "metadata": {
                    "legend_url": dataset.legend_url(index, timescale, slug) or "",
                    "zoom_levels": ",".join(str(z) for z in dataset.zoom_levels()),
                },
            }
    return None


def _head_ok(session: requests.Session, url: str) -> bool:
    try:
        response = session.head(url, timeout=15, allow_redirects=True)
        if response.status_code == 200:
            return True
        if response.status_code in {403, 404}:  # Some GCS buckets disallow HEAD; try GET
            response = session.get(url, timeout=15, stream=True)
            response.close()
            return response.status_code == 200
    except requests.RequestException:
        logger.debug("HEAD request failed", exc_info=True)
    return False


def _fetch_json(url: str, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    client = session or requests.Session()
    response = client.get(url, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from {url}")
    return payload


def _extract_valid_date(payload: Mapping[str, Any]) -> Optional[date]:
    for key in ("valid_date", "validDate", "endDate", "date", "timestamp"):
        value = payload.get(key)
        if isinstance(value, str):
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                continue
    return None


def _extract_timestamp(payload: Mapping[str, Any]) -> Optional[int]:
    for key in ("valid_time", "validTime", "endTime", "timestamp"):
        value = payload.get(key)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return _timestamp_micros(parsed.astimezone(timezone.utc))
            except ValueError:
                continue
    return None


def _timestamp_micros(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def _series_id(dataset: str, index: str, timescale: str, region_type: str, region_id: str) -> str:
    slug = f"{dataset}:{index}:{timescale}:{region_type}:{region_id}"
    return re.sub(r"[^A-Za-z0-9:_-]", "_", slug)


def _unit_for_index(index: str) -> str:
    normalized = index.upper()
    if normalized in {"SPI", "SPEI", "EDDI", "PDSI"}:
        return "index"
    return "value"


def _load_geographies(config: TrinoConfig, region_types: Iterable[str]) -> gpd.GeoDataFrame:
    if trino is None:  # pragma: no cover - environment guard
        raise RuntimeError("trino client is not installed")
    types = [rt for rt in region_types]
    if not types:
        raise ValueError("At least one region type must be provided")
    for rt in types:
        if not re.fullmatch(r"[A-Z0-9_]+", rt):
            raise ValueError(f"Invalid region type: {rt}")

    catalog = os.getenv("AURUM_TRINO_CATALOG", "iceberg")
    table = os.getenv("AURUM_TRINO_GEOGRAPHY_TABLE", "ref.geographies")
    placeholders = ",".join(f"'{rt}'" for rt in types)
    sql = f"SELECT region_type, region_id, ST_AsText(geometry) AS wkt FROM {catalog}.{table} WHERE region_type IN ({placeholders})"

    conn = trino.dbapi.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        http_scheme=config.http_scheme,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        raise RuntimeError(f"No geographies returned for {types}")

    data = {
        "region_type": [row[0] for row in rows],
        "region_id": [row[1] for row in rows],
        "geometry": [wkt.loads(row[2]) for row in rows],
    }
    gdf = gpd.GeoDataFrame(data, geometry="geometry", crs="EPSG:4326")
    return gdf


def _encode_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    prepared = dict(record)
    metadata = record.get("metadata")
    if metadata is None:
        prepared["metadata"] = None
    else:
        prepared["metadata"] = {str(k): str(v) for k, v in metadata.items() if v is not None}
    return prepared




USDM_SEVERITIES = ["D0", "D1", "D2", "D3", "D4"]


def resolve_usdm_snapshot(
    logical_date: date,
    catalog_path: Path,
    session: Optional[requests.Session] = None,
) -> Optional[Dict[str, Any]]:
    catalog = load_catalog(catalog_path)
    layer = catalog.vector_by_name("usdm")
    session = session or requests.Session()
    max_window = layer.latency.maximum_days or 0
    template = layer.assets.get("geojson_template")
    if not template:
        raise RuntimeError("USDM layer missing geojson_template")

    for weeks in range(max_window + 1):
        candidate = logical_date - timedelta(days=7 * weeks)
        slug = candidate.strftime("%Y%m%d")
        url = template.format(valid_date=slug)
        if not _head_ok(session, url):
            continue
        payload = _fetch_json(url, session=session)
        valid_date = _extract_valid_date(payload) or candidate
        return {
            "url": url,
            "valid_date": valid_date,
            "payload": payload,
        }
    return None


def process_usdm_snapshot(
    logical_date: date,
    catalog_path: Path,
    trino_config: TrinoConfig,
    kafka_config: KafkaConfig,
    job_id: str,
    tenant_id: str = "aurum",
    schema_version: str = "1",
) -> Dict[str, Any]:
    if AvroProducer is None or avro is None:
        raise RuntimeError("confluent-kafka[avro] is required for drought ingestion")
    catalog = load_catalog(catalog_path)
    snapshot = resolve_usdm_snapshot(logical_date, catalog_path)
    if snapshot is None:
        raise RuntimeError("No USDM snapshot available within latency window")

    geojson_url = snapshot["url"]
    valid_date: date = snapshot["valid_date"]
    payload = snapshot["payload"]
    features = payload.get("features", [])
    if not features:
        return {"records": 0}

    gdf = gpd.GeoDataFrame.from_features(features)
    if gdf.empty:
        return {"records": 0}
    if "DM" not in gdf.columns:
        raise RuntimeError("USDM GeoJSON missing DM field")

    gdf = gdf.set_crs("EPSG:4326", allow_override=True).to_crs("EPSG:5070")
    gdf["severity"] = gdf["DM"].astype(str).str.upper().map(_normalize_usdm_severity)

    severity_geoms: Dict[str, Any] = {}
    for level in USDM_SEVERITIES:
        subset = gdf[gdf["severity"] == level]
        if subset.empty:
            severity_geoms[level] = None
        else:
            severity_geoms[level] = subset.unary_union

    region_types = sorted({region.region_type for region in catalog.region_sets})
    geodf = _load_geographies(trino_config, region_types)
    geodf = geodf.to_crs("EPSG:5070")

    ingest_ts = _timestamp_micros(datetime.now(timezone.utc))
    as_of_ts = _extract_timestamp(payload) or ingest_ts
    records: List[Dict[str, Any]] = []

    for _, region in geodf.iterrows():
        geom = region.geometry
        total_area = geom.area
        if total_area <= 0:
            continue
        coverage: Dict[str, float] = {}
        for level in USDM_SEVERITIES:
            union_geom = severity_geoms.get(level)
            if union_geom is None or union_geom.is_empty:
                coverage[level] = 0.0
                continue
            intersection = geom.intersection(union_geom)
            area = intersection.area if not intersection.is_empty else 0.0
            coverage[level] = float(area / total_area)

        metadata = {
            "source_url": geojson_url,
        }

        records.append(
            {
                "tenant_id": tenant_id,
                "schema_version": schema_version,
                "ingest_ts": ingest_ts,
                "ingest_job_id": job_id,
                "region_type": region["region_type"],
                "region_id": region["region_id"],
                "valid_date": valid_date,
                "as_of": as_of_ts,
                "d0_frac": coverage.get("D0"),
                "d1_frac": coverage.get("D1"),
                "d2_frac": coverage.get("D2"),
                "d3_frac": coverage.get("D3"),
                "d4_frac": coverage.get("D4"),
                "source_url": geojson_url,
                "metadata": metadata,
            }
        )

    if not records:
        return {"records": 0}

    producer = AvroProducer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "schema.registry.url": kafka_config.schema_registry_url,
        },
        default_value_schema=avro.load(str(kafka_config.schema_file)),
    )
    for record in records:
        producer.produce(topic=kafka_config.topic, value=_encode_metadata(record))
    producer.flush()

    return {"records": len(records)}


def _normalize_usdm_severity(value: str) -> str:
    value = value.strip().upper()
    if value.startswith("D"):
        return value
    return f"D{value}"



def discover_vector_assets(
    logical_date: date,
    catalog_path: Path,
    layers: Optional[Iterable[str]] = None,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    catalog = load_catalog(catalog_path)
    selected = {layer.lower() for layer in layers} if layers else None
    session = session or requests.Session()
    assets: List[Dict[str, Any]] = []
    slug = logical_date.strftime("%Y%m%d")

    for layer in catalog.vector_layers:
        if layer.layer == "usdm":
            continue
        if selected and layer.layer.lower() not in selected:
            continue
        template = layer.assets.get("geojson_template") or layer.assets.get("json_template")
        if not template:
            continue
        durations = layer.assets.get("durations", [])
        durations = durations if isinstance(durations, list) and durations else [None]
        for duration in durations:
            url = _format_template(template, slug, duration)
            if not _head_ok(session, url):
                continue
            assets.append(
                {
                    "layer": layer.layer,
                    "url": url,
                    "duration": duration,
                    "logical_date": logical_date.isoformat(),
                }
            )
    return assets


def process_vector_asset(
    asset: Mapping[str, Any],
    kafka_config: KafkaConfig,
    job_id: str,
    tenant_id: str = "aurum",
    schema_version: str = "1",
) -> Dict[str, Any]:
    if AvroProducer is None or avro is None:
        raise RuntimeError("confluent-kafka[avro] is required for drought ingestion")
    url = asset["url"]
    layer = asset["layer"]
    payload = _fetch_json(url)
    features = payload.get("features")
    if not features:
        return {"records": 0}

    gdf = gpd.GeoDataFrame.from_features(features)
    if gdf.empty:
        return {"records": 0}

    ingest_ts = _timestamp_micros(datetime.now(timezone.utc))
    as_of_ts = _extract_timestamp(payload) or ingest_ts
    records: List[Dict[str, Any]] = []

    for idx, row in gdf.iterrows():
        geom = row.geometry
        properties = row.get("properties", {}) if isinstance(row, Mapping) else {}
        if not isinstance(properties, Mapping):
            properties = {}
        event_id = _vector_event_id(layer, row, idx)
        valid_start, valid_end = _vector_valid_window(properties)
        value = _vector_numeric(properties)
        unit = _vector_unit(properties)
        category = properties.get("category") or properties.get("severity")
        severity = properties.get("severity") or properties.get("status")
        records.append(
            {
                "tenant_id": tenant_id,
                "schema_version": schema_version,
                "ingest_ts": ingest_ts,
                "ingest_job_id": job_id,
                "layer": layer,
                "event_id": event_id,
                "region_type": None,
                "region_id": None,
                "valid_start": valid_start,
                "valid_end": valid_end,
                "value": value,
                "unit": unit,
                "category": category,
                "severity": severity,
                "source_url": url,
                "geometry_wkt": geom.wkt if geom is not None else None,
                "properties": {str(k): str(v) for k, v in properties.items() if v is not None},
            }
        )

    producer = AvroProducer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "schema.registry.url": kafka_config.schema_registry_url,
        },
        default_value_schema=avro.load(str(kafka_config.schema_file)),
    )

    for record in records:
        prepared = record.copy()
        if prepared["valid_start"] is not None:
            prepared["valid_start"] = prepared["valid_start"]
        if prepared["valid_end"] is not None:
            prepared["valid_end"] = prepared["valid_end"]
        producer.produce(topic=kafka_config.topic, value=_encode_metadata(prepared))
    producer.flush()
    return {"records": len(records)}


def _format_template(template: str, slug: str, duration: Optional[str]) -> str:
    params: Dict[str, str] = {}
    if "{valid_date}" in template:
        params["valid_date"] = slug
    if "{date}" in template:
        params["date"] = slug
    if duration is not None and "{duration}" in template:
        params["duration"] = str(duration)
    try:
        return template.format(**params)
    except KeyError:
        return template


def _vector_event_id(layer: str, row: Mapping[str, Any], idx: int) -> str:
    props = row.get("properties") if isinstance(row, Mapping) else None
    if isinstance(props, Mapping):
        for key in ("id", "event_id", "identifier"):
            value = props.get(key)
            if value:
                return f"{layer}:{value}"
    return f"{layer}:{idx}"


def _vector_valid_window(properties: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    start = properties.get("validStart") or properties.get("startTime") or properties.get("validTime")
    end = properties.get("validEnd") or properties.get("endTime")
    start_ts = _parse_optional_timestamp(start)
    end_ts = _parse_optional_timestamp(end)
    return start_ts, end_ts


def _parse_optional_timestamp(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(float(value) * 1_000_000)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return _timestamp_micros(parsed.astimezone(timezone.utc))
        except ValueError:
            return None
    return None


def _vector_numeric(properties: Mapping[str, Any]) -> Optional[float]:
    for key in ("value", "amount", "measurement", "forecast", "avg"):
        raw = properties.get(key)
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _vector_unit(properties: Mapping[str, Any]) -> Optional[str]:
    return properties.get("unit") or properties.get("units")

__all__ = [
    "KafkaConfig",
    "discover_raster_workload",
    "process_raster_asset",
    "resolve_usdm_snapshot",
    "process_usdm_snapshot",
    "discover_vector_assets",
    "process_vector_asset",
]
