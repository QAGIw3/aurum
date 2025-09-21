from __future__ import annotations

"""Command line entrypoints for drought raster processing workers."""

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import geopandas as gpd
import requests

from .catalog import DroughtCatalog, load_catalog
from .fetcher import AssetFetcher, HttpAsset
from .zonal import ZonalStatisticsEngine

logger = logging.getLogger(__name__)


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aurum drought worker utilities")
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path("config/droughtgov_catalog.json"),
        help="Catalog JSON with dataset metadata",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=Path("/tmp/aurum_drought"),
        help="Directory for temporary downloads and artifacts",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    subparsers = parser.add_subparsers(dest="command", required=True)

    discover = subparsers.add_parser("discover", help="List available raster assets for a dataset")
    discover.add_argument("dataset", help="Dataset key (e.g., nclimgrid)")

    process = subparsers.add_parser("process-raster", help="Download raster and compute zonal statistics")
    process.add_argument("dataset", help="Dataset key")
    process.add_argument("index", help="Index identifier (SPI, SPEI, etc.)")
    process.add_argument("timescale", help="Timescale (e.g., 3M)")
    process.add_argument("valid_date", help="Valid date (YYYY-MM-DD)")
    process.add_argument(
        "geography_file",
        type=Path,
        help="Path to GeoJSON/GeoParquet with region geometries (must include region_type and region_id columns)",
    )
    process.add_argument("--output", type=Path, help="Optional path to write JSON output")

    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    catalog = load_catalog(args.catalog)
    worker_dir = args.workdir
    fetcher = AssetFetcher(worker_dir)

    if args.command == "discover":
        entries = catalog.fetch_raster_manifest(args.dataset)
        logger.info("Discovered %s entries for %s", len(entries), args.dataset)
        print(json.dumps(entries, indent=2, default=_json_default))
        return 0

    if args.command == "process-raster":
        valid_dt = _parse_date(args.valid_date)
        bundle = catalog.asset_bundle(args.dataset, args.index, args.timescale, valid_dt)
        logger.info(
            "Processing raster",
            extra={
                "dataset": args.dataset,
                "index": args.index,
                "timescale": args.timescale,
                "valid_date": args.valid_date,
            },
        )

        info_payload = _safe_json_get(bundle.get("info")) if bundle.get("info") else None

        geotiff_url = bundle["geotiff"]
        geotiff_path = worker_dir / f"{args.dataset}_{args.index}_{args.timescale}_{args.valid_date}.tif"

        fetch_result = fetcher.fetch(HttpAsset(url=geotiff_url, destination=geotiff_path))
        logger.info(
            "Fetched raster",
            extra={
                "path": str(fetch_result.path),
                "bytes": fetch_result.bytes_downloaded,
                "cache": fetch_result.from_cache,
            },
        )

        geodf = gpd.read_file(args.geography_file)
        stats = ZonalStatisticsEngine().compute(fetch_result.path, geodf)

        payload = {
            "dataset": args.dataset,
            "index": args.index,
            "timescale": args.timescale,
            "valid_date": args.valid_date,
            "source_url": geotiff_url,
            "info": info_payload,
            "results": stats,
        }

        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(payload, indent=2, default=_json_default))
            logger.info("Wrote zonal statistics", extra={"output": str(args.output)})
        else:
            print(json.dumps(payload, indent=2, default=_json_default))
        return 0

    raise RuntimeError("Unknown command")


def _safe_json_get(url: str) -> Dict[str, Any]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
