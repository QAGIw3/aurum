#!/usr/bin/env python3
"""Code-generate EIA ingest dataset config entries from the harvested catalog."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.eia.mapping import (  # noqa: E402
    SeriesIdStrategy,
    build_column_expression,
    build_param_overrides,
    build_series_id_expr,
    derive_frequency_label,
    map_facets_to_columns,
)
from aurum.reference import eia_catalog  # noqa: E402
from aurum.reference.eia_catalog import EiaDataset  # noqa: E402
from aurum.reference.units import map_units  # noqa: E402
DEFAULT_OUTPUT = REPO_ROOT / "config" / "eia_ingest_datasets.generated.json"
DEFAULT_BASE_CONFIG = REPO_ROOT / "config" / "eia_ingest_overrides.json"


def _slugify(path: str) -> str:
    token = path.replace("-", "_").replace("/", "_")
    return token.lower()


def _escape_literal(value: str | None, default: str) -> str:
    text = value or default
    return text.replace("'", "''")


def _canonical_units(default_units: str | None) -> tuple[str | None, str | None]:
    if not default_units:
        return (None, None)
    token = default_units.strip()
    if not token or token.upper() in {"UNKNOWN", "NATIVE"}:
        return (None, None)
    try:
        currency, per_unit = map_units(token)
    except Exception:
        currency, per_unit = (None, None)
    if currency is None and per_unit is None:
        return (None, token)
    return (currency, per_unit or token)


@dataclass
class GeneratedDataset:
    source_name: str
    path: str
    data_path: str
    description: str | None
    schedule: str
    topic_var: str
    default_topic: str
    series_id_expr: str
    series_id_components: Sequence[str]
    frequency: str
    units_var: str
    default_units: str
    canonical_currency: str | None
    canonical_unit: str | None
    unit_conversion: dict[str, Any] | None
    area_expr: str
    sector_expr: str
    description_expr: str
    source_expr: str
    dataset_expr: str
    metadata_expr: str
    filter_expr: str
    param_overrides: list[dict[str, str]]
    source_strategy: str
    period_column: str
    date_format: str | None

    def as_config(self) -> dict[str, Any]:
        payload = asdict(self)
        components = payload.pop("series_id_components")
        strategy = payload.pop("source_strategy")
        payload["metadata_expr"] = self.metadata_expr
        payload["series_id_strategy"] = {
            "source": strategy,
            "components": list(components),
        }
        return payload


def _build_generated_entry(dataset: EiaDataset, *, default_schedule: str) -> GeneratedDataset:
    mapping = map_facets_to_columns(dataset.facets)
    series_strategy: SeriesIdStrategy = build_series_id_expr(dataset, mapping)

    source_name = f"eia_{_slugify(dataset.path)}"
    topic_stub = source_name.replace("eia_", "").replace("__", "_")
    default_topic = f"aurum.ref.eia.{topic_stub}.v1"

    area_expr = build_column_expression("area", mapping=mapping)
    sector_expr = build_column_expression("sector", mapping=mapping)
    dataset_expr = build_column_expression(
        "dataset",
        mapping=mapping,
        fallback=f"'{_escape_literal(dataset.name or dataset.path, dataset.path)}'",
    )
    description_expr = build_column_expression(
        "description",
        mapping=mapping,
        fallback=f"'{_escape_literal(dataset.description, dataset.path)}'",
    )
    source_expr = build_column_expression(
        "source",
        mapping=mapping,
        fallback="'EIA'",
    )

    canonical_currency, canonical_unit = _canonical_units(None)

    return GeneratedDataset(
        source_name=source_name,
        path=dataset.path,
        data_path=f"{dataset.path}/data",
        description=dataset.description,
        schedule=default_schedule,
        topic_var=f"aurum_{source_name}_topic",
        default_topic=default_topic,
        series_id_expr=series_strategy.expression,
        series_id_components=series_strategy.components,
        frequency=derive_frequency_label(dataset),
        units_var=f"aurum_{source_name}_units",
        default_units="UNKNOWN",
        canonical_currency=canonical_currency,
        canonical_unit=canonical_unit,
        unit_conversion=None,
        area_expr=area_expr,
        sector_expr=sector_expr,
        description_expr=description_expr,
        source_expr=source_expr,
        dataset_expr=dataset_expr,
        metadata_expr="NULL",
        filter_expr="TRUE",
        param_overrides=build_param_overrides(dataset),
        source_strategy=series_strategy.source,
        period_column="period",
        date_format=dataset.default_date_format,
    )


def _load_overrides(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _merge_override(entry: dict[str, Any], overrides: dict[str, Any], path: str) -> dict[str, Any]:
    patch = overrides.get(path) or overrides.get(entry.get("source_name"))
    if not patch:
        return entry
    merged = {**entry, **patch}
    if "source_name" in patch and patch["source_name"] != entry.get("source_name"):
        merged.setdefault("topic_var", f"aurum_{patch['source_name']}_topic")
        merged.setdefault("units_var", f"aurum_{patch['source_name']}_units")
    if "series_id_strategy" in entry and "series_id_strategy" in patch:
        merged["series_id_strategy"] = patch["series_id_strategy"]
    if "canonical_currency" not in patch and "canonical_unit" not in patch:
        currency, per_unit = _canonical_units(merged.get("default_units"))
        merged["canonical_currency"] = currency
        merged["canonical_unit"] = per_unit
    return merged


def generate_config(
    datasets: Iterable[EiaDataset],
    *,
    default_schedule: str,
    overrides: dict[str, Any],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for dataset in sorted(datasets, key=lambda d: d.path):
        generated = _build_generated_entry(dataset, default_schedule=default_schedule)
        entry = generated.as_config()
        entry = _merge_override(entry, overrides, dataset.path)
        entries.append(entry)
    return entries


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        type=Path,
        help=f"Path to write the generated config (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--catalog",
        default=None,
        type=Path,
        help="Optional alternate catalog JSON path",
    )
    parser.add_argument(
        "--overrides",
        default=DEFAULT_BASE_CONFIG,
        type=Path,
        help="JSON file containing per-source override fields to merge",
    )
    parser.add_argument(
        "--schedule",
        default="15 6 * * *",
        help="Default cron schedule for generated datasets",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated JSON to stdout without writing",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(list(argv) if argv is not None else sys.argv[1:])
    catalog_path = args.catalog
    if catalog_path:
        eia_catalog.load_catalog(catalog_path)
    datasets = list(eia_catalog.iter_datasets(catalog_path))
    overrides = _load_overrides(args.overrides)
    entries = generate_config(
        datasets,
        default_schedule=args.schedule,
        overrides=overrides,
    )
    payload = {"datasets": entries}
    output_path: Path = args.output
    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {len(entries)} dataset configs -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
