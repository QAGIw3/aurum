"""Helpers that canonicalise EIA facets and derive ingestion expressions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from aurum.reference.eia_catalog import EiaDataset


_SAFE_IDENTIFIER_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")


def _quote_identifier(name: str) -> str:
    if not name:
        return name
    if set(name) <= _SAFE_IDENTIFIER_CHARS and not name[0].isdigit():
        return name
    # SeaTunnel SQL uses backticks for field references containing punctuation.
    return f"`{name}`"


@dataclass(frozen=True)
class FacetMapping:
    area_fields: tuple[str, ...]
    sector_fields: tuple[str, ...]
    dataset_fields: tuple[str, ...]
    description_fields: tuple[str, ...]
    source_fields: tuple[str, ...]
    series_components: tuple[str, ...]

    def column_choices(self, column: str) -> tuple[str, ...]:
        if column == "area":
            return self.area_fields
        if column == "sector":
            return self.sector_fields
        if column == "dataset":
            return self.dataset_fields
        if column == "description":
            return self.description_fields
        if column == "source":
            return self.source_fields
        return ()


@dataclass(frozen=True)
class SeriesIdStrategy:
    expression: str
    components: tuple[str, ...]
    source: str


_FACET_RULES: Mapping[str, str] = {
    "area": "area",
    "areaid": "area",
    "balancingauthority": "area",
    "balancingauthorityid": "area",
    "balancing_authority": "area",
    "balancing_authority_id": "area",
    "ba": "area",
    "censusregionid": "area",
    "divisionid": "area",
    "duoarea": "area",
    "fromba": "area",
    "latlon": "area",
    "location": "area",
    "parent": "area",
    "plant": "area",
    "plantid": "area",
    "plantstateid": "area",
    "region": "area",
    "regionid": "area",
    "respondent": "area",
    "state": "area",
    "stateid": "area",
    "stateregionid": "area",
    "stateregion": "area",
    "subba": "area",
    "toba": "area",
    "timezone": "area",

    "sector": "sector",
    "sectorid": "sector",
    "process": "sector",
    "technology": "sector",
    "markettypeid": "sector",

    "product": "dataset",
    "productid": "dataset",
    "fueltype": "dataset",
    "fueltypeid": "dataset",
    "fuelid": "dataset",
    "coaltype": "dataset",
    "coalsupplier": "dataset",
    "exportimporttype": "dataset",
    "history": "dataset",
    "scenario": "dataset",
    "tableid": "dataset",
    "frequency": "dataset",
    "type": "dataset",
    "unit": "dataset",

    "series": "series",
    "seriesid": "series",
    "series_id": "series",
    "seriesname": "series",
}


_FACET_DESCRIPTION_SUFFIXES = ("-name", "_name", "description")
_SOURCE_FACETS = {"source", "sourceid", "publisher"}


def map_facets_to_columns(facets: Iterable[Mapping[str, str | None]]) -> FacetMapping:
    area: list[str] = []
    sector: list[str] = []
    dataset: list[str] = []
    description: list[str] = []
    source: list[str] = []
    series_components: list[str] = []

    for facet in facets:
        raw_id = facet.get("id")
        if not raw_id:
            continue
        facet_id = raw_id.strip()
        key = facet_id.lower().replace("-", "").replace("/", "")
        mapped = _FACET_RULES.get(key)
        if mapped == "area":
            area.append(facet_id)
        elif mapped == "sector":
            sector.append(facet_id)
        elif mapped == "dataset":
            dataset.append(facet_id)
        elif mapped == "series":
            series_components.append(facet_id)
        else:
            if any(facet_id.lower().endswith(suffix) for suffix in _FACET_DESCRIPTION_SUFFIXES):
                description.append(facet_id)
            elif facet_id.lower() in _SOURCE_FACETS:
                source.append(facet_id)

    return FacetMapping(
        area_fields=tuple(area),
        sector_fields=tuple(sector),
        dataset_fields=tuple(dataset),
        description_fields=tuple(description),
        source_fields=tuple(source),
        series_components=tuple(series_components),
    )


def _pick_column_expression(options: Sequence[str], fallback: str) -> str:
    if not options:
        return fallback
    exprs = [f"COALESCE({_quote_identifier(col)}, '')" for col in options]
    if len(exprs) == 1:
        return exprs[0]
    joined = " || '-' || ".join(exprs)
    return f"NULLIF({joined}, '')"


def build_series_id_expr(dataset: EiaDataset, mapping: FacetMapping | None = None) -> SeriesIdStrategy:
    mapping = mapping or map_facets_to_columns(dataset.facets)
    columns = {col.lower() for col in dataset.data_columns}

    if "series" in columns:
        return SeriesIdStrategy(expression="series", components=("series",), source="data-column")
    if "series_id" in columns:
        return SeriesIdStrategy(expression="series_id", components=("series_id",), source="data-column")
    if mapping.series_components:
        parts = [_quote_identifier(name) for name in mapping.series_components]
        if len(parts) == 1:
            return SeriesIdStrategy(
                expression=parts[0],
                components=mapping.series_components,
                source="facet",
            )
        expr = "CONCAT_WS('_', {})".format(", ".join(parts))
        return SeriesIdStrategy(expression=expr, components=mapping.series_components, source="facet")

    # Fallback to combining area/sector/product facets.
    candidate_components: list[str] = []
    for group in (mapping.area_fields, mapping.sector_fields, mapping.dataset_fields):
        candidate_components.extend(group)
    if candidate_components:
        parts = [_quote_identifier(name) for name in candidate_components]
        expr = "CONCAT_WS('_', {})".format(", ".join(parts))
        return SeriesIdStrategy(expression=expr, components=tuple(candidate_components), source="composite")

    # Last resort: use period + dataset path.
    escaped_path = dataset.path.replace("'", "''")
    default_expr = f"CONCAT('{escaped_path}', '_', period)"
    return SeriesIdStrategy(expression=default_expr, components=("period",), source="fallback")


def build_param_overrides(dataset: EiaDataset) -> list[dict[str, str]]:
    overrides: list[dict[str, str]] = []
    for idx, column in enumerate(dataset.data_columns):
        overrides.append({f"data[{idx}]": column})
    return overrides


def derive_frequency_label(dataset: EiaDataset, *, default: str = "OTHER") -> str:
    freq = dataset.default_frequency or ""
    if freq:
        return freq.upper()
    for entry in dataset.frequencies:
        candidate = entry.get("id") or entry.get("description")
        if candidate:
            return str(candidate).upper()
    return default


def build_column_expression(column: str, *, mapping: FacetMapping, fallback: str = "CAST(NULL AS STRING)") -> str:
    options = mapping.column_choices(column)
    return _pick_column_expression(options, fallback)
