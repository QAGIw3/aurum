from __future__ import annotations

from aurum.eia.mapping import (
    SeriesIdStrategy,
    build_param_overrides,
    build_series_id_expr,
    derive_frequency_label,
    map_facets_to_columns,
)
from aurum.reference.eia_catalog import EiaDataset


def _dataset(**kwargs):
    defaults = dict(
        path="test/dataset",
        name="Test Dataset",
        description="Example",
        frequencies=(
            {"id": "monthly", "description": "Monthly"},
        ),
        facets=(),
        data_columns=(),
        start_period="2020",
        end_period="2025",
        default_frequency="monthly",
        default_date_format="YYYY-MM",
        browser_total=None,
        browser_frequency=None,
        browser_date_format=None,
        warnings=(),
    )
    defaults.update(kwargs)
    return EiaDataset(**defaults)


def test_series_strategy_prefers_series_column() -> None:
    dataset = _dataset(data_columns=("series", "value"))
    mapping = map_facets_to_columns(dataset.facets)
    strategy = build_series_id_expr(dataset, mapping)
    assert isinstance(strategy, SeriesIdStrategy)
    assert strategy.expression == "series"
    assert strategy.components == ("series",)


def test_series_strategy_uses_facet_when_column_missing() -> None:
    dataset = _dataset(
        facets=(
            {"id": "seriesId", "description": "Series"},
            {"id": "duoarea", "description": "Area"},
        ),
        data_columns=("value",),
    )
    mapping = map_facets_to_columns(dataset.facets)
    strategy = build_series_id_expr(dataset, mapping)
    assert strategy.components == ("seriesId",)
    assert "seriesId" in strategy.expression


def test_series_strategy_falls_back_to_composite() -> None:
    dataset = _dataset(
        facets=(
            {"id": "duoarea", "description": "Area"},
            {"id": "process", "description": "Process"},
        ),
        data_columns=("value",),
    )
    mapping = map_facets_to_columns(dataset.facets)
    strategy = build_series_id_expr(dataset, mapping)
    assert strategy.components == ("duoarea", "process")
    assert strategy.expression.startswith("CONCAT_WS")


def test_param_overrides_enumerates_columns() -> None:
    dataset = _dataset(data_columns=("value", "foo", "bar"))
    overrides = build_param_overrides(dataset)
    assert overrides == [
        {"data[0]": "value"},
        {"data[1]": "foo"},
        {"data[2]": "bar"},
    ]


def test_frequency_derivation_prefers_default() -> None:
    dataset = _dataset(default_frequency="weekly", frequencies=())
    assert derive_frequency_label(dataset) == "WEEKLY"
