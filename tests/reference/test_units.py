from __future__ import annotations

import pytest

from aurum.reference.units import UnitsMapper, map_units


def test_map_units_normalises_spacing() -> None:
    currency, per_unit = map_units("$ / MWh")
    assert currency == "USD"
    assert per_unit == "MWh"


def test_map_units_unknown_returns_none() -> None:
    currency, per_unit = map_units("widgets", strict=False)
    assert currency is None
    assert per_unit is None


def test_map_units_strict_raises_for_unknown() -> None:
    mapper = UnitsMapper(strict=True)
    with pytest.raises(KeyError):
        mapper.map("widgets")


def test_map_units_allows_none_when_not_strict() -> None:
    currency, per_unit = map_units(None)
    assert currency is None and per_unit is None


def test_map_units_reload_from_custom_file(tmp_path) -> None:
    mapping_file = tmp_path / "units.csv"
    mapping_file.write_text("units_raw,currency,per_unit\nfoo,USD,WIDGET\n", encoding="utf-8")
    mapper = UnitsMapper(csv_path=mapping_file)
    assert mapper.map("foo") == ("USD", "WIDGET")
