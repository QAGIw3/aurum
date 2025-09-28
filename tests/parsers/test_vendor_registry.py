from datetime import date

import pandas as pd
import pytest

from aurum.parsers.vendor_curves import PARSERS, parse, parse_with_diagnostics, register


def test_register_and_dispatch(monkeypatch):
    called = {}

    def dummy_parser(path: str, asof: date) -> pd.DataFrame:
        called["path"] = path
        called["asof"] = asof
        return pd.DataFrame({"col": [1]})

    register("dummy", dummy_parser)
    result = parse("dummy", "file.xlsx", date(2024, 1, 1))
    assert called["path"] == "file.xlsx"
    assert called["asof"].year == 2024
    assert isinstance(result, pd.DataFrame)


def test_parse_unknown_vendor():
    with pytest.raises(ValueError):
        parse("unknown", "file", date.today())


def test_parse_with_diagnostics(tmp_path):
    csv_path = tmp_path / "sample.csv"
    pd.DataFrame(
        {
            "tenor": [
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
            ],
            "price": [50.0, 51.0, 52.0, 50.5, 51.2, 150.0],
        }
    ).to_csv(csv_path, index=False)

    result = parse_with_diagnostics("generic", str(csv_path), date(2024, 1, 1))
    assert hasattr(result, "dataframe")
    assert not result.dataframe.empty
    assert result.diagnostics.anomaly_confidence < 1.0  # outlier detected
    assert any("price" in message for message in result.diagnostics.validation_issues)


def teardown_module(module):
    PARSERS.pop("dummy", None)
