from datetime import date

import pandas as pd
import pytest

from aurum.parsers.vendor_curves import PARSERS, parse, register


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


def teardown_module(module):
    PARSERS.pop("dummy", None)
