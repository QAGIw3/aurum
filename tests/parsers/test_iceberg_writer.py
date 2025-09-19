import pandas as pd
import pytest


def test_write_to_iceberg_requires_dependency(monkeypatch):
    import sys

    for key in list(sys.modules):
        if key.startswith("pyiceberg"):
            monkeypatch.delitem(sys.modules, key, raising=False)
    monkeypatch.setitem(sys.modules, "pyiceberg", None)

    from aurum.parsers import iceberg_writer

    df = pd.DataFrame({
        "curve_key": ["dummy"],
        "asof_date": [pd.Timestamp("2025-01-01")],
    })

    with pytest.raises(RuntimeError, match="pyiceberg is required"):
        iceberg_writer.write_to_iceberg(df)
