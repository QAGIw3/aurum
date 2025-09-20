import sys
from types import SimpleNamespace

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


def test_write_scenario_output_requires_dependency(monkeypatch):
    import sys

    for key in list(sys.modules):
        if key.startswith("pyiceberg"):
            monkeypatch.delitem(sys.modules, key, raising=False)
    monkeypatch.setitem(sys.modules, "pyiceberg", None)

    from aurum.parsers import iceberg_writer

    df = pd.DataFrame({
        "scenario_id": ["demo"],
        "tenant_id": ["tenant"],
        "asof_date": [pd.Timestamp("2025-01-01")],
        "curve_key": ["ck"],
        "metric": ["mid"],
        "value": [0.0],
        "tenor_type": ["MONTHLY"],
        "tenor_label": ["2025-01"],
        "computed_ts": [pd.Timestamp("2025-01-01")],
    })

    with pytest.raises(RuntimeError, match="pyiceberg is required"):
        iceberg_writer.write_scenario_output(df)


def test_delete_existing_scenario_rows_includes_run_id(monkeypatch):
    from aurum.parsers import iceberg_writer

    calls = []

    def _equal_to(column, value):
        calls.append(("eq", column, value))
        return (column, value)

    def _and(left, right):
        calls.append(("and", left, right))
        return (left, right)

    monkeypatch.setitem(
        sys.modules,
        "pyiceberg.expressions",
        SimpleNamespace(EqualTo=_equal_to, And=_and),
    )

    frame = pd.DataFrame(
        {
            "tenant_id": ["t1", "t1"],
            "scenario_id": ["s1", "s1"],
            "run_id": ["r1", "r1"],
            "metric": ["mid", "mid"],
            "tenor_label": ["Jan", "Jan"],
            "curve_key": ["c1", "c2"],
        }
    )

    class DummyTable:
        def __init__(self):
            self.expressions = []

        def delete(self, expr):
            self.expressions.append(expr)

    table = DummyTable()
    iceberg_writer._delete_existing_scenario_rows(table, frame)

    eq_calls = [call for call in calls if call[0] == "eq"]
    assert [col for _, col, _ in eq_calls] == [
        "tenant_id",
        "scenario_id",
        "run_id",
        "metric",
        "tenor_label",
    ]
    assert len(table.expressions) == 1


def test_delete_existing_ppa_rows(monkeypatch):
    from aurum.parsers import iceberg_writer

    calls = []

    def _equal_to(column, value):
        calls.append(("eq", column, value))
        return (column, value)

    def _and(left, right):
        calls.append(("and", left, right))
        return (left, right)

    monkeypatch.setitem(
        sys.modules,
        "pyiceberg.expressions",
        SimpleNamespace(EqualTo=_equal_to, And=_and),
    )

    frame = pd.DataFrame(
        {
            "ppa_contract_id": ["ppa-1", "ppa-1"],
            "scenario_id": ["s1", "s1"],
            "period_start": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01")],
            "metric": ["cashflow", "cashflow"],
            "value": [10.0, 10.0],
        }
    )

    class DummyTable:
        def __init__(self):
            self.expressions = []

        def delete(self, expr):
            self.expressions.append(expr)

    table = DummyTable()
    iceberg_writer._delete_existing_ppa_rows(table, frame)

    eq_calls = [call for call in calls if call[0] == "eq"]
    assert [col for _, col, _ in eq_calls] == [
        "ppa_contract_id",
        "scenario_id",
        "period_start",
        "metric",
    ]
    assert len(table.expressions) == 1
