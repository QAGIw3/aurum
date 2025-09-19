from datetime import date

import pandas as pd
import pytest

from aurum.parsers.vendor_curves.parse_eugp import parse as parse_eugp
from aurum.parsers.vendor_curves.parse_pw import parse as parse_pw
from aurum.parsers.vendor_curves.parse_rp import parse as parse_rp


@pytest.fixture(scope="module")
def asof_date() -> date:
    return date(2025, 9, 12)


def test_parse_pw_monthly_mid(asof_date: date):
    df = parse_pw("files/EOD_PW_20250912_1430.xlsx", asof_date)
    assert not df.empty
    target = df[
        (df["iso"] == "PJM")
        & (df["location"] == "West")
        & (df["block"] == "ON_PEAK")
        & (df["tenor_label"] == "2025-09")
    ]
    assert not target.empty
    row = target.iloc[0]
    assert pytest.approx(row["mid"], rel=1e-6) == 46.75
    assert pytest.approx(row["bid"], rel=1e-6) == 46.69
    assert pytest.approx(row["ask"], rel=1e-6) == 46.81
    assert row["price_type"] == "MID"
    assert row["currency"] == "USD"
    assert row["per_unit"] == "MWh"


def test_parse_eugp_includes_spark_spread(asof_date: date):
    df = parse_eugp("files/EOD_EUGP_20250912_1430.xlsx", asof_date)
    assert not df.empty
    spark = df[
        (df["sheet_name"] == "Spark Spread")
        & (df["location"] == "Austrian")
        & (df["tenor_label"] == "2025-09")
    ]
    assert not spark.empty
    row = spark.iloc[0]
    assert row["spark_location"] == "Austrian_CEGH"
    assert pytest.approx(row["mid"], rel=1e-6) == 25.81827660034004
    assert row["currency"] == "EUR"
    assert row["per_unit"] == "MWh"


def test_parse_rp_includes_seasonal_blocks(asof_date: date):
    df = parse_rp("files/EOD_RP_20250912_1430.xlsx", asof_date)
    assert not df.empty
    seasonal = df[
        (df["sheet_name"] == "Seasonal Blocks - Mid")
        & (df["product"] == "Maryland Compliance Renewable Energy Credit Tier 1")
        & (df["tenor_label"] == "Calendar 2025")
    ]
    assert not seasonal.empty
    row = seasonal.iloc[0]
    assert pytest.approx(row["mid"], rel=1e-6) == 25.267
    assert row["currency"] == "USD"
    assert row["per_unit"] == "MWh"


def test_contract_month_cast(asof_date: date):
    df = parse_pw("files/EOD_PW_20250912_1430.xlsx", asof_date)
    monthly = df[df["tenor_type"] == "MONTHLY"]
    assert monthly["contract_month"].notna().all()
    assert isinstance(monthly.iloc[0]["contract_month"], date)
