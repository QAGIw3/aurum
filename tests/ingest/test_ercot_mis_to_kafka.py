from __future__ import annotations

import zipfile
import io

from scripts.ingest.ercot_mis_to_kafka import extract_csv_bytes, iter_rows, to_record

SAMPLE_CSV = """SettlementPoint,DeliveryDate,DeliveryHour,DeliveryInterval,SettlementPointPrice,SettlementPointLMP,Congestion,Loss,MarketType,SystemVersion
HB_HOUSTON,2024-01-01,1,1,37.25,35.0,1.5,0.75,RTM,1.0
"""


def build_zip(data: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("sample.csv", data)
    return buffer.getvalue()


def test_extract_csv_bytes() -> None:
    archive = build_zip(SAMPLE_CSV)
    csv_data = extract_csv_bytes(archive)
    assert csv_data.decode("utf-8").startswith("SettlementPoint")


def test_iter_rows_and_record() -> None:
    csv_bytes = SAMPLE_CSV.encode("utf-8")
    rows = list(iter_rows(csv_bytes))
    assert len(rows) == 1
    record = to_record(rows[0])
    assert record["iso_code"] == "ERCOT"
    assert record["interval_minutes"] == 15
    assert record["price_total"] == 37.25
