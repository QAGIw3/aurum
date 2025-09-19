from __future__ import annotations

import io
import zipfile
from datetime import datetime

import pytest

from scripts.ingest.caiso_prc_lmp_to_kafka import extract_xml_bytes, iter_records


SAMPLE_XML = """
<OASIS>
  <RTO>
    <REPORT_DATA>
      <MARKET_RUN_ID>RTPD</MARKET_RUN_ID>
      <NODE>P_NODE_1</NODE>
      <NODE_ID>12345</NODE_ID>
      <NODE_TYPE>NODE</NODE_TYPE>
      <LMP_PRC>45.67</LMP_PRC>
      <ENERGY_PRC>40.01</ENERGY_PRC>
      <CONGESTION_PRC>3.21</CONGESTION_PRC>
      <LOSS_PRC>2.45</LOSS_PRC>
      <INTERVALSTARTTIME_UTC>2024-01-01T08:00:00Z</INTERVALSTARTTIME_UTC>
      <INTERVALENDTIME_UTC>2024-01-01T09:00:00Z</INTERVALENDTIME_UTC>
    </REPORT_DATA>
  </RTO>
</OASIS>
""".strip()


def build_zip(data: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("sample.xml", data)
    return buffer.getvalue()


def test_extract_xml_bytes() -> None:
    archive = build_zip(SAMPLE_XML)
    xml = extract_xml_bytes(archive)
    assert xml.decode("utf-8").strip().startswith("<OASIS>")


def test_iter_records_basic() -> None:
    xml = SAMPLE_XML.encode("utf-8")
    records = list(iter_records(xml))
    assert len(records) == 1
    record = records[0]
    assert record["iso_code"] == "CAISO"
    assert record["location_id"] == "12345"
    assert pytest.approx(record["price_total"]) == 45.67
    start_dt = datetime.utcfromtimestamp(record["interval_start"] / 1_000_000)
    assert start_dt.hour == 8
