from __future__ import annotations

import hashlib
from datetime import datetime

from scripts.ingest.caiso_prc_lmp_to_kafka import iter_records


def test_iter_records_maps_xml_fields() -> None:
    xml_payload = b"""
    <OASIS>
      <REPORT_DATA>
        <INTERVALSTARTTIME_UTC>2024-01-01T08:00:00+00:00</INTERVALSTARTTIME_UTC>
        <INTERVALENDTIME_UTC>2024-01-01T09:00:00+00:00</INTERVALENDTIME_UTC>
        <MARKET_RUN_ID>RTPD</MARKET_RUN_ID>
        <NODE>TH_SP15</NODE>
        <NODE_ID>123</NODE_ID>
        <NODE_TYPE>node</NODE_TYPE>
        <LMP_PRC>45.12</LMP_PRC>
        <ENERGY_PRC>40.00</ENERGY_PRC>
        <CONGESTION_PRC>3.00</CONGESTION_PRC>
        <LOSS_PRC>2.12</LOSS_PRC>
        <NODE_SHORT>SP15</NODE_SHORT>
        <OASIS_RUN_ID>run-42</OASIS_RUN_ID>
      </REPORT_DATA>
    </OASIS>
    """

    records = list(iter_records(xml_payload))
    assert len(records) == 1
    record = records[0]

    start = datetime.fromisoformat("2024-01-01T08:00:00+00:00")
    end = datetime.fromisoformat("2024-01-01T09:00:00+00:00")
    expected_hash = hashlib.sha256(b"2024-01-01T08:00:00+00:00|TH_SP15|45.12").hexdigest()
    epoch_days = (start.date() - datetime(1970, 1, 1).date()).days

    assert record["iso_code"] == "CAISO"
    assert record["market"] == "RTPD"
    assert record["interval_start"] == int(start.timestamp() * 1_000_000)
    assert record["interval_end"] == int(end.timestamp() * 1_000_000)
    assert record["delivery_date"] == epoch_days
    assert record["location_id"] == "123"
    assert record["location_name"] == "TH_SP15"
    assert record["location_type"] == "NODE"
    assert record["price_total"] == 45.12
    assert record["price_energy"] == 40.0
    assert record["price_congestion"] == 3.0
    assert record["price_loss"] == 2.12
    assert record["currency"] == "USD"
    assert record["uom"] == "MWh"
    assert record["settlement_point"] == "SP15"
    assert record["source_run_id"] == "run-42"
    assert record["record_hash"] == expected_hash
