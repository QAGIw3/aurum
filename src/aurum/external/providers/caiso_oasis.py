"""CAISO OASIS SingleZip client with CSV-first parsing and XML fallback.

This module provides a resilient downloader and parsers for CAISO OASIS reports
using the SingleZip endpoint. It prefers `resultformat=6` (CSV-in-zip) to reduce
payload size and falls back to XML within the returned zip when CSV is not
available for a report.

The client is designed to be used by higher-level collectors that normalize
records and emit Avro to Kafka using the existing ExternalCollector helpers.
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

from ..collect import ExternalCollector, HttpRequest


DEFAULT_SINGLEZIP_URL = "https://oasis.caiso.com/oasisapi/SingleZip"


class CaisoInvalidRequest(Exception):
    """Raised when OASIS returns an INVALID_REQUEST zip payload."""


def _to_utc_micros(dt_text: str | None) -> Optional[int]:
    if not dt_text:
        return None
    # Normalize common CAISO variants to ISO8601
    t = dt_text.strip().replace("Z", "+00:00")
    # Many OASIS fields are returned as UTC or GMT without colon in offset.
    if t.endswith("-0000"):
        t = t[:-5] + "+00:00"
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        # Fallback patterns
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
            try:
                dt = datetime.strptime(t, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        else:
            return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _ordinal_date_from_micros(micros: int) -> int:
    epoch_days = datetime.utcfromtimestamp(0).date().toordinal()
    return int(datetime.utcfromtimestamp(micros / 1_000_000).date().toordinal() - epoch_days)


@dataclass(frozen=True)
class OasisRequest:
    """Declarative payload for a SingleZip query."""

    queryname: str
    startdatetime: str
    enddatetime: str
    market_run_id: Optional[str] = None
    resultformat: str = "6"  # CSV
    version: str = "1"
    # Optional filters
    node: Optional[str] = None
    grp_type: Optional[str] = None
    additional: Mapping[str, str] = None

    def to_params(self) -> Dict[str, str]:
        params: Dict[str, str] = {
            "queryname": self.queryname,
            "startdatetime": self.startdatetime,
            "enddatetime": self.enddatetime,
            "version": self.version,
            "resultformat": self.resultformat,
        }
        if self.market_run_id:
            params["market_run_id"] = self.market_run_id
        if self.node:
            params["node"] = self.node
        if self.grp_type:
            params["grp_type"] = self.grp_type
        if self.additional:
            params.update({str(k): str(v) for k, v in self.additional.items()})
        return params


class CaisoOasisClient:
    """Lightweight CAISO OASIS downloader with CSV-first parsing and XML fallback."""

    def __init__(
        self,
        http: ExternalCollector,
        *,
        singlezip_url: str = DEFAULT_SINGLEZIP_URL,
        max_retries: int = 5,
        backoff_seconds: float = 2.0,
    ) -> None:
        self.http = http
        self.singlezip_url = singlezip_url
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def fetch_archive(self, request: OasisRequest) -> bytes:
        """Download a SingleZip archive; raise for INVALID_REQUEST payloads.

        This uses CSV resultformat by default. Some endpoints ignore CSV and
        return XML only; the caller should attempt CSV parsing first and fall
        back to XML parsing on demand.
        """
        params = request.to_params()
        # The ExternalCollector already wraps retry/backoff; we do one-shot here
        response = self.http.request(
            HttpRequest(method="GET", path=self.singlezip_url, params=params)
        )
        content = response.content
        # Validate archive and check for INVALID_REQUEST sentinel
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                names = zf.namelist()
                if not names:
                    raise CaisoInvalidRequest("Empty zip from OASIS")
                upper = [n.upper() for n in names]
                if any(n.startswith("INVALID_REQUEST") for n in upper):
                    raise CaisoInvalidRequest("OASIS returned INVALID_REQUEST")
        except zipfile.BadZipFile as exc:  # Bubble up as collector-level error
            raise CaisoInvalidRequest(f"Bad zip from OASIS: {exc}")
        return content

    @staticmethod
    def iter_csv_rows(archive: bytes) -> Iterator[Dict[str, str]]:
        """Yield CSV rows from the first CSV found in the archive.

        If multiple CSV files exist, rows from all CSV files are concatenated.
        """
        with zipfile.ZipFile(io.BytesIO(archive)) as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".csv"):
                    continue
                data = zf.read(name).decode("utf-8", errors="replace")
                reader = csv.DictReader(io.StringIO(data))
                for row in reader:
                    # normalize keys to uppercase to match XML fields style
                    yield {str(k).upper(): (v if v is not None else "") for k, v in row.items()}

    @staticmethod
    def extract_xml_bytes(archive: bytes) -> bytes:
        with zipfile.ZipFile(io.BytesIO(archive)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".xml"):
                    return zf.read(name)
        raise CaisoInvalidRequest("No XML file found in CAISO zip payload")

    # --- Normalizers for common report types ---

    @staticmethod
    def normalize_lmp_row(row: Mapping[str, Any]) -> Dict[str, Any]:
        """Normalize PRC_LMP or PRC_INTVL_LMP row into IsoLmpRecord fields (dict)."""
        start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVALSTARTTIME_UTC")
        end = row.get("INTERVALENDTIME_GMT") or row.get("INTERVALENDTIME_UTC")
        interval_start = _to_utc_micros(start) or 0
        interval_end = _to_utc_micros(end)
        node = row.get("PNODE_NAME") or row.get("NODE") or row.get("PNODE") or ""
        node_id = row.get("PNODE_ID") or row.get("NODE_ID") or node
        energy = row.get("ENERGY_PRC") or row.get("LMP_ENERGY")
        cong = row.get("CONGESTION_PRC") or row.get("LMP_CONGESTION")
        loss = row.get("LOSS_PRC") or row.get("LMP_LOSS")
        try:
            e = float(energy) if energy not in (None, "") else None
        except Exception:
            e = None
        try:
            c = float(cong) if cong not in (None, "") else None
        except Exception:
            c = None
        try:
            l = float(loss) if loss not in (None, "") else None
        except Exception:
            l = None
        total = row.get("LMP_PRC") or row.get("LMP")
        try:
            t = float(total) if total not in (None, "") else None
        except Exception:
            t = None
        # If total missing, sum parts where possible
        if t is None and any(x is not None for x in (e, c, l)):
            t = (e or 0.0) + (c or 0.0) + (l or 0.0)
        market = (row.get("MARKET_RUN_ID") or row.get("MARKET") or "").upper()
        # Map CAISO variants to schema enum
        if market in ("DAM", "DAY_AHEAD"):
            market_enum = "DAY_AHEAD"
        elif market in ("RTM", "RTPD", "REAL_TIME"):
            market_enum = "REAL_TIME"
        elif market in ("FMM", "FIFTEEN_MINUTE"):
            market_enum = "FIFTEEN_MINUTE"
        else:
            market_enum = "UNKNOWN"
        record: Dict[str, Any] = {
            "iso_code": "CAISO",
            "market": market_enum,
            "delivery_date": _ordinal_date_from_micros(interval_start) if interval_start else 0,
            "interval_start": interval_start,
            "interval_end": interval_end,
            "interval_minutes": None,
            "location_id": str(node_id or ""),
            "location_name": str(node or "") or None,
            "location_type": (row.get("PNODE_TYPE") or row.get("NODE_TYPE") or "NODE").upper(),
            "zone": row.get("TAC_ZONE") or None,
            "hub": None,
            "timezone": "America/Los_Angeles",
            "price_total": float(t or 0.0),
            "price_energy": e,
            "price_congestion": c,
            "price_loss": l,
            "currency": "USD",
            "uom": "MWh",
            "settlement_point": row.get("NODE_SHORT") or None,
            "source_run_id": row.get("OASIS_RUN_ID") or None,
            "ingest_ts": 0,
            # record_hash filled by producer layer
            "record_hash": "",
            "metadata": None,
        }
        return record

    @staticmethod
    def normalize_load_row(row: Mapping[str, Any]) -> Dict[str, Any]:
        start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVALSTARTTIME_UTC")
        end = row.get("INTERVALENDTIME_GMT") or row.get("INTERVALENDTIME_UTC")
        mw = row.get("MW") or row.get("LOAD")
        try:
            value = float(mw) if mw not in (None, "") else 0.0
        except Exception:
            value = 0.0
        return {
            "iso_code": "CAISO",
            "area": row.get("TAC_ZONE") or row.get("AREA") or "SYSTEM",
            "interval_start": _to_utc_micros(start) or 0,
            "interval_end": _to_utc_micros(end),
            "interval_minutes": None,
            "mw": value,
            "ingest_ts": 0,
            "metadata": {
                k: str(v)
                for k, v in {
                    "market_run_id": row.get("MARKET_RUN_ID"),
                    "forecast": row.get("FORECAST"),
                    "series": row.get("SERIES"),
                }.items()
                if v not in (None, "")
            }
            or None,
        }

    @staticmethod
    def normalize_as_row(row: Mapping[str, Any]) -> Dict[str, Any]:
        start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVALSTARTTIME_UTC")
        end = row.get("INTERVALENDTIME_GMT") or row.get("INTERVALENDTIME_UTC")
        price = row.get("MCP") or row.get("COST") or row.get("PRICE")
        try:
            price_mcp = float(price) if price not in (None, "") else None
        except Exception:
            price_mcp = None
        market = (row.get("MARKET_RUN_ID") or row.get("MARKET") or "").upper()
        return {
            "iso_code": "CAISO",
            "market": market or "UNKNOWN",
            "product": row.get("AS_TYPE") or row.get("PRODUCT") or "UNKNOWN",
            "zone": row.get("TAC_ZONE") or row.get("ZONE") or "SYSTEM",
            "preliminary_final": row.get("PRELIMINARY_FINAL") or None,
            "interval_start": _to_utc_micros(start) or 0,
            "interval_end": _to_utc_micros(end),
            "interval_minutes": None,
            "price_mcp": price_mcp,
            "currency": "USD",
            "uom": "MWh",
            "ingest_ts": 0,
            "record_hash": "",
            "metadata": None,
        }


__all__ = [
    "CaisoOasisClient",
    "OasisRequest",
    "CaisoInvalidRequest",
]

