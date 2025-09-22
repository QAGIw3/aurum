"""CAISO OASIS collectors for LMP, AS, and Load/Forecast datasets.

These collectors orchestrate SingleZip downloads via :mod:`caiso_oasis`,
normalize rows to the repo's Avro schemas, emit to Kafka, and land raw zip
archives under `files/raw/caiso/oasis/<query>/<yyyymmdd>/...` with a small
provenance manifest.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from ..collect import CollectorConfig, ExternalCollector, create_avro_producer
from .caiso_oasis import (
    CaisoInvalidRequest,
    CaisoOasisClient,
    OasisRequest,
    _to_utc_micros,
)


SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "kafka" / "schemas"
ISO_LMP_SCHEMA_PATH = SCHEMAS_DIR / "iso.lmp.v1.avsc"
ISO_LOAD_SCHEMA_PATH = SCHEMAS_DIR / "iso.load.v1.avsc"
ISO_ASM_SCHEMA_PATH = SCHEMAS_DIR / "iso.asm.v1.avsc"
ISO_PNODE_SCHEMA_PATH = SCHEMAS_DIR / "iso.pnode.v1.avsc"


def _utc_now_micros() -> int:
    return int(datetime.utcnow().timestamp() * 1_000_000)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _record_hash_lmp(record: Mapping[str, Any]) -> str:
    # Stable hash using interval_start | location_id | price_total
    start = str(record.get("interval_start", ""))
    loc = str(record.get("location_id", ""))
    prc = str(record.get("price_total", ""))
    return hashlib.sha256(f"{start}|{loc}|{prc}".encode("utf-8")).hexdigest()


def _record_hash_asm(record: Mapping[str, Any]) -> str:
    base = f"{record.get('interval_start','')}|{record.get('zone','')}|{record.get('product','')}|{record.get('price_mcp','')}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CaisoKafkaConfig:
    bootstrap_servers: str
    schema_registry_url: str
    lmp_topic: str = "aurum.iso.caiso.lmp.v1"
    load_topic: str = "aurum.iso.caiso.load.v1"
    asm_topic: str = "aurum.iso.caiso.asm.v1"
    pnode_topic: str = "aurum.iso.caiso.pnode.v1"


class CaisoOasisCollector:
    """High-level orchestrator for CAISO OASIS report ingestion."""

    def __init__(
        self,
        *,
        http_collector: ExternalCollector,
        kafka_cfg: Optional[CaisoKafkaConfig] = None,
        landing_root: Optional[Path] = None,
        singlezip_url: str = "https://oasis.caiso.com/oasisapi/SingleZip",
    ) -> None:
        self.http = http_collector
        self.client = CaisoOasisClient(http_collector, singlezip_url=singlezip_url)
        self.kafka_cfg = kafka_cfg
        self.landing_root = landing_root or (Path(__file__).resolve().parents[3] / "files" / "raw" / "caiso" / "oasis")
        # Preload schemas if producing
        self._producer_cache: Dict[str, Any] = {}
        self._schemas: Dict[str, Mapping[str, Any]] = {}
        if kafka_cfg:
            import json as _json
            self._schemas["lmp"] = json.loads(ISO_LMP_SCHEMA_PATH.read_text(encoding="utf-8"))
            self._schemas["load"] = json.loads(ISO_LOAD_SCHEMA_PATH.read_text(encoding="utf-8"))
            self._schemas["asm"] = json.loads(ISO_ASM_SCHEMA_PATH.read_text(encoding="utf-8"))
            self._schemas["pnode"] = json.loads(ISO_PNODE_SCHEMA_PATH.read_text(encoding="utf-8"))

    # ---------- Public entry points ----------

    def ingest_prc_lmp(
        self,
        *,
        start_utc: datetime,
        end_utc: datetime,
        market_run_id: str,
        grp_all: bool = False,
        node: Optional[str] = None,
    ) -> int:
        """Ingest PRC_LMP (day-ahead hourly) or PRC_INTVL_LMP (real-time 5-min) depending on market.

        For grp_all=True, enforce hour-by-hour querying to comply with OASIS limits.
        Returns number of records produced to Kafka (if configured), otherwise parsed records count.
        """
        queryname = "PRC_INTVL_LMP" if market_run_id.upper() in ("RTPD", "RTM") else "PRC_LMP"
        windows = self._build_windows(start_utc, end_utc, step=timedelta(hours=1) if grp_all else timedelta(hours=24))
        total_records = 0
        for win_start, win_end in windows:
            req = OasisRequest(
                queryname=queryname,
                startdatetime=_fmt_gmt(win_start),
                enddatetime=_fmt_gmt(win_end),
                market_run_id=market_run_id,
                node=node,
                grp_type="ALL" if grp_all else None,
            )
            archive = self._download_with_bisect(req)
            self._land_archive(req, archive)
            # Try CSV first, fallback to XML
            rows = list(self.client.iter_csv_rows(archive))
            if not rows:
                from xml.etree import ElementTree as ET
                xml_bytes = self.client.extract_xml_bytes(archive)
                root = ET.fromstring(xml_bytes)
                rows = [
                    {child.tag.upper(): (child.text or "").strip() for child in report}
                    for report in root.findall(".//REPORT_DATA")
                ]
            normalized: List[Mapping[str, Any]] = []
            for row in rows:
                rec = self.client.normalize_lmp_row(row)
                rec["ingest_ts"] = _utc_now_micros()
                rec["record_hash"] = _record_hash_lmp(rec)
                normalized.append(rec)
            total_records += len(normalized)
            self._emit_avro(normalized, topic=self.kafka_cfg.lmp_topic if self.kafka_cfg else None, schema_key="lmp")
        return total_records

    def ingest_as_results(
        self,
        *,
        start_utc: datetime,
        end_utc: datetime,
        market_run_id: str,
    ) -> int:
        req = OasisRequest(
            queryname="AS_RESULTS",
            startdatetime=_fmt_gmt(start_utc),
            enddatetime=_fmt_gmt(end_utc),
            market_run_id=market_run_id,
        )
        archive = self._download_with_bisect(req)
        self._land_archive(req, archive)
        rows = list(self.client.iter_csv_rows(archive))
        if not rows:
            from xml.etree import ElementTree as ET
            xml_bytes = self.client.extract_xml_bytes(archive)
            root = ET.fromstring(xml_bytes)
            rows = [
                {child.tag.upper(): (child.text or "").strip() for child in report}
                for report in root.findall(".//REPORT_DATA")
            ]
        records: List[Mapping[str, Any]] = []
        for row in rows:
            rec = CaisoOasisClient.normalize_as_row(row)
            rec["ingest_ts"] = _utc_now_micros()
            rec["record_hash"] = _record_hash_asm(rec)
            records.append(rec)
        self._emit_avro(records, topic=self.kafka_cfg.asm_topic if self.kafka_cfg else None, schema_key="asm")
        return len(records)

    def ingest_load_and_forecast(
        self,
        *,
        start_utc: datetime,
        end_utc: datetime,
        market_run_id: str,
        include_renewables: bool = True,
    ) -> int:
        total = 0
        for query in ("SLD_FCST", *(["SLD_REN_FCST"] if include_renewables else [])):
            req = OasisRequest(
                queryname=query,
                startdatetime=_fmt_gmt(start_utc),
                enddatetime=_fmt_gmt(end_utc),
                market_run_id=market_run_id,
            )
            archive = self._download_with_bisect(req)
            self._land_archive(req, archive)
            rows = list(self.client.iter_csv_rows(archive))
            if not rows:
                from xml.etree import ElementTree as ET
                xml_bytes = self.client.extract_xml_bytes(archive)
                root = ET.fromstring(xml_bytes)
                rows = [
                    {child.tag.upper(): (child.text or "").strip() for child in report}
                    for report in root.findall(".//REPORT_DATA")
                ]
            records: List[Mapping[str, Any]] = []
            for row in rows:
                rec = CaisoOasisClient.normalize_load_row(row)
                rec["ingest_ts"] = _utc_now_micros()
                records.append(rec)
            self._emit_avro(records, topic=self.kafka_cfg.load_topic if self.kafka_cfg else None, schema_key="load")
            total += len(records)
        return total

    def ingest_nodes(self, *, effective_day: Optional[datetime] = None) -> int:
        """Ingest PNode registry using ATL_PNODE and emit to Kafka.

        effective_day selects a calendar day to anchor the query; defaults to today UTC.
        """
        day = (effective_day or datetime.utcnow().replace(tzinfo=timezone.utc)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start = day
        end = day + timedelta(days=1)
        req = OasisRequest(
            queryname="ATL_PNODE",
            startdatetime=_fmt_gmt(start),
            enddatetime=_fmt_gmt(end),
        )
        archive = self._download_with_bisect(req)
        self._land_archive(req, archive)
        # CSV-first
        rows = list(self.client.iter_csv_rows(archive))
        if not rows:
            from xml.etree import ElementTree as ET
            xml_bytes = self.client.extract_xml_bytes(archive)
            root = ET.fromstring(xml_bytes)
            rows = [
                {child.tag.upper(): (child.text or "").strip() for child in report}
                for report in root.findall(".//REPORT_DATA")
            ]
        records: List[Mapping[str, Any]] = []
        now = _utc_now_micros()
        for row in rows:
            records.append(
                {
                    "iso_code": "CAISO",
                    "pnode_id": str(row.get("PNODE_ID") or row.get("NODE_ID") or ""),
                    "pnode_name": row.get("PNODE_NAME") or row.get("NODE") or None,
                    "type": row.get("PNODE_TYPE") or row.get("NODE_TYPE") or None,
                    "zone": row.get("TAC_ZONE") or None,
                    "hub": None,
                    "effective_start": _to_utc_micros(row.get("EFFECTIVE_START_DATE")) or None,
                    "effective_end": _to_utc_micros(row.get("EFFECTIVE_END_DATE")) or None,
                    "ingest_ts": now,
                }
            )
        self._emit_avro(records, topic=self.kafka_cfg.pnode_topic if self.kafka_cfg else None, schema_key="pnode")
        return len(records)

    def ingest_apnodes(self, *, effective_day: Optional[datetime] = None) -> int:
        """Ingest APNode registry (aggregation points) via ATL_APNODE."""
        day = (effective_day or datetime.utcnow().replace(tzinfo=timezone.utc)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start = day
        end = day + timedelta(days=1)
        req = OasisRequest(
            queryname="ATL_APNODE",
            startdatetime=_fmt_gmt(start),
            enddatetime=_fmt_gmt(end),
        )
        archive = self._download_with_bisect(req)
        self._land_archive(req, archive)
        rows = list(self.client.iter_csv_rows(archive))
        if not rows:
            from xml.etree import ElementTree as ET
            xml_bytes = self.client.extract_xml_bytes(archive)
            root = ET.fromstring(xml_bytes)
            rows = [
                {child.tag.upper(): (child.text or "").strip() for child in report}
                for report in root.findall(".//REPORT_DATA")
            ]
        records: List[Mapping[str, Any]] = []
        now = _utc_now_micros()
        for row in rows:
            records.append(
                {
                    "iso_code": "CAISO",
                    "pnode_id": str(row.get("APNODE_ID") or row.get("NODE_ID") or row.get("PNODE_ID") or ""),
                    "pnode_name": row.get("APNODE_NAME") or row.get("NODE") or row.get("PNODE_NAME") or None,
                    "type": row.get("APNODE_TYPE") or row.get("NODE_TYPE") or "APNODE",
                    "zone": row.get("TAC_ZONE") or None,
                    "hub": None,
                    "effective_start": _to_utc_micros(row.get("EFFECTIVE_START_DATE")) or None,
                    "effective_end": _to_utc_micros(row.get("EFFECTIVE_END_DATE")) or None,
                    "ingest_ts": now,
                }
            )
        self._emit_avro(records, topic=self.kafka_cfg.pnode_topic if self.kafka_cfg else None, schema_key="pnode")
        return len(records)

    # ---------- Helpers ----------

    def _build_windows(self, start: datetime, end: datetime, *, step: timedelta) -> List[Tuple[datetime, datetime]]:
        s = start
        out: List[Tuple[datetime, datetime]] = []
        while s < end:
            e = min(s + step, end)
            out.append((s, e))
            s = e
        return out

    def _download_with_bisect(self, req: OasisRequest) -> bytes:
        """Download a window; if INVALID_REQUEST, bisect down to 1-hour granularity and retry."""
        try:
            return self.client.fetch_archive(req)
        except CaisoInvalidRequest:
            # Attempt to split the window if possible
            start = _parse_gmt(req.startdatetime)
            end = _parse_gmt(req.enddatetime)
            if not start or not end or (end - start) <= timedelta(hours=1):
                # Give up at 1h windows
                raise
            mid = start + (end - start) / 2
            left = OasisRequest(
                queryname=req.queryname,
                startdatetime=_fmt_gmt(start),
                enddatetime=_fmt_gmt(mid),
                market_run_id=req.market_run_id,
                resultformat=req.resultformat,
                version=req.version,
                node=req.node,
                grp_type=req.grp_type,
                additional=req.additional,
            )
            right = OasisRequest(
                queryname=req.queryname,
                startdatetime=_fmt_gmt(mid),
                enddatetime=_fmt_gmt(end),
                market_run_id=req.market_run_id,
                resultformat=req.resultformat,
                version=req.version,
                node=req.node,
                grp_type=req.grp_type,
                additional=req.additional,
            )
            left_bytes = self._download_with_bisect(left)
            right_bytes = self._download_with_bisect(right)
            # Stitch archives into a single zip
            return _stitch_archives([left_bytes, right_bytes])

    def _emit_avro(self, records: List[Mapping[str, Any]], *, topic: Optional[str], schema_key: str) -> int:
        if not records or not topic or not self.kafka_cfg:
            return 0
        # Build or reuse a producer bound to the topic
        key = f"{topic}:{schema_key}"
        producer = self._producer_cache.get(key)
        if producer is None:
            cfg = CollectorConfig(
                provider="caiso",
                base_url="",
                kafka_topic=topic,
                kafka_bootstrap_servers=self.kafka_cfg.bootstrap_servers,
                schema_registry_url=self.kafka_cfg.schema_registry_url,
                value_schema=self._schemas[schema_key],
            )
            producer = create_avro_producer(cfg)
            self._producer_cache[key] = producer
        # Emit
        count = 0
        for rec in records:
            try:
                producer.produce(topic=topic, value=rec)
            except Exception:
                raise
            count += 1
        producer.flush()
        return count

    def _land_archive(self, req: OasisRequest, archive: bytes) -> None:
        """Write the raw zip archive and a small manifest to landing storage."""
        # Root: files/raw/caiso/oasis/<query>/<yyyymmdd>/
        asof = _parse_gmt(req.startdatetime) or datetime.utcnow().replace(tzinfo=timezone.utc)
        day = asof.strftime("%Y%m%d")
        root = self.landing_root / req.queryname.lower() / day
        _ensure_dir(root)
        # Compose filename
        mrid = (req.market_run_id or "NA").upper()
        node = (req.node or req.grp_type or "NA").upper()
        fn = f"{req.queryname}_{mrid}_{node}_{day}.zip"
        (root / fn).write_bytes(archive)
        manifest = {
            "queryname": req.queryname,
            "params": req.to_params(),
            "saved_at": datetime.utcnow().isoformat() + "Z",
            "filename": fn,
            "size_bytes": len(archive),
        }
        (root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


# ---------- Utility functions ----------


def _fmt_gmt(dt: datetime) -> str:
    # CAISO expects -0000 suffix
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M-0000")


def _parse_gmt(s: str) -> Optional[datetime]:
    try:
        cleaned = s.replace("-0000", "+00:00")
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


def _stitch_archives(parts: Sequence[bytes]) -> bytes:
    """Combine multiple zip archives into one by concatenating members.

    If duplicate filenames are present, suffix them with an incrementing index.
    """
    out = io.BytesIO()
    name_counts: Dict[str, int] = {}
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for part in parts:
            with zipfile.ZipFile(io.BytesIO(part)) as zin:
                for name in zin.namelist():
                    data = zin.read(name)
                    base = name
                    if base in name_counts:
                        name_counts[base] += 1
                        idx = name_counts[base]
                        if "." in base:
                            stem, ext = base.rsplit(".", 1)
                            base = f"{stem}_{idx}.{ext}"
                        else:
                            base = f"{base}_{idx}"
                    else:
                        name_counts[base] = 0
                    zout.writestr(base, data)
    return out.getvalue()


__all__ = ["CaisoOasisCollector", "CaisoKafkaConfig"]
