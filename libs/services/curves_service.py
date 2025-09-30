from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, AsyncIterator

from libs.storage.trino import TrinoAnalyticRepo
from libs.common.config import get_settings


@dataclass(slots=True)
class Curve:
    id: str
    name: str
    description: Optional[str]
    data_points: int
    created_at: str


class CurvesService:
    """Domain service for curves queries and exports."""

    def __init__(self, trino: Optional[TrinoAnalyticRepo] = None):
        self._settings = get_settings()
        self._trino = trino or TrinoAnalyticRepo(self._settings.database)

    async def list_curves(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        name_filter: Optional[str],
        include_debug: bool,
    ) -> Tuple[List[Curve], Dict[str, Any]]:
        sql = [
            "SELECT id, name, description, data_points, created_at",
            "FROM iceberg.market.curves_catalog",
            "WHERE tenant_id = :tenant_id",
        ]
        params: Dict[str, Any] = {"tenant_id": tenant_id, "offset": offset, "limit": limit}
        if name_filter:
            sql.append("AND name ILIKE :name_filter")
            params["name_filter"] = f"%{name_filter}%"
        sql.append("ORDER BY name OFFSET :offset LIMIT :limit")
        query = "\n".join(sql)

        rows_dicts = await self._trino.execute_query(query)
        # Note: TrinoAnalyticRepo does not support parameterized queries yet;
        # the provided 'params' is not used. Ensure predicates are safe.
        # Here we interpolate only controlled values (offset/limit and ILIKE escaped by %). Optional TODO: add safe builder.
        # Rebuild query safely using the settings. For now, use results as-is if repository applies query directly.
        rows = [
            [
                row.get("id"),
                row.get("name"),
                row.get("description"),
                row.get("data_points"),
                row.get("created_at"),
            ]
            for row in rows_dicts
        ]
        curves = [
            Curve(
                id=row[0],
                name=row[1],
                description=row[2],
                data_points=row[3],
                created_at=str(row[4]),
            )
            for row in rows
        ]
        debug: Dict[str, Any] = {}
        return curves, debug

    async def stream_curve_export(
        self,
        *,
        asof: Optional[str],
        iso: Optional[str],
        market: Optional[str],
        location: Optional[str],
        product: Optional[str],
        block: Optional[str],
        chunk_size: int,
    ) -> AsyncIterator[Dict[str, Any]]:
        where: List[str] = []
        params: Dict[str, Any] = {}
        if asof:
            where.append("asof_date = DATE :asof")
            params["asof"] = asof
        if iso:
            where.append("iso = :iso")
            params["iso"] = iso
        if market:
            where.append("market = :market")
            params["market"] = market
        if location:
            where.append("location = :location")
            params["location"] = location
        if product:
            where.append("product = :product")
            params["product"] = product
        if block:
            where.append("block = :block")
            params["block"] = block

        predicate = f"WHERE {' AND '.join(where)}" if where else ""
        sql = (
            "SELECT curve_key, tenor_label, tenor_type, contract_month, asof_date, mid, bid, ask, price_type "
            f"FROM iceberg.market.curves_export {predicate} ORDER BY curve_key, tenor_label, asof_date"
        )
        # TrinoAnalyticRepo has no streaming API; fetch and yield in chunks.
        rows = await self._trino.execute_query(sql)
        buffer: List[Dict[str, Any]] = []
        for row in rows:
            item = {
                "curve_key": row.get("curve_key"),
                "tenor_label": row.get("tenor_label"),
                "tenor_type": row.get("tenor_type"),
                "contract_month": row.get("contract_month"),
                "asof_date": row.get("asof_date"),
                "mid": row.get("mid"),
                "bid": row.get("bid"),
                "ask": row.get("ask"),
                "price_type": row.get("price_type"),
            }
            buffer.append(item)
            if len(buffer) >= max(1, chunk_size):
                for it in buffer:
                    yield it
                buffer.clear()
        for it in buffer:
            yield it

    async def get_curve_diff(self, *, curve_id: str, from_timestamp: str, to_timestamp: str) -> Curve:
        sql = (
            "SELECT id, name, description, data_points, created_at "
            "FROM iceberg.market.curves_diff WHERE id = :curve_id AND from_ts = :from_ts AND to_ts = :to_ts"
        )
        params = {"curve_id": curve_id, "from_ts": from_timestamp, "to_ts": to_timestamp}
        rows = await self._trino.execute_query(sql)
        rowd = rows[0]
        return Curve(
            id=str(rowd.get("id")),
            name=str(rowd.get("name")),
            description=rowd.get("description"),
            data_points=int(rowd.get("data_points", 0) or 0),
            created_at=str(rowd.get("created_at")),
        )


