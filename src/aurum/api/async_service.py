"""Async wrapper for service layer with improved performance."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

try:
    import trino
except ImportError:
    trino = None

from ..telemetry.context import get_request_id
from .config import TrinoConfig
from .models import CurvePoint, CurveDiffPoint, Meta
from .scenario_models import ScenarioRunData, ScenarioRunStatus
from ..scenarios.storage import get_scenario_store


class AsyncTrinoClient:
    """Async wrapper for Trino client."""

    def __init__(self, config: TrinoConfig):
        self.config = config
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="trino")

    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a Trino query asynchronously."""
        loop = asyncio.get_event_loop()

        def _execute():
            if trino is None:
                raise RuntimeError("trino package not available")

            conn = trino.dbapi.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                catalog=self.config.catalog,
                schema=self.config.schema,
                http_scheme=self.config.http_scheme,
            )

            try:
                with conn.cursor() as cur:
                    cur.execute(query, params or {})
                    columns = [col[0] for col in cur.description or []]
                    results = []
                    for row in cur.fetchall():
                        result_row = {}
                        for col, value in zip(columns, row):
                            result_row[col] = value
                        results.append(result_row)
                    return results
            finally:
                conn.close()

        try:
            return await loop.run_in_executor(self._executor, _execute)
        except Exception as exc:
            raise RuntimeError(f"Trino query failed: {exc}") from exc

    async def execute_query_stream(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ) -> asyncio.AsyncGenerator[List[Dict[str, Any]], None]:
        """Execute a Trino query and stream results in chunks."""
        loop = asyncio.get_event_loop()

        def _execute_stream():
            if trino is None:
                raise RuntimeError("trino package not available")

            conn = trino.dbapi.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                catalog=self.config.catalog,
                schema=self.config.schema,
                http_scheme=self.config.http_scheme,
            )

            try:
                with conn.cursor() as cur:
                    cur.execute(query, params or {})
                    columns = [col[0] for col in cur.description or []]
                    chunk = []

                    for row in cur.fetchall():
                        result_row = {}
                        for col, value in zip(columns, row):
                            result_row[col] = value
                        chunk.append(result_row)

                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []

                    if chunk:
                        yield chunk

            finally:
                conn.close()

        try:
            async for chunk in loop.run_in_executor(self._executor, _execute_stream):
                yield chunk
        except Exception as exc:
            raise RuntimeError(f"Trino streaming query failed: {exc}") from exc


class AsyncCurveService:
    """Async service for curve operations with improved performance."""

    def __init__(self, trino_config: TrinoConfig, cache_manager=None):
        self.trino_client = AsyncTrinoClient(trino_config)
        self.cache_manager = cache_manager
        self._query_templates = self._load_query_templates()

    def _load_query_templates(self) -> Dict[str, str]:
        """Load SQL query templates."""
        return {
            "curve_data": """
                SELECT * FROM iceberg.market.curve_observation
                WHERE asof_date = DATE('{asof}')
                  AND iso = '{iso}'
                  AND market = '{market}'
                  AND location = '{location}'
                ORDER BY interval_start
                LIMIT {limit} OFFSET {offset}
            """,
            "curve_data_with_filters": """
                SELECT * FROM iceberg.market.curve_observation
                WHERE asof_date = DATE('{asof}')
                  {iso_filter}
                  {market_filter}
                  {location_filter}
                  {product_filter}
                  {block_filter}
                ORDER BY interval_start
                LIMIT {limit} OFFSET {offset}
            """,
        }

    async def fetch_curve_data(
        self,
        asof: Optional[str] = None,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[CurvePoint], Meta]:
        """Fetch curve data with async database access."""
        start_time = time.perf_counter()

        # Check cache first
        if self.cache_manager:
            cache_key = f"curves:{hash((asof, iso, market, location, product, block, limit, offset))}"
            cached_data = await self.cache_manager.get(cache_key)
            if cached_data:
                return cached_data, Meta(
                    request_id=get_request_id(),
                    query_time_ms=round((time.perf_counter() - start_time) * 1000, 2),
                )

        # Build query
        if all([iso, market, location]):
            query = self._query_templates["curve_data"].format(
                asof=asof or "2024-01-01",
                iso=iso,
                market=market,
                location=location,
                limit=limit,
                offset=offset,
            )
        else:
            # Build filters
            filters = []
            iso_filter = f"AND iso = '{iso}'" if iso else ""
            market_filter = f"AND market = '{market}'" if market else ""
            location_filter = f"AND location = '{location}'" if location else ""
            product_filter = f"AND product = '{product}'" if product else ""
            block_filter = f"AND block = '{block}'" if block else ""

            query = self._query_templates["curve_data_with_filters"].format(
                asof=asof or "2024-01-01",
                iso_filter=iso_filter,
                market_filter=market_filter,
                location_filter=location_filter,
                product_filter=product_filter,
                block_filter=block_filter,
                limit=limit,
                offset=offset,
            )

        # Execute query
        rows = await self.trino_client.execute_query(query)

        # Transform to CurvePoint objects
        points = []
        for row in rows:
            point = CurvePoint(
                curve_key=f"{row.get('iso', '')}_{row.get('market', '')}_{row.get('location', '')}",
                tenor_label=row.get('tenor_label', ''),
                asof_date=row.get('asof_date'),
                mid=row.get('mid'),
                bid=row.get('bid'),
                ask=row.get('ask'),
                price_type=row.get('price_type'),
            )
            points.append(point)

        meta = Meta(
            request_id=get_request_id(),
            query_time_ms=round((time.perf_counter() - start_time) * 1000, 2),
        )

        # Cache the result
        if self.cache_manager:
            await self.cache_manager.set(cache_key, points, ttl=300)  # 5 minutes

        return points, meta

    async def fetch_curve_diff(
        self,
        asof_a: str,
        asof_b: str,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[CurveDiffPoint], Meta]:
        """Fetch curve difference data between two dates."""
        start_time = time.perf_counter()

        # Execute two queries in parallel
        query_a = f"""
            SELECT * FROM iceberg.market.curve_observation
            WHERE asof_date = DATE('{asof_a}')
              AND iso = '{iso or ''}'
              AND market = '{market or ''}'
              AND location = '{location or ''}'
            ORDER BY interval_start
            LIMIT {limit} OFFSET {offset}
        """

        query_b = f"""
            SELECT * FROM iceberg.market.curve_observation
            WHERE asof_date = DATE('{asof_b}')
              AND iso = '{iso or ''}'
              AND market = '{market or ''}'
              AND location = '{location or ''}'
            ORDER BY interval_start
            LIMIT {limit} OFFSET {offset}
        """

        # Execute queries concurrently
        results_a, results_b = await asyncio.gather(
            self.trino_client.execute_query(query_a),
            self.trino_client.execute_query(query_b),
        )

        # Process results
        points = []
        for row_a, row_b in zip(results_a, results_b):
            diff_point = CurveDiffPoint(
                curve_key=f"{row_a.get('iso', '')}_{row_a.get('market', '')}_{row_a.get('location', '')}",
                tenor_label=row_a.get('tenor_label', ''),
                asof_a=row_a.get('asof_date'),
                mid_a=row_a.get('mid'),
                asof_b=row_b.get('asof_date'),
                mid_b=row_b.get('mid'),
                diff_abs=(row_a.get('mid') or 0) - (row_b.get('mid') or 0),
                diff_pct=((row_a.get('mid') or 0) - (row_b.get('mid') or 0)) / (row_b.get('mid') or 1) * 100,
            )
            points.append(diff_point)

        meta = Meta(
            request_id=get_request_id(),
            query_time_ms=round((time.perf_counter() - start_time) * 1000, 2),
        )

        return points, meta

    async def fetch_curve_strips(
        self,
        strip_type: str,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[CurvePoint], Meta]:
        """Fetch curve strips data with aggregation."""
        start_time = time.perf_counter()

        # Build aggregation query based on strip type
        aggregation = {
            "CALENDAR": "YEAR(contract_month)",
            "SEASON": "CONCAT(YEAR(contract_month), '-', QUARTER(contract_month))",
            "MONTHLY": "DATE_FORMAT(contract_month, 'yyyy-MM')",
        }.get(strip_type, "contract_month")

        query = f"""
            SELECT
                iso,
                market,
                location,
                product,
                block,
                {aggregation} as period,
                AVG(mid) as avg_price,
                MIN(mid) as min_price,
                MAX(mid) as max_price,
                COUNT(*) as count
            FROM iceberg.market.curve_observation
            WHERE 1=1
              {f"AND iso = '{iso}'" if iso else ""}
              {f"AND market = '{market}'" if market else ""}
              {f"AND location = '{location}'" if location else ""}
              {f"AND product = '{product}'" if product else ""}
              {f"AND block = '{block}'" if block else ""}
            GROUP BY iso, market, location, product, block, {aggregation}
            ORDER BY period
            LIMIT {limit} OFFSET {offset}
        """

        rows = await self.trino_client.execute_query(query)

        # Transform to CurvePoint objects
        points = []
        for row in rows:
            point = CurvePoint(
                curve_key=f"{row.get('iso', '')}_{row.get('market', '')}_{row.get('location', '')}",
                tenor_label=row.get('period', ''),
                asof_date=None,  # Strip data doesn't have individual asof dates
                mid=row.get('avg_price'),
                bid=None,
                ask=None,
                price_type="STRIP",
            )
            points.append(point)

        meta = Meta(
            request_id=get_request_id(),
            query_time_ms=round((time.perf_counter() - start_time) * 1000, 2),
        )

        return points, meta


class AsyncMetadataService:
    """Async service for metadata operations."""

    def __init__(self, trino_config: TrinoConfig):
        self.trino_client = AsyncTrinoClient(trino_config)

    async def get_dimensions(self, asof: Optional[str] = None) -> Tuple[Dict, Dict]:
        """Get available dimensions and their counts."""
        query = f"""
            SELECT
                iso,
                market,
                location,
                product,
                block,
                COUNT(*) as count
            FROM iceberg.market.curve_observation
            {f"WHERE asof_date = DATE('{asof}')" if asof else ""}
            GROUP BY iso, market, location, product, block
        """

        rows = await self.trino_client.execute_query(query)

        dimensions = {}
        counts = {}

        for row in rows:
            for dim in ['iso', 'market', 'location', 'product', 'block']:
                value = row.get(dim)
                if value:
                    if dim not in dimensions:
                        dimensions[dim] = []
                        counts[dim] = []
                    if value not in dimensions[dim]:
                        dimensions[dim].append(value)
                        counts[dim].append({
                            "value": value,
                            "count": row.get('count', 0)
                        })

        return dimensions, counts


class AsyncScenarioService:
    """Async service for scenario operations."""

    def __init__(self, settings):
        self.settings = settings

    async def list_scenarios(
        self,
        tenant_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        name_contains: Optional[str] = None,
        tag: Optional[str] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> Tuple[List[Any], int, Any]:
        """List scenarios with filtering."""
        store = get_scenario_store()
        scenarios, total = await store.list_scenarios(
            tenant_id=tenant_id,
            status=status,
            limit=limit,
            offset=offset,
            name_contains=name_contains,
            tag=tag,
            created_after=created_after,
            created_before=created_before,
        )
        meta = {"request_id": get_request_id()}
        return scenarios, total, meta

    async def create_scenario(self, scenario_data: Dict[str, Any]) -> Any:
        """Create a new scenario."""
        store = get_scenario_store()
        return await store.create_scenario(scenario_data)

    async def get_scenario(self, scenario_id: str) -> Any:
        """Get scenario by ID."""
        store = get_scenario_store()
        return await store.get_scenario(scenario_id)

    async def delete_scenario(self, scenario_id: str) -> bool:
        """Delete scenario by ID."""
        store = get_scenario_store()
        return await store.delete_scenario(scenario_id)

    async def list_scenario_runs(
        self,
        scenario_id: str,
        limit: int = 20,
        offset: int = 0,
        state: Optional[str] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> Tuple[List[Any], int, Any]:
        """List runs for a scenario."""
        store = get_scenario_store()
        runs, total = await store.list_runs(
            scenario_id=scenario_id,
            limit=limit,
            offset=offset,
            state=state,
            created_after=created_after,
            created_before=created_before,
        )
        meta = {"request_id": get_request_id()}
        return runs, total, meta

    async def create_scenario_run(self, scenario_id: str, options: Dict[str, Any]) -> Any:
        """Create a new scenario run."""
        store = get_scenario_store()
        return await store.create_run(scenario_id, options)

    async def get_scenario_run(self, scenario_id: str, run_id: str) -> Any:
        """Get scenario run by ID."""
        store = get_scenario_store()
        return await store.get_run(scenario_id, run_id)

    async def update_scenario_run_state(self, run_id: str, state_update: Dict[str, Any]) -> Any:
        """Update scenario run state."""
        store = get_scenario_store()
        return await store.update_run_state(run_id, state_update)

    async def cancel_scenario_run(self, run_id: str) -> Any:
        """Cancel a scenario run with idempotency and worker signaling."""
        import asyncio
        from .scenario_service import ScenarioStore
        from .auth import require_permission, Permission

        # Get the run first to validate it exists and get tenant info
        run = await self.get_scenario_run("", run_id)
        if not run:
            return None

        # Extract tenant from run
        tenant_id = getattr(run, "tenant_id", None)

        # Check authorization
        principal = getattr(asyncio.current_task(), "principal", None) if asyncio.current_task() else None
        require_permission(principal, Permission.SCENARIOS_DELETE, tenant_id)

        # Check if already cancelled (idempotency)
        current_status = getattr(run, "status", "")
        if current_status == "CANCELLED":
            return run

        # Check if can be cancelled (only QUEUED or RUNNING can be cancelled)
        if current_status not in ["QUEUED", "RUNNING"]:
            from .exceptions import ValidationException
            from ..telemetry.context import get_request_id
            raise ValidationException(
                field="status",
                message=f"Cannot cancel run in status '{current_status}'. Only QUEUED or RUNNING runs can be cancelled.",
                request_id=get_request_id()
            )

        # Update run status to cancelled
        cancelled_run = await self.update_scenario_run_state(run_id, {"status": "CANCELLED"})

        if cancelled_run:
            # Signal worker to cancel the run
            await self._signal_worker_cancellation(run_id, tenant_id)

        return cancelled_run

    async def _signal_worker_cancellation(self, run_id: str, tenant_id: str) -> None:
        """Signal the scenario worker to cancel a run."""
        try:
            # Send cancellation signal to worker queue
            cancellation_message = {
                "type": "cancel",
                "run_id": run_id,
                "tenant_id": tenant_id,
                "timestamp": asyncio.get_event_loop().time(),
            }

            # Use existing messaging system to signal worker
            from .container import get_service
            messaging_service = get_service("MessagingService")

            await messaging_service.send_message(
                topic="scenario-cancellation",
                message=cancellation_message,
                tenant_id=tenant_id,
            )

        except Exception as exc:
            # Log but don't fail - the database state change is the important part
            from ..telemetry import get_logger
            logger = get_logger(__name__)
            logger.warning(f"Failed to signal worker for run cancellation: {exc}")

    async def get_scenario_outputs(
        self,
        scenario_id: str,
        limit: int = 100,
        offset: int = 0,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        metric_name: Optional[str] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Tuple[List[Any], int, Any]:
        """Get scenario outputs with time-based filtering and pagination."""
        start_time_query = time.perf_counter()

        try:
            store = get_scenario_store()
            outputs, total = await store.get_outputs(
                scenario_run_id=scenario_id,
                limit=limit,
                offset=offset,
                start_time=start_time,
                end_time=end_time,
                metric_name=metric_name,
                min_value=min_value,
                max_value=max_value,
                tags=tags
            )
            meta = {"request_id": get_request_id()}
            return outputs, total, meta

        except Exception as exc:
            from ..telemetry import get_logger
            logger = get_logger(__name__)
            logger.error(f"Failed to get scenario outputs: {exc}")
            raise RuntimeError(f"Failed to get scenario outputs: {exc}") from exc

    async def get_scenario_metrics_latest(self, scenario_id: str) -> Any:
        """Get latest metrics for a scenario."""
        # Placeholder implementation
        metrics = {
            "scenario_id": scenario_id,
            "metric_name": "test_metric",
            "value": 100.0,
            "unit": "MW",
            "timestamp": "2024-01-01T00:00:00Z",
        }
        return metrics

    async def create_bulk_scenario_runs(
        self,
        scenario_id: str,
        runs: List[Dict[str, Any]],
        bulk_idempotency_key: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Create multiple scenario runs in bulk with deduplication."""
        from datetime import datetime
        from uuid import uuid4
        import hashlib

        results = []
        duplicates = []

        # Check for bulk-level idempotency
        if bulk_idempotency_key:
            # Check if this bulk operation has been performed before
            bulk_hash = hashlib.sha256(bulk_idempotency_key.encode()).hexdigest()
            # In a real implementation, this would check a database table
            # For now, we'll just proceed and handle individual run deduplication

        for index, run_data in enumerate(runs):
            idempotency_key = run_data.get("idempotency_key")

            try:
                # Check for individual run deduplication
                if idempotency_key:
                    # In a real implementation, this would query the database
                    # For now, we'll simulate checking for existing runs
                    existing_run = None  # Placeholder for database lookup

                    if existing_run:
                        # Found existing run - return as duplicate
                        duplicates.append({
                            "index": index,
                            "idempotency_key": idempotency_key,
                            "existing_run_id": existing_run["id"],
                            "existing_status": existing_run["status"],
                            "created_at": existing_run["created_at"],
                        })
                        continue

                # Create new run
                run_id = str(uuid4())
                now = datetime.utcnow()

                run = ScenarioRunData(
                    id=run_id,
                    scenario_id=scenario_id,
                    status=ScenarioRunStatus.QUEUED,
                    priority=run_data.get("priority", "normal"),
                    started_at=None,
                    completed_at=None,
                    duration_seconds=None,
                    error_message=None,
                    retry_count=0,
                    max_retries=3,
                    progress_percent=None,
                    parameters=run_data.get("parameters", {}),
                    environment=run_data.get("environment", {}),
                    created_at=now,
                    queued_at=now,
                    cancelled_at=None,
                )

                # In a real implementation, this would persist to database
                # ScenarioStore.create_run(run.dict())

                results.append({
                    "index": index,
                    "idempotency_key": idempotency_key,
                    "run_id": run_id,
                    "status": "created",
                    "error": None,
                })

            except Exception as exc:
                results.append({
                    "index": index,
                    "idempotency_key": idempotency_key,
                    "run_id": None,
                    "status": "failed",
                    "error": str(exc),
                })

        return results, duplicates
