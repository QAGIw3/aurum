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
from ..scenarios.monte_carlo import get_monte_carlo_engine, MonteCarloConfig
from ..scenarios.forecasting import get_forecasting_engine, ForecastConfig
from ..scenarios.feature_store import get_feature_store


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
                schema=self.config.database_schema,
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
                schema=self.config.database_schema,
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
        """Create a new scenario run using Monte Carlo or forecasting models."""
        store = get_scenario_store()
        mc_engine = get_monte_carlo_engine()
        forecast_engine = get_forecasting_engine()

        # Get the scenario first to validate it exists
        scenario = await store.get_scenario(scenario_id)
        if not scenario:
            raise ValueError(f"Scenario {scenario_id} not found")

        run_id = str(uuid4())

        # Check scenario type
        scenario_type = options.get("scenario_type", "monte_carlo")
        is_forecasting = scenario_type in ["forecasting", "backtest"]
        is_cross_asset = scenario_type == "cross_asset"

        # Create run record
        run_data = {
            "id": run_id,
            "scenario_id": scenario_id,
            "status": "running",
            "priority": options.get("priority", "normal"),
            "parameters": options.get("parameters", {}),
            "environment": options.get("environment", {}),
            "created_at": datetime.utcnow(),
            "started_at": datetime.utcnow(),
        }

        run = await store.create_run_from_dict(run_data)

        try:
            if is_cross_asset:
                # Run cross-asset scenario
                await self._run_cross_asset_scenario(run_id, scenario, options)
            elif is_forecasting:
                # Run forecasting scenario
                await self._run_forecasting_scenario(run_id, scenario, options)
            else:
                # Run Monte Carlo scenario
                await self._run_monte_carlo_scenario(run_id, scenario, options, mc_engine)

            # Update run status to completed
            await store.update_run_state(run_id, {
                "status": "succeeded",
                "completed_at": datetime.utcnow(),
                "duration_seconds": (datetime.utcnow() - run_data["created_at"]).total_seconds(),
            })

        except Exception as exc:
            # Update run status to failed
            await store.update_run_state(run_id, {
                "status": "failed",
                "error_message": str(exc),
                "completed_at": datetime.utcnow(),
            })
            raise

        return run

    async def _run_monte_carlo_scenario(self, run_id: str, scenario: Any, options: Dict[str, Any], engine: Any) -> None:
        """Run Monte Carlo simulation scenario."""
        # Extract Monte Carlo configuration from options
        mc_config = MonteCarloConfig(
            num_simulations=options.get("num_simulations", 1000),
            random_seed=options.get("seed"),
            confidence_level=options.get("confidence_level", 0.95),
        )

        # Define model parameters based on scenario type
        model_configs = self._get_model_configs_for_scenario(scenario, options)

        # Run simulations
        results = await engine.run_multi_model_simulation(
            model_configs=model_configs,
            global_config=mc_config,
            seed=options.get("seed"),
        )

        # Store results
        await self._store_simulation_results(run_id, results)

    async def _run_forecasting_scenario(self, run_id: str, scenario: Any, options: Dict[str, Any]) -> None:
        """Run forecasting scenario with backtesting."""
        forecast_engine = get_forecasting_engine()

        # Get historical data for forecasting
        historical_data = await self._get_historical_data_for_scenario(scenario, options)

        if historical_data is None:
            raise ValueError("No historical data available for forecasting scenario")

        # Extract forecasting configuration
        forecast_config = ForecastConfig(
            model_type=options.get("forecast_model", "ARIMA"),
            forecast_horizon=options.get("forecast_horizon", 24),
            seasonality_periods=options.get("seasonality_periods"),
            confidence_level=options.get("confidence_level", 0.95),
            cross_validation_folds=options.get("cv_folds", 5),
        )

        # Check if this is a backtest request
        if options.get("run_backtest", False):
            # Run backtesting
            backtest_result = await forecast_engine.backtest(
                historical_data,
                forecast_config.model_type,
                forecast_config,
                test_size=options.get("test_size", 0.2),
            )

            # Store backtest results
            await self._store_backtest_results(run_id, backtest_result)
        else:
            # Run forecasting
            forecast_result = await forecast_engine.forecast(
                historical_data,
                forecast_config.model_type,
                forecast_config,
                include_intervals=True,
            )

            # Store forecast results
            await self._store_forecast_results(run_id, forecast_result)

    async def _run_cross_asset_scenario(self, run_id: str, scenario: Any, options: Dict[str, Any]) -> None:
        """Run cross-asset scenario using feature store."""
        feature_store = get_feature_store()

        # Get scenario parameters
        start_date = options.get("start_date", datetime.now() - timedelta(days=30))
        end_date = options.get("end_date", datetime.now())
        geography = options.get("geography", "US")
        curve_families = scenario.metadata.get("curve_families", ["weather", "load", "price"])

        # Get cross-asset features
        features = await feature_store.get_features_for_scenario(
            scenario_id=scenario.id,
            curve_families=curve_families,
            start_date=start_date,
            end_date=end_date,
        )

        # Get target variable for modeling
        target_variable = options.get("target_variable", "lmp_price")

        # Select features for modeling
        feature_list = options.get("feature_list", [
            'temperature', 'humidity', 'wind_speed',
            'load_mw', 'load_change_1h', 'load_change_24h',
            'price_change_1h', 'price_volatility_24h',
            'temp_load_correlation_24h', 'load_price_correlation_24h',
            'is_peak_hour', 'is_weekend'
        ])

        # Get features and target for modeling
        X, y = await feature_store.iceberg_store.get_features_for_modeling(
            start_date=start_date,
            end_date=end_date,
            geography=geography,
            target_variable=target_variable,
            feature_list=feature_list,
        )

        # Store feature analysis results
        await self._store_cross_asset_results(run_id, features, X, y, target_variable)

    async def _store_cross_asset_results(
        self,
        run_id: str,
        features: pd.DataFrame,
        X: pd.DataFrame,
        y: pd.Series,
        target_variable: str
    ) -> None:
        """Store cross-asset analysis results."""
        store = get_scenario_store()

        # Store feature statistics
        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": "feature_count",
            "value": len(X.columns),
            "unit": "count",
            "created_at": datetime.utcnow(),
        })

        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": "data_points",
            "value": len(X),
            "unit": "count",
            "created_at": datetime.utcnow(),
        })

        # Store target variable statistics
        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": f"{target_variable}_mean",
            "value": float(y.mean()),
            "unit": "$/MWh" if target_variable == "lmp_price" else "MWh",
            "created_at": datetime.utcnow(),
        })

        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": f"{target_variable}_std",
            "value": float(y.std()),
            "unit": "$/MWh" if target_variable == "lmp_price" else "MWh",
            "created_at": datetime.utcnow(),
        })

        # Store correlation matrix summary
        correlation_matrix = X.corr()
        avg_correlation = correlation_matrix.values[np.triu_indices_from(correlation_matrix.values, k=1)].mean()

        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": "avg_feature_correlation",
            "value": float(avg_correlation),
            "unit": "ratio",
            "created_at": datetime.utcnow(),
        })

    async def _get_historical_data_for_scenario(self, scenario: Any, options: Dict[str, Any]) -> Optional[pd.Series]:
        """Get historical data for forecasting scenario."""
        # This is a placeholder - in a real implementation,
        # this would query the data warehouse for historical time series data
        # based on the scenario's curve families and time range

        # For now, generate synthetic data
        import pandas as pd
        import numpy as np

        curve_families = scenario.metadata.get("curve_families", [])
        days_back = options.get("historical_days", 365)

        if "demand" in curve_families or any("load" in cf.lower() for cf in curve_families):
            # Generate synthetic demand data
            dates = pd.date_range(
                start=datetime.now() - timedelta(days=days_back),
                periods=days_back * 24,
                freq='H'
            )

            # Base demand pattern
            base_demand = options.get("base_demand", 1000.0)

            # Generate realistic demand pattern
            demand_data = []
            for i, date in enumerate(dates):
                hour = date.hour

                # Daily pattern
                daily_factor = 0.7 + 0.6 * np.sin(2 * np.pi * hour / 24)

                # Weekly pattern
                weekday = date.weekday()
                weekly_factor = 1.2 if weekday < 5 else 0.8

                # Random variation
                random_factor = np.random.normal(1.0, 0.1)

                demand = base_demand * daily_factor * weekly_factor * random_factor
                demand_data.append(demand)

            return pd.Series(demand_data, index=dates)

        return None

    async def _store_backtest_results(self, run_id: str, backtest_result: Any) -> None:
        """Store backtesting results."""
        store = get_scenario_store()

        # Store summary metrics
        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": "mape",
            "value": backtest_result.mape,
            "unit": "percent",
            "created_at": datetime.utcnow(),
        })

        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": "smape",
            "value": backtest_result.smape,
            "unit": "percent",
            "created_at": datetime.utcnow(),
        })

        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": "rmse",
            "value": backtest_result.rmse,
            "unit": "MWh",
            "created_at": datetime.utcnow(),
        })

        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": "r2_score",
            "value": backtest_result.r2_score,
            "unit": "ratio",
            "created_at": datetime.utcnow(),
        })

    async def _store_forecast_results(self, run_id: str, forecast_result: Any) -> None:
        """Store forecasting results."""
        store = get_scenario_store()

        # Store forecast statistics
        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": f"{forecast_result.model_type.lower()}_forecast_mean",
            "value": np.mean(forecast_result.predictions),
            "unit": "MWh",
            "created_at": datetime.utcnow(),
        })

        await store.create_run_output({
            "scenario_run_id": run_id,
            "metric_name": f"{forecast_result.model_type.lower()}_forecast_std",
            "value": np.std(forecast_result.predictions),
            "unit": "MWh",
            "created_at": datetime.utcnow(),
        })

    def _get_model_configs_for_scenario(self, scenario: Any, options: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Get model configurations based on scenario parameters."""

        # Default configurations for different curve families
        model_configs = {}

        # Check curve families in scenario metadata
        curve_families = scenario.metadata.get("curve_families", [])

        if "demand" in curve_families or any("load" in cf.lower() for cf in curve_families):
            model_configs["demand"] = {
                "base_demand": options.get("base_demand", 1000.0),
                "trend_coefficient": options.get("trend_coefficient", 0.02),
                "seasonal_volatility": options.get("seasonal_volatility", 0.1),
                "weather_sensitivity": options.get("weather_sensitivity", 5.0),
                "economic_factor": options.get("economic_factor", 1.0),
            }

        if "renewable" in curve_families or any("solar" in cf.lower() or "wind" in cf.lower() for cf in curve_families):
            model_configs["renewable"] = {
                "capacity_mw": options.get("capacity_mw", 500.0),
                "capacity_factor": options.get("capacity_factor", 0.35),
                "intermittency_factor": options.get("intermittency_factor", 0.2),
                "curtailment_rate": options.get("curtailment_rate", 0.05),
                "forecast_error": options.get("forecast_error", 0.1),
            }

        if "price" in curve_families or any("lmp" in cf.lower() for cf in curve_families):
            model_configs["price"] = {
                "base_price": options.get("base_price", 50.0),
                "demand_elasticity": options.get("demand_elasticity", 0.3),
                "supply_shock_probability": options.get("supply_shock_probability", 0.1),
                "volatility": options.get("volatility", 0.15),
                "price_cap": options.get("price_cap", 1000.0),
                "price_floor": options.get("price_floor", -100.0),
            }

        return model_configs

    async def _store_simulation_results(self, run_id: str, results: Dict[str, Any]) -> None:
        """Store Monte Carlo simulation results."""
        store = get_scenario_store()

        for model_type, result in results.items():
            # Store summary statistics as scenario outputs
            await store.create_run_output({
                "scenario_run_id": run_id,
                "metric_name": f"{model_type}_mean",
                "value": result.mean,
                "unit": "MWh" if model_type in ["demand", "renewable"] else "$/MWh",
                "created_at": datetime.utcnow(),
            })

            await store.create_run_output({
                "scenario_run_id": run_id,
                "metric_name": f"{model_type}_std_dev",
                "value": result.std_dev,
                "unit": "MWh" if model_type in ["demand", "renewable"] else "$/MWh",
                "created_at": datetime.utcnow(),
            })

            await store.create_run_output({
                "scenario_run_id": run_id,
                "metric_name": f"{model_type}_ci_lower",
                "value": result.confidence_interval[0],
                "unit": "MWh" if model_type in ["demand", "renewable"] else "$/MWh",
                "created_at": datetime.utcnow(),
            })

            await store.create_run_output({
                "scenario_run_id": run_id,
                "metric_name": f"{model_type}_ci_upper",
                "value": result.confidence_interval[1],
                "unit": "MWh" if model_type in ["demand", "renewable"] else "$/MWh",
                "created_at": datetime.utcnow(),
            })

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
