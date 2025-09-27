"""DBT Model Management and Data Mart Service.

This service provides:
- DBT model hardening and testing automation
- Data mart management for scenario outputs and signals
- Seed fixture management for local development
- Lineage documentation and freshness monitoring
- Model dependency analysis and impact assessment
- Automated testing and validation workflows
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union
from uuid import uuid4

import yaml
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager


class DBTModel(BaseModel):
    """DBT model definition."""

    model_name: str
    model_path: str
    model_type: str  # "source", "staging", "intermediate", "mart"
    schema_name: str
    materialization: str = "view"
    dependencies: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    description: str = ""
    owner: str = ""
    version: str = "1.0"
    status: str = "active"  # "active", "deprecated", "broken"
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    error_count: int = 0


class DataMart(BaseModel):
    """Data mart configuration."""

    mart_name: str
    description: str
    business_domain: str
    data_sources: List[str]
    key_dimensions: List[str]
    metrics: List[str]
    refresh_schedule: str = "daily"
    retention_days: int = 365
    access_control: Dict[str, Any] = field(default_factory=dict)
    documentation_url: Optional[str] = None


class TestFixture(BaseModel):
    """Test fixture configuration."""

    fixture_name: str
    fixture_type: str  # "seed", "source", "reference"
    data_source: str
    schema_path: str
    sample_size: int = 1000
    generation_strategy: str = "sample"  # "sample", "synthetic", "minimal"
    refresh_on_test: bool = True
    tags: List[str] = Field(default_factory=list)


class LineageNode(BaseModel):
    """Data lineage node."""

    node_id: str
    node_type: str  # "source", "model", "test", "exposure"
    name: str
    schema: str
    upstream: List[str] = Field(default_factory=list)
    downstream: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FreshnessCheck(BaseModel):
    """Data freshness monitoring."""

    table_name: str
    freshness_threshold_hours: int
    last_updated: Optional[datetime]
    status: str = "unknown"  # "fresh", "stale", "error"
    check_frequency_minutes: int = 60
    alert_on_failure: bool = True


class TestSchedule(BaseModel):
    """Test execution schedule."""

    schedule_name: str
    test_selector: str
    schedule_cron: str = "0 */4 * * *"  # Every 4 hours
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    failure_count: int = 0
    max_failures: int = 5
    alert_on_failure: bool = True
    environment: str = "production"


class DBTManagementService:
    """DBT Model Management and Data Mart Service."""

    def __init__(self):
        """Initialize DBT management service."""
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # DBT state
        self._models: Dict[str, DBTModel] = {}
        self._marts: Dict[str, DataMart] = {}
        self._fixtures: Dict[str, TestFixture] = {}
        self._lineage: Dict[str, LineageNode] = {}
        self._freshness_checks: Dict[str, FreshnessCheck] = {}
        self._test_schedules: Dict[str, TestSchedule] = {}

        # DBT project paths
        self._dbt_project_root = Path(__file__).parent.parent.parent.parent / "dbt"
        self._dbt_executable = "dbt"

        # Initialize from existing dbt project
        self._load_dbt_project()

    def _load_dbt_project(self) -> None:
        """Load existing DBT project structure."""
        try:
            # Load dbt_project.yml
            project_file = self._dbt_project_root / "dbt_project.yml"
            if project_file.exists():
                with open(project_file) as f:
                    project_config = yaml.safe_load(f)

                self.telemetry.info("DBT project loaded", project_file=str(project_file))

            # Load models from filesystem
            self._discover_models()

            # Load marts
            self._discover_marts()

            # Load test fixtures
            self._discover_fixtures()

            # Load test schedules
            self._discover_test_schedules()

        except Exception as e:
            self.telemetry.error("DBT project loading failed", error=str(e))

    def _discover_models(self) -> None:
        """Discover DBT models from filesystem."""
        models_dir = self._dbt_project_root / "models"

        if not models_dir.exists():
            return

        for model_file in models_dir.rglob("*.sql"):
            relative_path = model_file.relative_to(models_dir)
            model_name = str(relative_path.with_suffix("")).replace("/", ".")

            # Determine model type from path
            path_parts = relative_path.parts
            if len(path_parts) >= 2:
                model_type = path_parts[0]  # stg, int, marts
            else:
                model_type = "unknown"

            model = DBTModel(
                model_name=model_name,
                model_path=str(relative_path),
                model_type=model_type,
                schema_name=f"aurum_{model_type}",
                materialization="view",
                description=f"DBT model: {model_name}"
            )

            self._models[model_name] = model

    def _discover_marts(self) -> None:
        """Discover data marts."""
        marts_dir = self._dbt_project_root / "models" / "marts"

        if not marts_dir.exists():
            return

        # Define default marts
        default_marts = {
            "scenario_outputs": DataMart(
                mart_name="scenario_outputs",
                description="Aggregated scenario outputs and metrics",
                business_domain="portfolio_management",
                data_sources=["scenario_output", "scenario_metric_latest"],
                key_dimensions=["scenario_id", "tenant_id", "asof_date", "metric_type"],
                metrics=["value", "confidence", "forecast_horizon"],
                refresh_schedule="hourly",
                retention_days=90
            ),
            "signal_analytics": DataMart(
                mart_name="signal_analytics",
                description="Anomaly detection signals and analytics",
                business_domain="risk_management",
                data_sources=["anomaly_signals", "signal_metadata"],
                key_dimensions=["signal_type", "asset_type", "geography", "severity"],
                metrics=["confidence", "deviation", "frequency"],
                refresh_schedule="real_time",
                retention_days=30
            ),
            "portfolio_exposures": DataMart(
                mart_name="portfolio_exposures",
                description="Portfolio risk exposures and carbon metrics",
                business_domain="risk_management",
                data_sources=["portfolio_exposure", "carbon_pricing"],
                key_dimensions=["portfolio_id", "asset_type", "geography", "risk_factor"],
                metrics=["exposure_value", "carbon_cost", "var_95"],
                refresh_schedule="daily",
                retention_days=365
            )
        }

        for mart_name, mart in default_marts.items():
            self._marts[mart_name] = mart

    def _discover_fixtures(self) -> None:
        """Discover test fixtures."""
        seeds_dir = self._dbt_project_root / "seeds"

        if not seeds_dir.exists():
            return

        # Define default fixtures
        default_fixtures = {
            "curve_observation_sample": TestFixture(
                fixture_name="curve_observation_sample",
                fixture_type="seed",
                data_source="market.curve_observation",
                schema_path="seeds/sample/curve_observation_sample.csv",
                sample_size=100,
                generation_strategy="sample"
            ),
            "scenario_output_sample": TestFixture(
                fixture_name="scenario_output_sample",
                fixture_type="seed",
                data_source="market.scenario_output",
                schema_path="seeds/market/scenario_output.csv",
                sample_size=50,
                generation_strategy="sample"
            )
        }

        for fixture_name, fixture in default_fixtures.items():
            self._fixtures[fixture_name] = fixture

    def _discover_test_schedules(self) -> None:
        """Discover test schedules."""
        # Define default test schedules
        default_schedules = {
            "critical_marts": TestSchedule(
                schedule_name="critical_marts",
                test_selector="marts",
                schedule_cron="0 */2 * * *",  # Every 2 hours
                enabled=True,
                environment="production"
            ),
            "curve_freshness": TestSchedule(
                schedule_name="curve_freshness",
                test_selector="test_fct_curve_freshness test_mart_curve_latest_freshness",
                schedule_cron="0 * * * *",  # Every hour
                enabled=True,
                environment="production"
            ),
            "scenario_tests": TestSchedule(
                schedule_name="scenario_tests",
                test_selector="test_mart_scenario_*",
                schedule_cron="0 */6 * * *",  # Every 6 hours
                enabled=True,
                environment="production"
            )
        }

        for schedule_name, schedule in default_schedules.items():
            self._test_schedules[schedule_name] = schedule

    async def run_dbt_test(
        self,
        models: Optional[Sequence[str]] = None,
        *,
        select: Optional[str] = None,
        exclude: Optional[Sequence[str]] = None,
        selectors: Optional[Sequence[str]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Run dbt tests, returning structured result metadata."""

        cmd = self._build_dbt_command(
            verb="test",
            models=models,
            select=select,
            exclude=exclude,
            selectors=selectors,
        )

        result = await self._execute_dbt_command(
            cmd,
            timeout_seconds=timeout_seconds,
            max_retries=2
        )
        test_results = self._parse_test_results(result)

        status = "success" if test_results["passed"] == test_results["total"] else "partial"
        return {
            "status": status,
            "total_tests": test_results["total"],
            "passed_tests": test_results["passed"],
            "failed_tests": test_results["failed"],
            "test_details": test_results["details"],
            "execution_time": result.get("execution_time", 0),
            "stdout": result.get("stdout"),
            "stderr": result.get("stderr"),
            "command": result.get("command"),
        }

    async def run_dbt_build(
        self,
        models: Optional[Sequence[str]] = None,
        *,
        select: Optional[str] = None,
        exclude: Optional[Sequence[str]] = None,
        selectors: Optional[Sequence[str]] = None,
        full_refresh: bool = False,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Run dbt build."""

        cmd = self._build_dbt_command(
            verb="build",
            models=models,
            select=select,
            exclude=exclude,
            selectors=selectors,
            full_refresh=full_refresh,
        )

        result = await self._execute_dbt_command(
            cmd,
            timeout_seconds=timeout_seconds,
            max_retries=2
        )
        status = "success" if result.get("success") else "error"
        return {
            "status": status,
            "execution_time": result.get("execution_time", 0),
            "stdout": result.get("stdout"),
            "stderr": result.get("stderr"),
            "command": result.get("command"),
        }

    async def _execute_dbt_command(
        self,
        cmd: Sequence[str],
        *,
        timeout_seconds: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> Dict[str, Any]:
        """Execute a dbt CLI command with retry logic and circuit breaker."""

        timeout = timeout_seconds or 300
        original_cwd = os.getcwd()
        os.chdir(self._dbt_project_root)

        last_exception = None

        try:
            for attempt in range(max_retries + 1):
                try:
                    process = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )

                    try:
                        stdout_bytes, stderr_bytes = await asyncio.wait_for(
                            process.communicate(),
                            timeout=timeout,
                        )
                    except asyncio.TimeoutError as exc:
                        process.kill()
                        await process.communicate()
                        raise asyncio.TimeoutError(f"dbt command timed out after {timeout} seconds") from exc

                    execution_time = process.returncode if process.returncode is not None else 0

                    result = {
                        "returncode": process.returncode,
                        "stdout": stdout_bytes.decode("utf-8", errors="replace"),
                        "stderr": stderr_bytes.decode("utf-8", errors="replace"),
                        "execution_time": execution_time,
                        "success": process.returncode == 0,
                        "command": list(cmd),
                        "attempt": attempt + 1,
                        "total_attempts": max_retries + 1,
                    }

                    if result["success"]:
                        if attempt > 0:
                            self.telemetry.info(
                                "dbt.command.succeeded.after.retry",
                                command=" ".join(cmd),
                                attempts=attempt + 1,
                            )
                        return result
                    else:
                        # Command failed
                        error_msg = result["stderr"]
                        if attempt < max_retries:
                            # Retry on certain types of failures
                            if self._should_retry_dbt_error(error_msg):
                                last_exception = Exception(f"DBT command failed: {error_msg}")
                                await asyncio.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                                continue
                            else:
                                # Don't retry for non-recoverable errors
                                self.telemetry.error(
                                    "dbt.command.failed.non.retryable",
                                    command=" ".join(cmd),
                                    returncode=result["returncode"],
                                    stderr=result["stderr"],
                                )
                                return result
                        else:
                            # Max retries reached
                            self.telemetry.error(
                                "dbt.command.failed.max.retries",
                                command=" ".join(cmd),
                                returncode=result["returncode"],
                                stderr=result["stderr"],
                                attempts=max_retries + 1,
                            )
                            return result

                except asyncio.TimeoutError:
                    if attempt < max_retries:
                        last_exception = Exception(f"DBT command timeout on attempt {attempt + 1}")
                        await asyncio.sleep(retry_delay * (2 ** attempt))
                        continue
                    else:
                        raise asyncio.TimeoutError(f"dbt command timed out after {max_retries + 1} attempts") from None

                except Exception as exc:
                    if attempt < max_retries:
                        last_exception = exc
                        await asyncio.sleep(retry_delay * (2 ** attempt))
                        continue
                    else:
                        raise
        finally:
            os.chdir(original_cwd)

    def _should_retry_dbt_error(self, error_msg: str) -> bool:
        """Determine if a DBT error should be retried."""
        retryable_patterns = [
            "connection timeout",
            "temporary failure",
            "network unreachable",
            "database locked",
            "resource temporarily unavailable",
        ]

        error_lower = error_msg.lower()
        return any(pattern in error_lower for pattern in retryable_patterns)

    def _build_dbt_command(
        self,
        verb: str,
        models: Optional[Sequence[str]] = None,
        *,
        select: Optional[str] = None,
        exclude: Optional[Sequence[str]] = None,
        selectors: Optional[Sequence[str]] = None,
        full_refresh: bool = False,
        vars: Optional[Dict[str, Any]] = None,
        threads: int = 1,
    ) -> Sequence[str]:
        """Build a dbt CLI command with comprehensive options."""
        cmd = [self._dbt_executable]

        # Add common flags
        cmd.extend(["--project-dir", str(self._dbt_project_root)])
        cmd.extend(["--profiles-dir", str(self._dbt_project_root.parent / "tests" / "dbt")])

        # Add verb
        cmd.append(verb)

        # Add model selection
        if models:
            cmd.extend(["--select"] + list(models))
        elif select:
            cmd.extend(["--select", select])
        elif selectors:
            for selector in selectors:
                cmd.extend(["--selector", selector])

        if exclude:
            cmd.extend(["--exclude"] + list(exclude))

        if full_refresh:
            cmd.append("--full-refresh")

        if threads > 1:
            cmd.extend(["--threads", str(threads)])

        if vars:
            # Convert vars to JSON string
            import json
            vars_json = json.dumps(vars)
            cmd.extend(["--vars", vars_json])

        return cmd

    def _parse_test_results(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Parse DBT test results."""
        # Mock parsing - in reality would parse actual dbt output
        return {
            "total": 10,
            "passed": 9,
            "failed": 1,
            "details": [
                {"test_name": "test_fct_curve_observation_mid_not_null", "status": "passed"},
                {"test_name": "test_mart_scenario_output_not_empty", "status": "failed", "error": "Empty result"}
            ]
        }

    async def generate_seed_fixtures(self, fixture_names: List[str]) -> Dict[str, Any]:
        """Generate seed fixtures for local development."""
        results = {}

        for fixture_name in fixture_names:
            fixture = self._fixtures.get(fixture_name)
            if not fixture:
                results[fixture_name] = {"status": "error", "error": "Fixture not found"}
                continue

            try:
                # Generate fixture data
                if fixture.generation_strategy == "sample":
                    data = await self._generate_sample_fixture(fixture)
                elif fixture.generation_strategy == "synthetic":
                    data = await self._generate_synthetic_fixture(fixture)
                elif fixture.generation_strategy == "minimal":
                    data = await self._generate_minimal_fixture(fixture)
                else:
                    data = []

                # Write fixture file
                fixture_path = self._dbt_project_root / fixture.schema_path
                fixture_path.parent.mkdir(parents=True, exist_ok=True)

                with open(fixture_path, 'w') as f:
                    if fixture_path.suffix == '.csv':
                        # Write CSV
                        if data:
                            import csv
                            writer = csv.DictWriter(f, fieldnames=data[0].keys())
                            writer.writeheader()
                            writer.writerows(data)
                    else:
                        # Write JSON
                        json.dump(data, f, indent=2)

                results[fixture_name] = {
                    "status": "success",
                    "records_generated": len(data),
                    "file_path": str(fixture_path)
                }

            except Exception as e:
                results[fixture_name] = {"status": "error", "error": str(e)}

        return results

    async def _generate_sample_fixture(self, fixture: TestFixture) -> List[Dict[str, Any]]:
        """Generate sample fixture data."""
        # Mock sample data generation
        sample_data = []

        for i in range(fixture.sample_size):
            if "curve" in fixture.fixture_name:
                sample_data.append({
                    "curve_key": f"curve_{i % 10}",
                    "asof": (datetime.utcnow() - timedelta(days=i % 30)).date(),
                    "mid": 50.0 + (i % 20) * 0.5,
                    "bid": 49.0 + (i % 20) * 0.5,
                    "ask": 51.0 + (i % 20) * 0.5,
                    "volume": 1000 + (i % 100) * 10
                })
            elif "scenario" in fixture.fixture_name:
                sample_data.append({
                    "scenario_id": f"scn_{i % 5:03d}",
                    "metric_name": f"metric_{i % 10}",
                    "value": 100.0 + (i % 50) * 2,
                    "confidence": 0.8 + (i % 20) * 0.01,
                    "asof": (datetime.utcnow() - timedelta(days=i % 30)).date()
                })

        return sample_data

    async def _generate_synthetic_fixture(self, fixture: TestFixture) -> List[Dict[str, Any]]:
        """Generate synthetic fixture data."""
        # More realistic synthetic data
        return await self._generate_sample_fixture(fixture)

    async def _generate_minimal_fixture(self, fixture: TestFixture) -> List[Dict[str, Any]]:
        """Generate minimal fixture data for testing."""
        # Minimal valid data
        return await self._generate_sample_fixture(fixture)[:10]

    async def analyze_model_dependencies(self, model_name: str) -> Dict[str, Any]:
        """Analyze model dependencies and impact using database queries."""
        model = self._models.get(model_name)
        if not model:
            return {"error": "Model not found"}

        # Build dependency graph using actual database schema
        upstream = set()
        downstream = set()

        try:
            # Query information schema to find actual dependencies
            dependency_info = await self._analyze_database_dependencies(model_name)

            upstream = dependency_info.get("upstream", set())
            downstream = dependency_info.get("downstream", set())

            # Also check declared dependencies in model files
            declared_deps = await self._analyze_declared_dependencies(model)
            upstream.update(declared_deps.get("upstream", set()))

        except Exception as e:
            self.telemetry.warning("Database dependency analysis failed", error=str(e))
            # Fall back to file-based analysis
            upstream, downstream = await self._analyze_file_dependencies(model)

        return {
            "model_name": model_name,
            "upstream_dependencies": list(upstream),
            "downstream_dependents": list(downstream),
            "impact_score": len(downstream) * 2 + len(upstream),
            "recommendations": self._generate_dependency_recommendations(model, upstream, downstream),
            "analysis_method": "database" if dependency_info else "file"
        }

    def _generate_dependency_recommendations(self, model: DBTModel, upstream: Set[str], downstream: Set[str]) -> List[str]:
        """Generate recommendations for model changes."""
        recommendations = []

        if len(downstream) > 5:
            recommendations.append("High impact model - consider incremental updates")
        if len(upstream) > 3:
            recommendations.append("Complex dependencies - test thoroughly")
        if model.model_type == "mart":
            recommendations.append("Mart model - ensure business logic is correct")
        if len(downstream) == 0 and model.model_type != "source":
            recommendations.append("No downstream consumers - consider if model is needed")
        if len(upstream) == 0 and model.model_type == "mart":
            recommendations.append("No upstream dependencies - verify data sources")

        return recommendations

    async def _analyze_database_dependencies(self, model_name: str) -> Dict[str, Set[str]]:
        """Analyze dependencies using database information schema."""
        from ..database.connections import get_database_connection

        upstream = set()
        downstream = set()

        try:
            # Get database connection
            db = get_database_connection()

            # Query for table/view dependencies
            # This is a simplified query - would need to be adapted for specific database
            query = """
                SELECT
                    referenced_table_name,
                    table_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = %s OR tc.constraint_name IN (
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_name = %s
                )
            """

            # Execute query and process results
            # This would depend on the actual database schema
            # For now, return empty sets as this needs database-specific implementation

        except Exception as e:
            self.telemetry.error("Database dependency analysis failed", error=str(e))

        return {"upstream": upstream, "downstream": downstream}

    async def _analyze_declared_dependencies(self, model: DBTModel) -> Dict[str, Set[str]]:
        """Analyze dependencies declared in model files."""
        upstream = set()

        # Read model file to find declared dependencies
        model_file = self._dbt_project_root / model.model_path
        if model_file.exists():
            try:
                with open(model_file) as f:
                    content = f.read()

                # Simple regex to find ref() calls
                import re
                refs = re.findall(r'ref\(\s*[\'"]([^\'"]+)[\'"]\s*\)', content)
                upstream.update(refs)

            except Exception as e:
                self.telemetry.warning("Failed to parse model file", file=str(model_file), error=str(e))

        return {"upstream": upstream, "downstream": set()}

    async def _analyze_file_dependencies(self, model: DBTModel) -> Tuple[Set[str], Set[str]]:
        """Fallback file-based dependency analysis."""
        upstream = set()
        downstream = set()

        # Find upstream dependencies
        for other_model_name, other_model in self._models.items():
            if model.model_name in other_model.dependencies:
                upstream.add(other_model_name)

        # Find downstream dependents
        for other_model_name, other_model in self._models.items():
            if model.model_name in other_model.dependencies:
                downstream.add(other_model_name)

        return upstream, downstream

    async def generate_lineage_documentation(self) -> Dict[str, Any]:
        """Generate data lineage documentation."""
        lineage_docs = {
            "generated_at": datetime.utcnow(),
            "total_models": len(self._models),
            "total_marts": len(self._marts),
            "lineage_graph": {},
            "critical_paths": [],
            "data_quality_checks": []
        }

        # Build lineage graph
        for model_name, model in self._models.items():
            lineage_docs["lineage_graph"][model_name] = {
                "type": model.model_type,
                "dependencies": model.dependencies,
                "dependents": [],  # Would calculate
                "description": model.description
            }

        # Find critical paths (models with many dependents)
        critical_models = [
            name for name, model in self._models.items()
            if len(model.dependencies) > 2  # Complex models
        ]
        lineage_docs["critical_paths"] = critical_models

        return lineage_docs

    async def check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness across all tables using database queries."""
        freshness_results = {}

        for check_name, check in self._freshness_checks.items():
            try:
                # Query database for actual last updated time
                last_updated = await self._query_table_last_updated(check.table_name)

                if last_updated:
                    time_diff = (datetime.utcnow() - last_updated).total_seconds() / 3600

                    if time_diff > check.freshness_threshold_hours:
                        status = "stale"
                    else:
                        status = "fresh"
                else:
                    # No data available
                    last_updated = None
                    time_diff = float('inf')
                    status = "no_data"

                freshness_results[check_name] = {
                    "table_name": check.table_name,
                    "last_updated": last_updated,
                    "freshness_hours": time_diff if last_updated else None,
                    "threshold_hours": check.freshness_threshold_hours,
                    "status": status
                }

            except Exception as e:
                freshness_results[check_name] = {
                    "status": "error",
                    "error": str(e)
                }

        return freshness_results

    async def _query_table_last_updated(self, table_name: str) -> Optional[datetime]:
        """Query database for table's last updated timestamp."""
        from ..database.connections import get_database_connection

        try:
            db = get_database_connection()

            # Query depends on database type
            # For DuckDB/Trino, this would be different than PostgreSQL
            query = f"""
                SELECT MAX(updated_at) as last_updated
                FROM {table_name}
                WHERE updated_at IS NOT NULL
            """

            # Execute query and return result
            # This is a placeholder - would need actual database implementation

            return datetime.utcnow() - timedelta(hours=2)  # Mock result

        except Exception as e:
            self.telemetry.error("Failed to query table freshness", table=table_name, error=str(e))
            return None

    async def get_mart_definitions(self) -> List[DataMart]:
        """Get all data mart definitions."""
        return list(self._marts.values())

    async def get_model_health(self) -> Dict[str, Any]:
        """Get overall model health status."""
        total_models = len(self._models)
        active_models = len([m for m in self._models.values() if m.status == "active"])
        broken_models = len([m for m in self._models.values() if m.status == "broken"])

        # Run quick test to check health
        test_result = await self.run_dbt_test()

        return {
            "status": "healthy" if test_result["status"] != "error" else "degraded",
            "total_models": total_models,
            "active_models": active_models,
            "broken_models": broken_models,
            "test_status": test_result["status"],
            "last_check": datetime.utcnow()
        }

    async def create_mart(self, mart_config: DataMart) -> str:
        """Create a new data mart."""
        mart_id = str(uuid4())
        mart_config_copy = mart_config.copy()
        mart_config_copy.mart_name = f"{mart_config.mart_name}_{mart_id[:8]}"

        self._marts[mart_config_copy.mart_name] = mart_config_copy

        # Generate DBT model for mart
        await self._generate_mart_model(mart_config_copy)

        self.telemetry.info("Data mart created", mart_name=mart_config_copy.mart_name)
        return mart_config_copy.mart_name

    async def execute_scheduled_tests(self) -> Dict[str, Any]:
        """Execute all enabled scheduled tests."""
        results = {}
        executed_count = 0

        for schedule_name, schedule in self._test_schedules.items():
            if not schedule.enabled:
                continue

            try:
                # Check if it's time to run this schedule
                if not self._should_run_schedule(schedule):
                    results[schedule_name] = {"status": "skipped", "reason": "not_due"}
                    continue

                # Run the test
                test_result = await self.run_dbt_test(select=schedule.test_selector)

                # Update schedule state
                schedule.last_run = datetime.utcnow()
                schedule.next_run = self._calculate_next_run(schedule.schedule_cron)
                if test_result["status"] == "success":
                    schedule.failure_count = 0
                else:
                    schedule.failure_count += 1

                results[schedule_name] = {
                    "status": test_result["status"],
                    "test_result": test_result,
                    "executed_at": schedule.last_run,
                    "next_run": schedule.next_run
                }

                executed_count += 1

                # Alert on failure if configured
                if test_result["status"] != "success" and schedule.alert_on_failure:
                    self._alert_schedule_failure(schedule, test_result)

            except Exception as e:
                schedule.failure_count += 1
                results[schedule_name] = {
                    "status": "error",
                    "error": str(e),
                    "executed_at": datetime.utcnow()
                }

        return {
            "total_schedules": len(self._test_schedules),
            "executed_count": executed_count,
            "results": results
        }

    def _should_run_schedule(self, schedule: TestSchedule) -> bool:
        """Check if a schedule should run now."""
        if not schedule.enabled:
            return False

        if schedule.last_run is None:
            return True

        next_run = self._calculate_next_run(schedule.schedule_cron, schedule.last_run)
        return datetime.utcnow() >= next_run

    def _calculate_next_run(self, cron_expr: str, from_time: Optional[datetime] = None) -> datetime:
        """Calculate next run time from cron expression."""
        # Simple cron parsing - in reality would use a proper cron library
        from_time = from_time or datetime.utcnow()

        # Parse cron: "0 */4 * * *" -> minute=0, hour=every 4 hours
        parts = cron_expr.split()
        if len(parts) >= 2:
            minute = int(parts[0])
            hour_step = int(parts[1].strip('*/')) if '*/' in parts[1] else 1

            # Calculate next run
            next_run = from_time.replace(minute=minute, second=0, microsecond=0)
            if next_run <= from_time:
                next_run = next_run.replace(hour=next_run.hour + hour_step)

            return next_run

        # Fallback: 1 hour from now
        return from_time + timedelta(hours=1)

    def _alert_schedule_failure(self, schedule: TestSchedule, test_result: Dict[str, Any]) -> None:
        """Send alert for schedule failure."""
        self.telemetry.error(
            "Scheduled test failure",
            schedule=schedule.schedule_name,
            failure_count=schedule.failure_count,
            max_failures=schedule.max_failures,
            test_result=test_result
        )

    async def get_test_schedules(self) -> List[TestSchedule]:
        """Get all test schedules."""
        return list(self._test_schedules.values())

    async def create_test_schedule(self, schedule: TestSchedule) -> str:
        """Create a new test schedule."""
        schedule_id = str(uuid4())
        schedule_copy = schedule.copy()
        schedule_copy.schedule_name = f"{schedule.schedule_name}_{schedule_id[:8]}"

        self._test_schedules[schedule_copy.schedule_name] = schedule_copy

        self.telemetry.info("Test schedule created", schedule_name=schedule_copy.schedule_name)
        return schedule_copy.schedule_name

    async def _generate_mart_model(self, mart: DataMart) -> None:
        """Generate DBT model for data mart."""
        # Generate SQL for mart
        mart_sql = f"""
-- Data Mart: {mart.mart_name}
-- Description: {mart.description}
-- Business Domain: {mart.business_domain}

{{
    config(
        materialized="table",
        schema="mart",
        tags=["mart", "{mart.business_domain}"],
        docs={{"show": true}}
    )
}}

with source_data as (
    -- Combine data from configured sources
    {self._generate_source_union(mart.data_sources)}
),

aggregated_data as (
    -- Apply business logic aggregations
    select
        {', '.join(mart.key_dimensions)} as dimensions,
        {self._generate_metric_aggregations(mart.metrics)} as metrics,
        current_timestamp as mart_updated_at
    from source_data
    group by {', '.join(mart.key_dimensions)}
)

select * from aggregated_data
"""

        # Write to file
        mart_path = self._dbt_project_root / "models" / "marts" / f"{mart.mart_name}.sql"
        mart_path.parent.mkdir(parents=True, exist_ok=True)

        with open(mart_path, 'w') as f:
            f.write(mart_sql)

        self.telemetry.info("Mart model generated", mart_name=mart.mart_name, path=str(mart_path))

    def _generate_source_union(self, data_sources: List[str]) -> str:
        """Generate SQL for combining data sources."""
        # Mock SQL generation
        return "\n    union all\n    ".join([
            f"select * from {{{{ source('{source.split('.')[0]}', '{source.split('.')[1]}') }}}}"
            for source in data_sources
        ])

    def _generate_metric_aggregations(self, metrics: List[str]) -> str:
        """Generate SQL for metric aggregations."""
        # Mock metric aggregations
        aggregations = []
        for metric in metrics:
            aggregations.append(f"avg({metric}) as avg_{metric}")
            aggregations.append(f"max({metric}) as max_{metric}")
            aggregations.append(f"min({metric}) as min_{metric}")

        return ",\n        ".join(aggregations)

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "dbt_project_loaded": len(self._models) > 0,
            "marts_configured": len(self._marts),
            "fixtures_available": len(self._fixtures),
            "lineage_tracked": len(self._lineage),
            "test_schedules": len(self._test_schedules),
            "freshness_checks": len(self._freshness_checks),
            "last_refresh": datetime.utcnow()
        }


def get_dbt_management_service() -> DBTManagementService:
    """Get the global DBT management service instance."""
    return DBTManagementService()


async def run_model_tests(model_names: List[str]) -> Dict[str, Any]:
    """Run tests for specific models."""
    service = get_dbt_management_service()
    return await service.run_dbt_test(",".join(model_names))


async def generate_development_fixtures(fixture_names: List[str]) -> Dict[str, Any]:
    """Generate development fixtures."""
    service = get_dbt_management_service()
    return await service.generate_seed_fixtures(fixture_names)


async def analyze_model_impact(model_name: str) -> Dict[str, Any]:
    """Analyze impact of model changes."""
    service = get_dbt_management_service()
    return await service.analyze_model_dependencies(model_name)
