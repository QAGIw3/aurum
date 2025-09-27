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
import json
import logging
import os
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
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
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
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
    tags: List[str] = field(default_factory=list)


class LineageNode(BaseModel):
    """Data lineage node."""

    node_id: str
    node_type: str  # "source", "model", "test", "exposure"
    name: str
    schema: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class FreshnessCheck(BaseModel):
    """Data freshness monitoring."""

    table_name: str
    freshness_threshold_hours: int
    last_updated: Optional[datetime]
    status: str = "unknown"  # "fresh", "stale", "error"
    check_frequency_minutes: int = 60
    alert_on_failure: bool = True


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

    async def run_dbt_test(self, model_name: Optional[str] = None) -> Dict[str, Any]:
        """Run DBT tests."""
        try:
            # Build dbt command
            cmd = [self._dbt_executable, "test"]

            if model_name:
                cmd.extend(["--models", model_name])

            # Execute test
            result = await self._execute_dbt_command(cmd)

            # Parse results
            test_results = self._parse_test_results(result)

            return {
                "status": "success" if test_results["passed"] == test_results["total"] else "partial",
                "total_tests": test_results["total"],
                "passed_tests": test_results["passed"],
                "failed_tests": test_results["failed"],
                "test_details": test_results["details"],
                "execution_time": result.get("execution_time", 0)
            }

        except Exception as e:
            self.telemetry.error("DBT test execution failed", error=str(e))
            return {"status": "error", "error": str(e)}

    async def run_dbt_build(self, models: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run DBT build."""
        try:
            cmd = [self._dbt_executable, "build"]

            if models:
                cmd.extend(["--models"] + models)

            result = await self._execute_dbt_command(cmd)

            return {
                "status": "success",
                "models_built": result.get("models_built", 0),
                "execution_time": result.get("execution_time", 0),
                "output": result.get("output", "")
            }

        except Exception as e:
            self.telemetry.error("DBT build failed", error=str(e))
            return {"status": "error", "error": str(e)}

    async def _execute_dbt_command(self, cmd: List[str]) -> Dict[str, Any]:
        """Execute DBT command."""
        try:
            # Change to dbt project directory
            original_cwd = os.getcwd()
            os.chdir(self._dbt_project_root)

            # Execute command
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            execution_time = time.time() - start_time

            # Restore original directory
            os.chdir(original_cwd)

            return {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "execution_time": execution_time,
                "success": result.returncode == 0
            }

        except subprocess.TimeoutExpired:
            os.chdir(original_cwd)
            return {"error": "Command timed out", "execution_time": 300}
        except Exception as e:
            os.chdir(original_cwd)
            return {"error": str(e)}

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
                    "scenario_id": f"scn_{i % 5"03d"}",
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
        """Analyze model dependencies and impact."""
        model = self._models.get(model_name)
        if not model:
            return {"error": "Model not found"}

        # Build dependency graph
        upstream = set()
        downstream = set()

        # Find upstream dependencies
        for other_model_name, other_model in self._models.items():
            if model_name in other_model.dependencies:
                upstream.add(other_model_name)

        # Find downstream dependents
        for other_model_name, other_model in self._models.items():
            if model_name in other_model.dependencies:
                downstream.add(other_model_name)

        return {
            "model_name": model_name,
            "upstream_dependencies": list(upstream),
            "downstream_dependents": list(downstream),
            "impact_score": len(downstream) * 2 + len(upstream),
            "recommendations": self._generate_dependency_recommendations(model, upstream, downstream)
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

        return recommendations

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
        """Check data freshness across all tables."""
        freshness_results = {}

        for check_name, check in self._freshness_checks.items():
            try:
                # Mock freshness check - in reality would query database
                last_updated = datetime.utcnow() - timedelta(hours=2)
                time_diff = (datetime.utcnow() - last_updated).total_seconds() / 3600

                if time_diff > check.freshness_threshold_hours:
                    status = "stale"
                else:
                    status = "fresh"

                freshness_results[check_name] = {
                    "table_name": check.table_name,
                    "last_updated": last_updated,
                    "freshness_hours": time_diff,
                    "threshold_hours": check.freshness_threshold_hours,
                    "status": status
                }

            except Exception as e:
                freshness_results[check_name] = {
                    "status": "error",
                    "error": str(e)
                }

        return freshness_results

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
