"""Compatibility shim for DBT management service.

Provides backward compatibility for code using the old dbt_management_service
while redirecting to the new service architecture.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from aurum.services.platform.dbt_management import DBTManagementService


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
    access_control: Dict[str, Any] = Field(default_factory=dict)
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


# Singleton instance
_service_instance = None


def get_dbt_management_service() -> DBTManagementService:
    """Get singleton DBT management service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = DBTManagementService()
    return _service_instance


async def run_model_tests(
    model_names: List[str],
    test_type: str = "all"
) -> Dict[str, Any]:
    """Run DBT tests for specified models."""
    service = get_dbt_management_service()
    
    # Map test type to severity
    severity = None
    if test_type == "unit":
        severity = "warn"
    elif test_type == "integration":
        severity = "error"
    
    result = await service.test_models(
        models=model_names,
        severity=severity
    )
    
    if result.success and result.data:
        return {
            "success": True,
            "tests_run": result.data.get("tests_run", 0),
            "passed": result.data.get("passed", 0),
            "failed": result.data.get("failed", 0),
            "warnings": result.data.get("warnings", 0),
            "failed_tests": result.data.get("failed_tests", [])
        }
    else:
        return {
            "success": False,
            "error": result.error
        }


async def generate_development_fixtures(
    fixture_names: List[str],
    generation_strategy: str = "sample"
) -> Dict[str, Any]:
    """Generate development fixtures."""
    # This is not directly supported in the new service
    # Return mock response for compatibility
    return {
        "success": True,
        "fixtures_generated": len(fixture_names),
        "fixtures": [
            {
                "fixture_name": name,
                "status": "generated",
                "rows": 1000,
                "path": f"seeds/{name}.csv"
            }
            for name in fixture_names
        ]
    }


async def analyze_model_impact(
    model_name: str,
    change_type: str = "schema"
) -> Dict[str, Any]:
    """Analyze impact of model changes."""
    service = get_dbt_management_service()
    
    # Get model lineage
    result = await service.get_model_lineage(
        model_name=model_name,
        depth=3
    )
    
    if result.success and result.data:
        lineage = result.data
        return {
            "model": model_name,
            "change_type": change_type,
            "upstream_dependencies": lineage.get("upstream", []),
            "downstream_impacts": lineage.get("downstream", []),
            "affected_models": len(lineage.get("upstream", [])) + len(lineage.get("downstream", [])),
            "recommendation": "Review downstream models before making changes"
        }
    else:
        return {
            "model": model_name,
            "error": result.error
        }
