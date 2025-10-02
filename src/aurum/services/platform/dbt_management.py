"""DBT management service for data transformation orchestration.

Implements business logic for DBT model management, testing, and data mart operations.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol
from datetime import datetime
import yaml

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError

logger = logging.getLogger(__name__)


class DBTExecutor(Protocol):
    """Protocol for DBT command execution."""
    
    async def run_command(self, command: List[str], cwd: Optional[Path] = None) -> Dict[str, Any]:
        """Execute DBT command."""
        ...
    
    async def parse_manifest(self, project_path: Path) -> Dict[str, Any]:
        """Parse DBT manifest file."""
        ...


class DBTManagementService(BaseService):
    """Service for DBT operations.
    
    DBT (data build tool) management provides:
    - Model compilation and execution
    - Test automation and validation
    - Data mart management
    - Lineage tracking and documentation
    - Model dependency analysis
    
    This service:
    - Orchestrates DBT runs
    - Manages model testing
    - Tracks model performance
    - Handles data mart operations
    - Provides model metadata
    """
    
    def __init__(self, executor: Optional[DBTExecutor] = None):
        """Initialize service with DBT executor.
        
        Args:
            executor: DBT command executor implementation
        """
        super().__init__()
        self._executor = executor or DefaultDBTExecutor()
        self._model_cache: Dict[str, Dict[str, Any]] = {}
    
    async def run_models(
        self,
        models: Optional[List[str]] = None,
        target: str = "dev",
        full_refresh: bool = False,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Run DBT models.
        
        Args:
            models: Specific models to run (None for all)
            target: DBT target environment
            full_refresh: Whether to rebuild from scratch
            context: Service context
            
        Returns:
            ServiceResult with run summary
        """
        self._track_operation("dbt_run_models", {"models_count": len(models) if models else "all"})
        
        try:
            # Build DBT command
            command = ["dbt", "run", "--target", target]
            
            if full_refresh:
                command.append("--full-refresh")
            
            if models:
                command.extend(["--models", " ".join(models)])
            
            # Execute DBT run
            result = await self._executor.run_command(command)
            
            # Parse results
            run_summary = {
                "models_run": result.get("results", []),
                "success_count": sum(1 for r in result.get("results", []) if r.get("status") == "success"),
                "error_count": sum(1 for r in result.get("results", []) if r.get("status") == "error"),
                "execution_time": result.get("elapsed_time"),
                "target": target
            }
            
            return ServiceResult.ok(run_summary)
            
        except Exception as e:
            logger.error(f"DBT run failed: {e}")
            return ServiceResult.error(f"DBT execution failed: {str(e)}")
    
    async def test_models(
        self,
        models: Optional[List[str]] = None,
        severity: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Run DBT tests.
        
        Args:
            models: Specific models to test (None for all)
            severity: Minimum severity level to test
            context: Service context
            
        Returns:
            ServiceResult with test results
        """
        self._track_operation("dbt_test_models", {"test_scope": "specific" if models else "all"})
        
        try:
            # Build test command
            command = ["dbt", "test"]
            
            if models:
                command.extend(["--models", " ".join(models)])
            
            if severity:
                command.extend(["--severity", severity])
            
            # Execute tests
            result = await self._executor.run_command(command)
            
            # Parse test results
            test_summary = {
                "tests_run": len(result.get("results", [])),
                "passed": sum(1 for r in result.get("results", []) if r.get("status") == "pass"),
                "failed": sum(1 for r in result.get("results", []) if r.get("status") == "fail"),
                "warnings": sum(1 for r in result.get("results", []) if r.get("status") == "warn"),
                "execution_time": result.get("elapsed_time"),
                "failed_tests": [
                    {
                        "test_name": r.get("unique_id"),
                        "error": r.get("message")
                    }
                    for r in result.get("results", [])
                    if r.get("status") == "fail"
                ]
            }
            
            return ServiceResult.ok(test_summary)
            
        except Exception as e:
            logger.error(f"DBT test failed: {e}")
            return ServiceResult.error(f"Test execution failed: {str(e)}")
    
    async def compile_models(
        self,
        models: Optional[List[str]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Compile DBT models without running.
        
        Args:
            models: Specific models to compile (None for all)
            context: Service context
            
        Returns:
            ServiceResult with compilation results
        """
        self._track_operation("dbt_compile", {"models": len(models) if models else "all"})
        
        try:
            command = ["dbt", "compile"]
            
            if models:
                command.extend(["--models", " ".join(models)])
            
            result = await self._executor.run_command(command)
            
            return ServiceResult.ok({
                "compiled_count": len(result.get("results", [])),
                "compilation_time": result.get("elapsed_time")
            })
            
        except Exception as e:
            logger.error(f"DBT compilation failed: {e}")
            return ServiceResult.error(f"Compilation failed: {str(e)}")
    
    async def get_model_lineage(
        self,
        model_name: str,
        depth: int = 3,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get model lineage information.
        
        Args:
            model_name: Model to analyze
            depth: Maximum depth for lineage
            context: Service context
            
        Returns:
            ServiceResult with lineage data
        """
        self._track_operation("dbt_get_lineage", {"model": model_name})
        
        try:
            # Get manifest data
            manifest = await self._get_manifest()
            
            if not manifest:
                return ServiceResult.error("Unable to load DBT manifest")
            
            # Find model in manifest
            model_data = None
            for node_id, node in manifest.get("nodes", {}).items():
                if node.get("name") == model_name:
                    model_data = node
                    break
            
            if not model_data:
                return ServiceResult.error(f"Model '{model_name}' not found")
            
            # Build lineage
            lineage = {
                "model": model_name,
                "upstream": self._get_upstream_models(model_data, manifest, depth),
                "downstream": self._get_downstream_models(model_data, manifest, depth),
                "sources": model_data.get("sources", []),
                "tests": self._get_model_tests(model_name, manifest)
            }
            
            return ServiceResult.ok(lineage)
            
        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            return ServiceResult.error(f"Lineage analysis failed: {str(e)}")
    
    async def create_data_mart(
        self,
        mart_name: str,
        source_models: List[str],
        schema: str,
        materialization: str = "table",
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Create a new data mart model.
        
        Args:
            mart_name: Name for the data mart
            source_models: Source models to aggregate
            schema: Target schema
            materialization: How to materialize (table/view)
            context: Service context
            
        Returns:
            ServiceResult with creation status
        """
        self._track_operation("dbt_create_mart", {"mart": mart_name})
        
        try:
            # Validate inputs
            if not mart_name or not source_models:
                return ServiceResult.error("Mart name and source models required")
            
            # Generate mart SQL
            mart_sql = self._generate_mart_sql(mart_name, source_models)
            
            # Create mart model file
            mart_path = Path(f"models/marts/{mart_name}.sql")
            mart_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write model with config
            model_content = f"""{{{{
    config(
        materialized='{materialization}',
        schema='{schema}',
        tags=['mart', 'auto-generated']
    )
}}}}

{mart_sql}
"""
            
            mart_path.write_text(model_content)
            
            # Compile to validate
            compile_result = await self.compile_models([mart_name])
            
            if not compile_result.success:
                # Rollback on failure
                mart_path.unlink()
                return ServiceResult.error("Mart compilation failed")
            
            return ServiceResult.ok({
                "mart_name": mart_name,
                "path": str(mart_path),
                "source_models": source_models,
                "materialization": materialization
            })
            
        except Exception as e:
            logger.error(f"Failed to create mart: {e}")
            return ServiceResult.error(f"Mart creation failed: {str(e)}")
    
    async def refresh_documentation(
        self,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Refresh DBT documentation.
        
        Args:
            context: Service context
            
        Returns:
            ServiceResult with documentation status
        """
        self._track_operation("dbt_refresh_docs", {})
        
        try:
            # Generate docs
            result = await self._executor.run_command(["dbt", "docs", "generate"])
            
            return ServiceResult.ok({
                "documentation_updated": True,
                "models_documented": len(result.get("results", [])),
                "generation_time": result.get("elapsed_time")
            })
            
        except Exception as e:
            logger.error(f"Documentation refresh failed: {e}")
            return ServiceResult.error(f"Documentation failed: {str(e)}")
    
    # Private helper methods
    
    async def _get_manifest(self) -> Optional[Dict[str, Any]]:
        """Load DBT manifest."""
        try:
            manifest_path = Path("target/manifest.json")
            if manifest_path.exists():
                import json
                return json.loads(manifest_path.read_text())
            else:
                # Generate manifest if missing
                await self._executor.run_command(["dbt", "compile"])
                if manifest_path.exists():
                    import json
                    return json.loads(manifest_path.read_text())
        except Exception as e:
            logger.error(f"Failed to load manifest: {e}")
        return None
    
    def _get_upstream_models(
        self,
        model: Dict[str, Any],
        manifest: Dict[str, Any],
        depth: int
    ) -> List[Dict[str, str]]:
        """Get upstream model dependencies."""
        upstream = []
        visited = set()
        
        def traverse(node_id: str, current_depth: int):
            if current_depth <= 0 or node_id in visited:
                return
            visited.add(node_id)
            
            node = manifest.get("nodes", {}).get(node_id)
            if node:
                upstream.append({
                    "name": node.get("name"),
                    "type": node.get("resource_type"),
                    "depth": depth - current_depth + 1
                })
                
                for dep in node.get("depends_on", {}).get("nodes", []):
                    traverse(dep, current_depth - 1)
        
        for dep in model.get("depends_on", {}).get("nodes", []):
            traverse(dep, depth)
        
        return upstream
    
    def _get_downstream_models(
        self,
        model: Dict[str, Any],
        manifest: Dict[str, Any],
        depth: int
    ) -> List[Dict[str, str]]:
        """Get downstream model dependencies."""
        model_id = model.get("unique_id")
        downstream = []
        
        for node_id, node in manifest.get("nodes", {}).items():
            deps = node.get("depends_on", {}).get("nodes", [])
            if model_id in deps:
                downstream.append({
                    "name": node.get("name"),
                    "type": node.get("resource_type")
                })
        
        return downstream
    
    def _get_model_tests(
        self,
        model_name: str,
        manifest: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Get tests for a model."""
        tests = []
        
        for node_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") == "test":
                deps = node.get("depends_on", {}).get("nodes", [])
                if any(model_name in dep for dep in deps):
                    tests.append({
                        "name": node.get("name"),
                        "type": node.get("test_metadata", {}).get("name", "generic")
                    })
        
        return tests
    
    def _generate_mart_sql(
        self,
        mart_name: str,
        source_models: List[str]
    ) -> str:
        """Generate SQL for data mart."""
        # Simple aggregation example
        sources = ",\n    ".join([f"ref('{model}')" for model in source_models])
        
        return f"""-- Auto-generated data mart: {mart_name}
-- Sources: {', '.join(source_models)}

WITH aggregated AS (
    SELECT 
        *
    FROM (
        {sources}
    )
)

SELECT * FROM aggregated
"""


class DefaultDBTExecutor:
    """Default DBT command executor using subprocess."""
    
    async def run_command(self, command: List[str], cwd: Optional[Path] = None) -> Dict[str, Any]:
        """Execute DBT command."""
        try:
            # Run in thread pool to avoid blocking
            import asyncio
            import json
            
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd
            )
            
            stdout, stderr = await proc.communicate()
            
            # Try to parse JSON output
            try:
                if stdout:
                    return json.loads(stdout.decode())
            except:
                pass
            
            # Fallback to basic result
            return {
                "results": [],
                "elapsed_time": 0,
                "success": proc.returncode == 0,
                "stderr": stderr.decode() if stderr else None
            }
            
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise ServiceError(f"DBT execution failed: {str(e)}")
    
    async def parse_manifest(self, project_path: Path) -> Dict[str, Any]:
        """Parse DBT manifest file."""
        manifest_path = project_path / "target" / "manifest.json"
        
        if not manifest_path.exists():
            raise ServiceError("Manifest not found - run dbt compile first")
        
        import json
        return json.loads(manifest_path.read_text())
