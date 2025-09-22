"""Enhanced OpenLineage integration for end-to-end data lineage tracking.

This module provides seamless integration across:
- Airflow DAGs and tasks
- dbt models and transformations
- Seatunnel jobs and pipelines
- Trino queries
- Custom data pipeline steps

It automatically tracks data flows between systems and provides
comprehensive lineage metadata for the entire data platform.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable, Awaitable, ContextManager
from functools import wraps
import threading

from .openlineage_emitter import OpenLineageEmitter, OpenLineageEvent
from ..logging import StructuredLogger, LogLevel, create_logger

logger = logging.getLogger(__name__)


@dataclass
class LineageContext:
    """Context for tracking lineage across pipeline steps."""

    run_id: str
    namespace: str = "aurum"
    job_name: str = ""
    component: str = ""  # airflow, dbt, seatunnel, trino, custom
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    inputs: List[Dict[str, str]] = field(default_factory=list)
    outputs: List[Dict[str, str]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_run_id: Optional[str] = None  # For nested lineage
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None


class OpenLineageManager:
    """Central manager for OpenLineage event emission across all components."""

    def __init__(self, openlineage_url: str = "http://localhost:5000", namespace: str = "aurum"):
        self.emitter = OpenLineageEmitter(openlineage_url, namespace)
        self.namespace = namespace
        self.contexts: Dict[str, LineageContext] = {}  # run_id -> context
        self.logger = create_logger(
            source_name="openlineage_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.openlineage.events",
            dataset="lineage_tracking"
        )

        # Thread-local storage for context management
        self._local = threading.local()

    def _get_current_context(self) -> Optional[LineageContext]:
        """Get the current lineage context for this thread."""
        return getattr(self._local, 'current_context', None)

    def _set_current_context(self, context: LineageContext):
        """Set the current lineage context for this thread."""
        self._local.current_context = context

    @asynccontextmanager
    async def lineage_context(
        self,
        component: str,
        job_name: str,
        parent_run_id: Optional[str] = None,
        inputs: Optional[List[Dict[str, str]]] = None,
        outputs: Optional[List[Dict[str, str]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ContextManager[LineageContext]:
        """Context manager for automatic lineage tracking."""

        # Generate run ID
        run_id = f"{component}_{job_name}_{int(datetime.now(timezone.utc).timestamp())}_{uuid.uuid4().hex[:8]}"

        # Create context
        context = LineageContext(
            run_id=run_id,
            namespace=self.namespace,
            job_name=job_name,
            component=component,
            start_time=datetime.now(timezone.utc),
            inputs=inputs or [],
            outputs=outputs or [],
            metadata=metadata or {},
            parent_run_id=parent_run_id
        )

        # Store context
        self.contexts[run_id] = context
        self._set_current_context(context)

        try:
            # Emit START event
            await self._emit_event(context, "START")
            yield context

        except Exception as e:
            # Emit FAIL event
            context.error_message = str(e)
            await self._emit_event(context, "FAIL")
            raise

        finally:
            # Emit COMPLETE or FAIL event
            context.end_time = datetime.now(timezone.utc)
            if context.execution_time_seconds is None and context.start_time:
                context.execution_time_seconds = (context.end_time - context.start_time).total_seconds()

            event_type = "FAIL" if context.error_message else "COMPLETE"
            await self._emit_event(context, event_type)

            # Clean up
            self.contexts.pop(run_id, None)
            self._set_current_context(None)

    async def _emit_event(self, context: LineageContext, event_type: str) -> bool:
        """Emit an OpenLineage event for the given context."""
        try:
            # Convert context to appropriate event format
            if context.component == "dbt":
                return self.emitter.emit_dbt_event(
                    model_name=context.job_name,
                    event_type=event_type,
                    run_id=context.run_id,
                    inputs=[inp["name"] for inp in context.inputs],
                    outputs=[out["name"] for out in context.outputs],
                    execution_time=context.execution_time_seconds,
                    error_message=context.error_message
                )
            elif context.component == "seatunnel":
                return self.emitter.emit_seatunnel_event(
                    job_name=context.job_name,
                    event_type=event_type,
                    run_id=context.run_id,
                    inputs=context.inputs,
                    outputs=context.outputs,
                    execution_time=context.execution_time_seconds,
                    error_message=context.error_message
                )
            elif context.component == "trino":
                return self.emitter.emit_trino_event(
                    query_id=context.job_name,
                    event_type=event_type,
                    run_id=context.run_id,
                    inputs=[inp["name"] for inp in context.inputs],
                    outputs=[out["name"] for out in context.outputs],
                    execution_time=context.execution_time_seconds,
                    error_message=context.error_message
                )
            else:
                # Custom component
                return self.emitter.emit_custom_event(
                    component=context.component,
                    job_name=context.job_name,
                    event_type=event_type,
                    run_id=context.run_id,
                    inputs=context.inputs,
                    outputs=context.outputs,
                    execution_time=context.execution_time_seconds,
                    error_message=context.error_message,
                    custom_facets=context.metadata
                )
        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Error emitting {event_type} event for {context.component}:{context.job_name}: {e}",
                "lineage_event_error",
                component=context.component,
                job_name=context.job_name,
                event_type=event_type,
                error=str(e)
            )
            return False

    async def record_lineage(
        self,
        component: str,
        job_name: str,
        event_type: str,
        run_id: Optional[str] = None,
        inputs: Optional[List[Dict[str, str]]] = None,
        outputs: Optional[List[Dict[str, str]]] = None,
        execution_time: Optional[float] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Record a lineage event manually."""
        context = LineageContext(
            run_id=run_id or f"{component}_{job_name}_{uuid.uuid4().hex[:8]}",
            namespace=self.namespace,
            job_name=job_name,
            component=component,
            inputs=inputs or [],
            outputs=outputs or [],
            metadata=metadata or {},
            error_message=error_message,
            execution_time_seconds=execution_time
        )

        return await self._emit_event(context, event_type)

    async def get_lineage_context(self, run_id: str) -> Optional[LineageContext]:
        """Get lineage context by run ID."""
        return self.contexts.get(run_id)


# Global lineage manager instance
_global_lineage_manager: Optional[OpenLineageManager] = None


async def get_lineage_manager() -> OpenLineageManager:
    """Get or create the global lineage manager."""
    global _global_lineage_manager

    if _global_lineage_manager is None:
        _global_lineage_manager = OpenLineageManager()

    return _global_lineage_manager


def lineage_tracker(component: str, job_name: str):
    """Decorator for automatic lineage tracking of functions."""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            lineage_manager = await get_lineage_manager()

            async with lineage_manager.lineage_context(component, job_name) as context:
                try:
                    start_time = time.time()
                    result = await func(*args, **kwargs)
                    context.execution_time_seconds = time.time() - start_time
                    return result
                except Exception as e:
                    context.error_message = str(e)
                    raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # For synchronous functions, run in a thread pool
            import asyncio
            return asyncio.run(async_wrapper(*args, **kwargs))

        # Return appropriate wrapper based on function type
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


# Airflow Integration
class AirflowLineageIntegration:
    """OpenLineage integration for Airflow DAGs and tasks."""

    @staticmethod
    async def track_dag_run(dag_id: str, run_id: str, tasks: List[str]) -> ContextManager[LineageContext]:
        """Track an Airflow DAG run with all its tasks."""
        lineage_manager = await get_lineage_manager()

        async with lineage_manager.lineage_context(
            component="airflow",
            job_name=dag_id,
            inputs=[],  # Would be populated from DAG configuration
            outputs=[],  # Would be populated from DAG configuration
            metadata={"dag_id": dag_id, "tasks": tasks, "run_id": run_id}
        ) as context:
            yield context

    @staticmethod
    async def track_task(
        dag_id: str,
        task_id: str,
        run_id: str,
        inputs: Optional[List[Dict[str, str]]] = None,
        outputs: Optional[List[Dict[str, str]]] = None
    ) -> ContextManager[LineageContext]:
        """Track an Airflow task execution."""
        lineage_manager = await get_lineage_manager()

        async with lineage_manager.lineage_context(
            component="airflow_task",
            job_name=task_id,
            parent_run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            metadata={"dag_id": dag_id, "task_id": task_id}
        ) as context:
            yield context


# dbt Integration
class DbtLineageIntegration:
    """Enhanced OpenLineage integration for dbt models."""

    @staticmethod
    async def track_model_execution(
        model_name: str,
        inputs: List[str],
        outputs: List[str],
        run_id: Optional[str] = None
    ) -> ContextManager[LineageContext]:
        """Track dbt model execution with comprehensive lineage."""
        lineage_manager = await get_lineage_manager()

        # Convert model names to dataset references
        input_datasets = [{"type": "dbt_model", "name": inp} for inp in inputs]
        output_datasets = [{"type": "dbt_model", "name": out} for out in outputs]

        async with lineage_manager.lineage_context(
            component="dbt",
            job_name=model_name,
            inputs=input_datasets,
            outputs=output_datasets,
            metadata={
                "model_name": model_name,
                "input_models": inputs,
                "output_models": outputs
            }
        ) as context:
            yield context

    @staticmethod
    async def track_dbt_run(
        run_id: str,
        models: List[str],
        project_dir: str = ""
    ) -> ContextManager[LineageContext]:
        """Track a complete dbt run."""
        lineage_manager = await get_lineage_manager()

        async with lineage_manager.lineage_context(
            component="dbt_run",
            job_name=f"dbt_run_{run_id}",
            inputs=[],  # Would be populated from dbt manifest
            outputs=[],  # Would be populated from dbt manifest
            metadata={
                "run_id": run_id,
                "models": models,
                "project_dir": project_dir
            }
        ) as context:
            yield context


# Seatunnel Integration
class SeatunnelLineageIntegration:
    """Enhanced OpenLineage integration for Seatunnel jobs."""

    @staticmethod
    async def track_job_execution(
        job_name: str,
        config_file: str,
        inputs: List[Dict[str, str]],
        outputs: List[Dict[str, str]]
    ) -> ContextManager[LineageContext]:
        """Track Seatunnel job execution."""
        lineage_manager = await get_lineage_manager()

        async with lineage_manager.lineage_context(
            component="seatunnel",
            job_name=job_name,
            inputs=inputs,
            outputs=outputs,
            metadata={
                "job_name": job_name,
                "config_file": config_file,
                "input_sources": [inp["name"] for inp in inputs],
                "output_destinations": [out["name"] for out in outputs]
            }
        ) as context:
            yield context

    @staticmethod
    async def track_pipeline(
        pipeline_name: str,
        jobs: List[str],
        run_id: Optional[str] = None
    ) -> ContextManager[LineageContext]:
        """Track a Seatunnel pipeline with multiple jobs."""
        lineage_manager = await get_lineage_manager()

        async with lineage_manager.lineage_context(
            component="seatunnel_pipeline",
            job_name=pipeline_name,
            inputs=[],  # Would be aggregated from all jobs
            outputs=[],  # Would be aggregated from all jobs
            metadata={
                "pipeline_name": pipeline_name,
                "jobs": jobs,
                "run_id": run_id
            }
        ) as context:
            yield context


# Trino Integration
class TrinoLineageIntegration:
    """Enhanced OpenLineage integration for Trino queries."""

    @staticmethod
    async def track_query(
        query_id: str,
        query_text: str,
        input_tables: List[str],
        output_tables: List[str],
        session_properties: Optional[Dict[str, str]] = None
    ) -> ContextManager[LineageContext]:
        """Track Trino query execution."""
        lineage_manager = await get_lineage_manager()

        # Convert table names to dataset references
        input_datasets = [{"type": "table", "name": table} for table in input_tables]
        output_datasets = [{"type": "table", "name": table} for table in output_tables]

        async with lineage_manager.lineage_context(
            component="trino",
            job_name=query_id,
            inputs=input_datasets,
            outputs=output_datasets,
            metadata={
                "query_id": query_id,
                "query_text": query_text,
                "input_tables": input_tables,
                "output_tables": output_tables,
                "session_properties": session_properties or {}
            }
        ) as context:
            yield context

    @staticmethod
    async def track_query_session(
        session_id: str,
        queries: List[str],
        user: str = ""
    ) -> ContextManager[LineageContext]:
        """Track a Trino query session."""
        lineage_manager = await get_lineage_manager()

        async with lineage_manager.lineage_context(
            component="trino_session",
            job_name=f"session_{session_id}",
            inputs=[],  # Would be aggregated from all queries
            outputs=[],  # Would be aggregated from all queries
            metadata={
                "session_id": session_id,
                "queries": queries,
                "user": user
            }
        ) as context:
            yield context


# Utility functions for manual lineage tracking

async def track_data_ingestion(
    source_name: str,
    dataset_name: str,
    source_type: str = "external_api",
    destination_type: str = "iceberg"
) -> bool:
    """Track data ingestion from external source."""
    lineage_manager = await get_lineage_manager()

    return await lineage_manager.record_lineage(
        component="ingestion",
        job_name=f"{source_name}_to_{dataset_name}",
        event_type="COMPLETE",
        inputs=[{"type": source_type, "name": source_name}],
        outputs=[{"type": destination_type, "name": dataset_name}],
        metadata={"source": source_name, "destination": dataset_name}
    )


async def track_data_transformation(
    input_dataset: str,
    output_dataset: str,
    transformation_type: str = "custom"
) -> bool:
    """Track data transformation between datasets."""
    lineage_manager = await get_lineage_manager()

    return await lineage_manager.record_lineage(
        component=transformation_type,
        job_name=f"{input_dataset}_to_{output_dataset}",
        event_type="COMPLETE",
        inputs=[{"type": "dataset", "name": input_dataset}],
        outputs=[{"type": "dataset", "name": output_dataset}],
        metadata={"input": input_dataset, "output": output_dataset}
    )


async def initialize_openlineage_integration():
    """Initialize the OpenLineage integration system."""
    manager = await get_lineage_manager()
    return manager
