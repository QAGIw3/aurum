"""OpenLineage event emitter for Aurum data pipeline components.

This module provides utilities to emit OpenLineage events for:
- dbt model runs
- Seatunnel job executions
- Trino query executions
- Custom data pipeline steps

OpenLineage events provide lineage tracking, execution metadata, and
observability across the entire data pipeline.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlparse

try:
    import requests
    from pydantic import BaseModel, Field
except ImportError:
    requests = None
    BaseModel = dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenLineage event types
EVENT_TYPES = {
    'START': 'START',
    'RUNNING': 'RUNNING',
    'COMPLETE': 'COMPLETE',
    'ABORT': 'ABORT',
    'FAIL': 'FAIL',
    'OTHER': 'OTHER'
}


class OpenLineageEvent(BaseModel):
    """OpenLineage event model following the OpenLineage specification."""

    eventType: str = Field(..., description="Type of the event")
    eventTime: str = Field(..., description="Timestamp of the event in ISO format")
    run: Dict[str, Any] = Field(..., description="Run metadata")
    job: Dict[str, Any] = Field(..., description="Job metadata")
    producer: str = Field(default="urn:aurum:openlineage:1.0.0", description="Producer URI")
    schemaURL: str = Field(default="https://openlineage.io/spec/1-0-5/OpenLineage.json", description="Schema URL")
    inputs: Optional[List[Dict[str, Any]]] = Field(default=None, description="Input datasets")
    outputs: Optional[List[Dict[str, Any]]] = Field(default=None, description="Output datasets")
    errorMessage: Optional[str] = Field(default=None, description="Error message if failed")


class DatasetFacet(BaseModel):
    """Dataset facet for OpenLineage events."""

    schema: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    columns: Optional[List[Dict[str, str]]] = None


class JobFacet(BaseModel):
    """Job facet for OpenLineage events."""

    jobType: Optional[str] = None
    processingType: Optional[str] = None
    documentation: Optional[str] = None


class RunFacet(BaseModel):
    """Run facet for OpenLineage events."""

    nominalTime: Optional[str] = None
    processingType: Optional[str] = None


class OpenLineageEmitter:
    """Emitter for OpenLineage events to track data pipeline execution."""

    def __init__(self, openlineage_url: str, namespace: str = "aurum"):
        self.openlineage_url = openlineage_url.rstrip('/')
        self.namespace = namespace
        self.session = requests.Session() if requests else None

    def _generate_run_id(self, job_name: str, run_id: Optional[str] = None) -> str:
        """Generate a unique run ID for the job."""
        if run_id:
            return run_id
        return f"{self.namespace}:{job_name}:{uuid.uuid4().hex[:8]}"

    def _generate_job_name(self, component: str, job_name: str) -> str:
        """Generate a standardized job name."""
        return f"{self.namespace}.{component}.{job_name}"

    def _generate_dataset_uri(self, dataset_type: str, dataset_name: str) -> str:
        """Generate a dataset URI following OpenLineage conventions."""
        return f"urn:aurum:dataset:{dataset_type}:{dataset_name}"

    def _create_dataset(self, dataset_type: str, dataset_name: str, facets: Optional[Dict] = None) -> Dict[str, Any]:
        """Create a dataset object for OpenLineage events."""
        dataset_uri = self._generate_dataset_uri(dataset_type, dataset_name)

        dataset = {
            "namespace": self.namespace,
            "name": f"{dataset_type}.{dataset_name}",
            "facets": facets or {}
        }

        return dataset

    def emit_dbt_event(
        self,
        model_name: str,
        event_type: str,
        run_id: Optional[str] = None,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
        execution_time: Optional[float] = None,
        error_message: Optional[str] = None,
        **kwargs
    ) -> bool:
        """Emit OpenLineage event for dbt model execution."""

        job_name = self._generate_job_name("dbt", model_name)
        run_id = self._generate_run_id(job_name, run_id)

        # Create job facets
        job_facets = {
            "jobType": {
                "processingType": "BATCH",
                "jobType": "TRANSFORMATION"
            },
            "documentation": {
                "description": f"dbt model: {model_name}"
            }
        }

        # Create run facets
        run_facets = {}
        if execution_time:
            run_facets["processingTime"] = {
                "processingTimeMs": int(execution_time * 1000)
            }

        # Create input datasets
        input_datasets = []
        if inputs:
            for input_model in inputs:
                input_datasets.append(
                    self._create_dataset("dbt_model", input_model)
                )

        # Create output datasets
        output_datasets = [
            self._create_dataset("dbt_model", model_name)
        ]

        if outputs:
            for output_model in outputs:
                output_datasets.append(
                    self._create_dataset("dbt_model", output_model)
                )

        # Create the event
        event = OpenLineageEvent(
            eventType=event_type,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run={
                "runId": run_id,
                "facets": run_facets
            },
            job={
                "namespace": self.namespace,
                "name": job_name,
                "facets": job_facets
            },
            inputs=input_datasets,
            outputs=output_datasets,
            errorMessage=error_message
        )

        return self._emit_event(event)

    def emit_seatunnel_event(
        self,
        job_name: str,
        event_type: str,
        run_id: Optional[str] = None,
        inputs: Optional[List[Dict[str, str]]] = None,
        outputs: Optional[List[Dict[str, str]]] = None,
        execution_time: Optional[float] = None,
        error_message: Optional[str] = None,
        **kwargs
    ) -> bool:
        """Emit OpenLineage event for Seatunnel job execution."""

        job_name = self._generate_job_name("seatunnel", job_name)
        run_id = self._generate_run_id(job_name, run_id)

        # Create job facets
        job_facets = {
            "jobType": {
                "processingType": "STREAMING" if "streaming" in job_name.lower() else "BATCH",
                "jobType": "INGESTION"
            },
            "documentation": {
                "description": f"Seatunnel job: {job_name}"
            }
        }

        # Create run facets
        run_facets = {}
        if execution_time:
            run_facets["processingTime"] = {
                "processingTimeMs": int(execution_time * 1000)
            }

        # Create input/output datasets
        input_datasets = []
        if inputs:
            for input_dataset in inputs:
                dataset_type = input_dataset.get('type', 'file')
                dataset_name = input_dataset.get('name', '')
                input_datasets.append(
                    self._create_dataset(dataset_type, dataset_name)
                )

        output_datasets = []
        if outputs:
            for output_dataset in outputs:
                dataset_type = output_dataset.get('type', 'iceberg')
                dataset_name = output_dataset.get('name', '')
                output_datasets.append(
                    self._create_dataset(dataset_type, dataset_name)
                )

        # Create the event
        event = OpenLineageEvent(
            eventType=event_type,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run={
                "runId": run_id,
                "facets": run_facets
            },
            job={
                "namespace": self.namespace,
                "name": job_name,
                "facets": job_facets
            },
            inputs=input_datasets,
            outputs=output_datasets,
            errorMessage=error_message
        )

        return self._emit_event(event)

    def emit_trino_event(
        self,
        query_id: str,
        event_type: str,
        run_id: Optional[str] = None,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
        query_text: Optional[str] = None,
        execution_time: Optional[float] = None,
        error_message: Optional[str] = None,
        **kwargs
    ) -> bool:
        """Emit OpenLineage event for Trino query execution."""

        job_name = self._generate_job_name("trino", f"query_{query_id}")
        run_id = self._generate_run_id(job_name, run_id)

        # Create job facets
        job_facets = {
            "jobType": {
                "processingType": "INTERACTIVE",
                "jobType": "QUERY"
            },
            "documentation": {
                "description": f"Trino query: {query_id}"
            }
        }

        if query_text:
            job_facets["queryText"] = {
                "queryText": query_text[:5000]  # Truncate long queries
            }

        # Create run facets
        run_facets = {}
        if execution_time:
            run_facets["processingTime"] = {
                "processingTimeMs": int(execution_time * 1000)
            }

        # Create input/output datasets
        input_datasets = []
        if inputs:
            for table_name in inputs:
                # Parse table name to determine type
                if table_name.startswith("iceberg."):
                    dataset_type = "iceberg_table"
                elif table_name.startswith("timescale."):
                    dataset_type = "timescale_table"
                else:
                    dataset_type = "table"

                input_datasets.append(
                    self._create_dataset(dataset_type, table_name)
                )

        output_datasets = []
        if outputs:
            for table_name in outputs:
                if table_name.startswith("iceberg."):
                    dataset_type = "iceberg_table"
                elif table_name.startswith("timescale."):
                    dataset_type = "timescale_table"
                else:
                    dataset_type = "table"

                output_datasets.append(
                    self._create_dataset(dataset_type, table_name)
                )

        # Create the event
        event = OpenLineageEvent(
            eventType=event_type,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run={
                "runId": run_id,
                "facets": run_facets
            },
            job={
                "namespace": self.namespace,
                "name": job_name,
                "facets": job_facets
            },
            inputs=input_datasets,
            outputs=output_datasets,
            errorMessage=error_message
        )

        return self._emit_event(event)

    def emit_custom_event(
        self,
        component: str,
        job_name: str,
        event_type: str,
        run_id: Optional[str] = None,
        inputs: Optional[List[Dict[str, str]]] = None,
        outputs: Optional[List[Dict[str, str]]] = None,
        execution_time: Optional[float] = None,
        error_message: Optional[str] = None,
        custom_facets: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> bool:
        """Emit a custom OpenLineage event for any data pipeline component."""

        job_name = self._generate_job_name(component, job_name)
        run_id = self._generate_run_id(job_name, run_id)

        # Create job facets
        job_facets = {
            "jobType": {
                "processingType": "BATCH",
                "jobType": "CUSTOM"
            },
            "documentation": {
                "description": f"{component} job: {job_name}"
            }
        }

        if custom_facets:
            job_facets.update(custom_facets)

        # Create run facets
        run_facets = {}
        if execution_time:
            run_facets["processingTime"] = {
                "processingTimeMs": int(execution_time * 1000)
            }

        # Create input/output datasets
        input_datasets = []
        if inputs:
            for input_dataset in inputs:
                dataset_type = input_dataset.get('type', 'dataset')
                dataset_name = input_dataset.get('name', '')
                input_datasets.append(
                    self._create_dataset(dataset_type, dataset_name)
                )

        output_datasets = []
        if outputs:
            for output_dataset in outputs:
                dataset_type = output_dataset.get('type', 'dataset')
                dataset_name = output_dataset.get('name', '')
                output_datasets.append(
                    self._create_dataset(dataset_type, dataset_name)
                )

        # Create the event
        event = OpenLineageEvent(
            eventType=event_type,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run={
                "runId": run_id,
                "facets": run_facets
            },
            job={
                "namespace": self.namespace,
                "name": job_name,
                "facets": job_facets
            },
            inputs=input_datasets,
            outputs=output_datasets,
            errorMessage=error_message
        )

        return self._emit_event(event)

    def _emit_event(self, event: OpenLineageEvent) -> bool:
        """Emit the OpenLineage event to the configured endpoint."""

        if not self.session:
            logger.warning("Requests library not available, cannot emit OpenLineage events")
            return False

        try:
            # Convert to dict if it's a Pydantic model
            if hasattr(event, 'dict'):
                event_dict = event.dict()
            else:
                event_dict = event

            response = self.session.post(
                f"{self.openlineage_url}/api/v1/lineage",
                json=event_dict,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            if response.status_code in [200, 201]:
                logger.debug(f"Successfully emitted OpenLineage event: {event.eventType} for {event.job['name']}")
                return True
            else:
                logger.warning(f"Failed to emit OpenLineage event: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error emitting OpenLineage event: {e}")
            return False


# Convenience functions for common use cases

def emit_dbt_start(model_name: str, run_id: Optional[str] = None, inputs: Optional[List[str]] = None) -> bool:
    """Emit a START event for a dbt model."""
    emitter = OpenLineageEmitter("http://localhost:5000")  # Default URL
    return emitter.emit_dbt_event(model_name, "START", run_id, inputs)


def emit_dbt_complete(model_name: str, run_id: Optional[str] = None,
                     inputs: Optional[List[str]] = None, outputs: Optional[List[str]] = None,
                     execution_time: Optional[float] = None) -> bool:
    """Emit a COMPLETE event for a dbt model."""
    emitter = OpenLineageEmitter("http://localhost:5000")  # Default URL
    return emitter.emit_dbt_event(model_name, "COMPLETE", run_id, inputs, outputs, execution_time)


def emit_dbt_fail(model_name: str, run_id: Optional[str] = None, error_message: str) -> bool:
    """Emit a FAIL event for a dbt model."""
    emitter = OpenLineageEmitter("http://localhost:5000")  # Default URL
    return emitter.emit_dbt_event(model_name, "FAIL", run_id, error_message=error_message)


def emit_seatunnel_start(job_name: str, inputs: Optional[List[Dict[str, str]]] = None) -> bool:
    """Emit a START event for a Seatunnel job."""
    emitter = OpenLineageEmitter("http://localhost:5000")  # Default URL
    return emitter.emit_seatunnel_event(job_name, "START", inputs=inputs)


def emit_seatunnel_complete(job_name: str, inputs: Optional[List[Dict[str, str]]] = None,
                           outputs: Optional[List[Dict[str, str]]] = None,
                           execution_time: Optional[float] = None) -> bool:
    """Emit a COMPLETE event for a Seatunnel job."""
    emitter = OpenLineageEmitter("http://localhost:5000")  # Default URL
    return emitter.emit_seatunnel_event(job_name, "COMPLETE", inputs=inputs, outputs=outputs, execution_time=execution_time)


def emit_trino_start(query_id: str, query_text: Optional[str] = None) -> bool:
    """Emit a START event for a Trino query."""
    emitter = OpenLineageEmitter("http://localhost:5000")  # Default URL
    return emitter.emit_trino_event(query_id, "START", query_text=query_text)


def emit_trino_complete(query_id: str, inputs: Optional[List[str]] = None,
                       outputs: Optional[List[str]] = None,
                       execution_time: Optional[float] = None) -> bool:
    """Emit a COMPLETE event for a Trino query."""
    emitter = OpenLineageEmitter("http://localhost:5000")  # Default URL
    return emitter.emit_trino_event(query_id, "COMPLETE", inputs=inputs, outputs=outputs, execution_time=execution_time)
