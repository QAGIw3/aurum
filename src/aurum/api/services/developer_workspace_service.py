"""Developer Workspace Service for notebooks and API exploration.

This service provides:
- JupyterHub integration with scoped secrets
- Notebook pod management and resource allocation
- API exploration examples and templates
- ML training notebook templates
- Interactive API documentation and testing
- Developer environment provisioning
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4

import yaml
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager


class NotebookEnvironment(BaseModel):
    """Notebook environment configuration."""

    environment_id: str
    environment_name: str
    description: str
    base_image: str = "jupyter/scipy-notebook:latest"
    resource_limits: Dict[str, str] = field(default_factory=lambda: {
        "cpu": "2",
        "memory": "4Gi"
    })
    resource_requests: Dict[str, str] = field(default_factory=lambda: {
        "cpu": "500m",
        "memory": "1Gi"
    })
    storage_size: str = "10Gi"
    environment_variables: Dict[str, str] = field(default_factory=dict)
    mounted_secrets: List[str] = field(default_factory=list)
    allowed_packages: List[str] = field(default_factory=list)
    network_policy: str = "restricted"
    idle_timeout_minutes: int = 60
    max_runtime_hours: int = 8


class NotebookSession(BaseModel):
    """Active notebook session."""

    session_id: str
    environment_id: str
    user_id: str
    tenant_id: str
    status: str = "starting"  # "starting", "running", "stopping", "stopped", "failed"
    pod_name: Optional[str] = None
    pod_ip: Optional[str] = None
    start_time: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    resource_usage: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    notebook_url: Optional[str] = None


class NotebookTemplate(BaseModel):
    """Notebook template for common use cases."""

    template_id: str
    template_name: str
    description: str
    category: str  # "api_exploration", "ml_training", "data_analysis", "forecasting"
    base_notebook_path: str
    required_packages: List[str] = field(default_factory=list)
    sample_queries: List[Dict[str, Any]] = field(default_factory=list)
    documentation_links: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)


class DeveloperWorkspaceService:
    """Developer Workspace Service for notebooks and API exploration."""

    def __init__(self):
        """Initialize developer workspace service."""
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Workspace state
        self._environments: Dict[str, NotebookEnvironment] = {}
        self._sessions: Dict[str, NotebookSession] = {}
        self._templates: Dict[str, NotebookTemplate] = {}

        # Enhanced features
        self._active_collaborators: Dict[str, Set[str]] = {}  # session_id -> set of user_ids
        self._session_snapshots: Dict[str, List[Dict[str, Any]]] = {}  # session_id -> snapshots
        self._api_documentation_cache: Dict[str, Any] = {}
        self._code_snippets: Dict[str, List[Dict[str, Any]]] = {}

        # Real-time collaboration
        self._collaboration_enabled = True
        self._snapshot_interval_minutes = 5

        # API integration
        self._api_docs_url = "https://docs.aurum.dev/api/"
        self._openapi_spec_cache: Optional[Dict[str, Any]] = None

        # Initialize default environments and templates
        self._initialize_default_environments()
        self._initialize_default_templates()
        self._initialize_api_documentation()
        self._initialize_code_snippets()

    def _initialize_default_environments(self) -> None:
        """Initialize default notebook environments."""
        # Standard ML environment
        self._environments["ml_standard"] = NotebookEnvironment(
            environment_id="ml_standard",
            environment_name="ML Development",
            description="Standard ML development environment with PyTorch, TensorFlow, and scikit-learn",
            base_image="jupyter/scipy-notebook:latest",
            resource_limits={"cpu": "4", "memory": "8Gi"},
            resource_requests={"cpu": "1", "memory": "2Gi"},
            storage_size="20Gi",
            environment_variables={
                "JUPYTER_ENABLE_LAB": "yes",
                "PIP_TRUSTED_HOST": "pypi.org"
            },
            allowed_packages=["torch", "tensorflow", "scikit-learn", "pandas", "numpy", "matplotlib"],
            network_policy="restricted"
        )

        # API exploration environment
        self._environments["api_explorer"] = NotebookEnvironment(
            environment_id="api_explorer",
            environment_name="API Explorer",
            description="Lightweight environment for API exploration and testing",
            base_image="jupyter/minimal-notebook:latest",
            resource_limits={"cpu": "1", "memory": "2Gi"},
            resource_requests={"cpu": "500m", "memory": "1Gi"},
            storage_size="5Gi",
            environment_variables={
                "JUPYTER_ENABLE_LAB": "yes"
            },
            allowed_packages=["requests", "pandas", "matplotlib"],
            network_policy="api_access"
        )

    def _initialize_api_documentation(self) -> None:
        """Initialize API documentation cache."""
        try:
            # Mock API documentation - in reality would fetch from OpenAPI spec
            self._api_documentation_cache = {
                "endpoints": {
                    "/v2/curves": {
                        "method": "GET",
                        "summary": "Retrieve historical curve data",
                        "parameters": ["asof", "limit", "geography"],
                        "response_schema": {"type": "object", "properties": {"data": {"type": "array"}}},
                        "examples": [
                            {
                                "name": "Get recent curves",
                                "code": "response = requests.get('http://localhost:8000/v2/curves', params={'limit': 10})"
                            }
                        ]
                    },
                    "/v2/forecasting": {
                        "method": "POST",
                        "summary": "Generate probabilistic forecast",
                        "parameters": ["forecast_type", "target_variable", "geography", "start_date", "end_date"],
                        "response_schema": {"type": "object", "properties": {"forecast_id": {"type": "string"}}},
                        "examples": [
                            {
                                "name": "Generate load forecast",
                                "code": "forecast_data = {'forecast_type': 'load', 'target_variable': 'load_mw', 'geography': 'US', 'start_date': '2024-01-01', 'end_date': '2024-01-31'}\nresponse = requests.post('http://localhost:8000/v2/forecasting', json=forecast_data)"
                            }
                        ]
                    }
                },
                "authentication": {
                    "type": "bearer",
                    "header": "Authorization: Bearer YOUR_TOKEN",
                    "description": "Use your API token in the Authorization header"
                },
                "base_url": "http://localhost:8000",
                "version": "2.0"
            }

            self.logger.info("API documentation initialized")

        except Exception as e:
            self.logger.error("Failed to initialize API documentation", error=str(e))
            self._api_documentation_cache = {}

    def _initialize_code_snippets(self) -> None:
        """Initialize code snippets for common operations."""
        self._code_snippets = {
            "data_retrieval": [
                {
                    "name": "Get Market Data",
                    "language": "python",
                    "code": "import requests\n\nresponse = requests.get('http://localhost:8000/v2/curves', params={'limit': 10})\ndata = response.json()['data']\nprint(f'Retrieved {len(data)} curve records')",
                    "description": "Basic data retrieval example"
                },
                {
                    "name": "Pandas DataFrame",
                    "language": "python",
                    "code": "import pandas as pd\nimport requests\n\nresponse = requests.get('http://localhost:8000/v2/curves')\ndf = pd.DataFrame(response.json()['data'])\nprint(df.head())",
                    "description": "Convert API response to pandas DataFrame"
                }
            ],
            "forecasting": [
                {
                    "name": "Generate Forecast",
                    "language": "python",
                    "code": "import requests\n\nforecast_data = {\n    'forecast_type': 'load',\n    'target_variable': 'load_mw',\n    'geography': 'US',\n    'start_date': '2024-01-01',\n    'end_date': '2024-01-31'\n}\n\nresponse = requests.post('http://localhost:8000/v2/forecasting', json=forecast_data)\nforecast_id = response.json()['forecast_id']\nprint(f'Forecast generated: {forecast_id}')",
                    "description": "Generate a probabilistic forecast"
                }
            ],
            "risk_analysis": [
                {
                    "name": "Calculate VaR",
                    "language": "python",
                    "code": "import requests\n\nvar_data = {\n    'portfolio_id': 'portfolio_123',\n    'confidence_level': 0.95,\n    'time_horizon_days': 1\n}\n\nresponse = requests.post('http://localhost:8000/v2/risk-engine/risk/calculate', json=var_data)\nvar_result = response.json()\nprint(f'VaR 95%: ${var_result[\"var_95\"]}')",
                    "description": "Calculate Value at Risk for a portfolio"
                }
            ]
        }

    def _initialize_default_templates(self) -> None:
        """Initialize default notebook templates."""
        # API exploration template
        self._templates["api_exploration"] = NotebookTemplate(
            template_id="api_exploration",
            template_name="API Exploration",
            description="Template for exploring Aurum API endpoints",
            category="api_exploration",
            base_notebook_path="templates/api_exploration.ipynb",
            required_packages=["requests", "pandas"],
            sample_queries=[
                {
                    "name": "Get Market Data",
                    "endpoint": "/v2/curves",
                    "method": "GET",
                    "params": {"asof": "2024-01-01", "limit": 10},
                    "description": "Retrieve historical curve data"
                },
                {
                    "name": "Run Scenario",
                    "endpoint": "/v2/scenarios",
                    "method": "POST",
                    "params": {
                        "name": "Test Scenario",
                        "assumptions": [{"type": "market_growth", "value": 0.05}]
                    },
                    "description": "Create and run a scenario"
                }
            ],
            documentation_links=[
                "https://docs.aurum.dev/api/",
                "https://docs.aurum.dev/notebooks/getting-started"
            ],
            tags=["api", "exploration", "beginner"]
        )

        # ML training template
        self._templates["ml_training"] = NotebookTemplate(
            template_id="ml_training",
            template_name="ML Model Training",
            description="Template for training ML models with Aurum data",
            category="ml_training",
            base_notebook_path="templates/ml_training.ipynb",
            required_packages=["torch", "pandas", "numpy", "scikit-learn"],
            sample_queries=[
                {
                    "name": "Load Feature Data",
                    "code": "from aurum.api.services.feature_store_service import get_feature_store_service\nfeatures = await get_feature_store_service().get_features_for_modeling()",
                    "description": "Load features for model training"
                },
                {
                    "name": "Train Forecasting Model",
                    "code": "from aurum.api.services.model_registry_service import train_load_forecasting_model\nmodel = await train_load_forecasting_model(features)",
                    "description": "Train a load forecasting model"
                }
            ],
            documentation_links=[
                "https://docs.aurum.dev/ml/training/",
                "https://docs.aurum.dev/notebooks/ml-workflows"
            ],
            tags=["ml", "training", "forecasting"]
        )

    async def create_notebook_environment(self, environment: NotebookEnvironment) -> str:
        """Create a new notebook environment."""
        env_id = environment.environment_id
        self._environments[env_id] = environment

        # Generate Kubernetes YAML for the environment
        k8s_yaml = self._generate_k8s_yaml(environment)

        # Store YAML for deployment
        await self.cache_manager.set(f"env_yaml:{env_id}", k8s_yaml, ttl_seconds=3600)

        self.telemetry.info("Notebook environment created", environment_id=env_id)
        return env_id

    def _generate_k8s_yaml(self, environment: NotebookEnvironment) -> str:
        """Generate Kubernetes YAML for notebook deployment."""
        yaml_content = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": f"aurum-notebook-{environment.environment_id}",
                "labels": {
                    "app": "aurum-notebook",
                    "environment": environment.environment_id
                }
            },
            "spec": {
                "containers": [{
                    "name": "notebook",
                    "image": environment.base_image,
                    "resources": {
                        "limits": environment.resource_limits,
                        "requests": environment.resource_requests
                    },
                    "env": [
                        {"name": k, "value": v} for k, v in environment.environment_variables.items()
                    ],
                    "ports": [{"containerPort": 8888}],
                    "volumeMounts": [
                        {
                            "name": "workspace",
                            "mountPath": "/home/jovyan/work"
                        }
                    ]
                }],
                "volumes": [{
                    "name": "workspace",
                    "emptyDir": {"sizeLimit": environment.storage_size}
                }],
                "restartPolicy": "Never"
            }
        }

        return yaml.dump(yaml_content, default_flow_style=False)

    async def start_notebook_session(
        self,
        environment_id: str,
        user_id: str,
        tenant_id: str,
        configuration: Dict[str, Any] = None
    ) -> str:
        """Start a new notebook session."""
        session_id = str(uuid4())

        # Get environment
        environment = self._environments.get(environment_id)
        if not environment:
            raise ValueError(f"Environment {environment_id} not found")

        # Create session
        session = NotebookSession(
            session_id=session_id,
            environment_id=environment_id,
            user_id=user_id,
            tenant_id=tenant_id,
            status="starting",
            start_time=datetime.utcnow(),
            last_activity=datetime.utcnow()
        )

        self._sessions[session_id] = session

        # Start session in background
        asyncio.create_task(self._manage_notebook_session(session_id))

        self.telemetry.info("Notebook session started", session_id=session_id, user_id=user_id)
        return session_id

    async def _manage_notebook_session(self, session_id: str) -> None:
        """Manage notebook session lifecycle."""
        session = self._sessions[session_id]

        try:
            # Simulate pod creation and startup
            await asyncio.sleep(5)  # Simulate startup time

            session.status = "running"
            session.pod_name = f"aurum-notebook-{session_id}"
            session.pod_ip = "10.0.0.100"  # Mock IP
            session.notebook_url = f"http://{session.pod_ip}:8888"

            # Monitor session activity
            while session.status == "running":
                await asyncio.sleep(60)  # Check every minute

                # Update last activity
                session.last_activity = datetime.utcnow()

                # Check for idle timeout
                if environment.idle_timeout_minutes and session.last_activity:
                    idle_time = (datetime.utcnow() - session.last_activity).total_seconds() / 60
                    if idle_time > environment.idle_timeout_minutes:
                        await self._stop_notebook_session(session_id)
                        break

        except Exception as e:
            session.status = "failed"
            session.error_message = str(e)
            self.telemetry.error("Session management failed", session_id=session_id, error=str(e))

    async def _stop_notebook_session(self, session_id: str) -> None:
        """Stop notebook session."""
        session = self._sessions.get(session_id)
        if not session:
            return

        session.status = "stopping"

        # Simulate pod cleanup
        await asyncio.sleep(2)

        session.status = "stopped"
        self.telemetry.info("Notebook session stopped", session_id=session_id)

    async def get_session_status(self, session_id: str) -> Optional[NotebookSession]:
        """Get notebook session status."""
        return self._sessions.get(session_id)

    async def list_user_sessions(self, user_id: str) -> List[NotebookSession]:
        """List active sessions for a user."""
        return [s for s in self._sessions.values() if s.user_id == user_id and s.status == "running"]

    async def get_environment_templates(self, category: Optional[str] = None) -> List[NotebookTemplate]:
        """Get available notebook templates."""
        templates = list(self._templates.values())

        if category:
            templates = [t for t in templates if t.category == category]

        return templates

    async def deploy_notebook_template(
        self,
        template_id: str,
        session_id: str,
        customizations: Dict[str, Any] = None
    ) -> str:
        """Deploy a notebook template to a session."""
        template = self._templates.get(template_id)
        if not template:
            raise ValueError(f"Template {template_id} not found")

        session = self._sessions.get(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        # Generate customized notebook
        notebook_content = await self._customize_notebook_template(template, customizations or {})

        # Store notebook in session workspace
        notebook_path = f"/tmp/session_{session_id}/notebook.ipynb"

        # In real implementation, would copy to pod filesystem
        await self.cache_manager.set(f"notebook:{session_id}", notebook_content, ttl_seconds=86400)

        self.telemetry.info("Notebook template deployed", session_id=session_id, template_id=template_id)
        return notebook_path

    async def _customize_notebook_template(
        self,
        template: NotebookTemplate,
        customizations: Dict[str, Any]
    ) -> str:
        """Customize notebook template with user preferences."""
        # Mock notebook content
        notebook_content = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [f"# {template.template_name}\n\n{template.description}"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Import required libraries\n",
                        "import requests\n",
                        "import pandas as pd\n",
                        "import matplotlib.pyplot as plt\n",
                        "from datetime import datetime\n",
                        "\n",
                        "# Aurum API base URL\n",
                        "API_BASE = 'http://localhost:8000'\n",
                        "\n",
                        "# Authentication (replace with your token)\n",
                        "headers = {'Authorization': 'Bearer YOUR_TOKEN_HERE'}\n"
                    ]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }

        # Add sample queries
        if template.sample_queries:
            cells = notebook_content["cells"]
            for query in template.sample_queries:
                cells.append({
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        f"# {query['name']}",
                        f"response = requests.{query['method'].lower()}(",
                        f"    f\"{API_BASE}{query['endpoint']}\",",
                        f"    headers=headers,",
                        f"    params={query['params']}",
                        ")\n",
                        "print(response.json())"
                    ]
                })

        return json.dumps(notebook_content)

    async def get_api_examples(self, endpoint_category: str = "all") -> Dict[str, Any]:
        """Get API usage examples and documentation."""
        examples = {
            "getting_started": {
                "title": "Getting Started with Aurum API",
                "description": "Basic examples for API authentication and data retrieval",
                "examples": [
                    {
                        "name": "Health Check",
                        "method": "GET",
                        "endpoint": "/health",
                        "description": "Check API health status",
                        "code": "response = requests.get('http://localhost:8000/health')"
                    },
                    {
                        "name": "List Curves",
                        "method": "GET",
                        "endpoint": "/v2/curves",
                        "description": "Retrieve curve data with pagination",
                        "code": "response = requests.get('http://localhost:8000/v2/curves', params={'limit': 10})"
                    }
                ]
            },
            "forecasting": {
                "title": "Forecasting Examples",
                "description": "Examples for probabilistic forecasting and model usage",
                "examples": [
                    {
                        "name": "Generate Forecast",
                        "method": "POST",
                        "endpoint": "/v2/forecasting",
                        "description": "Generate probabilistic forecast",
                        "code": "forecast_data = {\n    'forecast_type': 'load',\n    'start_date': '2024-01-01',\n    'end_date': '2024-01-31'\n}\nresponse = requests.post('http://localhost:8000/v2/forecasting', json=forecast_data)"
                    }
                ]
            },
            "risk_analysis": {
                "title": "Risk Analysis Examples",
                "description": "Examples for risk calculation and portfolio analysis",
                "examples": [
                    {
                        "name": "Calculate VaR",
                        "method": "POST",
                        "endpoint": "/v2/risk-engine/risk/calculate",
                        "description": "Calculate Value at Risk for portfolio",
                        "code": "var_data = {\n    'portfolio_id': 'portfolio_123',\n    'confidence_level': 0.95,\n    'time_horizon_days': 1\n}\nresponse = requests.post('http://localhost:8000/v2/risk-engine/risk/calculate', json=var_data)"
                    }
                ]
            }
        }

        if endpoint_category == "all":
            return examples
        else:
            return {endpoint_category: examples.get(endpoint_category, {})}

    async def create_developer_guide(self, user_id: str) -> str:
        """Create personalized developer guide."""
        guide_content = f"""
# Aurum Developer Guide - Personalized for {user_id}

## Getting Started

1. **API Authentication**
   - Use your API token in the Authorization header
   - Format: `Bearer YOUR_TOKEN_HERE`

2. **Available Environments**
   - ML Development: Full ML stack with PyTorch/TensorFlow
   - API Explorer: Lightweight environment for API testing

3. **Common Workflows**
   - Data exploration and visualization
   - Model training and evaluation
   - Risk analysis and scenario testing

## Quick Examples

### Check API Health
```python
import requests
response = requests.get('http://localhost:8000/health')
print(response.json())
```

### Load Historical Data
```python
import pandas as pd
response = requests.get('http://localhost:8000/v2/curves', params={{'limit': 100}})
data = pd.DataFrame(response.json()['data'])
```

### Train a Model
```python
from aurum.api.services.model_registry_service import train_load_forecasting_model
model = await train_load_forecasting_model(data)
```

## Next Steps

- Explore the full API documentation
- Try the ML training templates
- Join the developer community

Generated on: {datetime.utcnow().isoformat()}
"""

        return guide_content

    async def join_session_collaboration(self, session_id: str, user_id: str) -> bool:
        """Join a notebook session for collaboration.

        Args:
            session_id: Session identifier
            user_id: User identifier joining the session

        Returns:
            True if successfully joined
        """
        try:
            if session_id not in self._sessions:
                raise ValueError(f"Session {session_id} not found")

            session = self._sessions[session_id]
            if session.status != "running":
                raise ValueError(f"Session {session_id} is not running")

            # Initialize collaborators set if not exists
            if session_id not in self._active_collaborators:
                self._active_collaborators[session_id] = set()

            # Add user to collaborators
            self._active_collaborators[session_id].add(user_id)

            # Initialize snapshots for session if not exists
            if session_id not in self._session_snapshots:
                self._session_snapshots[session_id] = []

            self.telemetry.info(
                "User joined session collaboration",
                session_id=session_id,
                user_id=user_id,
                total_collaborators=len(self._active_collaborators[session_id])
            )

            return True

        except Exception as e:
            self.telemetry.error("Failed to join session collaboration", session_id=session_id, user_id=user_id, error=str(e))
            return False

    async def leave_session_collaboration(self, session_id: str, user_id: str) -> bool:
        """Leave a notebook session collaboration.

        Args:
            session_id: Session identifier
            user_id: User identifier leaving the session

        Returns:
            True if successfully left
        """
        try:
            if session_id not in self._active_collaborators:
                return False

            self._active_collaborators[session_id].discard(user_id)

            # Clean up empty collaborator sets
            if not self._active_collaborators[session_id]:
                del self._active_collaborators[session_id]

            self.telemetry.info(
                "User left session collaboration",
                session_id=session_id,
                user_id=user_id
            )

            return True

        except Exception as e:
            self.telemetry.error("Failed to leave session collaboration", session_id=session_id, user_id=user_id, error=str(e))
            return False

    async def get_session_collaborators(self, session_id: str) -> List[str]:
        """Get list of users collaborating on a session.

        Args:
            session_id: Session identifier

        Returns:
            List of user IDs collaborating on the session
        """
        if session_id not in self._active_collaborators:
            return []

        return list(self._active_collaborators[session_id])

    async def create_session_snapshot(self, session_id: str, snapshot_data: Dict[str, Any]) -> str:
        """Create a snapshot of the current notebook session.

        Args:
            session_id: Session identifier
            snapshot_data: Snapshot data including notebook content

        Returns:
            Snapshot ID
        """
        try:
            snapshot_id = str(uuid4())

            snapshot = {
                "snapshot_id": snapshot_id,
                "session_id": session_id,
                "created_at": datetime.utcnow(),
                "data": snapshot_data
            }

            if session_id not in self._session_snapshots:
                self._session_snapshots[session_id] = []

            self._session_snapshots[session_id].append(snapshot)

            # Keep only last 10 snapshots per session
            if len(self._session_snapshots[session_id]) > 10:
                self._session_snapshots[session_id] = self._session_snapshots[session_id][-10:]

            self.telemetry.info(
                "Session snapshot created",
                session_id=session_id,
                snapshot_id=snapshot_id
            )

            return snapshot_id

        except Exception as e:
            self.telemetry.error("Failed to create session snapshot", session_id=session_id, error=str(e))
            raise

    async def get_session_snapshots(self, session_id: str) -> List[Dict[str, Any]]:
        """Get list of snapshots for a session.

        Args:
            session_id: Session identifier

        Returns:
            List of session snapshots
        """
        if session_id not in self._session_snapshots:
            return []

        return self._session_snapshots[session_id]

    async def get_api_documentation(self, endpoint: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive API documentation.

        Args:
            endpoint: Optional specific endpoint to get documentation for

        Returns:
            API documentation
        """
        try:
            if endpoint and endpoint in self._api_documentation_cache["endpoints"]:
                return self._api_documentation_cache["endpoints"][endpoint]
            else:
                return self._api_documentation_cache

        except Exception as e:
            self.telemetry.error("Failed to get API documentation", endpoint=endpoint, error=str(e))
            return {"error": str(e)}

    async def get_code_snippets(self, category: Optional[str] = None, language: str = "python") -> List[Dict[str, Any]]:
        """Get code snippets for common operations.

        Args:
            category: Optional category filter
            language: Programming language filter

        Returns:
            List of code snippets
        """
        try:
            snippets = []

            if category and category in self._code_snippets:
                snippets.extend(self._code_snippets[category])
            else:
                # Return all snippets
                for category_snippets in self._code_snippets.values():
                    snippets.extend(category_snippets)

            # Filter by language
            if language:
                snippets = [s for s in snippets if s.get("language") == language]

            return snippets

        except Exception as e:
            self.telemetry.error("Failed to get code snippets", category=category, language=language, error=str(e))
            return []

    async def create_notebook_from_template(
        self,
        template_id: str,
        session_id: str,
        customizations: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a notebook from a template with customizations.

        Args:
            template_id: Template identifier
            session_id: Session identifier
            customizations: Optional customizations to apply

        Returns:
            Notebook path
        """
        try:
            template = self._templates.get(template_id)
            if not template:
                raise ValueError(f"Template {template_id} not found")

            session = self._sessions.get(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")

            # Generate customized notebook content
            notebook_content = await self._generate_customized_notebook(template, customizations or {})

            # Store notebook in session workspace
            notebook_path = f"/tmp/session_{session_id}/notebook_{template_id}.ipynb"

            # In real implementation, would copy to pod filesystem
            await self.cache_manager.set(
                f"notebook:{session_id}:{template_id}",
                notebook_content,
                ttl_seconds=86400  # 24 hour cache
            )

            self.telemetry.info(
                "Notebook created from template",
                session_id=session_id,
                template_id=template_id,
                notebook_path=notebook_path
            )

            return notebook_path

        except Exception as e:
            self.telemetry.error(
                "Failed to create notebook from template",
                template_id=template_id,
                session_id=session_id,
                error=str(e)
            )
            raise

    async def _generate_customized_notebook(self, template: NotebookTemplate, customizations: Dict[str, Any]) -> str:
        """Generate customized notebook content from template."""
        # Enhanced notebook generation with customizations
        notebook_content = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [f"# {template.template_name}\n\n{template.description}"]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## Setup and Authentication\n\nConfigure your environment and authenticate with the Aurum API."]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Import required libraries\n",
                        "import requests\n",
                        "import pandas as pd\n",
                        "import matplotlib.pyplot as plt\n",
                        "import seaborn as sns\n",
                        "from datetime import datetime, timedelta\n",
                        "\n",
                        "# Configure matplotlib for better plots\n",
                        "plt.style.use('seaborn-v0_8')\n",
                        "%matplotlib inline\n",
                        "\n",
                        "# Aurum API configuration\n",
                        "API_BASE = 'http://localhost:8000'\n",
                        "API_TOKEN = 'YOUR_TOKEN_HERE'\n",
                        "\n",
                        "# Headers for API requests\n",
                        "headers = {\n",
                        "    'Authorization': f'Bearer {API_TOKEN}',\n",
                        "    'Content-Type': 'application/json'\n",
                        "}\n"
                    ]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                },
                "language_info": {
                    "name": "python",
                    "version": "3.8.0"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }

        # Add template-specific content
        if template.sample_queries:
            notebook_content["cells"].append({
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## Sample Queries and Examples\n\nExplore the Aurum API with these example queries."]
            })

            for i, query in enumerate(template.sample_queries):
                notebook_content["cells"].append({
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        f"# {query.get('name', f'Query {i+1}')}\n",
                        f"print('Executing: {query.get('description', '')}')\n",
                        "# Add actual API call code here
                    ]
                })

        # Add customizations
        if customizations:
            notebook_content["cells"].append({
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## Customizations Applied\n\nThis notebook has been customized with your specific requirements."]
            })

        return json.dumps(notebook_content)

    async def get_session_activity_feed(self, session_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get activity feed for a session.

        Args:
            session_id: Session identifier
            limit: Maximum activities to return

        Returns:
            List of session activities
        """
        try:
            # Mock activity feed - in reality would track real activities
            activities = []

            session = self._sessions.get(session_id)
            if not session:
                return activities

            # Generate mock activities based on session state
            activities.append({
                "activity_id": str(uuid4()),
                "activity_type": "session_started",
                "user_id": session.user_id,
                "timestamp": session.start_time or datetime.utcnow(),
                "description": f"Notebook session started",
                "metadata": {"session_id": session_id}
            })

            # Add collaboration activities
            collaborators = await self.get_session_collaborators(session_id)
            for collaborator in collaborators:
                activities.append({
                    "activity_id": str(uuid4()),
                    "activity_type": "user_joined",
                    "user_id": collaborator,
                    "timestamp": datetime.utcnow() - timedelta(minutes=len(collaborators)),
                    "description": f"User joined collaboration",
                    "metadata": {"session_id": session_id}
                })

            # Add snapshot activities
            snapshots = await self.get_session_snapshots(session_id)
            for snapshot in snapshots[-5:]:  # Last 5 snapshots
                activities.append({
                    "activity_id": str(uuid4()),
                    "activity_type": "snapshot_created",
                    "user_id": session.user_id,
                    "timestamp": snapshot["created_at"],
                    "description": f"Notebook snapshot created",
                    "metadata": {"session_id": session_id, "snapshot_id": snapshot["snapshot_id"]}
                })

            # Sort by timestamp (most recent first)
            activities.sort(key=lambda a: a["timestamp"], reverse=True)

            return activities[:limit]

        except Exception as e:
            self.telemetry.error("Failed to get session activity feed", session_id=session_id, error=str(e))
            return []

    async def export_session_notebook(self, session_id: str, format: str = "ipynb") -> bytes:
        """Export the current notebook session.

        Args:
            session_id: Session identifier
            format: Export format (ipynb, html, pdf)

        Returns:
            Exported notebook content as bytes
        """
        try:
            session = self._sessions.get(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")

            # Get the latest notebook content
            notebook_data = await self.cache_manager.get(f"notebook:{session_id}")

            if not notebook_data:
                raise ValueError(f"No notebook data found for session {session_id}")

            if format == "ipynb":
                return json.dumps(notebook_data).encode('utf-8')
            elif format == "html":
                # Convert to HTML (simplified)
                return f"<html><body><h1>Notebook Export</h1><pre>{json.dumps(notebook_data, indent=2)}</pre></body></html>".encode('utf-8')
            else:
                raise ValueError(f"Unsupported export format: {format}")

        except Exception as e:
            self.telemetry.error("Failed to export session notebook", session_id=session_id, format=format, error=str(e))
            raise

    async def get_service_health(self) -> Dict[str, Any]:
        """Get enhanced service health status."""
        active_sessions = len([s for s in self._sessions.values() if s.status == "running"])
        active_collaborators = sum(len(collaborators) for collaborators in self._active_collaborators.values())

        return {
            "status": "healthy",
            "environments_available": len(self._environments),
            "templates_available": len(self._templates),
            "active_sessions": active_sessions,
            "total_sessions": len(self._sessions),
            "active_collaborators": active_collaborators,
            "snapshots_stored": sum(len(snapshots) for snapshots in self._session_snapshots.values()),
            "api_documentation_cached": len(self._api_documentation_cache),
            "code_snippets_available": sum(len(snippets) for snippets in self._code_snippets.values()),
            "collaboration_enabled": self._collaboration_enabled,
            "last_activity": datetime.utcnow()
        }


def get_developer_workspace_service() -> DeveloperWorkspaceService:
    """Get the global developer workspace service instance."""
    return DeveloperWorkspaceService()


async def create_notebook_session(
    environment_id: str,
    user_id: str,
    tenant_id: str
) -> str:
    """Create a new notebook session."""
    service = get_developer_workspace_service()
    return await service.start_notebook_session(environment_id, user_id, tenant_id)


async def get_api_documentation() -> Dict[str, Any]:
    """Get comprehensive API documentation."""
    service = get_developer_workspace_service()
    return await service.get_api_examples()
