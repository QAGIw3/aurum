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

# ruff: noqa: B008
import asyncio
import json
import random
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from uuid import uuid4

import yaml
from pydantic import BaseModel, Field, ValidationError, ValidationInfo, field_validator

from ..logging.structured_logger import get_logger

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from ..cache.cache_governance import TTLPolicy
from ..cache.enhanced_cache_manager import CacheNamespace

MAX_ENVIRONMENT_HISTORY = 5
DEFAULT_IDLE_TIMEOUT_MINUTES = 60
DEFAULT_MAX_RUNTIME_HOURS = 8
MAX_SESSION_HISTORY = 20
SESSION_PERSIST_TTL = TTLPolicy.MEDIUM
DEFAULT_SESSION_TTL_MINUTES = 240
DEFAULT_STORAGE_QUOTA_GB = 50
DEFAULT_MAX_CONCURRENT_SESSIONS = 5
BYTES_PER_GIB = 1024 ** 3
BYTES_PER_MIB = 1024 ** 2
BYTES_PER_TIB = 1024 ** 4


class StorageQuotaExceeded(RuntimeError):
    """Raised when a tenant exceeds the configured storage quota."""

    def __init__(self, tenant_id: str, quota_bytes: int, requested_bytes: int, current_bytes: int):
        self.tenant_id = tenant_id
        self.quota_bytes = quota_bytes
        self.requested_bytes = requested_bytes
        self.current_bytes = current_bytes
        quota_gb = quota_bytes / BYTES_PER_GIB
        requested_gb = requested_bytes / BYTES_PER_GIB
        current_gb = current_bytes / BYTES_PER_GIB
        super().__init__(
            f"Tenant {tenant_id} exceeds storage quota of {quota_gb} GiB "
            f"(current={current_gb:.2f} GiB, requested={requested_gb:.2f} GiB)"
        )


class SessionLimitExceeded(RuntimeError):
    """Raised when a tenant exceeds the number of concurrent sessions."""

    def __init__(self, tenant_id: str, limit: int):
        self.tenant_id = tenant_id
        self.limit = limit
        super().__init__(
            f"Tenant {tenant_id} exceeded the active notebook session limit of {limit}"
        )


class TenantAccessError(PermissionError):
    """Raised when a tenant attempts to access another tenant's resource."""

    def __init__(self, tenant_id: str, resource_tenant: str, resource_id: str):
        self.tenant_id = tenant_id
        self.resource_tenant = resource_tenant
        self.resource_id = resource_id
        super().__init__(
            f"Tenant {tenant_id} cannot access resource {resource_id} belonging to tenant {resource_tenant}"
        )


class NotebookEnvironment(BaseModel):
    """Notebook environment configuration."""

    environment_id: str
    environment_name: str
    description: str
    base_image: str = "jupyter/scipy-notebook:latest"
    resource_limits: Dict[str, str] = Field(default_factory=lambda: {
        "cpu": "2",
        "memory": "4Gi"
    })
    resource_requests: Dict[str, str] = Field(default_factory=lambda: {
        "cpu": "500m",
        "memory": "1Gi"
    })
    storage_size: str = "10Gi"
    environment_variables: Dict[str, str] = Field(default_factory=dict)
    mounted_secrets: List[str] = Field(default_factory=list)
    allowed_packages: List[str] = Field(default_factory=list)
    network_policy: str = "restricted"
    idle_timeout_minutes: int = 60
    max_runtime_hours: int = 8

    @field_validator("environment_id", "environment_name", "description", mode="before")
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        if not value:
            raise ValueError(f"{info.field_name} must not be empty")
        return value

    @field_validator("resource_limits", "resource_requests", mode="before")
    def _validate_resources(cls, value: Dict[str, str], info: ValidationInfo) -> Dict[str, str]:
        if not value:
            raise ValueError(f"{info.field_name} must be provided")
        required = {"cpu", "memory"}
        missing = required - set(value.keys())
        if missing:
            raise ValueError(f"{info.field_name} missing keys: {', '.join(sorted(missing))}")
        return value

    @field_validator("idle_timeout_minutes", "max_runtime_hours", mode="before")
    def _validate_timeouts(cls, value: int, info: ValidationInfo) -> int:
        if value <= 0:
            raise ValueError(f"{info.field_name} must be positive")
        return value


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
    expires_at: Optional[datetime] = None
    resource_usage: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    notebook_url: Optional[str] = None

class NotebookTemplate(BaseModel):
    """Notebook template for common use cases."""

    template_id: str
    template_name: str
    description: str
    category: str  # "api_exploration", "ml_training", "data_analysis", "forecasting"
    base_notebook_path: str
    required_packages: List[str] = Field(default_factory=list)
    sample_queries: List[Dict[str, Any]] = Field(default_factory=list)
    documentation_links: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class DeveloperWorkspaceService:
    """Developer Workspace Service for notebooks and API exploration."""

    _GLOBAL_TENANT_SCOPE = "__default__"

    def __init__(self):
        """Initialize developer workspace service."""
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()
        self.logger = get_logger(__name__)

        # Cache keys and namespaces
        self._env_cache_prefix = "developer_workspace:environments"
        self._session_cache_prefix = "developer_workspace:sessions"
        self._api_docs_cache_key = "developer_workspace:api_docs"
        self._code_snippets_cache_key = "developer_workspace:code_snippets"

        # Workspace state
        self._environments: Dict[str, NotebookEnvironment] = {}
        self._environment_metadata: Dict[str, Dict[str, Any]] = {}
        self._default_environments: Dict[str, NotebookEnvironment] = {}
        self._sessions: Dict[str, NotebookSession] = {}
        self._session_metadata: Dict[str, Dict[str, Any]] = {}
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

        # State loading flags
        self._environments_loaded: Set[str] = set()
        self._sessions_loaded = False
        self._api_docs_loaded = False

        # Index keys for cache persistence
        self._session_index_key = f"{self._session_cache_prefix}:index"

        # Locks for lazy-loading
        self._environment_lock = asyncio.Lock()
        self._session_lock = asyncio.Lock()
        self._api_docs_lock = asyncio.Lock()

        # Paths for documentation loading
        project_root = Path(__file__).resolve().parents[4]
        self._openapi_candidates = [
            project_root / "openapi" / "aurum.yaml",
            project_root / "openapi" / "aurum.generated.yaml",
        ]

        # Session management tuning (seconds)
        self._session_startup_delay_seconds = 5
        self._session_monitor_interval_seconds = 60

        # Initialize default environments and templates
        self._initialize_default_environments()
        self._initialize_default_templates()

        # Tenant enforcement state
        self._tenant_storage_quota_gb: Dict[str, int] = {}
        self._tenant_storage_usage_bytes: defaultdict[str, int] = defaultdict(int)
        self._tenant_active_sessions: defaultdict[str, Set[str]] = defaultdict(set)
        self._tenant_session_limits: Dict[str, int] = {}

        # Environment snapshots for auditing
        self._environment_versions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._session_versions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Defer API documentation loading until first use
        self._api_documentation_cache = {}
        self._code_snippets = {}

    def _initialize_default_environments(self) -> None:
        """Initialize default notebook environments."""
        self._default_environments = {
            "ml_standard": NotebookEnvironment(
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
        ),
            "api_explorer": NotebookEnvironment(
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
        }

        # Merge defaults into active state without overwriting overrides
        for env_id, environment in self._default_environments.items():
            storage_key = self._environment_storage_key(None, env_id)
            self._environments.setdefault(storage_key, environment)

    def _load_openapi_spec(candidates: List[Path]) -> Optional[Dict[str, Any]]:
        for path in candidates:
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8") as handle:
                    if path.suffix.lower() == ".json":
                        return json.load(handle)
                    return yaml.safe_load(handle)
            except Exception as exc:  # pragma: no cover - defensive
                logging.getLogger(__name__).warning("Failed to load OpenAPI spec", path=str(path), error=str(exc))
        return None

    def _initialize_api_documentation(self) -> None:
        """Initialize API documentation cache from OpenAPI spec."""
        try:
            if self._openapi_spec_cache is None:
                self._openapi_spec_cache = self._load_openapi_spec(self._openapi_candidates)

            if not self._openapi_spec_cache:
                raise RuntimeError("OpenAPI specification not available")

            spec = self._openapi_spec_cache
            base_url = spec.get("servers", [{}])[0].get("url", "http://localhost:8000")
            version = spec.get("info", {}).get("version", "unknown")

            endpoints: Dict[str, Dict[str, Any]] = {}
            categorized_snippets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

            for path, methods in spec.get("paths", {}).items():
                for http_method, operation in methods.items():
                    http_method_upper = http_method.upper()
                    if http_method_upper not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
                        continue

                    operation_entry = self._build_operation_entry(path, http_method_upper, operation)
                    endpoint_key = f"{http_method_upper} {path}"
                    endpoints[endpoint_key] = operation_entry

                    for snippet in operation_entry.get("examples", []):
                        category = snippet.get("category") or (operation.get("tags") or ["uncategorized"])[0]
                        snippet.setdefault("endpoint", path)
                        snippet.setdefault("method", http_method_upper)
                        categorized_snippets[category].append(snippet)

            self._api_documentation_cache = {
                "endpoints": endpoints,
                "base_url": base_url,
                "version": version,
                "generated_at": datetime.utcnow().isoformat(),
            }

            self._code_snippets = categorized_snippets
            self.logger.info("API documentation initialized from OpenAPI spec", version=version)

        except Exception as e:
            self.logger.error("Failed to initialize API documentation", error=str(e))
            self._api_documentation_cache = {}
            self._code_snippets = {}

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
                    "code": "from aurum.api.services.feature_store_shim import get_feature_store_service\nfeatures = await get_feature_store_service().get_features_for_modeling()",
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

    def _current_actor(self) -> str:
        actor = get_request_id()
        if actor:
            return actor
        tenant = get_tenant_id()
        if tenant:
            return tenant
        return "system"

    def _tenant_scope(self, tenant_id: Optional[str]) -> str:
        return tenant_id or self._GLOBAL_TENANT_SCOPE

    def _environment_storage_key(self, tenant_id: Optional[str], environment_id: str) -> str:
        scope = self._tenant_scope(tenant_id)
        return f"{scope}:{environment_id}"

    def _split_environment_storage_key(self, storage_key: str) -> tuple[str, str]:
        if ":" not in storage_key:
            return (self._GLOBAL_TENANT_SCOPE, storage_key)
        scope, env_id = storage_key.split(":", 1)
        if not env_id:
            return (scope or self._GLOBAL_TENANT_SCOPE, storage_key)
        return scope or self._GLOBAL_TENANT_SCOPE, env_id

    def _environment_cache_key(self, tenant_id: Optional[str], environment_id: str, suffix: str) -> str:
        scope = self._tenant_scope(tenant_id)
        return f"{self._env_cache_prefix}:{scope}:{environment_id}:{suffix}"

    def _environment_index_key_for(self, tenant_id: Optional[str]) -> str:
        scope = self._tenant_scope(tenant_id)
        return f"{self._env_cache_prefix}:{scope}:index"

    def _environment_lookup_keys(
        self,
        tenant_id: Optional[str],
        environment_id: str,
        *,
        include_default: bool = True,
    ) -> List[str]:
        keys = [self._environment_storage_key(tenant_id, environment_id)]
        if include_default and tenant_id is not None:
            keys.append(self._environment_storage_key(None, environment_id))
        return keys

    def _resolve_environment(
        self,
        tenant_id: str,
        environment_id: str,
        *,
        include_default: bool = True,
    ) -> Optional[NotebookEnvironment]:
        for key in self._environment_lookup_keys(tenant_id, environment_id, include_default=include_default):
            environment = self._environments.get(key)
            if environment is not None:
                return environment
        return None

    def _resolve_environment_metadata(
        self,
        tenant_id: str,
        environment_id: str,
        *,
        include_default: bool = True,
    ) -> Optional[Dict[str, Any]]:
        for key in self._environment_lookup_keys(tenant_id, environment_id, include_default=include_default):
            metadata = self._environment_metadata.get(key)
            if metadata is not None:
                return dict(metadata)
        return None

    async def _ensure_environments_loaded(self, tenant_id: Optional[str]) -> None:
        scope = self._tenant_scope(tenant_id)
        if scope in self._environments_loaded:
            return

        async with self._environment_lock:
            if scope in self._environments_loaded:
                return

            await self._load_cached_environments(scope)

            if scope == self._GLOBAL_TENANT_SCOPE:
                timestamp = datetime.utcnow().isoformat()
                for env_id, environment in self._default_environments.items():
                    storage_key = self._environment_storage_key(None, env_id)
                    if storage_key not in self._environments:
                        self._environments[storage_key] = environment
                    metadata = self._environment_metadata.setdefault(storage_key, {})
                    metadata.setdefault("environment_id", env_id)
                    metadata.setdefault("tenant_id", None)
                    metadata.setdefault("created_at", timestamp)
                    metadata.setdefault("updated_at", timestamp)
                    metadata.setdefault("created_by", "system")
                    metadata.setdefault("updated_by", "system")
                    metadata.setdefault("source", "default")
                    metadata.setdefault("is_default", True)
                    metadata.setdefault("version", 1)
                    self._environment_versions.setdefault(storage_key, [])

            self._environments_loaded.add(scope)

    async def _load_cached_environments(self, tenant_scope: str) -> None:
        try:
            index: List[str] = await self.cache_manager.get(
                self._environment_index_key_for(tenant_scope),
                namespace=CacheNamespace.SYSTEM_CONFIG,
                default=[],
            )

            if not index:
                return

            loaded = 0
            for env_id in index:
                data = await self.cache_manager.get(
                    self._environment_cache_key(tenant_scope, env_id, "data"),
                    namespace=CacheNamespace.SYSTEM_CONFIG,
                    default=None,
                )

                if not data:
                    continue

                try:
                    environment = NotebookEnvironment(**data)
                except ValidationError as exc:  # pragma: no cover - defensive
                    self.telemetry.warning(
                        "Cached notebook environment failed validation",
                        environment_id=env_id,
                        tenant_scope=tenant_scope,
                        error=str(exc),
                    )
                    continue

                storage_key = self._environment_storage_key(tenant_scope, env_id)
                self._environments[storage_key] = environment
                metadata = await self.cache_manager.get(
                    self._environment_cache_key(tenant_scope, env_id, "metadata"),
                    namespace=CacheNamespace.SYSTEM_CONFIG,
                    default=None,
                )
                if isinstance(metadata, dict):
                    metadata.setdefault("environment_id", env_id)
                    metadata.setdefault(
                        "tenant_id",
                        None if tenant_scope == self._GLOBAL_TENANT_SCOPE else tenant_scope,
                    )
                    self._environment_metadata[storage_key] = metadata

                history = await self.cache_manager.get(
                    self._environment_cache_key(tenant_scope, env_id, "history"),
                    namespace=CacheNamespace.SYSTEM_CONFIG,
                    default=[],
                )
                if history:
                    self._environment_versions[storage_key] = history[-MAX_ENVIRONMENT_HISTORY:]

                loaded += 1

            if loaded:
                self.telemetry.info(
                    "Loaded notebook environments from cache",
                    count=loaded,
                    tenant_scope=tenant_scope,
                )

        except Exception as exc:  # pragma: no cover - defensive
            self.telemetry.error(
                "Failed to load cached notebook environments",
                tenant_scope=tenant_scope,
                error=str(exc),
            )

    async def _write_environment_index(self, tenant_id: Optional[str]) -> None:
        scope = self._tenant_scope(tenant_id)
        try:
            environment_ids = sorted(
                env_id
                for storage_key in self._environments.keys()
                for key_scope, env_id in [self._split_environment_storage_key(storage_key)]
                if key_scope == scope
            )
            await self.cache_manager.set(
                self._environment_index_key_for(scope),
                environment_ids,
                namespace=CacheNamespace.SYSTEM_CONFIG,
                ttl_policy=TTLPolicy.PERSISTENT,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self.telemetry.error(
                "Failed to write environment index",
                tenant_scope=scope,
                error=str(exc),
            )

    async def _persist_environment(
        self,
        tenant_id: Optional[str],
        environment: NotebookEnvironment,
        *,
        is_new: bool,
        updated_by: Optional[str] = None
    ) -> None:
        env_id = environment.environment_id
        actor = updated_by or self._current_actor()
        timestamp = datetime.utcnow()
        timestamp_iso = timestamp.isoformat()
        storage_key = self._environment_storage_key(tenant_id, env_id)
        is_default = tenant_id is None and env_id in self._default_environments
        source = "default" if is_default else "user"

        metadata = self._environment_metadata.get(storage_key, {}).copy()
        if not metadata:
            metadata = {
                "environment_id": env_id,
                "tenant_id": None if tenant_id is None else tenant_id,
                "created_at": timestamp_iso,
                "created_by": actor,
                "source": source,
            }

        metadata.setdefault("environment_id", env_id)
        metadata.setdefault("tenant_id", None if tenant_id is None else tenant_id)
        metadata.setdefault("created_at", timestamp_iso)
        metadata.setdefault("created_by", actor)
        metadata.setdefault("source", source)
        metadata["updated_at"] = timestamp_iso
        metadata["updated_by"] = actor
        metadata["is_default"] = is_default
        metadata["version"] = metadata.get("version", 0) + 1

        k8s_yaml = self._generate_k8s_yaml(environment)
        yaml_cache_key = self._environment_cache_key(tenant_id, env_id, "k8s")
        metadata["k8s_yaml_key"] = yaml_cache_key

        self._environments[storage_key] = environment
        self._environment_metadata[storage_key] = metadata

        history_entry = {
            "version": metadata["version"],
            "updated_at": timestamp_iso,
            "updated_by": actor,
            "environment": environment.dict(),
        }
        history = self._environment_versions.setdefault(storage_key, [])
        history.append(history_entry)
        if len(history) > MAX_ENVIRONMENT_HISTORY:
            self._environment_versions[storage_key] = history[-MAX_ENVIRONMENT_HISTORY:]
            history = self._environment_versions[storage_key]

        payloads = [
            (self._environment_cache_key(tenant_id, env_id, "data"), environment.dict()),
            (self._environment_cache_key(tenant_id, env_id, "metadata"), metadata),
            (self._environment_cache_key(tenant_id, env_id, "history"), history),
            (yaml_cache_key, k8s_yaml),
            (f"env_yaml:{self._tenant_scope(tenant_id)}:{env_id}", k8s_yaml),
        ]

        if tenant_id is None:
            payloads.append((f"env_yaml:{env_id}", k8s_yaml))

        for key, value in payloads:
            await self.cache_manager.set(
                key,
                value,
                namespace=CacheNamespace.SYSTEM_CONFIG,
                ttl_policy=TTLPolicy.PERSISTENT,
            )

        await self._write_environment_index(tenant_id)

        self.telemetry.info(
            "Notebook environment persisted",
            environment_id=env_id,
            tenant_scope=self._tenant_scope(tenant_id),
            version=metadata["version"],
            is_new=is_new,
            actor=actor,
        )
        self.telemetry.increment_counter(
            "developer_workspace_environment_upserts",
            category=MetricCategory.BUSINESS,
            is_new=str(is_new).lower(),
        )

    async def _delete_environment_from_cache(self, tenant_id: Optional[str], environment_id: str) -> None:
        scope = self._tenant_scope(tenant_id)
        for suffix in ("data", "metadata", "history", "k8s"):
            await self.cache_manager.delete(
                self._environment_cache_key(tenant_id, environment_id, suffix),
                namespace=CacheNamespace.SYSTEM_CONFIG,
            )

        await self.cache_manager.delete(
            f"env_yaml:{scope}:{environment_id}",
            namespace=CacheNamespace.SYSTEM_CONFIG,
        )
        if tenant_id is None:
            await self.cache_manager.delete(
                f"env_yaml:{environment_id}",
                namespace=CacheNamespace.SYSTEM_CONFIG,
            )

    async def create_notebook_environment(self, tenant_id: str, environment: NotebookEnvironment) -> str:
        """Create a new notebook environment scoped to a tenant."""
        await self._ensure_environments_loaded(tenant_id)

        env_id = environment.environment_id
        existing = self._resolve_environment(tenant_id, env_id, include_default=False)
        if existing is not None:
            raise ValueError(f"Environment {env_id} already exists for tenant {tenant_id}")

        await self._persist_environment(tenant_id, environment, is_new=True)
        return env_id

    async def update_notebook_environment(
        self,
        tenant_id: str,
        environment_id: str,
        updates: Dict[str, Any]
    ) -> NotebookEnvironment:
        await self._ensure_environments_loaded(tenant_id)

        current = self._resolve_environment(tenant_id, environment_id, include_default=True)
        if current is None:
            raise ValueError(f"Environment {environment_id} not found for tenant {tenant_id}")

        data = current.dict()
        data.update(updates)

        try:
            updated_environment = NotebookEnvironment(**data)
        except ValidationError as exc:
            self.telemetry.error(
                "Invalid notebook environment update",
                tenant_id=tenant_id,
                environment_id=environment_id,
                error=str(exc),
            )
            raise

        storage_key = self._environment_storage_key(tenant_id, environment_id)
        is_new_override = storage_key not in self._environments
        await self._persist_environment(tenant_id, updated_environment, is_new=is_new_override)
        return updated_environment

    async def delete_notebook_environment(self, tenant_id: str, environment_id: str) -> bool:
        await self._ensure_environments_loaded(tenant_id)

        storage_key = self._environment_storage_key(tenant_id, environment_id)
        environment = self._environments.get(storage_key)
        if not environment:
            return False

        self._environments.pop(storage_key, None)
        self._environment_metadata.pop(storage_key, None)
        self._environment_versions.pop(storage_key, None)

        await self._delete_environment_from_cache(tenant_id, environment_id)
        await self._write_environment_index(tenant_id)

        actor = self._current_actor()
        self.telemetry.info(
            "Notebook environment deleted",
            environment_id=environment_id,
            tenant_id=tenant_id,
            actor=actor,
        )
        self.telemetry.increment_counter(
            "developer_workspace_environment_deletes",
            category=MetricCategory.BUSINESS,
        )

        return True

    async def list_notebook_environments(self, tenant_id: str) -> List[NotebookEnvironment]:
        await self._ensure_environments_loaded(tenant_id)

        combined: Dict[str, NotebookEnvironment] = {}
        for storage_key, environment in self._environments.items():
            scope, env_id = self._split_environment_storage_key(storage_key)
            if scope == self._GLOBAL_TENANT_SCOPE:
                combined.setdefault(env_id, environment)
            elif scope == tenant_id:
                combined[env_id] = environment

        return [combined[key].model_copy(deep=True) for key in sorted(combined.keys())]

    async def get_notebook_environment(self, tenant_id: str, environment_id: str) -> Optional[NotebookEnvironment]:
        await self._ensure_environments_loaded(tenant_id)
        environment = self._resolve_environment(tenant_id, environment_id)
        return environment.model_copy(deep=True) if environment else None

    async def get_notebook_environment_metadata(
        self,
        tenant_id: str,
        environment_id: str,
    ) -> Optional[Dict[str, Any]]:
        await self._ensure_environments_loaded(tenant_id)
        return self._resolve_environment_metadata(tenant_id, environment_id)

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

    def _ensure_storage_quota_initialized(self, tenant_id: str) -> None:
        if tenant_id not in self._tenant_storage_quota_gb:
            self._tenant_storage_quota_gb[tenant_id] = DEFAULT_STORAGE_QUOTA_GB
        _ = self._tenant_storage_usage_bytes[tenant_id]
        _ = self._tenant_active_sessions[tenant_id]

    def _configured_session_limit(self, tenant_id: str) -> int:
        return self._tenant_session_limits.get(tenant_id, DEFAULT_MAX_CONCURRENT_SESSIONS)

    def _enforce_session_limit(self, tenant_id: str) -> None:
        limit = self._configured_session_limit(tenant_id)
        if len(self._tenant_active_sessions[tenant_id]) >= limit:
            raise SessionLimitExceeded(tenant_id, limit)

    def _register_active_session(self, tenant_id: str, session_id: str) -> None:
        self._tenant_active_sessions[tenant_id].add(session_id)

    def _remove_active_session(self, tenant_id: str, session_id: str) -> None:
        bucket = self._tenant_active_sessions.get(tenant_id)
        if not bucket:
            return
        bucket.discard(session_id)
        if not bucket:
            self._tenant_active_sessions.pop(tenant_id, None)

    def _enforce_storage_quota(self, tenant_id: str, requested_bytes: int) -> None:
        quota_bytes = int(self._tenant_storage_quota_gb[tenant_id] * BYTES_PER_GIB)
        current_bytes = self._tenant_storage_usage_bytes[tenant_id]
        if current_bytes + requested_bytes > quota_bytes:
            raise StorageQuotaExceeded(tenant_id, quota_bytes, requested_bytes, current_bytes)

    def _track_tenant_storage_usage(self, tenant_id: str, notebook_size_bytes: int) -> None:
        if notebook_size_bytes <= 0:
            return
        self._tenant_storage_usage_bytes[tenant_id] += notebook_size_bytes

    def _release_tenant_storage_usage(self, tenant_id: str, notebook_size_bytes: int) -> None:
        if notebook_size_bytes <= 0:
            return
        current = self._tenant_storage_usage_bytes.get(tenant_id, 0)
        remaining = max(0, current - notebook_size_bytes)
        if remaining:
            self._tenant_storage_usage_bytes[tenant_id] = remaining
        else:
            self._tenant_storage_usage_bytes.pop(tenant_id, None)

    def _coerce_positive_bytes(self, value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(max(value, 0))
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return 0
            try:
                return int(max(float(candidate), 0))
            except ValueError:
                return 0
        return 0

    def _storage_string_to_bytes(self, value: str) -> int:
        if not value:
            return 0
        candidate = value.strip().lower()
        multiplier = 1
        suffixes = {
            "gib": BYTES_PER_GIB,
            "gi": BYTES_PER_GIB,
            "gb": BYTES_PER_GIB,
            "mib": 1024 ** 2,
            "mi": 1024 ** 2,
            "mb": 1024 ** 2,
        }
        for suffix, factor in suffixes.items():
            if candidate.endswith(suffix):
                multiplier = factor
                candidate = candidate[: -len(suffix)]
                break
        try:
            numeric = float(candidate)
        except ValueError:
            return 0
        return int(max(numeric, 0) * multiplier)

    def _estimate_session_storage_bytes(
        self,
        environment: NotebookEnvironment,
        configuration: Dict[str, Any],
    ) -> int:
        raw_estimate = configuration.get("estimated_notebook_size_bytes")
        if raw_estimate is not None:
            return self._coerce_positive_bytes(raw_estimate)
        return self._storage_string_to_bytes(environment.storage_size)

    def _resolve_session_storage_bytes(
        self,
        environment: NotebookEnvironment,
        configuration: Dict[str, Any],
    ) -> int:
        estimate = configuration.get("estimated_notebook_size_bytes")
        if estimate is not None:
            return self._coerce_positive_bytes(estimate)
        return self._storage_string_to_bytes(environment.storage_size)

    async def start_notebook_session(
        self,
        environment_id: str,
        user_id: str,
        tenant_id: str,
        configuration: Dict[str, Any] = None
    ) -> str:
        """Start a new notebook session."""
        configuration = dict(configuration or {})

        await self._ensure_environments_loaded(tenant_id)
        environment = self._resolve_environment(tenant_id, environment_id)
        if not environment:
            raise ValueError(f"Environment {environment_id} not found")

        self._ensure_storage_quota_initialized(tenant_id)
        self._enforce_session_limit(tenant_id)

        notebook_size_bytes = self._resolve_session_storage_bytes(environment, configuration)
        self._enforce_storage_quota(tenant_id, notebook_size_bytes)

        session_id = str(uuid4())

        session = NotebookSession(
            session_id=session_id,
            environment_id=environment_id,
            user_id=user_id,
            tenant_id=tenant_id,
            status="starting",
            start_time=datetime.utcnow(),
            last_activity=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(minutes=DEFAULT_SESSION_TTL_MINUTES),
        )

        try:
            self._sessions[session_id] = session
            self._register_active_session(tenant_id, session_id)
            if notebook_size_bytes:
                self._track_tenant_storage_usage(tenant_id, notebook_size_bytes)

            configuration.setdefault("estimated_notebook_size_bytes", notebook_size_bytes)

            self._session_metadata[session_id] = {
                "session_id": session_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "environment_id": environment_id,
                "created_at": session.start_time.isoformat(),
                "status_history": [(session.status, session.start_time.isoformat())],
                "configuration": configuration,
                "notebook_size_bytes": notebook_size_bytes,
            }
            self._session_versions.setdefault(session_id, [])

            await self._persist_session(session_id)
            await self._write_session_index()

        except Exception:
            self._remove_active_session(tenant_id, session_id)
            if notebook_size_bytes:
                self._release_tenant_storage_usage(tenant_id, notebook_size_bytes)
            self._sessions.pop(session_id, None)
            self._session_metadata.pop(session_id, None)
            self._session_versions.pop(session_id, None)
            raise

        asyncio.create_task(self._manage_notebook_session(session_id, environment))

        self.telemetry.info(
            "Notebook session started",
            session_id=session_id,
            user_id=user_id,
            environment_id=environment_id,
        )
        self.telemetry.increment_counter(
            "developer_workspace_session_starts",
            category=MetricCategory.BUSINESS,
        )
        return session_id

    async def _manage_notebook_session(
        self,
        session_id: str,
        environment: NotebookEnvironment
    ) -> None:
        """Manage notebook session lifecycle."""
        session = self._sessions[session_id]

        try:
            # Simulate pod creation and startup
            await asyncio.sleep(self._session_startup_delay_seconds)  # Simulate startup time

            session.status = "running"
            session.pod_name = f"aurum-notebook-{session_id}"
            session.pod_ip = "10.0.0.100"  # Mock IP
            session.notebook_url = f"http://{session.pod_ip}:8888"
            self._record_session_status(session_id, session.status)
            await self._persist_session(session_id)

            # Monitor session activity
            while session.status == "running":
                await asyncio.sleep(self._session_monitor_interval_seconds)

                if session.expires_at and datetime.utcnow() >= session.expires_at:
                    session.status = "expired"
                    self.telemetry.info(
                        "Notebook session expired",
                        session_id=session_id,
                        expires_at=session.expires_at.isoformat(),
                    )
                    await self._stop_notebook_session(session_id, reason="expired")
                    break

                # Update last activity heartbeat
                session.last_activity = datetime.utcnow()

                # Check for idle timeout
                idle_timeout = environment.idle_timeout_minutes or DEFAULT_IDLE_TIMEOUT_MINUTES
                max_runtime = environment.max_runtime_hours or DEFAULT_MAX_RUNTIME_HOURS

                if session.last_activity:
                    idle_minutes = (datetime.utcnow() - session.last_activity).total_seconds() / 60
                    if idle_minutes > idle_timeout:
                        session.status = "idle_timeout"
                        self.telemetry.warning(
                            "Notebook session idle timeout",
                            session_id=session_id,
                            idle_minutes=idle_minutes,
                            idle_timeout_minutes=idle_timeout,
                        )
                        await self._stop_notebook_session(session_id, reason="idle_timeout")
                        break

                if session.start_time:
                    runtime_hours = (datetime.utcnow() - session.start_time).total_seconds() / 3600
                    if runtime_hours > max_runtime:
                        session.status = "runtime_exceeded"
                        self.telemetry.warning(
                            "Notebook session exceeded max runtime",
                            session_id=session_id,
                            runtime_hours=runtime_hours,
                            max_runtime_hours=max_runtime,
                        )
                        await self._stop_notebook_session(session_id, reason="runtime_exceeded")
                        break

                session.resource_usage = self._generate_mock_resource_usage(environment)
                await self._persist_session(session_id)

        except Exception as e:
            session.status = "failed"
            session.error_message = str(e)
            self._record_session_status(session_id, session.status)
            await self._persist_session(session_id)
            notebook_size_bytes = self._session_metadata.get(session_id, {}).get("notebook_size_bytes", 0)
            self._release_tenant_storage_usage(session.tenant_id, notebook_size_bytes)
            self._remove_active_session(session.tenant_id, session_id)
            self.telemetry.error("Session management failed", session_id=session_id, error=str(e))

    def _session_resource_key(self, session: NotebookSession) -> str:
        return f"notebook:{session.tenant_id}:{session.session_id}"

    async def _stop_notebook_session(self, session_id: str, reason: str = "user_requested") -> None:
        """Stop notebook session."""
        session = self._sessions.get(session_id)
        if not session:
            return

        session.status = "stopping"
        self._record_session_status(session_id, session.status)
        await self._persist_session(session_id)

        # Simulate pod cleanup
        await asyncio.sleep(2)

        session.status = "stopped"
        session.last_activity = datetime.utcnow()
        self._record_session_status(session_id, session.status)
        await self._persist_session(session_id)

        notebook_size_bytes = self._session_metadata.get(session_id, {}).get("notebook_size_bytes", 0)
        self._release_tenant_storage_usage(session.tenant_id, notebook_size_bytes)
        self._remove_active_session(session.tenant_id, session_id)

        self.telemetry.info(
            "Notebook session stopped",
            session_id=session_id,
            reason=reason,
        )
        self.telemetry.increment_counter(
            "developer_workspace_session_stops",
            category=MetricCategory.BUSINESS,
            reason=reason,
        )

        await self._delete_session_from_cache(session_id)

        session_resource_key = self._session_resource_key(session)
        await self.cache_manager.delete(session_resource_key, namespace=CacheNamespace.USER_DATA)

    async def get_session_status(self, session_id: str, tenant_id: str) -> Optional[NotebookSession]:
        """Get notebook session status for requesting tenant."""
        await self._ensure_sessions_loaded()
        session = self._sessions.get(session_id)
        if not session:
            return None
        if session.tenant_id != tenant_id:
            raise TenantAccessError(tenant_id, session.tenant_id, session_id)
        return session

    async def list_user_sessions(self, user_id: str) -> List[NotebookSession]:
        """List active sessions for a user."""
        await self._ensure_sessions_loaded()
        return [s for s in self._sessions.values() if s.user_id == user_id and s.status == "running"]

    async def terminate_notebook_session(
        self,
        session_id: str,
        tenant_id: str,
        reason: str = "user_requested",
    ) -> bool:
        await self._ensure_sessions_loaded()

        session = self._sessions.get(session_id)
        if not session:
            return False

        if session.tenant_id != tenant_id:
            raise TenantAccessError(tenant_id, session.tenant_id, session_id)

        if session.status in {"stopped", "stopping"}:
            return False

        await self._stop_notebook_session(session_id, reason=reason)
        return True

    async def _write_session_index(self) -> None:
        try:
            session_ids = sorted(self._sessions.keys())
            await self.cache_manager.set(
                self._session_index_key,
                session_ids,
                namespace=CacheNamespace.USER_DATA,
                ttl_policy=SESSION_PERSIST_TTL,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self.telemetry.error("Failed to write session index", error=str(exc))

    async def _persist_session(self, session_id: str) -> None:
        session = self._sessions.get(session_id)
        if not session:
            return

        metadata = self._session_metadata.get(session_id, {}).copy()
        metadata.setdefault("session_id", session_id)
        metadata.setdefault("tenant_id", session.tenant_id)
        metadata.setdefault("user_id", session.user_id)
        metadata.setdefault("environment_id", session.environment_id)
        metadata.setdefault(
            "created_at",
            session.start_time.isoformat() if session.start_time else datetime.utcnow().isoformat(),
        )
        metadata.setdefault("configuration", {})
        metadata["updated_at"] = datetime.utcnow().isoformat()
        metadata["status_history"] = metadata.get("status_history", [])[-MAX_SESSION_HISTORY:]

        self._session_metadata[session_id] = metadata

        history_entry = {
            "status": session.status,
            "timestamp": datetime.utcnow().isoformat(),
            "resource_usage": session.resource_usage,
        }
        history = self._session_versions.setdefault(session_id, [])
        history.append(history_entry)
        if len(history) > MAX_SESSION_HISTORY:
            self._session_versions[session_id] = history[-MAX_SESSION_HISTORY:]
            history = self._session_versions[session_id]

        payloads = [
            (f"{self._session_cache_prefix}:{session_id}:data", session.dict()),
            (f"{self._session_cache_prefix}:{session_id}:metadata", metadata),
            (f"{self._session_cache_prefix}:{session_id}:history", history),
        ]

        for key, value in payloads:
            await self.cache_manager.set(
                key,
                value,
                namespace=CacheNamespace.USER_DATA,
                ttl_policy=SESSION_PERSIST_TTL,
            )

    async def _delete_session_from_cache(self, session_id: str) -> None:
        for suffix in ("data", "metadata", "history"):
            await self.cache_manager.delete(
                f"{self._session_cache_prefix}:{session_id}:{suffix}",
                namespace=CacheNamespace.USER_DATA,
            )

    def _record_session_status(self, session_id: str, status: str) -> None:
        metadata = self._session_metadata.setdefault(session_id, {})
        history = metadata.setdefault("status_history", [])
        history.append((status, datetime.utcnow().isoformat()))
        metadata["status_history"] = history[-MAX_SESSION_HISTORY:]

    def _generate_mock_resource_usage(self, environment: NotebookEnvironment) -> Dict[str, Any]:
        return {
            "cpu_millicores": random.randint(
                200,
                int(float(environment.resource_limits.get("cpu", "1")) * 1000),
            ),
            "memory_bytes": random.randint(256 * 1024 * 1024, 2 * 1024 * 1024 * 1024),
            "last_updated": datetime.utcnow().isoformat(),
        }

    async def _ensure_sessions_loaded(self) -> None:
        if self._sessions_loaded:
            return

        async with self._session_lock:
            if self._sessions_loaded:
                return

            await self._load_cached_sessions()
            self._sessions_loaded = True

    async def _load_cached_sessions(self) -> None:
        try:
            index: List[str] = await self.cache_manager.get(
                self._session_index_key,
                namespace=CacheNamespace.USER_DATA,
                default=[],
            )

            if not index:
                return

            loaded = 0
            for session_id in index:
                data = await self.cache_manager.get(
                    f"{self._session_cache_prefix}:{session_id}:data",
                    namespace=CacheNamespace.USER_DATA,
                    default=None,
                )
                if not data:
                    continue

                try:
                    session = NotebookSession(**data)
                except ValidationError as exc:  # pragma: no cover - defensive
                    self.telemetry.warning(
                        "Cached notebook session failed validation",
                        session_id=session_id,
                        error=str(exc),
                    )
                    continue

                self._sessions[session_id] = session

                metadata = await self.cache_manager.get(
                    f"{self._session_cache_prefix}:{session_id}:metadata",
                    namespace=CacheNamespace.USER_DATA,
                    default=None,
                )
                if isinstance(metadata, dict):
                    self._session_metadata[session_id] = metadata

                history = await self.cache_manager.get(
                    f"{self._session_cache_prefix}:{session_id}:history",
                    namespace=CacheNamespace.USER_DATA,
                    default=[],
                )
                if history:
                    self._session_versions[session_id] = history[-MAX_SESSION_HISTORY:]

                loaded += 1

            if loaded:
                self.telemetry.info(
                    "Loaded notebook sessions from cache",
                    count=loaded,
                )

        except Exception as exc:  # pragma: no cover - defensive
            self.telemetry.error("Failed to load cached notebook sessions", error=str(exc))

    async def _ensure_api_docs_loaded(self) -> None:
        if self._api_docs_loaded:
            return

        async with self._api_docs_lock:
            if self._api_docs_loaded:
                return

            self._initialize_api_documentation()
            self._initialize_code_snippets()
            self._api_docs_loaded = True

    def _build_operation_entry(
        self,
        path: str,
        http_method: str,
        operation: Dict[str, Any]
    ) -> Dict[str, Any]:
        parameters = operation.get("parameters", []).copy()
        if "requestBody" in operation:
            parameters.append({
                "name": "body",
                "in": "requestBody",
                "required": operation.get("requestBody", {}).get("required", False),
                "schema": operation.get("requestBody", {}).get("content", {}),
            })

        responses = operation.get("responses", {})
        default_response = responses.get("200") or next(iter(responses.values())) if responses else {}
        response_schema: Dict[str, Any] = {}
        if isinstance(default_response, dict):
            content = default_response.get("content", {})
            if content:
                first_media = next(iter(content.values()))
                response_schema = first_media.get("schema", {})

        examples = []
        for example in operation.get("x-examples", []) or []:
            example_entry = {
                "name": example.get("name"),
                "description": example.get("description"),
                "language": example.get("language", "python"),
                "code": example.get("code"),
                "category": example.get("category"),
            }
            examples.append(example_entry)

        endpoint_info = {
            "path": path,
            "method": http_method,
            "summary": operation.get("summary", ""),
            "description": operation.get("description", ""),
            "parameters": parameters,
            "response_schema": response_schema,
            "tags": operation.get("tags", []),
            "operation_id": operation.get("operationId"),
            "examples": examples,
        }

        return endpoint_info

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

            if not self._code_snippets:
                await self._ensure_api_docs_loaded()

            if category and category in self._code_snippets:
                snippets.extend(self._code_snippets[category])
            elif category:
                return []
            else:
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
                        "# Add actual API call code here\n",
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
