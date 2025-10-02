"""Workspace Manager Service.

This service handles developer workspace lifecycle management including
workspace creation, deletion, resource allocation, and access control.

Extracted from the monolithic developer_workspace_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from uuid import uuid4
from collections import defaultdict

from pydantic import BaseModel, Field, field_validator

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


# Constants
DEFAULT_IDLE_TIMEOUT_MINUTES = 60
DEFAULT_MAX_RUNTIME_HOURS = 8
DEFAULT_STORAGE_QUOTA_GB = 50
DEFAULT_MAX_CONCURRENT_SESSIONS = 5
BYTES_PER_GB = 1024 ** 3


class StorageQuotaExceeded(RuntimeError):
    """Raised when a tenant exceeds the configured storage quota."""
    pass


class SessionLimitExceeded(RuntimeError):
    """Raised when a tenant exceeds the concurrent session limit."""
    pass


class NotebookEnvironment(BaseModel):
    """Represents a notebook compute environment configuration."""
    
    environment_id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: Optional[str] = None
    image: str = "jupyter/base-notebook:latest"
    cpu_request: str = "100m"
    cpu_limit: str = "1000m"
    memory_request: str = "512Mi"
    memory_limit: str = "2Gi"
    gpu_enabled: bool = False
    gpu_count: int = 0
    environment_variables: Dict[str, str] = Field(default_factory=dict)
    pip_packages: List[str] = Field(default_factory=list)
    conda_packages: List[str] = Field(default_factory=list)
    mounted_secrets: List[str] = Field(default_factory=list)
    volume_mounts: List[Dict[str, str]] = Field(default_factory=list)
    idle_timeout_minutes: int = DEFAULT_IDLE_TIMEOUT_MINUTES
    max_runtime_hours: int = DEFAULT_MAX_RUNTIME_HOURS
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    tags: List[str] = Field(default_factory=list)
    tenant_scoped: bool = True
    tenant_id: Optional[str] = None
    allowed_tenants: Set[str] = Field(default_factory=set)
    shared: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


class NotebookSession(BaseModel):
    """Represents an active notebook session."""
    
    session_id: str = Field(default_factory=lambda: str(uuid4()))
    environment_id: str
    user_id: str
    tenant_id: str
    pod_name: Optional[str] = None
    namespace: str = "notebooks"
    status: str = "pending"  # pending, starting, running, stopping, terminated
    jupyter_url: Optional[str] = None
    token: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    terminated_at: Optional[datetime] = None
    termination_reason: Optional[str] = None
    resource_usage: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v: str) -> str:
        valid_statuses = {"pending", "starting", "running", "stopping", "terminated"}
        if v not in valid_statuses:
            raise ValueError(f"Invalid status: {v}")
        return v


class WorkspaceRepository(BaseRepository):
    """Repository interface for workspace operations."""
    
    async def save_environment(self, environment: NotebookEnvironment) -> NotebookEnvironment:
        """Save or update a notebook environment."""
        raise NotImplementedError
    
    async def get_environment(self, environment_id: str) -> Optional[NotebookEnvironment]:
        """Get an environment by ID."""
        raise NotImplementedError
    
    async def list_environments(
        self,
        tenant_id: Optional[str] = None,
        shared: Optional[bool] = None,
        tags: Optional[List[str]] = None
    ) -> List[NotebookEnvironment]:
        """List environments with optional filters."""
        raise NotImplementedError
    
    async def delete_environment(self, environment_id: str) -> bool:
        """Delete an environment."""
        raise NotImplementedError
    
    async def save_session(self, session: NotebookSession) -> NotebookSession:
        """Save or update a notebook session."""
        raise NotImplementedError
    
    async def get_session(self, session_id: str) -> Optional[NotebookSession]:
        """Get a session by ID."""
        raise NotImplementedError
    
    async def list_sessions(
        self,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[NotebookSession]:
        """List sessions with optional filters."""
        raise NotImplementedError


class WorkspaceManagerService(BaseService):
    """
    Developer workspace lifecycle management service.
    
    This service handles workspace environment configuration, session lifecycle,
    resource allocation, and access control for developer notebooks.
    """
    
    def __init__(
        self,
        repository: Optional[WorkspaceRepository] = None,
        k8s_client: Optional[Any] = None,  # Interface to Kubernetes
        cache_enabled: bool = True,
        cache_ttl: int = 300
    ):
        """
        Initialize the workspace manager service.
        
        Args:
            repository: Repository for data persistence
            k8s_client: Kubernetes client for pod management
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.k8s_client = k8s_client  # In real impl, inject from DI
        self.logger = logging.getLogger(__name__)
        
        # Resource tracking
        self._tenant_storage_quota_gb: Dict[str, int] = {}
        self._tenant_storage_usage_bytes: defaultdict[str, int] = defaultdict(int)
        self._tenant_active_sessions: defaultdict[str, Set[str]] = defaultdict(set)
        self._tenant_session_limits: Dict[str, int] = {}
        
        # Session monitoring
        self._session_monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._monitor_interval = 60  # seconds
    
    def _get_default_repository(self) -> WorkspaceRepository:
        """Get default repository from DI container."""
        # TODO: Integrate with DI container
        class MockRepository(WorkspaceRepository):
            def __init__(self):
                self.environments = {}
                self.sessions = {}
            
            async def save_environment(self, environment: NotebookEnvironment) -> NotebookEnvironment:
                self.environments[environment.environment_id] = environment
                return environment
            
            async def get_environment(self, environment_id: str) -> Optional[NotebookEnvironment]:
                return self.environments.get(environment_id)
            
            async def list_environments(self, **kwargs) -> List[NotebookEnvironment]:
                envs = list(self.environments.values())
                if kwargs.get('tenant_id'):
                    envs = [e for e in envs if e.tenant_id == kwargs['tenant_id'] or e.shared]
                return envs
            
            async def delete_environment(self, environment_id: str) -> bool:
                return self.environments.pop(environment_id, None) is not None
            
            async def save_session(self, session: NotebookSession) -> NotebookSession:
                self.sessions[session.session_id] = session
                return session
            
            async def get_session(self, session_id: str) -> Optional[NotebookSession]:
                return self.sessions.get(session_id)
            
            async def list_sessions(self, **kwargs) -> List[NotebookSession]:
                sessions = list(self.sessions.values())
                if kwargs.get('user_id'):
                    sessions = [s for s in sessions if s.user_id == kwargs['user_id']]
                if kwargs.get('tenant_id'):
                    sessions = [s for s in sessions if s.tenant_id == kwargs['tenant_id']]
                if kwargs.get('status'):
                    sessions = [s for s in sessions if s.status == kwargs['status']]
                return sessions
        
        return MockRepository()
    
    async def create_environment(
        self,
        name: str,
        image: str,
        created_by: str,
        tenant_id: Optional[str] = None,
        description: Optional[str] = None,
        cpu_limit: str = "1000m",
        memory_limit: str = "2Gi",
        pip_packages: Optional[List[str]] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        shared: bool = False
    ) -> NotebookEnvironment:
        """
        Create a new notebook environment configuration.
        
        Args:
            name: Environment name
            image: Docker image for the environment
            created_by: User creating the environment
            tenant_id: Tenant ID for scoping
            description: Environment description
            cpu_limit: CPU resource limit
            memory_limit: Memory resource limit
            pip_packages: Python packages to install
            environment_variables: Environment variables
            shared: Whether environment is shared across tenants
            
        Returns:
            Created NotebookEnvironment
        """
        environment = NotebookEnvironment(
            name=name,
            description=description,
            image=image,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            pip_packages=pip_packages or [],
            environment_variables=environment_variables or {},
            created_by=created_by,
            tenant_id=tenant_id,
            shared=shared,
            tenant_scoped=not shared
        )
        
        # Save environment
        environment = await self.repository.save_environment(environment)
        
        self.logger.info(
            f"Created notebook environment: {name}",
            extra={
                "environment_id": environment.environment_id,
                "tenant_id": tenant_id,
                "shared": shared
            }
        )
        
        # Emit metric
        await self._emit_metric(
            "environment_created",
            tags={"shared": str(shared)}
        )
        
        return environment
    
    async def update_environment(
        self,
        environment_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        pip_packages: Optional[List[str]] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        tags: Optional[List[str]] = None
    ) -> NotebookEnvironment:
        """
        Update an existing notebook environment.
        
        Args:
            environment_id: Environment to update
            name: New name
            description: New description
            pip_packages: New package list
            environment_variables: New environment variables
            tags: New tags
            
        Returns:
            Updated NotebookEnvironment
        """
        environment = await self.get_environment(environment_id)
        if not environment:
            raise ValueError(f"Environment {environment_id} not found")
        
        # Update fields
        if name is not None:
            environment.name = name
        if description is not None:
            environment.description = description
        if pip_packages is not None:
            environment.pip_packages = pip_packages
        if environment_variables is not None:
            environment.environment_variables = environment_variables
        if tags is not None:
            environment.tags = tags
        
        environment.updated_at = datetime.utcnow()
        
        # Save environment
        environment = await self.repository.save_environment(environment)
        
        self.logger.info(f"Updated environment {environment_id}")
        
        return environment
    
    async def delete_environment(
        self,
        environment_id: str,
        force: bool = False
    ) -> bool:
        """
        Delete a notebook environment.
        
        Args:
            environment_id: Environment to delete
            force: Force deletion even if sessions exist
            
        Returns:
            True if deleted successfully
        """
        # Check for active sessions
        sessions = await self.repository.list_sessions(status="running")
        active_sessions = [s for s in sessions if s.environment_id == environment_id]
        
        if active_sessions and not force:
            raise ValueError(
                f"Cannot delete environment with {len(active_sessions)} active sessions"
            )
        
        # Terminate active sessions if forcing
        if force and active_sessions:
            for session in active_sessions:
                await self.terminate_session(session.session_id, "Environment deleted")
        
        # Delete environment
        success = await self.repository.delete_environment(environment_id)
        
        if success:
            self.logger.info(f"Deleted environment {environment_id}")
            await self._emit_metric("environment_deleted")
        
        return success
    
    async def get_environment(self, environment_id: str) -> Optional[NotebookEnvironment]:
        """
        Get a notebook environment by ID.
        
        Args:
            environment_id: Environment identifier
            
        Returns:
            NotebookEnvironment if found
        """
        # Check cache
        cache_key = f"environment:{environment_id}"
        if self.cache_enabled:
            cached = await self._get_from_cache(cache_key)
            if cached:
                return NotebookEnvironment(**cached)
        
        # Load from repository
        environment = await self.repository.get_environment(environment_id)
        
        if environment and self.cache_enabled:
            await self._set_cache(cache_key, environment.dict(), ttl=self.cache_ttl)
        
        return environment
    
    async def list_environments(
        self,
        tenant_id: Optional[str] = None,
        include_shared: bool = True,
        tags: Optional[List[str]] = None
    ) -> List[NotebookEnvironment]:
        """
        List available notebook environments.
        
        Args:
            tenant_id: Filter by tenant
            include_shared: Include shared environments
            tags: Filter by tags
            
        Returns:
            List of NotebookEnvironment instances
        """
        environments = await self.repository.list_environments(
            tenant_id=tenant_id,
            shared=include_shared if tenant_id else None,
            tags=tags
        )
        
        return environments
    
    async def create_session(
        self,
        environment_id: str,
        user_id: str,
        tenant_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> NotebookSession:
        """
        Create a new notebook session.
        
        Args:
            environment_id: Environment to use
            user_id: User creating the session
            tenant_id: Tenant ID
            metadata: Additional metadata
            
        Returns:
            Created NotebookSession
            
        Raises:
            ValueError: If environment not found
            SessionLimitExceeded: If tenant session limit exceeded
        """
        # Validate environment
        environment = await self.get_environment(environment_id)
        if not environment:
            raise ValueError(f"Environment {environment_id} not found")
        
        # Check tenant access
        if environment.tenant_scoped and environment.tenant_id != tenant_id:
            if not environment.shared and tenant_id not in environment.allowed_tenants:
                raise ValueError(f"Tenant {tenant_id} not allowed to use this environment")
        
        # Check session limits
        await self._check_session_limits(tenant_id)
        
        # Create session
        session = NotebookSession(
            environment_id=environment_id,
            user_id=user_id,
            tenant_id=tenant_id,
            metadata=metadata or {}
        )
        
        # Save session
        session = await self.repository.save_session(session)
        
        # Track session
        self._tenant_active_sessions[tenant_id].add(session.session_id)
        
        # Start session asynchronously
        asyncio.create_task(self._start_session(session.session_id))
        
        self.logger.info(
            f"Created notebook session",
            extra={
                "session_id": session.session_id,
                "user_id": user_id,
                "tenant_id": tenant_id,
                "environment_id": environment_id
            }
        )
        
        # Emit metric
        await self._emit_metric(
            "session_created",
            tags={"tenant_id": tenant_id}
        )
        
        return session
    
    async def _check_session_limits(self, tenant_id: str):
        """Check if tenant has reached session limits."""
        limit = self._tenant_session_limits.get(tenant_id, DEFAULT_MAX_CONCURRENT_SESSIONS)
        active_sessions = await self.repository.list_sessions(
            tenant_id=tenant_id,
            status="running"
        )
        
        if len(active_sessions) >= limit:
            raise SessionLimitExceeded(
                f"Tenant {tenant_id} has reached the limit of {limit} concurrent sessions"
            )
    
    async def _start_session(self, session_id: str):
        """Start a notebook session (simulate K8s pod creation)."""
        try:
            # Simulate startup delay
            await asyncio.sleep(5)
            
            # Get session
            session = await self.repository.get_session(session_id)
            if not session or session.status != "pending":
                return
            
            # Update to starting
            session.status = "starting"
            session.started_at = datetime.utcnow()
            await self.repository.save_session(session)
            
            # Simulate pod creation
            session.pod_name = f"notebook-{session.session_id[:8]}"
            session.jupyter_url = f"http://notebooks.local/{session.pod_name}"
            session.token = str(uuid4())
            
            # Update to running
            session.status = "running"
            session.last_activity = datetime.utcnow()
            await self.repository.save_session(session)
            
            self.logger.info(f"Session {session_id} started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start session {session_id}: {e}")
            # Update session status
            session = await self.repository.get_session(session_id)
            if session:
                session.status = "terminated"
                session.termination_reason = str(e)
                await self.repository.save_session(session)
    
    async def terminate_session(
        self,
        session_id: str,
        reason: str = "User requested"
    ) -> NotebookSession:
        """
        Terminate a notebook session.
        
        Args:
            session_id: Session to terminate
            reason: Termination reason
            
        Returns:
            Updated NotebookSession
        """
        session = await self.repository.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")
        
        if session.status == "terminated":
            return session
        
        # Update status
        session.status = "stopping"
        await self.repository.save_session(session)
        
        # Simulate pod deletion
        if session.pod_name and self.k8s_client:
            # In real impl, would delete K8s pod
            pass
        
        # Update to terminated
        session.status = "terminated"
        session.terminated_at = datetime.utcnow()
        session.termination_reason = reason
        await self.repository.save_session(session)
        
        # Clean up tracking
        self._tenant_active_sessions[session.tenant_id].discard(session_id)
        
        self.logger.info(
            f"Terminated session {session_id}: {reason}",
            extra={"session_id": session_id, "reason": reason}
        )
        
        # Emit metric
        await self._emit_metric(
            "session_terminated",
            tags={"reason": reason.replace(" ", "_").lower()}
        )
        
        return session
    
    async def get_session(self, session_id: str) -> Optional[NotebookSession]:
        """Get a notebook session by ID."""
        return await self.repository.get_session(session_id)
    
    async def list_sessions(
        self,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[NotebookSession]:
        """List notebook sessions with optional filters."""
        return await self.repository.list_sessions(
            user_id=user_id,
            tenant_id=tenant_id,
            status=status
        )
    
    async def update_session_activity(self, session_id: str):
        """Update session last activity timestamp."""
        session = await self.repository.get_session(session_id)
        if session and session.status == "running":
            session.last_activity = datetime.utcnow()
            await self.repository.save_session(session)
    
    async def set_tenant_quota(
        self,
        tenant_id: str,
        storage_quota_gb: int,
        max_concurrent_sessions: int
    ):
        """Set resource quotas for a tenant."""
        self._tenant_storage_quota_gb[tenant_id] = storage_quota_gb
        self._tenant_session_limits[tenant_id] = max_concurrent_sessions
        
        self.logger.info(
            f"Set tenant quotas",
            extra={
                "tenant_id": tenant_id,
                "storage_quota_gb": storage_quota_gb,
                "max_sessions": max_concurrent_sessions
            }
        )
    
    async def get_tenant_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get current resource usage for a tenant."""
        active_sessions = await self.repository.list_sessions(
            tenant_id=tenant_id,
            status="running"
        )
        
        return {
            "storage_used_bytes": self._tenant_storage_usage_bytes[tenant_id],
            "storage_quota_bytes": self._tenant_storage_quota_gb.get(tenant_id, DEFAULT_STORAGE_QUOTA_GB) * BYTES_PER_GB,
            "active_sessions": len(active_sessions),
            "session_limit": self._tenant_session_limits.get(tenant_id, DEFAULT_MAX_CONCURRENT_SESSIONS)
        }
    
    async def start_monitoring(self):
        """Start session monitoring background task."""
        if self._session_monitor_task and not self._session_monitor_task.done():
            return
        
        self._shutdown_event.clear()
        self._session_monitor_task = asyncio.create_task(self._monitor_sessions())
        self.logger.info("Session monitoring started")
    
    async def stop_monitoring(self):
        """Stop session monitoring background task."""
        self._shutdown_event.set()
        
        if self._session_monitor_task and not self._session_monitor_task.done():
            self._session_monitor_task.cancel()
            try:
                await self._session_monitor_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Session monitoring stopped")
    
    async def _monitor_sessions(self):
        """Monitor sessions for idle timeout and resource usage."""
        while not self._shutdown_event.is_set():
            try:
                # Get all running sessions
                sessions = await self.repository.list_sessions(status="running")
                
                for session in sessions:
                    # Check idle timeout
                    if session.last_activity:
                        idle_time = datetime.utcnow() - session.last_activity
                        environment = await self.get_environment(session.environment_id)
                        
                        if environment and idle_time > timedelta(minutes=environment.idle_timeout_minutes):
                            await self.terminate_session(
                                session.session_id,
                                f"Idle timeout ({environment.idle_timeout_minutes} minutes)"
                            )
                    
                    # Check max runtime
                    if session.started_at:
                        runtime = datetime.utcnow() - session.started_at
                        environment = await self.get_environment(session.environment_id)
                        
                        if environment and runtime > timedelta(hours=environment.max_runtime_hours):
                            await self.terminate_session(
                                session.session_id,
                                f"Max runtime exceeded ({environment.max_runtime_hours} hours)"
                            )
                
                await asyncio.sleep(self._monitor_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Session monitoring error: {e}")
                await asyncio.sleep(self._monitor_interval)
    
    async def _emit_metric(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Emit a metric (placeholder for actual implementation)."""
        # TODO: Integrate with telemetry service
        self.logger.debug(f"Metric: {metric_name}={value}, tags={tags}")
    
    async def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache (placeholder)."""
        # TODO: Integrate with cache service
        return None
    
    async def _set_cache(self, key: str, value: Dict[str, Any], ttl: int):
        """Set value in cache (placeholder)."""
        # TODO: Integrate with cache service
        pass
