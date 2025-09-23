"""Idempotency key management for reliable request deduplication across restarts."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field

from ..core.models import AurumBaseModel
from ..telemetry.context import get_request_id
from ..observability.metrics import get_metrics_client


class IdempotencyKey(AurumBaseModel):
    """Model for idempotency key tracking."""
    key: str = Field(..., description="Idempotency key")
    request_id: str = Field(..., description="Request ID")
    operation: str = Field(..., description="Operation being performed")
    payload_hash: str = Field(..., description="Hash of request payload")
    result: Optional[Dict[str, Any]] = Field(None, description="Cached result")
    status: str = Field(default="processing", description="Status: processing, completed, failed")
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    expires_at: float = Field(..., description="TTL timestamp")


class IdempotencyConfig(AurumBaseModel):
    """Configuration for idempotency system."""
    enabled: bool = Field(default=True)
    default_ttl_seconds: int = Field(default=300, description="Default TTL for idempotency keys")
    max_ttl_seconds: int = Field(default=3600, description="Maximum TTL for idempotency keys")
    cleanup_interval_seconds: int = Field(default=60, description="Cleanup interval")
    max_payload_size_bytes: int = Field(default=1048576, description="Max payload size for hashing")
    storage_backend: str = Field(default="redis", description="Storage backend: redis, postgres")


class IdempotencyManager:
    """Manages idempotency keys with persistence across service restarts."""

    def __init__(self, config: IdempotencyConfig):
        self.config = config
        self.metrics = get_metrics_client()
        self._redis_client = None
        self._postgres_client = None
        self._cleanup_task: Optional[asyncio.Task] = None

    async def initialize(self, redis_client=None, postgres_client=None):
        """Initialize the idempotency manager with storage clients."""
        self._redis_client = redis_client
        self._postgres_client = postgres_client

        if self.config.enabled:
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_keys())

    async def close(self):
        """Close the idempotency manager and cleanup resources."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    @asynccontextmanager
    async def idempotent_operation(
        self,
        idempotency_key: str,
        operation: str,
        payload: Dict[str, Any],
        ttl_seconds: Optional[int] = None
    ):
        """Context manager for idempotent operations."""
        # Generate payload hash
        payload_hash = self._hash_payload(payload)

        # Check if key exists
        existing_result = await self._get_cached_result(
            idempotency_key, operation, payload_hash
        )

        if existing_result:
            self.metrics.increment_counter("idempotency_cache_hit")
            yield existing_result
            return

        # Mark as processing
        await self._set_processing(idempotency_key, operation, payload_hash, ttl_seconds)

        try:
            self.metrics.increment_counter("idempotency_cache_miss")
            yield None
        except Exception as e:
            # Mark as failed
            await self._set_failed(idempotency_key, operation, payload_hash, str(e))
            raise
        else:
            # Operation completed successfully, caller should set result
            pass

    async def set_result(
        self,
        idempotency_key: str,
        operation: str,
        payload_hash: str,
        result: Dict[str, Any]
    ):
        """Set the result for a completed idempotent operation."""
        key_data = IdempotencyKey(
            key=idempotency_key,
            request_id=get_request_id(),
            operation=operation,
            payload_hash=payload_hash,
            result=result,
            status="completed",
            expires_at=time.time() + self.config.default_ttl_seconds
        )

        await self._store_key_data(key_data)
        self.metrics.increment_counter("idempotency_result_cached")

    async def get_result(self, idempotency_key: str, operation: str) -> Optional[Dict[str, Any]]:
        """Get cached result for an idempotency key."""
        # Try to get from cache/storage
        key_data = await self._get_key_data(idempotency_key, operation)
        if key_data and key_data.status == "completed" and key_data.result:
            return key_data.result
        return None

    async def is_processing(self, idempotency_key: str, operation: str) -> bool:
        """Check if an operation is currently being processed."""
        key_data = await self._get_key_data(idempotency_key, operation)
        return key_data is not None and key_data.status == "processing"

    async def _get_cached_result(
        self,
        idempotency_key: str,
        operation: str,
        payload_hash: str
    ) -> Optional[Dict[str, Any]]:
        """Get cached result if it matches the payload hash."""
        key_data = await self._get_key_data(idempotency_key, operation)
        if (key_data and
            key_data.status == "completed" and
            key_data.result and
            key_data.payload_hash == payload_hash):
            return key_data.result
        return None

    async def _set_processing(
        self,
        idempotency_key: str,
        operation: str,
        payload_hash: str,
        ttl_seconds: Optional[int]
    ):
        """Mark an operation as processing."""
        ttl = min(
            ttl_seconds or self.config.default_ttl_seconds,
            self.config.max_ttl_seconds
        )

        key_data = IdempotencyKey(
            key=idempotency_key,
            request_id=get_request_id(),
            operation=operation,
            payload_hash=payload_hash,
            status="processing",
            expires_at=time.time() + ttl
        )

        await self._store_key_data(key_data)

    async def _set_failed(
        self,
        idempotency_key: str,
        operation: str,
        payload_hash: str,
        error: str
    ):
        """Mark an operation as failed."""
        key_data = IdempotencyKey(
            key=idempotency_key,
            request_id=get_request_id(),
            operation=operation,
            payload_hash=payload_hash,
            status="failed",
            expires_at=time.time() + self.config.default_ttl_seconds
        )

        await self._store_key_data(key_data)

    def _hash_payload(self, payload: Dict[str, Any]) -> str:
        """Generate hash of request payload for comparison."""
        import hashlib

        # Limit payload size for hashing
        payload_str = json.dumps(payload, sort_keys=True)
        if len(payload_str) > self.config.max_payload_size_bytes:
            payload_str = payload_str[:self.config.max_payload_size_bytes]

        return hashlib.sha256(payload_str.encode()).hexdigest()

    async def _store_key_data(self, key_data: IdempotencyKey):
        """Store key data in the configured storage backend."""
        if not self.config.enabled:
            return

        key_data.updated_at = time.time()

        if self._redis_client:
            await self._store_redis(key_data)
        elif self._postgres_client:
            await self._store_postgres(key_data)
        else:
            # Fallback to in-memory storage for testing
            await self._store_memory(key_data)

    async def _store_redis(self, key_data: IdempotencyKey):
        """Store key data in Redis."""
        redis_key = f"idempotency:{key_data.key}:{key_data.operation}"

        data = key_data.model_dump()
        ttl = int(key_data.expires_at - time.time())

        await self._redis_client.setex(
            redis_key,
            ttl,
            json.dumps(data, default=str)
        )

    async def _store_postgres(self, key_data: IdempotencyKey):
        """Store key data in PostgreSQL."""
        # This would use the actual database client
        # For now, implement as a stub
        pass

    async def _store_memory(self, key_data: IdempotencyKey):
        """Store key data in memory (for testing)."""
        # Implementation would go here
        pass

    async def _get_key_data(self, idempotency_key: str, operation: str) -> Optional[IdempotencyKey]:
        """Retrieve key data from storage."""
        if not self.config.enabled:
            return None

        if self._redis_client:
            return await self._get_redis(idempotency_key, operation)
        elif self._postgres_client:
            return await self._get_postgres(idempotency_key, operation)
        else:
            return await self._get_memory(idempotency_key, operation)

    async def _get_redis(self, idempotency_key: str, operation: str) -> Optional[IdempotencyKey]:
        """Retrieve key data from Redis."""
        redis_key = f"idempotency:{idempotency_key}:{operation}"
        data = await self._redis_client.get(redis_key)

        if data:
            try:
                data_dict = json.loads(data)
                return IdempotencyKey(**data_dict)
            except (json.JSONDecodeError, ValueError):
                # Clean up corrupted data
                await self._redis_client.delete(redis_key)
                return None

        return None

    async def _get_postgres(self, idempotency_key: str, operation: str) -> Optional[IdempotencyKey]:
        """Retrieve key data from PostgreSQL."""
        # Implementation would go here
        return None

    async def _get_memory(self, idempotency_key: str, operation: str) -> Optional[IdempotencyKey]:
        """Retrieve key data from memory."""
        return None

    async def _cleanup_expired_keys(self):
        """Background task to clean up expired keys."""
        while True:
            try:
                await asyncio.sleep(self.config.cleanup_interval_seconds)

                if self._redis_client:
                    await self._cleanup_redis()
                elif self._postgres_client:
                    await self._cleanup_postgres()

                self.metrics.gauge("idempotency_cleanup_runs", 1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.metrics.increment_counter("idempotency_cleanup_errors")
                # Continue running despite errors

    async def _cleanup_redis(self):
        """Clean up expired keys in Redis."""
        # This would scan for and delete expired keys
        # Implementation depends on Redis scanning capabilities
        pass

    async def _cleanup_postgres(self):
        """Clean up expired keys in PostgreSQL."""
        # Implementation would go here
        pass

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the idempotency system."""
        return {
            "enabled": self.config.enabled,
            "storage_backend": self.config.storage_backend,
            "default_ttl_seconds": self.config.default_ttl_seconds,
            "cleanup_interval_seconds": self.config.cleanup_interval_seconds,
        }


# Global idempotency manager instance
_idempotency_manager: Optional[IdempotencyManager] = None


def get_idempotency_manager() -> IdempotencyManager:
    """Get the global idempotency manager instance."""
    if _idempotency_manager is None:
        raise RuntimeError("Idempotency manager not initialized")
    return _idempotency_manager


def initialize_idempotency_manager(
    config: IdempotencyConfig,
    redis_client=None,
    postgres_client=None
) -> IdempotencyManager:
    """Initialize the global idempotency manager."""
    global _idempotency_manager
    _idempotency_manager = IdempotencyManager(config)
    asyncio.create_task(_idempotency_manager.initialize(redis_client, postgres_client))
    return _idempotency_manager


def idempotency_key_required(func):
    """Decorator to require idempotency key for API endpoints."""
    import functools
    import inspect

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Check if idempotency key is in headers
        if 'request' in kwargs:
            request = kwargs['request']
            idempotency_key = request.headers.get('Idempotency-Key')

            if not idempotency_key:
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=400,
                    detail="Idempotency-Key header is required for this operation"
                )

            # Store idempotency key in request state for use in the handler
            request.state.idempotency_key = idempotency_key

        return await func(*args, **kwargs)

    return wrapper


def idempotency_context(idempotency_key: str, operation: str):
    """Context manager for idempotent operations."""
    import contextlib

    @contextlib.asynccontextmanager
    async def context_manager(payload: Dict[str, Any], ttl_seconds: Optional[int] = None):
        manager = get_idempotency_manager()

        async with manager.idempotent_operation(
            idempotency_key, operation, payload, ttl_seconds
        ) as result:
            if result:
                yield result
            else:
                yield None

    return context_manager


__all__ = [
    "IdempotencyKey",
    "IdempotencyConfig",
    "IdempotencyManager",
    "get_idempotency_manager",
    "initialize_idempotency_manager",
    "idempotency_key_required",
    "idempotency_context",
]
