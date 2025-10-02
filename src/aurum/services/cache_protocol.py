"""Shared cache protocol for all services.

Provides a common protocol interface for cache implementations,
allowing services to work with any cache backend (Redis, Memcached, in-memory, etc.).
"""

from __future__ import annotations

from typing import Any, Optional, Protocol


class CacheProtocol(Protocol):
    """Protocol defining the cache interface for services.
    
    All cache implementations (Redis, Memcached, in-memory, etc.) should
    implement this protocol to be usable by services.
    
    This follows the Dependency Inversion Principle - services depend on
    this abstraction rather than concrete cache implementations.
    
    Example implementations:
    - RedisCache
    - MemcachedCache
    - InMemoryCache
    - MultiTierCache
    """
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found or expired
        """
        ...
    
    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value in cache with TTL.
        
        Args:
            key: Cache key
            value: Value to cache (should be serializable)
            ttl: Time-to-live in seconds
        """
        ...
    
    async def delete(self, key: str) -> None:
        """Delete value from cache.
        
        Args:
            key: Cache key
        """
        ...
    
    async def exists(self, key: str) -> bool:
        """Check if key exists in cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key exists, False otherwise
        """
        ...
    
    async def clear(self, pattern: Optional[str] = None) -> int:
        """Clear cache entries matching pattern.
        
        Args:
            pattern: Optional pattern to match keys (e.g., "user:*")
                    If None, clears all entries
            
        Returns:
            Number of entries cleared
        """
        ...


__all__ = ["CacheProtocol"]

