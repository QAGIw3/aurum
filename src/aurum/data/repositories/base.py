"""Base repository interface.

Provides common patterns for all repositories.
"""

from __future__ import annotations

from abc import ABC
from typing import Generic, TypeVar, Optional

from aurum.core import AurumSettings

T = TypeVar('T')


class BaseRepository(ABC, Generic[T]):
    """Base repository providing common functionality.
    
    Repositories:
    - Implement domain logic
    - Coordinate multiple DAOs
    - Abstract database details from services
    - Provide domain-specific query methods
    
    Following Repository Pattern from DDD.
    """
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize repository with settings.
        
        Args:
            settings: Application settings. If None, loads from environment.
        """
        self.settings = settings or self._load_settings()
    
    def _load_settings(self) -> AurumSettings:
        """Load settings from environment."""
        from aurum.core.settings import get_settings
        return get_settings()

