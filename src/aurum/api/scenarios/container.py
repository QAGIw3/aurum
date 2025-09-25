"""Dependency injection container for scenario services.

This module provides service factory functions and dependency injection
utilities for the scenario management system.
"""

from __future__ import annotations

from typing import Type, TypeVar

from .async_service import AsyncScenarioService
from .scenario_service import InMemoryScenarioStore

T = TypeVar('T')


def get_service(service_class: Type[T]) -> T:
    """Get an instance of the specified service class.

    This is a simple factory function that creates service instances.
    In a more sophisticated system, this might use a dependency injection
    container with proper lifecycle management.

    Args:
        service_class: The service class to instantiate

    Returns:
        An instance of the service class
    """
    if service_class == AsyncScenarioService:
        # Create AsyncScenarioService instance
        # In a real implementation, this would handle dependencies
        store = InMemoryScenarioStore()
        return service_class(store=store)
    else:
        raise ValueError(f"Unknown service class: {service_class}")


__all__ = ["get_service"]
