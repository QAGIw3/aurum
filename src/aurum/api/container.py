"""Lightweight service container stubs for the Aurum API."""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Type, TypeVar

T = TypeVar("T")


class SingletonServiceProvider:
    """Very small registry that stores singleton instances."""

    def __init__(self) -> None:
        self._instances: Dict[Type[Any], Any] = {}
        self._factories: Dict[Type[Any], Callable[[], Any]] = {}

    def register_singleton(self, service_type: Type[T], instance: T) -> None:
        self._instances[service_type] = instance

    def register_factory(self, service_type: Type[T], factory: Callable[[], T]) -> None:
        self._factories[service_type] = factory

    def get(self, service_type: Type[T]) -> T:
        if service_type in self._instances:
            return self._instances[service_type]
        if service_type in self._factories:
            instance = self._factories[service_type]()
            self._instances[service_type] = instance
            return instance
        raise KeyError(f"No service registered for {service_type!r}")


_service_provider = SingletonServiceProvider()


def get_service_provider() -> SingletonServiceProvider:
    """Return the global service provider."""

    return _service_provider


def configure_services() -> None:  # pragma: no cover - placeholder
    """Placeholder configuration hook."""

    return None


def get_service(service_type: Type[T]) -> T:
    """Resolve a singleton service by type."""

    return _service_provider.get(service_type)


__all__ = [
    "SingletonServiceProvider",
    "get_service_provider",
    "configure_services",
    "get_service",
]
