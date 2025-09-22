"""Schema Registry management and compatibility enforcement."""

from __future__ import annotations

from .registry_manager import (
    SchemaCompatibilityMode,
    SchemaRegistryConfig,
    SchemaRegistryManager,
    SchemaInfo,
    SchemaRegistryError,
    SchemaRegistryConnectionError,
    SchemaCompatibilityError,
    SubjectRegistrationError,
)
from .compatibility_checker import CompatibilityChecker, CompatibilityResult
from .contracts import SubjectContracts
from .codegen import ContractModelError, generate_model, get_model, payload_from_contract

__all__ = [
    "SchemaRegistryManager",
    "SchemaCompatibilityMode",
    "SchemaRegistryConfig",
    "SchemaInfo",
    "SchemaRegistryError",
    "SchemaRegistryConnectionError",
    "SchemaCompatibilityError",
    "SubjectRegistrationError",
    "CompatibilityChecker",
    "CompatibilityResult",
    "SubjectContracts",
    "ContractModelError",
    "generate_model",
    "get_model",
    "payload_from_contract",
]
