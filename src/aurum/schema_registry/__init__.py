"""Schema Registry management and compatibility enforcement."""

from __future__ import annotations

from .registry_manager import SchemaRegistryManager, SchemaCompatibilityMode
from .schema_evolution import SchemaEvolutionManager, EvolutionRule
from .subject_manager import SubjectManager, SubjectRegistrationError
from .compatibility_checker import CompatibilityChecker, CompatibilityResult

__all__ = [
    "SchemaRegistryManager",
    "SchemaCompatibilityMode",
    "SchemaEvolutionManager",
    "EvolutionRule",
    "SubjectManager",
    "SubjectRegistrationError",
    "CompatibilityChecker",
    "CompatibilityResult"
]
