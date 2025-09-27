"""Domain service façade modules.

These modules provide a stable, typed surface for routers while the legacy
`aurum.api.service` is gradually decomposed. Import services from here instead
of the legacy module to ease future refactors.

Phase 1.3 Service Layer Decomposition:
- Each domain has dedicated service classes
- Services implement standard interfaces for consistency  
- DAO pattern separates data access from business logic
- >85% test coverage requirement for service layer
"""

from .base_service import (
    ServiceInterface,
    QueryableServiceInterface, 
    DimensionalServiceInterface,
    ExportableServiceInterface,
)
from .curves_service import CurvesService
from .metadata_service import MetadataService
from .ppa_service import PpaService
from .drought_service import DroughtService
from .iso_service import IsoService
from .eia_service import EiaService
from .scenario_service import ScenarioService

__all__ = [
    # Base interfaces
    "ServiceInterface",
    "QueryableServiceInterface",
    "DimensionalServiceInterface", 
    "ExportableServiceInterface",
    # Domain services
    "CurvesService",
    "MetadataService",
    "PpaService",
    "DroughtService",
    "IsoService",
    "EiaService",
    "ScenarioService",
]
