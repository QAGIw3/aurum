"""Experimental DAO implementations.

This subpackage houses the newer, generic DAO abstractions and DAOs that were
previously located under `aurum.api.daos`. Keeping them under
`aurum.api.dao.experimental` avoids naming collisions with legacy/current DAO
implementations in `aurum.api.dao` while providing a clear migration path.

Public classes are re-exported here for convenience.
"""

# Re-export experimental/base DAOs
from .base_dao import BaseDAO, TrinoDAO, CacheDAO as BaseCacheDAO  # type: ignore F401

# Re-export concrete DAOs
from .curves_dao import CurvesDAO, CurveFilter  # type: ignore F401
from .scenarios_dao import (
    ScenariosDAO,
    ScenarioFilter,
    ScenarioRunFilter,
)  # type: ignore F401
from .cache_dao import CacheDAO  # type: ignore F401

__all__ = [
    # base abstractions
    "BaseDAO",
    "TrinoDAO",
    "BaseCacheDAO",
    # concrete impls
    "CurvesDAO",
    "CurveFilter",
    "ScenariosDAO",
    "ScenarioFilter",
    "ScenarioRunFilter",
    "CacheDAO",
]
