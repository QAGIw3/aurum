"""Domain service layer package.

This package hosts application-level services that encapsulate business logic
and orchestrate repository access. Routers and CLIs should depend on these
services rather than repositories or storage clients directly.
"""

from .curves_service import CurvesService, Curve

__all__: list[str] = [
    "CurvesService",
    "Curve",
]

