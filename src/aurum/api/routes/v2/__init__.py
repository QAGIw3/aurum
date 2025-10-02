"""V2 API routes using modern patterns.

All v2 routes follow standardized patterns:
- Dependency injection for services
- Service layer integration
- Consistent error handling
- Optional caching support
- Standard response formats
"""

from __future__ import annotations

from typing import List

try:
    from fastapi import APIRouter
except ImportError:
    APIRouter = None  # type: ignore

# Import all v2 routers
try:
    from .curves import router as curves_router
except ImportError:
    curves_router = None

try:
    from .scenarios import router as scenarios_router
except ImportError:
    scenarios_router = None

try:
    from .metadata import router as metadata_router
except ImportError:
    metadata_router = None

try:
    from .ppa import router as ppa_router
except ImportError:
    ppa_router = None

try:
    from .iso import router as iso_router
except ImportError:
    iso_router = None


def get_v2_routers() -> List[APIRouter]:
    """Get all v2 routers for registration.
    
    Returns:
        List of v2 APIRouter instances
        
    Usage:
        ```python
        from aurum.api.routes.v2 import get_v2_routers
        
        app = FastAPI()
        for router in get_v2_routers():
            app.include_router(router)
        ```
    """
    routers = []
    
    if curves_router:
        routers.append(curves_router)
    
    if scenarios_router:
        routers.append(scenarios_router)
    
    if metadata_router:
        routers.append(metadata_router)
    
    if ppa_router:
        routers.append(ppa_router)
    
    if iso_router:
        routers.append(iso_router)
    
    return routers


__all__ = [
    "curves_router",
    "scenarios_router",
    "metadata_router",
    "ppa_router",
    "iso_router",
    "get_v2_routers",
]

