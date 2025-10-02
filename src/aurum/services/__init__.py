"""Business logic services for the Aurum platform.

Services orchestrate business logic and coordinate multiple repositories
and external systems. They sit above the data layer and below the API layer.

Architecture:
- Services implement business logic
- Services depend on repositories (not DAOs directly)
- Services are injected into API routes via dependency injection

Organization:
- core/ - Core domain services (curves, scenarios, metadata)
- external/ - External data services (EIA, NOAA, etc.)
- ml/ - Machine learning and analytics services
- platform/ - Platform services (governance, monitoring, etc.)

Following the Service Layer pattern from Domain-Driven Design.
"""

from .base import (
    BaseService,
    ServiceContext,
    ServiceResult,
    ServiceError,
    ValidationError,
    NotFoundError,
)

__all__ = [
    "BaseService",
    "ServiceContext",
    "ServiceResult",
    "ServiceError",
    "ValidationError",
    "NotFoundError",
]

