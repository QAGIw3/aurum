"""API response schemas and contracts."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from libs.core import AurumBaseModel, CurveKey


class PriceObservationResponse(AurumBaseModel):
    """Price observation API response schema."""
    
    curve: CurveKey
    interval_start: datetime
    interval_end: Optional[datetime] = None
    delivery_date: date
    price: Optional[float] = None
    currency: str = "USD"
    unit: str = "MWh"


class PaginationResponse(AurumBaseModel):
    """Pagination metadata for API responses."""
    
    total: Optional[int] = None
    limit: int
    offset: int
    count: int


class CurveListResponse(AurumBaseModel):
    """Curve list API response."""
    
    curves: List[CurveKey]
    pagination: PaginationResponse


class ObservationListResponse(AurumBaseModel):
    """Observation list API response."""
    
    observations: List[PriceObservationResponse]
    pagination: PaginationResponse


class ScenarioResponse(AurumBaseModel):
    """Scenario API response schema."""
    
    id: str
    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    status: str = "draft"
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    version: int = 1


class ScenarioListResponse(AurumBaseModel):
    """Scenario list API response."""
    
    scenarios: List[ScenarioResponse]
    pagination: PaginationResponse


class ErrorResponse(AurumBaseModel):
    """Standard error response schema."""
    
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HealthResponse(AurumBaseModel):
    """Health check response schema."""
    
    status: str  # "healthy", "degraded", "unhealthy"
    version: Optional[str] = None
    components: Optional[Dict[str, Dict[str, Any]]] = None