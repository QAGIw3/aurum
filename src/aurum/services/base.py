"""Base service interfaces and common functionality.

Provides foundation for all business logic services following SOLID principles.
"""

from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, Optional, TypeVar
from datetime import datetime

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class ServiceContext:
    """Context information for service operations.
    
    Provides request-scoped information like tenant, user, trace IDs, etc.
    """
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Generate IDs if not provided."""
        if not self.request_id:
            import uuid
            self.request_id = str(uuid.uuid4())


@dataclass
class ServiceResult(Generic[T]):
    """Result wrapper for service operations.
    
    Provides consistent return structure with data, metadata, and timing.
    """
    data: T
    success: bool = True
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: Optional[float] = None
    
    @classmethod
    def ok(cls, data: T, metadata: Optional[Dict[str, Any]] = None) -> ServiceResult[T]:
        """Create successful result."""
        return cls(
            data=data,
            success=True,
            metadata=metadata or {}
        )
    
    @classmethod
    def error(cls, error: str, metadata: Optional[Dict[str, Any]] = None) -> ServiceResult[None]:
        """Create error result."""
        return cls(
            data=None,  # type: ignore
            success=False,
            error=error,
            metadata=metadata or {}
        )


class ServiceError(Exception):
    """Base exception for service layer errors."""
    
    def __init__(self, message: str, code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code or "SERVICE_ERROR"
        self.details = details or {}


class ValidationError(ServiceError):
    """Validation error in service layer."""
    
    def __init__(self, message: str, field: Optional[str] = None, **kwargs):
        super().__init__(message, code="VALIDATION_ERROR", **kwargs)
        self.field = field


class NotFoundError(ServiceError):
    """Resource not found error."""
    
    def __init__(self, resource: str, identifier: str, **kwargs):
        message = f"{resource} not found: {identifier}"
        super().__init__(message, code="NOT_FOUND", **kwargs)
        self.resource = resource
        self.identifier = identifier


class BaseService(ABC):
    """Abstract base class for all services.
    
    Provides common functionality:
    - Logging
    - Error handling
    - Context management
    - Metrics collection hooks
    
    Following SOLID principles:
    - Single Responsibility: Business logic only
    - Open/Closed: Extensible via composition
    - Liskov Substitution: All services interchangeable
    - Interface Segregation: Minimal base interface
    - Dependency Inversion: Depends on repository abstractions
    """
    
    def __init__(self):
        """Initialize service with logging."""
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _log_operation(
        self,
        operation: str,
        context: Optional[ServiceContext] = None,
        **kwargs
    ) -> None:
        """Log service operation with context."""
        log_data = {
            "operation": operation,
            "service": self.__class__.__name__,
            **kwargs
        }
        
        if context:
            log_data.update({
                "tenant_id": context.tenant_id,
                "user_id": context.user_id,
                "request_id": context.request_id,
            })
        
        self.logger.info("service_operation", extra=log_data)
    
    def _handle_error(
        self,
        error: Exception,
        operation: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceError:
        """Convert exceptions to service errors."""
        self.logger.error(
            f"Service error in {operation}",
            exc_info=True,
            extra={
                "operation": operation,
                "service": self.__class__.__name__,
                "error_type": type(error).__name__,
                "tenant_id": context.tenant_id if context else None,
            }
        )
        
        # Convert known errors
        if isinstance(error, ServiceError):
            return error
        
        # Wrap unknown errors
        return ServiceError(
            message=str(error),
            code="INTERNAL_ERROR",
            details={"original_error": type(error).__name__}
        )

