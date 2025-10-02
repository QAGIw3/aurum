"""Factory for creating and coordinating model management services."""

from __future__ import annotations

import logging
from typing import Dict, Optional

from .interfaces import (
    IModelManagementService,
    IModelTrainingService,
    IModelComparisonService,
    IModelSchedulingService
)
from .management_service import ModelManagementService, DefaultAuditLogger
from .training_service import ModelTrainingService
from .comparison_service import ModelComparisonService
from .scheduling_service import ModelSchedulingService
from .interfaces import IAuditLogger, ITelemetryProvider


class ModelServiceFactory:
    """Factory for creating and coordinating model services."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.services: Dict[str, object] = {}

    def create_management_service(
        self,
        audit_logger: Optional[IAuditLogger] = None,
        telemetry_provider: Optional[ITelemetryProvider] = None
    ) -> IModelManagementService:
        """Create the model management service with dependency injection."""
        if "management" not in self.services:
            # Create default implementations if not provided
            if audit_logger is None:
                audit_logger = DefaultAuditLogger()

            self.services["management"] = ModelManagementService(
                audit_logger=audit_logger,
                telemetry_provider=telemetry_provider
            )
            self.logger.info("Created ModelManagementService with dependency injection")

        return self.services["management"]

    def create_training_service(self) -> IModelTrainingService:
        """Create the model training service."""
        if "training" not in self.services:
            management_service = self.create_management_service()
            self.services["training"] = ModelTrainingService(management_service)
            self.logger.info("Created ModelTrainingService")

        return self.services["training"]

    def create_comparison_service(self) -> IModelComparisonService:
        """Create the model comparison service."""
        if "comparison" not in self.services:
            management_service = self.create_management_service()
            self.services["comparison"] = ModelComparisonService(management_service)
            self.logger.info("Created ModelComparisonService")

        return self.services["comparison"]

    def create_scheduling_service(self) -> IModelSchedulingService:
        """Create the model scheduling service."""
        if "scheduling" not in self.services:
            training_service = self.create_training_service()
            scheduling_service = ModelSchedulingService(training_service)

            # Start the scheduler automatically
            import asyncio
            asyncio.create_task(scheduling_service.start())

            self.services["scheduling"] = scheduling_service
            self.logger.info("Created ModelSchedulingService")

        return self.services["scheduling"]

    def get_all_services(self) -> Dict[str, object]:
        """Get all created services."""
        # Ensure all services are created
        self.create_management_service()
        self.create_training_service()
        self.create_comparison_service()
        self.create_scheduling_service()

        return dict(self.services)

    def stop_all_services(self) -> None:
        """Stop all services."""
        if "scheduling" in self.services:
            import asyncio
            asyncio.create_task(self.services["scheduling"].stop())

        self.logger.info("Stopped all model services")


# Global factory instance
_model_service_factory: Optional[ModelServiceFactory] = None


def get_model_service_factory() -> ModelServiceFactory:
    """Get the global model service factory."""
    global _model_service_factory
    if _model_service_factory is None:
        _model_service_factory = ModelServiceFactory()
    return _model_service_factory


def get_model_management_service(
    audit_logger: Optional[IAuditLogger] = None,
    telemetry_provider: Optional[ITelemetryProvider] = None
) -> IModelManagementService:
    """Get the model management service with optional dependency injection."""
    return get_model_service_factory().create_management_service(
        audit_logger=audit_logger,
        telemetry_provider=telemetry_provider
    )


def get_model_training_service() -> IModelTrainingService:
    """Get the model training service."""
    return get_model_service_factory().create_training_service()


def get_model_comparison_service() -> IModelComparisonService:
    """Get the model comparison service."""
    return get_model_service_factory().create_comparison_service()


def get_model_scheduling_service() -> IModelSchedulingService:
    """Get the model scheduling service."""
    return get_model_service_factory().create_scheduling_service()
