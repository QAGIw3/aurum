"""Domain-specific exceptions for model management services."""

from __future__ import annotations


class AurumModelException(Exception):
    """Base exception for model management domain errors."""

    def __init__(self, message: str, model_name: str = None, version: str = None):
        super().__init__(message)
        self.model_name = model_name
        self.version = version


class ModelNotFoundException(AurumModelException):
    """Raised when a requested model is not found."""

    def __init__(self, model_name: str):
        super().__init__(f"Model '{model_name}' not found", model_name=model_name)


class ModelVersionNotFoundException(AurumModelException):
    """Raised when a requested model version is not found."""

    def __init__(self, model_name: str, version: str):
        super().__init__(f"Model version '{version}' not found for model '{model_name}'",
                        model_name=model_name, version=version)


class TrainingJobNotFoundException(AurumModelException):
    """Raised when a requested training job is not found."""

    def __init__(self, job_id: str):
        super().__init__(f"Training job '{job_id}' not found")


class TrainingJobAlreadyExistsException(AurumModelException):
    """Raised when attempting to start a training job that already exists."""

    def __init__(self, model_name: str):
        super().__init__(f"Training job already exists for model '{model_name}'", model_name=model_name)


class ModelValidationException(AurumModelException):
    """Raised when model validation fails."""

    def __init__(self, message: str, model_name: str = None, validation_errors: dict = None):
        super().__init__(message, model_name=model_name)
        self.validation_errors = validation_errors or {}


class ScheduleNotFoundException(AurumModelException):
    """Raised when a requested schedule is not found."""

    def __init__(self, schedule_id: str):
        super().__init__(f"Schedule '{schedule_id}' not found")


class InvalidModelConfigurationException(AurumModelException):
    """Raised when model configuration is invalid."""

    def __init__(self, message: str, model_name: str = None, config_errors: dict = None):
        super().__init__(message, model_name=model_name)
        self.config_errors = config_errors or {}


class ModelComparisonException(AurumModelException):
    """Raised when model comparison fails."""

    def __init__(self, message: str, model_name: str = None):
        super().__init__(message, model_name=model_name)


class ServiceUnavailableException(AurumModelException):
    """Raised when a dependent service is unavailable."""

    def __init__(self, service_name: str, reason: str = None):
        message = f"Service '{service_name}' is unavailable"
        if reason:
            message += f": {reason}"
        super().__init__(message)
        self.service_name = service_name
