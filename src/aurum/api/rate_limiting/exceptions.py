from __future__ import annotations

"""Exceptions for rate limiting functionality."""

from typing import Optional


class ServiceUnavailableException(Exception):
    """Exception raised when a service is unavailable due to rate limiting."""

    def __init__(self, service_name: str, retry_after: Optional[int] = None):
        self.service_name = service_name
        self.retry_after = retry_after
        super().__init__(f"Service {service_name} is unavailable")


class RateLimitExceededException(Exception):
    """Exception raised when rate limit is exceeded."""

    def __init__(self, limit_name: str, retry_after: Optional[int] = None):
        self.limit_name = limit_name
        self.retry_after = retry_after
        super().__init__(f"Rate limit {limit_name} exceeded")


class QuotaExceededException(Exception):
    """Exception raised when quota is exceeded."""

    def __init__(self, quota_name: str, reset_time: Optional[str] = None):
        self.quota_name = quota_name
        self.reset_time = reset_time
        super().__init__(f"Quota {quota_name} exceeded")
