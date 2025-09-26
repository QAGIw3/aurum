"""Enhanced input validation and sanitization for security hardening."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, validator
from pydantic.types import ConstrainedFloat, ConstrainedInt, ConstrainedStr


# Custom constrained types for strict validation
class SafeString(ConstrainedStr):
    """String type that disallows potentially dangerous characters."""
    min_length = 0
    max_length = 10000
    regex = re.compile(r'^[a-zA-Z0-9\s\-_\.\(\)\[\]{}:;,@#$%&*!+=\[\]\'"\/\\|`~]*$')

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')
        if len(v) < cls.min_length or len(v) > cls.max_length:
            raise ValueError(f'String length must be between {cls.min_length} and {cls.max_length}')
        if not cls.regex.match(v):
            raise ValueError('String contains unsafe characters')
        return cls(v)


class SafeIdentifier(ConstrainedStr):
    """Safe identifier type for API paths and resource names."""
    min_length = 1
    max_length = 100
    regex = re.compile(r'^[a-zA-Z0-9\-_]+$')

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')
        if len(v) < cls.min_length or len(v) > cls.max_length:
            raise ValueError(f'Identifier length must be between {cls.min_length} and {cls.max_length}')
        if not cls.regex.match(v):
            raise ValueError('Identifier contains invalid characters')
        return cls(v)


class SafePath(ConstrainedStr):
    """Safe path type for file paths and API paths."""
    min_length = 1
    max_length = 1000
    regex = re.compile(r'^[a-zA-Z0-9\s\-_/\.\(\)\[\]{}:;,@#$%&*!+=\[\]\'"\\|`~]*$')

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')
        if len(v) < cls.min_length or len(v) > cls.max_length:
            raise ValueError(f'Path length must be between {cls.min_length} and {cls.max_length}')
        if not cls.regex.match(v):
            raise ValueError('Path contains unsafe characters')
        # Prevent directory traversal
        if '..' in v or v.startswith('/') and len(v) > 1:
            raise ValueError('Path contains directory traversal')
        return cls(v)


class SafeURL(ConstrainedStr):
    """Safe URL type that validates URL structure."""
    max_length = 2000

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')
        if len(v) > cls.max_length:
            raise ValueError(f'URL too long (max {cls.max_length} characters)')
        try:
            parsed = urlparse(v)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError('Invalid URL format')
            if parsed.scheme not in ['http', 'https']:
                raise ValueError('Only HTTP/HTTPS URLs allowed')
        except Exception:
            raise ValueError('Invalid URL format')
        return cls(v)


class PositiveInt(ConstrainedInt):
    """Positive integer type."""
    ge = 1


class NonNegativeInt(ConstrainedInt):
    """Non-negative integer type."""
    ge = 0


class PositiveFloat(ConstrainedFloat):
    """Positive float type."""
    gt = 0.0


class PercentageFloat(ConstrainedFloat):
    """Percentage float type (0-100)."""
    ge = 0.0
    le = 100.0


# Base validation models
class BaseRequestModel(BaseModel):
    """Base model for all request models with security enhancements."""

    class Config:
        # Enable strict validation
        validate_assignment = True
        use_enum_values = True
        # Disallow extra fields
        extra = "forbid"
        # Validate all fields by default
        validate_all = True


class PaginationRequest(BaseRequestModel):
    """Standard pagination request model."""
    limit: int = Field(10, ge=1, le=1000, description="Maximum number of items to return")
    offset: Optional[int] = Field(None, ge=0, description="Offset for pagination")
    cursor: Optional[str] = Field(None, description="Cursor for cursor-based pagination")


class TenantRequest(BaseRequestModel):
    """Base model for tenant-scoped requests."""
    tenant_id: SafeIdentifier = Field(..., description="Tenant identifier")


class UserRequest(BaseRequestModel):
    """Base model for user-scoped requests."""
    user_id: SafeIdentifier = Field(..., description="User identifier")


# Security validators
def validate_no_sql_injection(value: str, field_name: str = "field") -> str:
    """Validate that string doesn't contain SQL injection patterns."""
    dangerous_patterns = [
        r';\s*--',
        r';\s*/\*',
        r'union\s+select',
        r'select\s+.*\s+from',
        r'insert\s+into',
        r'update\s+.*\s+set',
        r'delete\s+from',
        r'drop\s+table',
        r'alter\s+table',
        r'exec\s*\(',
        r'sp_executesql',
        r'xp_cmdshell',
    ]

    value_lower = value.lower()
    for pattern in dangerous_patterns:
        if re.search(pattern, value_lower, re.IGNORECASE):
            raise ValueError(f"{field_name} contains potentially dangerous SQL pattern")

    return value


def validate_no_xss(value: str, field_name: str = "field") -> str:
    """Validate that string doesn't contain XSS patterns."""
    dangerous_patterns = [
        r'<script[^>]*>.*?</script>',
        r'javascript:',
        r'on\w+\s*=',
        r'<iframe[^>]*>.*?</iframe>',
        r'<object[^>]*>.*?</object>',
        r'<embed[^>]*>.*?</embed>',
        r'<form[^>]*>.*?</form>',
    ]

    value_lower = value.lower()
    for pattern in dangerous_patterns:
        if re.search(pattern, value_lower, re.IGNORECASE):
            raise ValueError(f"{field_name} contains potentially dangerous XSS pattern")

    return value


def validate_no_path_traversal(value: str, field_name: str = "field") -> str:
    """Validate that path doesn't contain traversal attempts."""
    if '..' in value or value.startswith('/'):
        raise ValueError(f"{field_name} contains path traversal attempt")
    return value


def validate_email_format(value: str) -> str:
    """Validate email format."""
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', value):
        raise ValueError("Invalid email format")
    return value


def validate_phone_format(value: str) -> str:
    """Validate phone number format."""
    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', value)
    if len(digits_only) < 10 or len(digits_only) > 15:
        raise ValueError("Phone number must be between 10 and 15 digits")
    return value


def sanitize_string(value: str) -> str:
    """Sanitize string by removing potentially dangerous characters."""
    if not isinstance(value, str):
        return value

    # Remove null bytes and other control characters
    value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)

    # Remove potential HTML/script tags
    value = re.sub(r'<[^>]*>', '', value)

    # Trim whitespace
    value = value.strip()

    return value


# Enhanced field validators for Pydantic models
def create_safe_string_validator(field_name: str = "field"):
    """Create a validator function for safe strings."""
    def validate_safe_string(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')

        # Sanitize first
        v = sanitize_string(v)

        # Then validate for SQL injection
        validate_no_sql_injection(v, field_name)

        # Then validate for XSS
        validate_no_xss(v, field_name)

        return v
    return validate_safe_string


def create_path_validator(field_name: str = "field"):
    """Create a validator function for safe paths."""
    def validate_path(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')

        v = sanitize_string(v)
        validate_no_path_traversal(v, field_name)
        validate_no_sql_injection(v, field_name)

        return v
    return validate_path


# Security middleware models
class SecurityHeaders(BaseRequestModel):
    """Security headers validation model."""
    user_agent: Optional[str] = Field(None, description="User agent string")
    referer: Optional[SafeURL] = Field(None, description="Referer URL")
    origin: Optional[SafeURL] = Field(None, description="Origin URL")
    x_forwarded_for: Optional[str] = Field(None, description="Forwarded for header")
    x_real_ip: Optional[str] = Field(None, description="Real IP header")

    @validator('x_forwarded_for', 'x_real_ip')
    def validate_ip_headers(cls, v):
        if v and not re.match(r'^(\d{1,3}\.){3}\d{1,3}(,\s*(\d{1,3}\.){3}\d{1,3})*$', v):
            raise ValueError('Invalid IP address format')
        return v


# Rate limiting models
class RateLimitRequest(BaseRequestModel):
    """Rate limit configuration request."""
    tenant_id: SafeIdentifier = Field(..., description="Tenant identifier")
    endpoint_pattern: SafeString = Field(..., description="Endpoint pattern to rate limit")
    requests_per_minute: PositiveInt = Field(..., description="Requests per minute limit")
    burst_limit: PositiveInt = Field(..., description="Burst request limit")


class SecurityAuditEvent(BaseRequestModel):
    """Security audit event model."""
    event_type: str = Field(..., description="Type of security event")
    user_id: Optional[SafeIdentifier] = Field(None, description="User identifier")
    tenant_id: Optional[SafeIdentifier] = Field(None, description="Tenant identifier")
    resource: SafeString = Field(..., description="Resource accessed")
    action: str = Field(..., description="Action performed")
    ip_address: str = Field(..., description="Client IP address")
    user_agent: Optional[str] = Field(None, description="User agent")
    timestamp: float = Field(..., description="Event timestamp")
    details: Dict[str, Any] = Field(default_factory=dict, description="Event details")

    @validator('ip_address')
    def validate_ip(cls, v):
        if not re.match(r'^(\d{1,3}\.){3}\d{1,3}$', v):
            raise ValueError('Invalid IP address format')
        return v

    @validator('event_type', 'action')
    def validate_safe_strings(cls, v):
        return sanitize_string(v)


# Input sanitization utilities
def sanitize_request_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize all string values in request data."""
    sanitized = {}
    for key, value in data.items():
        if isinstance(value, str):
            sanitized[key] = sanitize_string(value)
        elif isinstance(value, dict):
            sanitized[key] = sanitize_request_data(value)
        elif isinstance(value, list):
            sanitized[key] = [sanitize_string(item) if isinstance(item, str) else item for item in value]
        else:
            sanitized[key] = value
    return sanitized


def validate_and_sanitize_query_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and sanitize query parameters."""
    # Check for dangerous query parameters
    dangerous_params = ['sql', 'query', 'exec', 'eval', 'include', 'require']
    for param in dangerous_params:
        if param in params:
            raise ValueError(f"Query parameter '{param}' is not allowed")

    return sanitize_request_data(params)


# Export commonly used models
__all__ = [
    "BaseRequestModel",
    "PaginationRequest",
    "TenantRequest",
    "UserRequest",
    "SafeString",
    "SafeIdentifier",
    "SafePath",
    "SafeURL",
    "PositiveInt",
    "NonNegativeInt",
    "PositiveFloat",
    "PercentageFloat",
    "SecurityHeaders",
    "RateLimitRequest",
    "SecurityAuditEvent",
    "sanitize_request_data",
    "validate_and_sanitize_query_params",
    "validate_no_sql_injection",
    "validate_no_xss",
    "validate_no_path_traversal",
    "create_safe_string_validator",
    "create_path_validator",
]
