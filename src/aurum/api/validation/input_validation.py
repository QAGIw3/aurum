"""Enhanced input validation with security and performance features."""

from __future__ import annotations

import re
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Union, Callable
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation error severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """Result of validation operation."""
    is_valid: bool
    errors: List[str] = None
    warnings: List[str] = None
    sanitized_value: Any = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
    
    def add_error(self, message: str) -> None:
        """Add validation error."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add validation warning."""
        self.warnings.append(message)


class ValidationRule(ABC):
    """Abstract base class for validation rules."""
    
    @abstractmethod
    def validate(self, value: Any, context: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """Validate value against this rule."""
        pass


class RequiredRule(ValidationRule):
    """Rule to check if value is required."""
    
    def __init__(self, field_name: str):
        self.field_name = field_name
    
    def validate(self, value: Any, context: Optional[Dict[str, Any]] = None) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if value is None or (isinstance(value, str) and not value.strip()):
            result.add_error(f"{self.field_name} is required")
        
        return result


class TypeRule(ValidationRule):
    """Rule to check value type."""
    
    def __init__(self, expected_type: type, field_name: str):
        self.expected_type = expected_type
        self.field_name = field_name
    
    def validate(self, value: Any, context: Optional[Dict[str, Any]] = None) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if value is not None and not isinstance(value, self.expected_type):
            result.add_error(f"{self.field_name} must be of type {self.expected_type.__name__}")
        
        return result


class RangeRule(ValidationRule):
    """Rule to check if numeric value is within range."""
    
    def __init__(self, min_value: Optional[Union[int, float]] = None, 
                 max_value: Optional[Union[int, float]] = None, field_name: str = "value"):
        self.min_value = min_value
        self.max_value = max_value
        self.field_name = field_name
    
    def validate(self, value: Any, context: Optional[Dict[str, Any]] = None) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if value is not None:
            try:
                numeric_value = float(value)
                
                if self.min_value is not None and numeric_value < self.min_value:
                    result.add_error(f"{self.field_name} must be >= {self.min_value}")
                
                if self.max_value is not None and numeric_value > self.max_value:
                    result.add_error(f"{self.field_name} must be <= {self.max_value}")
                    
            except (ValueError, TypeError):
                result.add_error(f"{self.field_name} must be a valid number")
        
        return result


class RegexRule(ValidationRule):
    """Rule to validate against regex pattern."""
    
    def __init__(self, pattern: str, field_name: str, message: Optional[str] = None):
        self.pattern = re.compile(pattern)
        self.field_name = field_name
        self.message = message or f"{field_name} format is invalid"
    
    def validate(self, value: Any, context: Optional[Dict[str, Any]] = None) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if value is not None:
            if not isinstance(value, str):
                result.add_error(f"{self.field_name} must be a string")
            elif not self.pattern.match(value):
                result.add_error(self.message)
        
        return result


class ChoiceRule(ValidationRule):
    """Rule to validate value is in allowed choices."""
    
    def __init__(self, choices: Set[Any], field_name: str, case_sensitive: bool = True):
        self.choices = choices if case_sensitive else {str(c).lower() for c in choices}
        self.field_name = field_name
        self.case_sensitive = case_sensitive
    
    def validate(self, value: Any, context: Optional[Dict[str, Any]] = None) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if value is not None:
            check_value = value if self.case_sensitive else str(value).lower()
            
            if check_value not in self.choices:
                result.add_error(f"{self.field_name} must be one of: {', '.join(map(str, self.choices))}")
        
        return result


class DateRangeRule(ValidationRule):
    """Rule to validate date range."""
    
    def __init__(self, start_field: str = "start_date", end_field: str = "end_date",
                 max_range_days: Optional[int] = None):
        self.start_field = start_field
        self.end_field = end_field
        self.max_range_days = max_range_days
    
    def validate(self, value: Any, context: Optional[Dict[str, Any]] = None) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        
        if not context:
            return result
        
        start_date = context.get(self.start_field)
        end_date = context.get(self.end_field)
        
        if start_date and end_date:
            # Convert to date objects if they're strings
            if isinstance(start_date, str):
                try:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                except ValueError:
                    result.add_error(f"{self.start_field} must be in YYYY-MM-DD format")
                    return result
            
            if isinstance(end_date, str):
                try:
                    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                except ValueError:
                    result.add_error(f"{self.end_field} must be in YYYY-MM-DD format")
                    return result
            
            # Validate range
            if start_date > end_date:
                result.add_error(f"{self.start_field} must be before or equal to {self.end_field}")
            
            if self.max_range_days:
                range_days = (end_date - start_date).days
                if range_days > self.max_range_days:
                    result.add_error(f"Date range cannot exceed {self.max_range_days} days")
        
        return result


class EnhancedValidator:
    """Enhanced validator with multiple rules and sanitization."""
    
    def __init__(self):
        self._rules: Dict[str, List[ValidationRule]] = {}
        self._sanitizers: Dict[str, List[Callable]] = {}
    
    def add_rule(self, field: str, rule: ValidationRule) -> None:
        """Add validation rule for field."""
        if field not in self._rules:
            self._rules[field] = []
        self._rules[field].append(rule)
    
    def add_sanitizer(self, field: str, sanitizer: Callable[[Any], Any]) -> None:
        """Add sanitizer function for field."""
        if field not in self._sanitizers:
            self._sanitizers[field] = []
        self._sanitizers[field].append(sanitizer)
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against all rules."""
        result = ValidationResult(is_valid=True)
        sanitized_data = {}
        
        # First pass: sanitize values
        for field, value in data.items():
            sanitized_value = value
            
            if field in self._sanitizers:
                for sanitizer in self._sanitizers[field]:
                    try:
                        sanitized_value = sanitizer(sanitized_value)
                    except Exception as e:
                        logger.warning(f"Sanitizer failed for field {field}: {e}")
            
            sanitized_data[field] = sanitized_value
        
        # Second pass: validate sanitized values
        for field, rules in self._rules.items():
            value = sanitized_data.get(field)
            
            for rule in rules:
                rule_result = rule.validate(value, sanitized_data)
                
                if not rule_result.is_valid:
                    result.is_valid = False
                    result.errors.extend(rule_result.errors)
                
                result.warnings.extend(rule_result.warnings)
        
        result.sanitized_value = sanitized_data
        return result


# Common validation functions
def validate_iso_code(iso: str) -> ValidationResult:
    """Validate ISO code."""
    validator = EnhancedValidator()
    validator.add_rule("iso", RequiredRule("ISO code"))
    validator.add_rule("iso", TypeRule(str, "ISO code"))
    validator.add_rule("iso", ChoiceRule(
        {"PJM", "CAISO", "ERCOT", "ISONE", "NYISO", "MISO", "SPP"},
        "ISO code",
        case_sensitive=False
    ))
    
    return validator.validate({"iso": iso})


def validate_date_range(start_date: Optional[date], end_date: Optional[date], 
                       max_days: int = 365) -> ValidationResult:
    """Validate date range."""
    validator = EnhancedValidator()
    validator.add_rule("dates", DateRangeRule(max_range_days=max_days))
    
    return validator.validate({
        "start_date": start_date,
        "end_date": end_date
    })


def validate_pagination(limit: Optional[int], offset: Optional[int] = None) -> ValidationResult:
    """Validate pagination parameters."""
    validator = EnhancedValidator()
    
    if limit is not None:
        validator.add_rule("limit", TypeRule(int, "limit"))
        validator.add_rule("limit", RangeRule(min_value=1, max_value=10000, field_name="limit"))
    
    if offset is not None:
        validator.add_rule("offset", TypeRule(int, "offset"))
        validator.add_rule("offset", RangeRule(min_value=0, field_name="offset"))
    
    return validator.validate({"limit": limit, "offset": offset})


# Sanitization functions
def sanitize_input(value: Any) -> Any:
    """Basic input sanitization."""
    if isinstance(value, str):
        # Remove null bytes and control characters
        value = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', value)
        
        # Trim whitespace
        value = value.strip()
        
        # Limit length to prevent DoS
        if len(value) > 10000:
            value = value[:10000]
    
    return value


def sanitize_sql_input(value: str) -> str:
    """Sanitize input to prevent SQL injection."""
    if not isinstance(value, str):
        return value
    
    # Remove common SQL injection patterns
    dangerous_patterns = [
        r"('|(\\')|(;|\s|^)(\s*)(union|select|insert|update|delete|drop|create|alter|exec|execute)(\s)",
        r"(script|javascript|vbscript|onload|onerror|onclick)",
        r"(<|&lt;)(script|iframe|object|embed|form).*?(/?>|&gt;)",
    ]
    
    cleaned_value = value
    for pattern in dangerous_patterns:
        cleaned_value = re.sub(pattern, '', cleaned_value, flags=re.IGNORECASE)
    
    return cleaned_value


def sanitize_html_input(value: str) -> str:
    """Sanitize HTML input to prevent XSS."""
    if not isinstance(value, str):
        return value
    
    # Remove script tags and event handlers
    html_patterns = [
        r'<script.*?>.*?</script>',
        r'<.*?on\w+\s*=.*?>',
        r'javascript:',
        r'vbscript:',
        r'data:text/html',
    ]
    
    cleaned_value = value
    for pattern in html_patterns:
        cleaned_value = re.sub(pattern, '', cleaned_value, flags=re.IGNORECASE | re.DOTALL)
    
    return cleaned_value


# Pre-configured validators for common use cases
def create_curve_validator() -> EnhancedValidator:
    """Create validator for curve API parameters."""
    validator = EnhancedValidator()
    
    # ISO validation
    validator.add_rule("iso", RequiredRule("ISO"))
    validator.add_rule("iso", ChoiceRule(
        {"PJM", "CAISO", "ERCOT", "ISONE", "NYISO", "MISO", "SPP"},
        "ISO",
        case_sensitive=False
    ))
    
    # Date range validation
    validator.add_rule("dates", DateRangeRule(max_range_days=365))
    
    # Pagination validation
    validator.add_rule("limit", RangeRule(min_value=1, max_value=10000, field_name="limit"))
    
    # Sanitizers
    validator.add_sanitizer("iso", lambda x: x.upper() if isinstance(x, str) else x)
    validator.add_sanitizer("market", sanitize_input)
    validator.add_sanitizer("location", sanitize_input)
    
    return validator


def create_scenario_validator() -> EnhancedValidator:
    """Create validator for scenario API parameters."""
    validator = EnhancedValidator()
    
    # Scenario name validation
    validator.add_rule("name", RequiredRule("Scenario name"))
    validator.add_rule("name", RegexRule(
        r'^[a-zA-Z0-9_\-\s]{1,100}$',
        "Scenario name",
        "Scenario name must be 1-100 characters, alphanumeric, spaces, underscores, and hyphens only"
    ))
    
    # Sanitizers
    validator.add_sanitizer("name", sanitize_input)
    validator.add_sanitizer("description", sanitize_html_input)
    
    return validator