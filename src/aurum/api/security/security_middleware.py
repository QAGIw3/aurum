"""Enhanced security middleware with comprehensive protection."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Any
from ipaddress import IPv4Network, AddressValueError, ip_address

try:
    from fastapi import Request, Response, HTTPException
    from starlette.middleware.base import BaseHTTPMiddleware
except ImportError:
    Request = None
    Response = None
    HTTPException = None
    BaseHTTPMiddleware = None

from ...core import get_current_context

logger = logging.getLogger(__name__)


@dataclass
class SecurityConfig:
    """Security middleware configuration."""
    enable_rate_limiting: bool = True
    enable_ip_filtering: bool = True
    enable_api_key_validation: bool = True
    enable_request_size_limit: bool = True
    enable_cors_protection: bool = True
    enable_security_headers: bool = True
    
    # Rate limiting
    default_rate_limit: int = 1000  # requests per minute
    burst_limit: int = 100  # requests per 10 seconds
    
    # Request limits
    max_request_size: int = 10 * 1024 * 1024  # 10MB
    max_header_size: int = 8192  # 8KB
    
    # IP filtering
    blocked_ips: Set[str] = field(default_factory=set)
    allowed_ips: Optional[Set[str]] = None  # None means all IPs allowed
    
    # API keys
    valid_api_keys: Set[str] = field(default_factory=set)
    api_key_header: str = "X-API-Key"
    
    # CORS
    allowed_origins: List[str] = field(default_factory=lambda: ["*"])
    allowed_methods: List[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    allowed_headers: List[str] = field(default_factory=lambda: ["*"])


class RateLimitRule:
    """Rate limiting rule implementation."""
    
    def __init__(self, requests_per_minute: int = 1000, burst_limit: int = 100):
        self.requests_per_minute = requests_per_minute
        self.burst_limit = burst_limit
        self._request_counts: Dict[str, List[float]] = {}
        self._burst_counts: Dict[str, List[float]] = {}
        self._lock = asyncio.Lock()
    
    async def is_allowed(self, client_id: str) -> tuple[bool, Dict[str, Any]]:
        """Check if request is allowed under rate limits."""
        async with self._lock:
            current_time = time.time()
            
            # Clean old entries
            self._cleanup_old_entries(current_time)
            
            # Initialize client if not exists
            if client_id not in self._request_counts:
                self._request_counts[client_id] = []
                self._burst_counts[client_id] = []
            
            # Check burst limit (last 10 seconds)
            burst_window_start = current_time - 10
            recent_burst_requests = [
                req_time for req_time in self._burst_counts[client_id]
                if req_time > burst_window_start
            ]
            
            if len(recent_burst_requests) >= self.burst_limit:
                return False, {
                    "error": "Burst rate limit exceeded",
                    "burst_limit": self.burst_limit,
                    "retry_after": 10
                }
            
            # Check per-minute limit
            minute_window_start = current_time - 60
            recent_requests = [
                req_time for req_time in self._request_counts[client_id]
                if req_time > minute_window_start
            ]
            
            if len(recent_requests) >= self.requests_per_minute:
                return False, {
                    "error": "Rate limit exceeded", 
                    "rate_limit": self.requests_per_minute,
                    "retry_after": 60
                }
            
            # Record this request
            self._request_counts[client_id].append(current_time)
            self._burst_counts[client_id].append(current_time)
            
            return True, {
                "requests_remaining": self.requests_per_minute - len(recent_requests) - 1,
                "burst_remaining": self.burst_limit - len(recent_burst_requests) - 1
            }
    
    def _cleanup_old_entries(self, current_time: float) -> None:
        """Remove old entries to prevent memory leaks."""
        cutoff_time = current_time - 3600  # Keep 1 hour of history
        
        for client_id in list(self._request_counts.keys()):
            self._request_counts[client_id] = [
                req_time for req_time in self._request_counts[client_id]
                if req_time > cutoff_time
            ]
            self._burst_counts[client_id] = [
                req_time for req_time in self._burst_counts[client_id]
                if req_time > cutoff_time
            ]
            
            # Remove empty entries
            if not self._request_counts[client_id]:
                del self._request_counts[client_id]
                del self._burst_counts[client_id]


class IPWhitelistRule:
    """IP whitelist/blacklist rule."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self._blocked_networks = self._parse_ip_ranges(config.blocked_ips)
        self._allowed_networks = self._parse_ip_ranges(config.allowed_ips) if config.allowed_ips else None
    
    def _parse_ip_ranges(self, ip_set: Set[str]) -> List[IPv4Network]:
        """Parse IP addresses and ranges into network objects."""
        networks = []
        for ip_str in ip_set:
            try:
                if '/' in ip_str:
                    # CIDR notation
                    networks.append(IPv4Network(ip_str))
                else:
                    # Single IP
                    networks.append(IPv4Network(f"{ip_str}/32"))
            except AddressValueError as e:
                logger.warning(f"Invalid IP address/range: {ip_str}: {e}")
        return networks
    
    def is_allowed(self, client_ip: str) -> tuple[bool, Optional[str]]:
        """Check if IP is allowed."""
        try:
            client_addr = ip_address(client_ip)
        except AddressValueError:
            return False, f"Invalid IP address: {client_ip}"
        
        # Check if IP is blocked
        for network in self._blocked_networks:
            if client_addr in network:
                return False, f"IP {client_ip} is blocked"
        
        # Check if IP is in allowlist (if allowlist is configured)
        if self._allowed_networks is not None:
            for network in self._allowed_networks:
                if client_addr in network:
                    return True, None
            return False, f"IP {client_ip} is not in allowlist"
        
        return True, None


class APIKeyRule:
    """API key validation rule."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self._hashed_keys = {
            hashlib.sha256(key.encode()).hexdigest(): key
            for key in config.valid_api_keys
        }
    
    def is_valid(self, api_key: Optional[str]) -> tuple[bool, Optional[str]]:
        """Validate API key."""
        if not self.config.valid_api_keys:
            # No API keys configured, allow all
            return True, None
        
        if not api_key:
            return False, "API key is required"
        
        # Hash the provided key for comparison
        hashed_key = hashlib.sha256(api_key.encode()).hexdigest()
        
        if hashed_key in self._hashed_keys:
            return True, None
        
        return False, "Invalid API key"


class SecurityMiddleware:
    """Comprehensive security middleware."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.rate_limiter = RateLimitRule(
            config.default_rate_limit,
            config.burst_limit
        ) if config.enable_rate_limiting else None
        
        self.ip_filter = IPWhitelistRule(config) if config.enable_ip_filtering else None
        self.api_key_validator = APIKeyRule(config) if config.enable_api_key_validation else None
    
    async def __call__(self, request: Request, call_next: Callable) -> Response:
        """Process request through security checks."""
        if Request is None or Response is None:
            return await call_next(request)
        
        # Get client information
        client_ip = self._get_client_ip(request)
        client_id = self._get_client_id(request, client_ip)
        
        try:
            # Request size check
            if self.config.enable_request_size_limit:
                content_length = request.headers.get("content-length")
                if content_length and int(content_length) > self.config.max_request_size:
                    return self._create_error_response(413, "Request too large")
            
            # IP filtering
            if self.ip_filter:
                allowed, message = self.ip_filter.is_allowed(client_ip)
                if not allowed:
                    logger.warning(f"IP blocked: {client_ip} - {message}")
                    return self._create_error_response(403, "Access denied")
            
            # Rate limiting
            if self.rate_limiter:
                allowed, rate_info = await self.rate_limiter.is_allowed(client_id)
                if not allowed:
                    logger.warning(f"Rate limit exceeded for {client_id}")
                    response = self._create_error_response(429, rate_info.get("error", "Rate limit exceeded"))
                    if "retry_after" in rate_info:
                        response.headers["Retry-After"] = str(rate_info["retry_after"])
                    return response
            
            # API key validation
            if self.api_key_validator:
                api_key = request.headers.get(self.config.api_key_header)
                valid, message = self.api_key_validator.is_valid(api_key)
                if not valid:
                    logger.warning(f"Invalid API key from {client_ip}")
                    return self._create_error_response(401, message or "Unauthorized")
            
            # Process request
            response = await call_next(request)
            
            # Add security headers
            if self.config.enable_security_headers:
                self._add_security_headers(response)
            
            # Add rate limit headers
            if self.rate_limiter and "rate_info" in locals():
                if "requests_remaining" in rate_info:
                    response.headers["X-RateLimit-Remaining"] = str(rate_info["requests_remaining"])
                response.headers["X-RateLimit-Limit"] = str(self.config.default_rate_limit)
            
            return response
        
        except Exception as e:
            logger.error(f"Security middleware error: {e}")
            return self._create_error_response(500, "Internal security error")
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request."""
        # Check for forwarded headers (reverse proxy)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take the first IP in the chain
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()
        
        # Fallback to direct connection
        return request.client.host if request.client else "unknown"
    
    def _get_client_id(self, request: Request, client_ip: str) -> str:
        """Generate client ID for rate limiting."""
        # Use API key if available, otherwise IP
        api_key = request.headers.get(self.config.api_key_header)
        if api_key:
            return f"key:{hashlib.sha256(api_key.encode()).hexdigest()[:16]}"
        
        return f"ip:{client_ip}"
    
    def _create_error_response(self, status_code: int, message: str) -> Response:
        """Create standardized error response."""
        context = get_current_context()
        
        error_data = {
            "error": message,
            "status_code": status_code,
            "request_id": context.request_id,
            "timestamp": time.time()
        }
        
        return Response(
            content=str(error_data),
            status_code=status_code,
            headers={"Content-Type": "application/json"}
        )
    
    def _add_security_headers(self, response: Response) -> None:
        """Add security headers to response."""
        security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY", 
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Content-Security-Policy": "default-src 'self'",
        }
        
        for header, value in security_headers.items():
            response.headers[header] = value


# Create middleware class compatible with Starlette/FastAPI
if BaseHTTPMiddleware is not None:
    class SecurityHTTPMiddleware(BaseHTTPMiddleware):
        """HTTP middleware wrapper for security."""
        
        def __init__(self, app, config: SecurityConfig):
            super().__init__(app)
            self.security_middleware = SecurityMiddleware(config)
        
        async def dispatch(self, request: Request, call_next) -> Response:
            return await self.security_middleware(request, call_next)
else:
    SecurityHTTPMiddleware = None