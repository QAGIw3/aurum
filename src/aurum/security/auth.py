"""Enhanced authentication and authorization with explicit policy checks."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Union
from dataclasses import dataclass
from enum import Enum

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import jwt

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from .audit import log_auth_success, log_auth_failure, log_access_denied
from .validation import SafeIdentifier, SecurityHeaders


class Permission(str, Enum):
    """Permission levels for authorization."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    AUDIT = "audit"


class ResourceType(str, Enum):
    """Resource types for authorization."""
    SCENARIO = "scenario"
    DATA_SOURCE = "data_source"
    USER = "user"
    TENANT = "tenant"
    SYSTEM = "system"
    CONFIGURATION = "configuration"


@dataclass(frozen=True)
class AuthPolicy:
    """Authorization policy for resource access."""
    resource_type: ResourceType
    permissions: Set[Permission]
    tenant_scoped: bool = True
    user_scoped: bool = False

    def allows(self, permission: Permission, tenant_id: Optional[str] = None, user_id: Optional[str] = None) -> bool:
        """Check if policy allows the given permission."""
        return permission in self.permissions


class AuthError(Exception):
    """Base exception for authentication/authorization errors."""
    pass


class InvalidTokenError(AuthError):
    """Raised when JWT token is invalid."""
    pass


class InsufficientPermissionsError(AuthError):
    """Raised when user lacks required permissions."""
    pass


class ResourceNotFoundError(AuthError):
    """Raised when resource is not found (for 404 vs 403 distinction)."""
    pass


class SecurityConfig(BaseModel):
    """Security configuration model."""
    jwt_secret: str = Field(..., description="JWT secret key")
    jwt_algorithm: str = Field("HS256", description="JWT algorithm")
    token_expiration_hours: int = Field(24, description="Token expiration time")
    required_permissions: Dict[str, List[Permission]] = Field(default_factory=dict)
    admin_users: List[str] = Field(default_factory=list)
    audit_enabled: bool = Field(True, description="Enable audit logging")


class UserContext(BaseModel):
    """User context from JWT token."""
    user_id: SafeIdentifier = Field(..., description="User identifier")
    tenant_id: SafeIdentifier = Field(..., description="Tenant identifier")
    permissions: List[Permission] = Field(default_factory=list, description="User permissions")
    roles: List[str] = Field(default_factory=list, description="User roles")
    is_admin: bool = Field(False, description="Whether user has admin privileges")
    token_data: Dict[str, Any] = Field(default_factory=dict, description="Additional token data")

    class Config:
        validate_assignment = True


class AuthorizationManager:
    """Manages authentication and authorization policies."""

    def __init__(self, config: SecurityConfig):
        self.config = config
        self.security = HTTPBearer(auto_error=False)
        self._policies: Dict[str, AuthPolicy] = self._default_policies()

    def _default_policies(self) -> Dict[str, AuthPolicy]:
        """Get default authorization policies."""
        return {
            "/health": AuthPolicy(ResourceType.SYSTEM, {Permission.READ}, tenant_scoped=False),
            "/ready": AuthPolicy(ResourceType.SYSTEM, {Permission.READ}, tenant_scoped=False),
            "/metrics": AuthPolicy(ResourceType.SYSTEM, {Permission.READ}, tenant_scoped=False),
            "/docs": AuthPolicy(ResourceType.SYSTEM, {Permission.READ}, tenant_scoped=False),
            "/openapi.json": AuthPolicy(ResourceType.SYSTEM, {Permission.READ}, tenant_scoped=False),
            "/v2/scenarios": AuthPolicy(ResourceType.SCENARIO, {Permission.READ}),
            "/v2/scenarios/{scenario_id}": AuthPolicy(ResourceType.SCENARIO, {Permission.READ}),
            "/v2/scenarios": AuthPolicy(ResourceType.SCENARIO, {Permission.WRITE}, requires_admin=True),
            "/v2/admin/*": AuthPolicy(ResourceType.SYSTEM, {Permission.ADMIN}, requires_admin=True),
        }

    def get_policy(self, path: str, method: str) -> Optional[AuthPolicy]:
        """Get authorization policy for a path and method."""
        # Normalize path
        path = re.sub(r'/{[^}]+}', '/{id}', path)

        # Find matching policy
        for pattern, policy in self._policies.items():
            if self._matches_pattern(pattern, path):
                return policy

        return None

    def _matches_pattern(self, pattern: str, path: str) -> bool:
        """Check if path matches pattern (simple implementation)."""
        # Convert pattern to regex
        regex_pattern = pattern.replace('{id}', r'[^/]+')
        regex_pattern = f'^{regex_pattern}$'
        return bool(re.match(regex_pattern, path))

    async def authenticate_request(
        self,
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = None
    ) -> Optional[UserContext]:
        """Authenticate request and return user context."""
        # Extract client info
        client_ip = self._get_client_ip(request)
        user_agent = request.headers.get("user-agent")

        try:
            # If no credentials provided, check for optional auth endpoints
            if not credentials:
                # Some endpoints might not require authentication
                return None

            # Validate token format
            token = credentials.credentials
            if not token or not token.startswith("Bearer "):
                await self._log_auth_failure("invalid_token_format", None, None, client_ip, user_agent)
                return None

            # Decode and validate JWT
            user_context = self._decode_jwt(token)

            # Log successful authentication
            await self._log_auth_success(user_context, client_ip, user_agent)

            return user_context

        except InvalidTokenError as e:
            await self._log_auth_failure(str(e), None, None, client_ip, user_agent)
            return None
        except Exception as e:
            await self._log_auth_failure(f"auth_error: {str(e)}", None, None, client_ip, user_agent)
            return None

    def _decode_jwt(self, token: str) -> UserContext:
        """Decode and validate JWT token."""
        try:
            payload = jwt.decode(
                token,
                self.config.jwt_secret,
                algorithms=[self.config.jwt_algorithm]
            )

            # Extract required fields
            user_id = payload.get("user_id")
            tenant_id = payload.get("tenant_id")
            permissions = payload.get("permissions", [])
            roles = payload.get("roles", [])
            is_admin = payload.get("is_admin", False)

            if not user_id or not tenant_id:
                raise InvalidTokenError("Missing required fields in token")

            # Validate user_id and tenant_id format
            user_id = SafeIdentifier.validate(user_id)
            tenant_id = SafeIdentifier.validate(tenant_id)

            # Convert permissions to enum values
            try:
                permissions = [Permission(p) for p in permissions]
            except ValueError:
                permissions = []

            return UserContext(
                user_id=user_id,
                tenant_id=tenant_id,
                permissions=permissions,
                roles=roles,
                is_admin=is_admin,
                token_data=payload
            )

        except jwt.ExpiredSignatureError:
            raise InvalidTokenError("Token has expired")
        except jwt.InvalidTokenError:
            raise InvalidTokenError("Invalid token")
        except Exception as e:
            raise InvalidTokenError(f"Token validation failed: {str(e)}")

    def authorize_request(
        self,
        user_context: Optional[UserContext],
        path: str,
        method: str,
        resource_id: Optional[str] = None
    ) -> None:
        """Authorize request based on user context and policy."""
        client_ip = self._get_client_ip_from_request()  # Would need to be passed in

        policy = self.get_policy(path, method)

        if not policy:
            # No policy defined - allow by default but audit
            log_structured(
                "warning",
                "no_policy_defined",
                path=path,
                method=method,
                user_id=user_context.user_id if user_context else None,
                tenant_id=user_context.tenant_id if user_context else None,
            )
            return

        # Check authentication requirement
        if user_context is None:
            # Endpoint requires authentication
            log_access_denied(
                user_id=None,
                tenant_id=None,
                resource=path,
                action=method,
                ip_address=client_ip,
                reason="authentication_required"
            )
            raise HTTPException(
                status_code=401,
                detail="Authentication required"
            )

        # Check tenant scope
        if policy.tenant_scoped:
            context_tenant_id = get_tenant_id()
            if context_tenant_id and context_tenant_id != user_context.tenant_id:
                log_access_denied(
                    user_id=user_context.user_id,
                    tenant_id=user_context.tenant_id,
                    resource=path,
                    action=method,
                    ip_address=client_ip,
                    reason="tenant_mismatch"
                )
                raise HTTPException(
                    status_code=403,
                    detail="Access denied: tenant mismatch"
                )

        # Check permissions
        required_permission = self._get_required_permission(method)
        if not policy.allows(required_permission, user_context.tenant_id, user_context.user_id):
            log_access_denied(
                user_id=user_context.user_id,
                tenant_id=user_context.tenant_id,
                resource=path,
                action=method,
                ip_address=client_ip,
                reason=f"insufficient_permissions: {required_permission}"
            )
            raise HTTPException(
                status_code=403,
                detail=f"Access denied: insufficient permissions ({required_permission})"
            )

        # Check admin requirement
        if hasattr(policy, 'requires_admin') and policy.requires_admin and not user_context.is_admin:
            log_access_denied(
                user_id=user_context.user_id,
                tenant_id=user_context.tenant_id,
                resource=path,
                action=method,
                ip_address=client_ip,
                reason="admin_access_required"
            )
            raise HTTPException(
                status_code=403,
                detail="Access denied: admin access required"
            )

    def _get_required_permission(self, method: str) -> Permission:
        """Get required permission for HTTP method."""
        method_permissions = {
            "GET": Permission.READ,
            "HEAD": Permission.READ,
            "POST": Permission.WRITE,
            "PUT": Permission.WRITE,
            "PATCH": Permission.WRITE,
            "DELETE": Permission.DELETE,
        }
        return method_permissions.get(method, Permission.READ)

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address from request."""
        # Check X-Forwarded-For header first (for proxies)
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        # Check X-Real-IP header
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        # Fall back to client host
        return request.client.host if request.client else "unknown"

    def _get_client_ip_from_request(self) -> str:
        """Get client IP - would need to be implemented based on how it's called."""
        # This would typically be extracted from the request context
        return "unknown"

    async def _log_auth_success(
        self,
        user_context: UserContext,
        ip_address: str,
        user_agent: Optional[str]
    ) -> None:
        """Log successful authentication."""
        if self.config.audit_enabled:
            log_auth_success(
                user_id=user_context.user_id,
                tenant_id=user_context.tenant_id,
                ip_address=ip_address,
                user_agent=user_agent,
                auth_method="jwt"
            )

    async def _log_auth_failure(
        self,
        reason: str,
        user_id: Optional[str],
        tenant_id: Optional[str],
        ip_address: str,
        user_agent: Optional[str]
    ) -> None:
        """Log authentication failure."""
        if self.config.audit_enabled:
            log_auth_failure(
                reason=reason,
                user_id=user_id,
                tenant_id=tenant_id,
                ip_address=ip_address,
                user_agent=user_agent,
                auth_method="jwt"
            )


# Global auth manager instance
_auth_manager: Optional[AuthorizationManager] = None


def get_auth_manager() -> AuthorizationManager:
    """Get the global authorization manager."""
    global _auth_manager
    if _auth_manager is None:
        # This would need to be configured with actual security config
        config = SecurityConfig(jwt_secret="default-secret-key")
        _auth_manager = AuthorizationManager(config)
    return _auth_manager


def require_auth(permissions: List[Permission] = None, resource_type: ResourceType = None):
    """Decorator to require authentication and authorization."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # This would be integrated into the FastAPI dependency system
            # For now, this is a placeholder
            return await func(*args, **kwargs)
        return wrapper
    return decorator


# Utility functions for consistent error handling
def create_403_response(detail: str, request_id: Optional[str] = None) -> HTTPException:
    """Create a 403 Forbidden response with consistent format."""
    return HTTPException(
        status_code=403,
        detail={
            "error": "AccessDenied",
            "message": detail,
            "request_id": request_id,
            "timestamp": "2023-12-01T00:00:00Z"  # Would be dynamic
        }
    )


def create_404_response(resource_type: str, resource_id: str, request_id: Optional[str] = None) -> HTTPException:
    """Create a 404 Not Found response with consistent format."""
    return HTTPException(
        status_code=404,
        detail={
            "error": "NotFound",
            "message": f"{resource_type} with ID '{resource_id}' not found",
            "request_id": request_id,
            "timestamp": "2023-12-01T00:00:00Z"  # Would be dynamic
        }
    )


def create_401_response(detail: str, request_id: Optional[str] = None) -> HTTPException:
    """Create a 401 Unauthorized response with consistent format."""
    return HTTPException(
        status_code=401,
        detail={
            "error": "AuthenticationRequired",
            "message": detail,
            "request_id": request_id,
            "timestamp": "2023-12-01T00:00:00Z"  # Would be dynamic
        }
    )
