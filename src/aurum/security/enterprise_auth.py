"""Enterprise authentication integration for Phase 4 implementation."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from aurum.core.settings import AurumSettings

logger = logging.getLogger(__name__)


class AuthProvider(str, Enum):
    """Supported enterprise authentication providers."""
    AZURE_AD = "azure_ad"
    OKTA = "okta" 
    SAML = "saml"
    LDAP = "ldap"
    OAUTH2 = "oauth2"
    LOCAL = "local"


class TenantTier(str, Enum):
    """Enterprise tenant tiers for different feature access."""
    BASIC = "basic"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"
    ENTERPRISE_PLUS = "enterprise_plus"


@dataclass
class EnterpriseAuthConfig:
    """Enterprise authentication configuration."""
    
    provider: AuthProvider
    provider_config: Dict[str, Any]
    sso_enabled: bool = True
    mfa_required: bool = True
    session_timeout_minutes: int = 480  # 8 hours
    max_concurrent_sessions: int = 5
    password_policy: Dict[str, Any] = None
    audit_login_attempts: bool = True
    
    def __post_init__(self):
        if self.password_policy is None:
            self.password_policy = {
                "min_length": 12,
                "require_uppercase": True,
                "require_lowercase": True,
                "require_numbers": True,
                "require_special_chars": True,
                "password_history": 12,
                "max_age_days": 90
            }


@dataclass
class TenantConfiguration:
    """Enterprise tenant configuration."""
    
    tenant_id: str
    tenant_name: str
    tier: TenantTier
    domain_restrictions: List[str]  # Email domain restrictions
    auth_config: EnterpriseAuthConfig
    feature_flags: Dict[str, bool]
    resource_limits: Dict[str, Any]
    compliance_requirements: List[str]
    data_residency: Optional[str] = None  # Region requirement
    
    def __post_init__(self):
        if not self.feature_flags:
            self.feature_flags = self._get_default_features_for_tier()
        if not self.resource_limits:
            self.resource_limits = self._get_default_limits_for_tier()
    
    def _get_default_features_for_tier(self) -> Dict[str, bool]:
        """Get default feature flags based on tenant tier."""
        base_features = {
            "api_access": True,
            "basic_reporting": True,
            "data_access": True
        }
        
        if self.tier in [TenantTier.PROFESSIONAL, TenantTier.ENTERPRISE, TenantTier.ENTERPRISE_PLUS]:
            base_features.update({
                "advanced_analytics": True,
                "bulk_operations": True,
                "custom_dashboards": True
            })
        
        if self.tier in [TenantTier.ENTERPRISE, TenantTier.ENTERPRISE_PLUS]:
            base_features.update({
                "workflow_automation": True,
                "integration_apis": True,
                "advanced_security": True,
                "audit_logging": True
            })
        
        if self.tier == TenantTier.ENTERPRISE_PLUS:
            base_features.update({
                "multi_region": True,
                "custom_integrations": True,
                "priority_support": True,
                "sla_guarantees": True
            })
        
        return base_features
    
    def _get_default_limits_for_tier(self) -> Dict[str, Any]:
        """Get default resource limits based on tenant tier."""
        limits = {
            TenantTier.BASIC: {
                "api_calls_per_hour": 1000,
                "concurrent_users": 10,
                "data_retention_days": 90,
                "storage_gb": 10
            },
            TenantTier.PROFESSIONAL: {
                "api_calls_per_hour": 10000,
                "concurrent_users": 50,
                "data_retention_days": 365,
                "storage_gb": 100
            },
            TenantTier.ENTERPRISE: {
                "api_calls_per_hour": 100000,
                "concurrent_users": 200,
                "data_retention_days": 2555,  # 7 years
                "storage_gb": 1000
            },
            TenantTier.ENTERPRISE_PLUS: {
                "api_calls_per_hour": 1000000,
                "concurrent_users": 1000,
                "data_retention_days": 3653,  # 10 years
                "storage_gb": 10000
            }
        }
        return limits.get(self.tier, limits[TenantTier.BASIC])


class EnterpriseAuthManager:
    """Enhanced enterprise authentication manager for Phase 4."""

    def __init__(self, settings: AurumSettings):
        self.settings = settings
        self.tenant_configs: Dict[str, TenantConfiguration] = {}
        self.auth_sessions: Dict[str, Dict[str, Any]] = {}
        self.failed_attempts: Dict[str, List[datetime]] = {}
        
    def register_enterprise_tenant(self, config: TenantConfiguration) -> None:
        """Register a new enterprise tenant configuration."""
        self.tenant_configs[config.tenant_id] = config
        logger.info(f"Registered enterprise tenant: {config.tenant_name} ({config.tier})")
    
    def get_tenant_config(self, tenant_id: str) -> Optional[TenantConfiguration]:
        """Get tenant configuration by ID."""
        return self.tenant_configs.get(tenant_id)
    
    def validate_tenant_access(self, tenant_id: str, user_email: str) -> bool:
        """Validate if user has access to tenant based on domain restrictions."""
        config = self.get_tenant_config(tenant_id)
        if not config:
            return False
        
        if not config.domain_restrictions:
            return True
        
        user_domain = user_email.split('@')[-1].lower() if '@' in user_email else ''
        return user_domain in [domain.lower() for domain in config.domain_restrictions]
    
    def check_feature_access(self, tenant_id: str, feature: str) -> bool:
        """Check if tenant has access to a specific feature."""
        config = self.get_tenant_config(tenant_id)
        if not config:
            return False
        
        return config.feature_flags.get(feature, False)
    
    def get_resource_limit(self, tenant_id: str, resource: str) -> Optional[Any]:
        """Get resource limit for tenant."""
        config = self.get_tenant_config(tenant_id)
        if not config:
            return None
        
        return config.resource_limits.get(resource)
    
    def track_login_attempt(self, user_id: str, tenant_id: str, success: bool, 
                           ip_address: str, user_agent: str) -> None:
        """Track login attempts for audit and security monitoring."""
        attempt_time = datetime.now()
        
        if not success:
            if user_id not in self.failed_attempts:
                self.failed_attempts[user_id] = []
            self.failed_attempts[user_id].append(attempt_time)
            
            # Clean old attempts (older than 1 hour)
            cutoff = attempt_time - timedelta(hours=1)
            self.failed_attempts[user_id] = [
                attempt for attempt in self.failed_attempts[user_id] 
                if attempt > cutoff
            ]
        
        # Log for audit trail
        logger.info(
            f"Login attempt: user={user_id}, tenant={tenant_id}, "
            f"success={success}, ip={ip_address}, time={attempt_time}"
        )
    
    def is_account_locked(self, user_id: str) -> bool:
        """Check if account is locked due to failed attempts."""
        if user_id not in self.failed_attempts:
            return False
        
        recent_failures = len(self.failed_attempts[user_id])
        max_attempts = 5  # Configurable
        
        return recent_failures >= max_attempts
    
    def create_auth_session(self, user_id: str, tenant_id: str, 
                           session_data: Dict[str, Any]) -> str:
        """Create authenticated session."""
        config = self.get_tenant_config(tenant_id)
        if not config:
            raise ValueError(f"Unknown tenant: {tenant_id}")
        
        session_id = f"sess_{int(time.time())}_{user_id}_{tenant_id}"
        timeout_minutes = config.auth_config.session_timeout_minutes
        
        self.auth_sessions[session_id] = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "created_at": datetime.now(),
            "expires_at": datetime.now() + timedelta(minutes=timeout_minutes),
            "last_activity": datetime.now(),
            "session_data": session_data
        }
        
        return session_id
    
    def validate_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Validate and refresh session."""
        session = self.auth_sessions.get(session_id)
        if not session:
            return None
        
        now = datetime.now()
        if now > session["expires_at"]:
            del self.auth_sessions[session_id]
            return None
        
        # Update last activity
        session["last_activity"] = now
        return session
    
    def cleanup_expired_sessions(self) -> int:
        """Clean up expired sessions."""
        now = datetime.now()
        expired_sessions = [
            session_id for session_id, session in self.auth_sessions.items()
            if now > session["expires_at"]
        ]
        
        for session_id in expired_sessions:
            del self.auth_sessions[session_id]
        
        return len(expired_sessions)
    
    def get_tenant_compliance_requirements(self, tenant_id: str) -> List[str]:
        """Get compliance requirements for tenant."""
        config = self.get_tenant_config(tenant_id)
        return config.compliance_requirements if config else []
    
    def validate_data_residency(self, tenant_id: str, target_region: str) -> bool:
        """Validate data residency requirements."""
        config = self.get_tenant_config(tenant_id)
        if not config or not config.data_residency:
            return True  # No restrictions
        
        return config.data_residency == target_region