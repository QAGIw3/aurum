"""Vault-backed secrets rotation and short-lived token management system."""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from ..logging import LogLevel, create_logger
from ..observability.metrics import get_metrics_client
from .vault_client import VaultConfig, VaultCredentialProvider

logger = logging.getLogger(__name__)


class TokenType(str, Enum):
    """Types of tokens for different use cases."""
    DATABASE = "database"
    API_CLIENT = "api_client"
    SERVICE_ACCOUNT = "service_account"
    KAFKA_PRODUCER = "kafka_producer"
    KAFKA_CONSUMER = "kafka_consumer"
    EXTERNAL_API = "external_api"


class RotationPolicy(str, Enum):
    """Rotation policies for different secret types."""
    IMMEDIATE = "immediate"      # Rotate immediately when compromised
    SCHEDULED = "scheduled"      # Rotate on schedule
    ON_ACCESS = "on_access"      # Rotate when accessed (lazy rotation)
    ON_DEMAND = "on_demand"      # Rotate when requested


@dataclass
class TokenConfig:
    """Configuration for token generation."""

    token_type: TokenType
    name: str
    description: str = ""

    # Token properties
    ttl_seconds: int = 3600  # 1 hour default
    max_uses: Optional[int] = None  # Unlimited by default
    policies: List[str] = field(default_factory=list)

    # Rotation settings
    rotation_policy: RotationPolicy = RotationPolicy.SCHEDULED
    rotation_interval_hours: int = 24
    auto_rotate: bool = True

    # Security settings
    bind_to_ip: bool = False
    allowed_ips: List[str] = field(default_factory=list)
    require_mfa: bool = False

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now())
    last_rotated: Optional[datetime] = None
    rotation_count: int = 0

    def get_vault_policy_name(self) -> str:
        """Generate Vault policy name for this token."""
        return f"aurum-{self.token_type.value}-{self.name}"


@dataclass
class TokenInstance:
    """Instance of a generated token."""

    token_id: str
    token_config: TokenConfig
    vault_token: str
    created_at: datetime
    expires_at: datetime
    issued_by: str = ""
    used_count: int = 0
    last_used: Optional[datetime] = None
    is_revoked: bool = False

    # Token metadata
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None

    def is_expired(self) -> bool:
        """Check if token is expired."""
        return datetime.now() > self.expires_at

    def is_near_expiry(self, threshold_minutes: int = 30) -> bool:
        """Check if token is near expiry."""
        return datetime.now() + timedelta(minutes=threshold_minutes) > self.expires_at

    def can_be_used(self) -> bool:
        """Check if token can still be used."""
        if self.is_revoked:
            return False
        if self.is_expired():
            return False
        if self.token_config.max_uses and self.used_count >= self.token_config.max_uses:
            return False
        return True

    def record_usage(self, client_ip: Optional[str] = None, user_agent: Optional[str] = None):
        """Record token usage."""
        self.used_count += 1
        self.last_used = datetime.now()
        if client_ip:
            self.client_ip = client_ip
        if user_agent:
            self.user_agent = user_agent


@dataclass
class SecretRotationConfig:
    """Configuration for secrets rotation."""

    # Rotation intervals (in hours)
    database_credentials_hours: int = 24
    api_keys_hours: int = 12
    service_tokens_hours: int = 6
    kafka_credentials_hours: int = 48

    # Token settings
    default_token_ttl_hours: int = 1
    max_token_ttl_hours: int = 8
    token_renewal_threshold_hours: int = 2

    # Rotation policies
    enable_auto_rotation: bool = True
    enable_lazy_rotation: bool = True
    require_manual_approval: bool = False

    # Security settings
    enable_ip_binding: bool = True
    enforce_token_isolation: bool = True
    audit_token_usage: bool = True

    # Performance settings
    max_concurrent_rotations: int = 5
    rotation_batch_size: int = 10

    # Notification settings
    notify_on_rotation: bool = True
    notify_on_compromise: bool = True


class SecretsRotationManager:
    """Manager for automated secrets rotation and short-lived token lifecycle."""

    def __init__(self, vault_config: VaultConfig, rotation_config: SecretRotationConfig):
        self.vault_config = vault_config
        self.rotation_config = rotation_config

        # Initialize components
        self.credential_provider = VaultCredentialProvider(vault_config)
        self.metrics = get_metrics_client()
        self.logger = create_logger(
            source_name="secrets_rotation_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.secrets.rotation",
            dataset="secrets_rotation"
        )

        # Token management
        self.active_tokens: Dict[str, TokenInstance] = {}
        self.token_configs: Dict[str, TokenConfig] = {}
        self.revoked_tokens: Set[str] = set()

        # Rotation state
        self.is_rotating: bool = False
        self.rotation_queue: asyncio.Queue = asyncio.Queue()
        self.rotation_tasks: Set[asyncio.Task] = set()

        # Audit trail
        self.audit_events: List[Dict[str, Any]] = []

        logger.info("Secrets Rotation Manager initialized")

    async def initialize(self):
        """Initialize the secrets rotation system."""
        try:
            await self.credential_provider.initialize()

            # Load existing token configurations
            await self._load_token_configurations()

            # Start background rotation service
            if self.rotation_config.enable_auto_rotation:
                await self._start_background_rotation()

            logger.info("Secrets rotation system initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize secrets rotation system: {e}")
            raise

    async def create_token_config(
        self,
        name: str,
        token_type: TokenType,
        policies: List[str],
        ttl_hours: int = 1,
        **kwargs
    ) -> TokenConfig:
        """Create a new token configuration."""
        config = TokenConfig(
            token_type=token_type,
            name=name,
            policies=policies,
            ttl_seconds=ttl_hours * 3600,
            **kwargs
        )

        self.token_configs[f"{token_type.value}:{name}"] = config

        # Create corresponding Vault policy
        await self._create_vault_policy(config)

        # Audit event
        await self._record_audit_event("token_config_created", {
            "token_type": token_type.value,
            "name": name,
            "policies": policies,
            "ttl_hours": ttl_hours
        })

        logger.info(f"Created token configuration: {token_type.value}:{name}")
        return config

    async def get_short_lived_token(
        self,
        token_type: TokenType,
        name: str,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Optional[TokenInstance]:
        """Get a short-lived token for the specified configuration."""
        config_key = f"{token_type.value}:{name}"

        if config_key not in self.token_configs:
            logger.error(f"Token configuration not found: {config_key}")
            return None

        config = self.token_configs[config_key]

        # Check if we need to rotate based on policy
        if config.auto_rotate and config.rotation_policy == RotationPolicy.ON_ACCESS:
            # Lazy rotation - create new token on each access
            return await self._create_new_token(config, client_ip, user_agent, session_id)

        # Find or create a valid token
        existing_token = await self._find_valid_token(config)
        if existing_token:
            existing_token.record_usage(client_ip, user_agent)
            return existing_token

        # Create new token
        return await self._create_new_token(config, client_ip, user_agent, session_id)

    async def _create_new_token(
        self,
        config: TokenConfig,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Optional[TokenInstance]:
        """Create a new token instance."""
        try:
            # Generate unique token ID
            token_id = str(uuid.uuid4())

            # Create Vault token
            vault_response = await self._create_vault_token(config)

            if not vault_response:
                logger.error(f"Failed to create Vault token for {config.name}")
                return None

            # Create token instance
            expires_at = datetime.now() + timedelta(seconds=config.ttl_seconds)

            token_instance = TokenInstance(
                token_id=token_id,
                token_config=config,
                vault_token=vault_response['client_token'],
                created_at=datetime.now(),
                expires_at=expires_at,
                issued_by="secrets_rotation_manager",
                client_ip=client_ip,
                user_agent=user_agent,
                session_id=session_id
            )

            # Store token
            self.active_tokens[token_id] = token_instance
            config.last_rotated = datetime.now()
            config.rotation_count += 1

            # Emit metrics
            self.metrics.increment_counter("secrets.tokens_created")
            self.metrics.gauge("secrets.active_tokens", len(self.active_tokens))

            # Audit event
            await self._record_audit_event("token_created", {
                "token_id": token_id,
                "token_type": config.token_type.value,
                "name": config.name,
                "ttl_seconds": config.ttl_seconds
            })

            logger.info(f"Created new token {token_id} for {config.token_type.value}:{config.name}")
            return token_instance

        except Exception as e:
            logger.error(f"Failed to create token for {config.name}: {e}")
            return None

    async def _create_vault_token(self, config: TokenConfig) -> Optional[Dict[str, Any]]:
        """Create a token in Vault with appropriate policies."""
        try:
            # Create or update Vault policy
            await self._create_vault_policy(config)

            # Create token with policies
            token_data = {
                "policies": [config.get_vault_policy_name()] + config.policies,
                "ttl": f"{config.ttl_seconds}s",
                "renewable": True,
                "no_parent": True
            }

            # Add IP binding if enabled
            if self.rotation_config.enable_ip_binding and config.bind_to_ip and config.allowed_ips:
                token_data["bound_cidrs"] = config.allowed_ips

            # Create token via Vault API
            response = self.credential_provider.client.create_token(data=token_data)

            if response and 'auth' in response and 'client_token' in response['auth']:
                return response['auth']

            return None

        except Exception as e:
            logger.error(f"Failed to create Vault token: {e}")
            return None

    async def _create_vault_policy(self, config: TokenConfig):
        """Create or update Vault policy for token configuration."""
        try:
            policy_name = config.get_vault_policy_name()

            # Define policy based on token type
            policy_content = self._generate_policy_for_token_type(config.token_type)

            # Create policy via Vault API
            policy_data = {
                "name": policy_name,
                "policy": policy_content
            }

            # This would typically use Vault's policy API
            # For now, we'll log the policy creation
            logger.info(f"Created/updated Vault policy {policy_name} for {config.token_type.value}")

        except Exception as e:
            logger.error(f"Failed to create Vault policy for {config.name}: {e}")

    def _generate_policy_for_token_type(self, token_type: TokenType) -> str:
        """Generate Vault policy content for token type."""
        policies = {
            TokenType.DATABASE: """
path "database/*" {
    capabilities = ["read"]
}
path "auth/token/renew" {
    capabilities = ["update"]
}
""",
            TokenType.API_CLIENT: """
path "secret/data/aurum/api/*" {
    capabilities = ["read"]
}
path "auth/token/renew" {
    capabilities = ["update"]
}
""",
            TokenType.SERVICE_ACCOUNT: """
path "secret/data/aurum/services/*" {
    capabilities = ["read"]
}
path "auth/token/renew" {
    capabilities = ["update"]
}
""",
            TokenType.KAFKA_PRODUCER: """
path "secret/data/aurum/kafka/*" {
    capabilities = ["read"]
}
path "auth/token/renew" {
    capabilities = ["update"]
}
""",
            TokenType.KAFKA_CONSUMER: """
path "secret/data/aurum/kafka/*" {
    capabilities = ["read"]
}
path "auth/token/renew" {
    capabilities = ["update"]
}
""",
            TokenType.EXTERNAL_API: """
path "secret/data/aurum/external/*" {
    capabilities = ["read"]
}
path "auth/token/renew" {
    capabilities = ["update"]
}
"""
        }

        return policies.get(token_type, policies[TokenType.API_CLIENT])

    async def _find_valid_token(self, config: TokenConfig) -> Optional[TokenInstance]:
        """Find a valid token for the configuration."""
        for token_id, token_instance in self.active_tokens.items():
            if (token_instance.token_config.name == config.name and
                token_instance.token_config.token_type == config.token_type and
                token_instance.can_be_used()):

                return token_instance
        return None

    async def rotate_token(self, token_id: str) -> bool:
        """Manually rotate a specific token."""
        if token_id not in self.active_tokens:
            logger.warning(f"Token not found: {token_id}")
            return False

        token = self.active_tokens[token_id]

        try:
            # Revoke old token
            await self.revoke_token(token_id, reason="manual_rotation")

            # Create new token
            new_token = await self._create_new_token(token.token_config)

            if new_token:
                logger.info(f"Successfully rotated token {token_id}")
                return True

        except Exception as e:
            logger.error(f"Failed to rotate token {token_id}: {e}")

        return False

    async def revoke_token(self, token_id: str, reason: str = "unknown") -> bool:
        """Revoke a token."""
        if token_id not in self.active_tokens:
            logger.warning(f"Token not found for revocation: {token_id}")
            return False

        token = self.active_tokens[token_id]

        try:
            # Revoke in Vault
            self.credential_provider.client.revoke_token(token.vault_token)

            # Update token state
            token.is_revoked = True
            self.revoked_tokens.add(token_id)

            # Move to separate storage for audit
            del self.active_tokens[token_id]

            # Audit event
            await self._record_audit_event("token_revoked", {
                "token_id": token_id,
                "reason": reason,
                "token_type": token.token_config.token_type.value,
                "name": token.token_config.name
            })

            # Emit metrics
            self.metrics.increment_counter("secrets.tokens_revoked")

            logger.info(f"Revoked token {token_id} - Reason: {reason}")
            return True

        except Exception as e:
            logger.error(f"Failed to revoke token {token_id}: {e}")
            return False

    async def cleanup_expired_tokens(self):
        """Clean up expired and revoked tokens."""
        expired_tokens = []
        revoked_tokens_to_remove = []

        current_time = datetime.now()

        # Find expired tokens
        for token_id, token in self.active_tokens.items():
            if token.is_expired() or token.is_revoked:
                expired_tokens.append(token_id)

        # Find old revoked tokens to remove from tracking
        for token_id in self.revoked_tokens:
            if token_id in self.active_tokens:
                # Token was revoked but still in active list
                expired_tokens.append(token_id)
                revoked_tokens_to_remove.append(token_id)

        # Clean up tokens
        for token_id in expired_tokens:
            token = self.active_tokens.get(token_id)
            if token:
                await self._record_audit_event("token_expired", {
                    "token_id": token_id,
                    "token_type": token.token_config.token_type.value,
                    "name": token.token_config.name
                })

            del self.active_tokens[token_id]

        # Remove from revoked set
        for token_id in revoked_tokens_to_remove:
            self.revoked_tokens.discard(token_id)

        if expired_tokens:
            self.metrics.gauge("secrets.active_tokens", len(self.active_tokens))
            logger.info(f"Cleaned up {len(expired_tokens)} expired/revoked tokens")

    async def _start_background_rotation(self):
        """Start background rotation service."""
        async def rotation_worker():
            while True:
                try:
                    # Process rotation queue
                    if not self.rotation_queue.empty():
                        rotation_task = await self.rotation_queue.get()
                        await rotation_task
                        self.rotation_queue.task_done()

                    # Check for tokens that need rotation
                    await self._check_and_queue_rotations()

                    # Clean up expired tokens
                    await self.cleanup_expired_tokens()

                    # Wait before next cycle
                    await asyncio.sleep(60)  # Check every minute

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in rotation worker: {e}")
                    await asyncio.sleep(30)

        # Start worker task
        task = asyncio.create_task(rotation_worker())
        self.rotation_tasks.add(task)
        task.add_done_callback(self.rotation_tasks.discard)

        logger.info("Background rotation service started")

    async def _check_and_queue_rotations(self):
        """Check for tokens that need rotation and queue them."""
        current_time = datetime.now()

        for config_key, config in self.token_configs.items():
            if not config.auto_rotate:
                continue

            # Check if rotation is needed based on policy
            if config.rotation_policy == RotationPolicy.SCHEDULED:
                if config.last_rotated:
                    time_since_rotation = current_time - config.last_rotated
                    if time_since_rotation.total_seconds() > (config.rotation_interval_hours * 3600):
                        await self._queue_token_rotation(config)
            elif config.rotation_policy == RotationPolicy.ON_ACCESS:
                # Lazy rotation handled in get_short_lived_token
                pass

    async def _queue_token_rotation(self, config: TokenConfig):
        """Queue rotation for a token configuration."""
        async def rotate_config():
            try:
                # Find all active tokens for this config
                tokens_to_rotate = [
                    token for token in self.active_tokens.values()
                    if (token.token_config.name == config.name and
                        token.token_config.token_type == config.token_type)
                ]

                # Rotate tokens in batches
                batch_size = min(self.rotation_config.rotation_batch_size, len(tokens_to_rotate))

                for i in range(0, len(tokens_to_rotate), batch_size):
                    batch = tokens_to_rotate[i:i + batch_size]

                    # Rotate batch
                    rotation_tasks = [self.rotate_token(token.token_id) for token in batch]
                    await asyncio.gather(*rotation_tasks, return_exceptions=True)

                    # Brief pause between batches
                    if i + batch_size < len(tokens_to_rotate):
                        await asyncio.sleep(1)

                logger.info(f"Completed rotation for {config.token_type.value}:{config.name}")

            except Exception as e:
                logger.error(f"Failed to rotate tokens for {config.token_type.value}:{config.name}: {e}")

        # Queue the rotation task
        await self.rotation_queue.put(rotate_config())

    async def _record_audit_event(self, event_type: str, details: Dict[str, Any]):
        """Record an audit event."""
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "details": details,
            "source": "secrets_rotation_manager"
        }

        self.audit_events.append(event)

        # Emit metrics
        self.metrics.increment_counter(f"secrets.audit.{event_type}")

        # Log structured event
        self.logger.log(
            LogLevel.INFO,
            f"Audit event: {event_type}",
            event_type,
            **details
        )

    async def _load_token_configurations(self):
        """Load existing token configurations from Vault."""
        try:
            # This would load configurations from Vault or configuration store
            # For now, we'll start with empty configurations
            pass
        except Exception as e:
            logger.warning(f"Failed to load token configurations: {e}")

    async def get_rotation_status(self) -> Dict[str, Any]:
        """Get current rotation status."""
        return {
            "active_tokens": len(self.active_tokens),
            "token_configurations": len(self.token_configs),
            "revoked_tokens": len(self.revoked_tokens),
            "is_rotating": self.is_rotating,
            "rotation_queue_size": self.rotation_queue.qsize(),
            "last_audit_event": self.audit_events[-1] if self.audit_events else None
        }

    async def shutdown(self):
        """Shutdown the secrets rotation system."""
        # Cancel rotation tasks
        for task in self.rotation_tasks:
            task.cancel()

        # Revoke all active tokens
        for token_id in list(self.active_tokens.keys()):
            await self.revoke_token(token_id, "system_shutdown")

        logger.info("Secrets rotation system shutdown complete")


# Global rotation manager
_global_rotation_manager: Optional[SecretsRotationManager] = None


async def get_secrets_rotation_manager() -> SecretsRotationManager:
    """Get or create the global secrets rotation manager."""
    global _global_rotation_manager

    if _global_rotation_manager is None:
        vault_config = VaultConfig()
        rotation_config = SecretRotationConfig()
        _global_rotation_manager = SecretsRotationManager(vault_config, rotation_config)
        await _global_rotation_manager.initialize()

    return _global_rotation_manager


async def initialize_secrets_rotation():
    """Initialize the secrets rotation system."""
    manager = await get_secrets_rotation_manager()
    return manager
