"""Vault client for secure credential management.

This module provides integration with HashiCorp Vault for managing
API keys, database credentials, and other sensitive configuration.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import hvac
    VAULT_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    hvac = None
    VAULT_AVAILABLE = False

from aurum.observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


@dataclass
class VaultConfig:
    """Configuration for Vault client."""

    vault_url: str = ""
    vault_token: Optional[str] = None
    vault_role_id: Optional[str] = None
    vault_secret_id: Optional[str] = None
    vault_path_prefix: str = "aurum"
    vault_mount_point: str = "secret"
    enable_kubernetes_auth: bool = False
    kubernetes_role: str = "aurum"
    kubernetes_token_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    # SSL/TLS configuration
    verify_ssl: bool = True
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ca_cert_file: Optional[str] = None

    def __post_init__(self):
        self.vault_url = os.getenv("VAULT_ADDR", self.vault_url)
        self.vault_token = os.getenv("VAULT_TOKEN", self.vault_token)
        self.vault_role_id = os.getenv("VAULT_ROLE_ID", self.vault_role_id)
        self.vault_secret_id = os.getenv("VAULT_SECRET_ID", self.vault_secret_id)

        if not self.vault_url:
            raise ValueError("Vault URL is required")


class _StubVaultClient:
    """Minimal in-memory Vault client used when hvac is unavailable."""

    def __init__(self):
        self.token: Optional[str] = None
        self._tokens: Dict[str, Dict[str, Any]] = {}
        self._secrets: Dict[str, Dict[str, Any]] = {}

    def create_token(self, data: Dict[str, Any]) -> Dict[str, Any]:
        token = f"stub-{uuid.uuid4()}"
        self._tokens[token] = {"data": data, "revoked": False}
        return {"auth": {"client_token": token}}

    def revoke_token(self, token: str) -> None:
        if token in self._tokens:
            self._tokens[token]["revoked"] = True

    def read(self, path: str) -> Dict[str, Any]:
        return {"data": self._secrets.get(path, {})}

    def write(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        self._secrets[path] = data
        return {"data": data}

    def list_secret_backends(self) -> Dict[str, Any]:
        return {"data": ["secret/"]}

    # Compatibility helpers used by SecretsRotationManager tests
    def auth_approle(self, *args, **kwargs):
        return {"auth": {"client_token": f"stub-{uuid.uuid4()}"}}

    def auth_kubernetes(self, *args, **kwargs):
        return {"auth": {"client_token": f"stub-{uuid.uuid4()}"}}


class VaultCredentialProvider:
    """Provides credentials from Vault with caching and renewal."""

    def __init__(self, config: VaultConfig):
        self.config = config
        self._use_stub = not VAULT_AVAILABLE
        self.client: Optional[Any] = None
        self._credential_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, float] = {}
        self._cache_ttl: Dict[str, int] = {}
        self.metrics = get_metrics_client()

        # Cache settings
        self.default_cache_ttl = 300  # 5 minutes
        self.critical_cache_ttl = 60  # 1 minute for critical credentials

    async def initialize(self):
        """Initialize the Vault client."""
        try:
            self.client = await self._create_vault_client()
            await self._authenticate()

            # Test the connection
            await self._test_connection()

            logger.info("Vault client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Vault client: {e}")
            raise

    async def _create_vault_client(self) -> Any:
        """Create and configure the Vault client."""
        if self._use_stub:
            return _StubVaultClient()

        client = hvac.Client(
            url=self.config.vault_url,
            verify=self.config.verify_ssl,
        )

        # SSL configuration
        if self.config.cert_file and self.config.key_file:
            client.cert = (self.config.cert_file, self.config.key_file)

        if self.config.ca_cert_file:
            client.verify = self.config.ca_cert_file

        return client

    async def _authenticate(self):
        """Authenticate with Vault."""
        if not self.client:
            raise RuntimeError("Vault client not initialized")

        if self._use_stub:
            # Stub client does not require authentication
            self.client.token = "stub-token"
            return

        if self.config.vault_token:
            # Token authentication
            self.client.token = self.config.vault_token
            logger.info("Using token authentication")

        elif self.config.vault_role_id and self.config.vault_secret_id:
            # AppRole authentication
            auth_response = self.client.auth_approle(
                role_id=self.config.vault_role_id,
                secret_id=self.config.vault_secret_id
            )

            if auth_response and auth_response['auth']['client_token']:
                self.client.token = auth_response['auth']['client_token']
                logger.info("AppRole authentication successful")
            else:
                raise RuntimeError("AppRole authentication failed")

        elif self.config.enable_kubernetes_auth:
            # Kubernetes authentication
            with open(self.config.kubernetes_token_path, 'r') as f:
                jwt_token = f.read().strip()

            auth_response = self.client.auth_kubernetes(
                role=self.config.kubernetes_role,
                jwt=jwt_token
            )

            if auth_response and auth_response['auth']['client_token']:
                self.client.token = auth_response['auth']['client_token']
                logger.info("Kubernetes authentication successful")
            else:
                raise RuntimeError("Kubernetes authentication failed")

        else:
            raise RuntimeError("No authentication method configured")

    async def _test_connection(self):
        """Test the Vault connection."""
        if not self.client:
            raise RuntimeError("Vault client not initialized")

        try:
            # Try to read from a known path or list mounts
            self.client.list_secret_backends()
            logger.info("Vault connection test successful")

        except Exception as e:
            logger.error(f"Vault connection test failed: {e}")
            raise

    async def get_credential(self, path: str, key: str, cache: bool = True) -> Optional[str]:
        """Get a credential from Vault."""
        if not self.client:
            await self.initialize()

        cache_key = f"{path}:{key}"

        # Check cache first
        if cache and cache_key in self._credential_cache:
            cache_time = self._cache_timestamps.get(cache_key, 0)
            ttl = self._cache_ttl.get(cache_key, self.default_cache_ttl)

            if time.time() - cache_time < ttl:
                return self._credential_cache[cache_key].get(key)

        try:
            # Read from Vault
            secret_path = f"{self.config.vault_path_prefix}/{path}"
            response = self.client.read(secret_path)

            if response and 'data' in response:
                data = response['data']

                # Cache the entire response
                if cache:
                    self._credential_cache[cache_key] = data
                    self._cache_timestamps[cache_key] = time.time()
                    self._cache_ttl[cache_key] = self.default_cache_ttl

                credential = data.get(key)
                if credential:
                    self.metrics.increment_counter("vault.credentials_accessed")
                    return credential

            self.metrics.increment_counter("vault.credentials_not_found")
            logger.warning(f"Credential not found: {path}:{key}")

        except Exception as e:
            self.metrics.increment_counter("vault.credentials_access_errors")
            logger.error(f"Error accessing credential {path}:{key}: {e}")

        return None

    async def get_api_key(self, iso_code: str) -> Optional[str]:
        """Get API key for an ISO."""
        return await self.get_credential(f"api_keys/{iso_code}", "key")

    async def get_database_credentials(self, database_name: str) -> Optional[Dict[str, str]]:
        """Get database credentials."""
        credentials = {}

        for field in ['username', 'password', 'host', 'port', 'database']:
            value = await self.get_credential(f"databases/{database_name}", field, cache=False)
            if value:
                credentials[field] = value

        return credentials if len(credentials) == 5 else None

    async def get_kafka_credentials(self) -> Optional[Dict[str, str]]:
        """Get Kafka credentials."""
        credentials = {}

        for field in ['bootstrap_servers', 'security_protocol', 'sasl_mechanism', 'sasl_username', 'sasl_password']:
            value = await self.get_credential("kafka", field)
            if value:
                credentials[field] = value

        return credentials if credentials else None

    async def get_aws_credentials(self) -> Optional[Dict[str, str]]:
        """Get AWS credentials."""
        credentials = {}

        for field in ['access_key_id', 'secret_access_key', 'region']:
            value = await self.get_credential("aws", field, cache=False)
            if value:
                credentials[field] = value

        return credentials if len(credentials) >= 2 else None

    async def refresh_cache(self, path: str = None):
        """Refresh the credential cache."""
        if path:
            # Refresh specific path
            cache_keys_to_remove = [k for k in self._credential_cache.keys() if k.startswith(f"{path}:")]
            for key in cache_keys_to_remove:
                self._credential_cache.pop(key, None)
                self._cache_timestamps.pop(key, None)
                self._cache_ttl.pop(key, None)
        else:
            # Refresh all cache
            self._credential_cache.clear()
            self._cache_timestamps.clear()
            self._cache_ttl.clear()

        logger.info("Credential cache refreshed")

    async def health_check(self) -> Dict[str, Any]:
        """Perform a health check of the Vault integration."""
        if not self.client:
            return {"status": "error", "message": "Vault client not initialized"}

        try:
            # Test authentication
            auth_status = self.client.is_authenticated()

            # Test read access
            test_path = f"{self.config.vault_path_prefix}/test"
            try:
                self.client.read(test_path)
                read_access = True
            except:
                read_access = False

            status = "healthy" if auth_status and read_access else "degraded"

            return {
                "status": status,
                "authenticated": auth_status,
                "read_access": read_access,
                "cache_size": len(self._credential_cache),
                "vault_url": self.config.vault_url
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}


# Global vault provider instance
_global_vault_provider: Optional[VaultCredentialProvider] = None


async def get_vault_provider() -> VaultCredentialProvider:
    """Get or create the global vault provider."""
    global _global_vault_provider

    if _global_vault_provider is None:
        config = VaultConfig()
        _global_vault_provider = VaultCredentialProvider(config)
        await _global_vault_provider.initialize()

    return _global_vault_provider


# Convenience functions for common credentials
async def get_iso_api_key(iso_code: str) -> Optional[str]:
    """Get API key for an ISO from Vault."""
    provider = await get_vault_provider()
    return await provider.get_api_key(iso_code.upper())


async def get_database_config(database_name: str) -> Optional[Dict[str, str]]:
    """Get database configuration from Vault."""
    provider = await get_vault_provider()
    return await provider.get_database_credentials(database_name)


async def get_kafka_config() -> Optional[Dict[str, str]]:
    """Get Kafka configuration from Vault."""
    provider = await get_vault_provider()
    return await provider.get_kafka_credentials()


async def get_aws_config() -> Optional[Dict[str, str]]:
    """Get AWS configuration from Vault."""
    provider = await get_vault_provider()
    return await provider.get_aws_credentials()


# Patch environment variables with Vault credentials
async def patch_environment_with_vault_credentials():
    """Patch environment variables with credentials from Vault."""
    try:
        provider = await get_vault_provider()

        # Patch ISO API keys
        for iso_code in ['CAISO', 'MISO', 'PJM', 'ERCOT', 'SPP']:
            api_key = await provider.get_api_key(iso_code)
            if api_key:
                env_var = f"AURUM_{iso_code}_API_KEY"
                os.environ[env_var] = api_key
                logger.info(f"Patched environment variable: {env_var}")

        # Patch database credentials
        db_creds = await provider.get_database_credentials("aurum")
        if db_creds:
            os.environ["AURUM_DATABASE_URL"] = (
                f"postgresql://{db_creds['username']}:{db_creds['password']}@"
                f"{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
            )
            logger.info("Patched database URL")

        # Patch Kafka credentials
        kafka_config = await provider.get_kafka_credentials()
        if kafka_config:
            if 'bootstrap_servers' in kafka_config:
                os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka_config['bootstrap_servers']
            if 'sasl_username' in kafka_config:
                os.environ["KAFKA_SASL_USERNAME"] = kafka_config['sasl_username']
            if 'sasl_password' in kafka_config:
                os.environ["KAFKA_SASL_PASSWORD"] = kafka_config['sasl_password']
            logger.info("Patched Kafka configuration")

        # Patch AWS credentials
        aws_config = await provider.get_aws_credentials()
        if aws_config:
            if 'access_key_id' in aws_config:
                os.environ["AWS_ACCESS_KEY_ID"] = aws_config['access_key_id']
            if 'secret_access_key' in aws_config:
                os.environ["AWS_SECRET_ACCESS_KEY"] = aws_config['secret_access_key']
            if 'region' in aws_config:
                os.environ["AWS_DEFAULT_REGION"] = aws_config['region']
            logger.info("Patched AWS configuration")

        logger.info("Environment patched with Vault credentials")

    except Exception as e:
        logger.error(f"Failed to patch environment with Vault credentials: {e}")


# Configuration loader for ISO adapters
class VaultConfigLoader:
    """Loads configuration from Vault for ISO adapters."""

    def __init__(self):
        self.vault_provider: Optional[VaultCredentialProvider] = None

    async def initialize(self):
        """Initialize the vault provider."""
        self.vault_provider = await get_vault_provider()

    async def get_iso_config(self, iso_code: str) -> Dict[str, Any]:
        """Get configuration for an ISO from Vault."""
        if not self.vault_provider:
            await self.initialize()

        config = {
            "api_key": await self.vault_provider.get_api_key(iso_code),
            "base_url": await self.vault_provider.get_credential(f"config/{iso_code}", "base_url"),
            "timeout": int(await self.vault_provider.get_credential(f"config/{iso_code}", "timeout") or 30),
            "max_retries": int(await self.vault_provider.get_credential(f"config/{iso_code}", "max_retries") or 3),
            "rate_limit": int(await self.vault_provider.get_credential(f"config/{iso_code}", "rate_limit") or 60)
        }

        # Remove None values
        return {k: v for k, v in config.items() if v is not None}
