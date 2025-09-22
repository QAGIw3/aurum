"""Airflow integration for Vault-backed secrets rotation and short-lived tokens."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import os

from .secrets_rotation_manager import (
    SecretsRotationManager,
    TokenType,
    TokenInstance,
    get_secrets_rotation_manager
)
from ..logging import StructuredLogger, LogLevel, create_logger

logger = logging.getLogger(__name__)


class AirflowSecretsManager:
    """Manager for Airflow DAG secrets and short-lived tokens."""

    def __init__(self):
        self.rotation_manager: Optional[SecretsRotationManager] = None
        self.logger = create_logger(
            source_name="airflow_secrets",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.airflow.secrets",
            dataset="airflow_secrets"
        )

    async def initialize(self):
        """Initialize the Airflow secrets manager."""
        self.rotation_manager = await get_secrets_rotation_manager()
        logger.info("Airflow secrets manager initialized")

    async def get_database_credentials(self, db_name: str) -> Dict[str, str]:
        """Get database credentials with short-lived tokens."""
        token = await self.rotation_manager.get_short_lived_token(
            TokenType.DATABASE,
            db_name,
            client_ip=self._get_client_ip()
        )

        if not token:
            raise RuntimeError(f"Failed to get database token for {db_name}")

        # Get database credentials from Vault using the token
        credentials = await self._get_credentials_from_vault(token, f"database/{db_name}")

        return {
            'username': credentials.get('username', ''),
            'password': credentials.get('password', ''),
            'host': credentials.get('host', ''),
            'port': credentials.get('port', ''),
            'database': credentials.get('database', ''),
            'connection_string': credentials.get('connection_string', ''),
            'ssl_mode': credentials.get('ssl_mode', 'prefer')
        }

    async def get_kafka_credentials(self, kafka_type: str = "producer") -> Dict[str, str]:
        """Get Kafka credentials with short-lived tokens."""
        token_type = TokenType.KAFKA_PRODUCER if kafka_type == "producer" else TokenType.KAFKA_CONSUMER
        token_name = f"kafka_{kafka_type}"

        token = await self.rotation_manager.get_short_lived_token(
            token_type,
            token_name,
            client_ip=self._get_client_ip()
        )

        if not token:
            raise RuntimeError(f"Failed to get Kafka token for {kafka_type}")

        # Get Kafka credentials from Vault using the token
        credentials = await self._get_credentials_from_vault(token, f"kafka/{kafka_type}")

        return {
            'bootstrap_servers': credentials.get('bootstrap_servers', ''),
            'security_protocol': credentials.get('security_protocol', 'PLAINTEXT'),
            'sasl_mechanism': credentials.get('sasl_mechanism', ''),
            'sasl_username': credentials.get('sasl_username', ''),
            'sasl_password': credentials.get('sasl_password', ''),
            'ssl_cafile': credentials.get('ssl_cafile', ''),
            'ssl_certfile': credentials.get('ssl_certfile', ''),
            'ssl_keyfile': credentials.get('ssl_keyfile', ''),
        }

    async def get_api_credentials(self, api_name: str) -> Dict[str, str]:
        """Get API credentials with short-lived tokens."""
        token = await self.rotation_manager.get_short_lived_token(
            TokenType.EXTERNAL_API,
            api_name,
            client_ip=self._get_client_ip()
        )

        if not token:
            raise RuntimeError(f"Failed to get API token for {api_name}")

        # Get API credentials from Vault using the token
        credentials = await self._get_credentials_from_vault(token, f"external/{api_name}")

        return {
            'api_key': credentials.get('api_key', ''),
            'api_secret': credentials.get('api_secret', ''),
            'base_url': credentials.get('base_url', ''),
            'auth_url': credentials.get('auth_url', ''),
            'client_id': credentials.get('client_id', ''),
            'client_secret': credentials.get('client_secret', ''),
        }

    async def get_service_account_token(self, service_name: str) -> str:
        """Get service account token for internal services."""
        token = await self.rotation_manager.get_short_lived_token(
            TokenType.SERVICE_ACCOUNT,
            service_name,
            client_ip=self._get_client_ip()
        )

        if not token:
            raise RuntimeError(f"Failed to get service token for {service_name}")

        return token.vault_token

    async def _get_credentials_from_vault(self, token: TokenInstance, path: str) -> Dict[str, Any]:
        """Get credentials from Vault using the token."""
        try:
            # This would use the token to authenticate with Vault
            # For now, we'll simulate the credential retrieval
            # In a real implementation, this would create a new Vault client with the token

            # Simulate credential retrieval
            # In practice, this would be:
            # temp_client = hvac.Client(url=self.vault_url, token=token.vault_token)
            # response = temp_client.read(f"{self.vault_path_prefix}/{path}")
            # return response['data']['data'] if response else {}

            # For demonstration, return mock credentials
            mock_credentials = {
                'database/test': {
                    'username': 'aurum_user',
                    'password': 'mock_password',
                    'host': 'localhost',
                    'port': '5432',
                    'database': 'aurum_test'
                },
                'kafka/producer': {
                    'bootstrap_servers': 'localhost:9092',
                    'security_protocol': 'PLAINTEXT'
                },
                'kafka/consumer': {
                    'bootstrap_servers': 'localhost:9092',
                    'security_protocol': 'PLAINTEXT'
                },
                'external/test_api': {
                    'api_key': 'mock_api_key',
                    'base_url': 'https://api.example.com'
                }
            }

            return mock_credentials.get(path, {})

        except Exception as e:
            logger.error(f"Failed to get credentials from Vault for {path}: {e}")
            raise

    def _get_client_ip(self) -> Optional[str]:
        """Get client IP address for token tracking."""
        try:
            # In Airflow, this could be the worker IP or task instance info
            return os.environ.get('AIRFLOW_CTX_DAG_ID', 'unknown')
        except Exception:
            return None

    async def rotate_dag_secrets(self, dag_id: str) -> bool:
        """Manually rotate all secrets for a DAG."""
        try:
            # Find all tokens associated with this DAG
            dag_tokens = [
                token_id for token_id, token in self.rotation_manager.active_tokens.items()
                if token.session_id == dag_id
            ]

            rotation_success = True

            for token_id in dag_tokens:
                success = await self.rotation_manager.rotate_token(token_id)
                if not success:
                    rotation_success = False

            if rotation_success:
                logger.info(f"Successfully rotated secrets for DAG {dag_id}")
            else:
                logger.warning(f"Some secrets failed to rotate for DAG {dag_id}")

            return rotation_success

        except Exception as e:
            logger.error(f"Failed to rotate secrets for DAG {dag_id}: {e}")
            return False

    async def get_rotation_status(self, dag_id: Optional[str] = None) -> Dict[str, Any]:
        """Get rotation status for DAG or all DAGs."""
        status = await self.rotation_manager.get_rotation_status()

        if dag_id:
            # Filter for specific DAG
            dag_tokens = [
                token for token in self.rotation_manager.active_tokens.values()
                if token.session_id == dag_id
            ]

            status.update({
                'dag_id': dag_id,
                'dag_tokens': len(dag_tokens),
                'dag_token_details': [
                    {
                        'token_id': token.token_id,
                        'token_type': token.token_config.token_type.value,
                        'name': token.token_config.name,
                        'expires_at': token.expires_at.isoformat(),
                        'is_near_expiry': token.is_near_expiry()
                    }
                    for token in dag_tokens
                ]
            })

        return status


class DAGSecretsContext:
    """Context manager for DAG secrets with automatic rotation."""

    def __init__(self, dag_id: str, secrets_manager: AirflowSecretsManager):
        self.dag_id = dag_id
        self.secrets_manager = secrets_manager
        self.tokens: List[TokenInstance] = []

    async def __aenter__(self):
        """Enter the DAG secrets context."""
        logger.info(f"Entering secrets context for DAG {self.dag_id}")

        # Record the DAG session
        self.session_context = {
            'dag_id': self.dag_id,
            'start_time': datetime.now(),
            'secrets_acquired': []
        }

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the DAG secrets context."""
        logger.info(f"Exiting secrets context for DAG {self.dag_id}")

        # Clean up tokens
        for token in self.tokens:
            try:
                await self.secrets_manager.rotation_manager.revoke_token(
                    token.token_id,
                    "dag_context_exit"
                )
            except Exception as e:
                logger.warning(f"Failed to revoke token {token.token_id}: {e}")

        # Log context completion
        duration = datetime.now() - self.session_context['start_time']
        logger.info(
            f"DAG {self.dag_id} secrets context completed in {duration.total_seconds():.2f}s",
            "dag_secrets_context_completed",
            dag_id=self.dag_id,
            duration_seconds=duration.total_seconds(),
            secrets_count=len(self.session_context['secrets_acquired'])
        )

    async def get_database_credentials(self, db_name: str) -> Dict[str, str]:
        """Get database credentials within the DAG context."""
        token = await self.secrets_manager.get_database_credentials(db_name)
        if token:
            self.tokens.append(token)
            self.session_context['secrets_acquired'].append({
                'type': 'database',
                'name': db_name,
                'token_id': token.token_id
            })

        return await self.secrets_manager.get_database_credentials(db_name)

    async def get_kafka_credentials(self, kafka_type: str = "producer") -> Dict[str, str]:
        """Get Kafka credentials within the DAG context."""
        token = await self.secrets_manager.get_kafka_credentials(kafka_type)
        if token:
            self.tokens.append(token)
            self.session_context['secrets_acquired'].append({
                'type': 'kafka',
                'name': kafka_type,
                'token_id': token.token_id
            })

        return token

    async def get_api_credentials(self, api_name: str) -> Dict[str, str]:
        """Get API credentials within the DAG context."""
        token = await self.secrets_manager.get_api_credentials(api_name)
        if token:
            self.tokens.append(token)
            self.session_context['secrets_acquired'].append({
                'type': 'api',
                'name': api_name,
                'token_id': token.token_id
            })

        return token


# Global Airflow secrets manager
_global_airflow_secrets: Optional[AirflowSecretsManager] = None


async def get_airflow_secrets_manager() -> AirflowSecretsManager:
    """Get or create the global Airflow secrets manager."""
    global _global_airflow_secrets

    if _global_airflow_secrets is None:
        _global_airflow_secrets = AirflowSecretsManager()
        await _global_airflow_secrets.initialize()

    return _global_airflow_secrets


@asynccontextmanager
async def dag_secrets_context(dag_id: str):
    """Context manager for DAG secrets with automatic cleanup."""
    secrets_manager = await get_airflow_secrets_manager()
    context = DAGSecretsContext(dag_id, secrets_manager)

    async with context:
        yield context


def get_database_credentials(db_name: str) -> Dict[str, str]:
    """Synchronous wrapper for getting database credentials."""
    return asyncio.run(_get_database_credentials_async(db_name))


async def _get_database_credentials_async(db_name: str) -> Dict[str, str]:
    """Async implementation of database credentials retrieval."""
    secrets_manager = await get_airflow_secrets_manager()
    return await secrets_manager.get_database_credentials(db_name)


def get_kafka_credentials(kafka_type: str = "producer") -> Dict[str, str]:
    """Synchronous wrapper for getting Kafka credentials."""
    return asyncio.run(_get_kafka_credentials_async(kafka_type))


async def _get_kafka_credentials_async(kafka_type: str) -> Dict[str, str]:
    """Async implementation of Kafka credentials retrieval."""
    secrets_manager = await get_airflow_secrets_manager()
    return await secrets_manager.get_kafka_credentials(kafka_type)


def get_api_credentials(api_name: str) -> Dict[str, str]:
    """Synchronous wrapper for getting API credentials."""
    return asyncio.run(_get_api_credentials_async(api_name))


async def _get_api_credentials_async(api_name: str) -> Dict[str, str]:
    """Async implementation of API credentials retrieval."""
    secrets_manager = await get_airflow_secrets_manager()
    return await secrets_manager.get_api_credentials(api_name)


def get_service_account_token(service_name: str) -> str:
    """Synchronous wrapper for getting service account token."""
    return asyncio.run(_get_service_account_token_async(service_name))


async def _get_service_account_token_async(service_name: str) -> str:
    """Async implementation of service account token retrieval."""
    secrets_manager = await get_airflow_secrets_manager()
    return await secrets_manager.get_service_account_token(service_name)


# Utility functions for common patterns
async def get_postgres_credentials(db_name: str = "aurum") -> Dict[str, str]:
    """Get PostgreSQL credentials with appropriate defaults."""
    creds = await _get_database_credentials_async(db_name)

    # Add PostgreSQL-specific defaults
    creds.setdefault('port', '5432')
    creds.setdefault('ssl_mode', 'prefer')

    return creds


async def get_clickhouse_credentials(db_name: str = "aurum") -> Dict[str, str]:
    """Get ClickHouse credentials with appropriate defaults."""
    creds = await _get_database_credentials_async(db_name)

    # Add ClickHouse-specific defaults
    creds.setdefault('port', '9000')
    creds.setdefault('protocol', 'native')

    return creds


async def get_redis_credentials(service_name: str = "cache") -> Dict[str, str]:
    """Get Redis credentials with appropriate defaults."""
    creds = await _get_database_credentials_async(service_name)

    # Add Redis-specific defaults
    creds.setdefault('port', '6379')
    creds.setdefault('db', '0')

    return creds
