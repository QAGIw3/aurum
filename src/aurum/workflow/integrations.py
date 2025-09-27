"""Integration ecosystem and webhook management for Phase 4."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class IntegrationType(str, Enum):
    """Types of external integrations."""
    REST_API = "rest_api"
    WEBHOOK = "webhook"
    DATABASE = "database"
    MESSAGE_QUEUE = "message_queue"
    FILE_SYSTEM = "file_system"
    EMAIL = "email"
    CUSTOM = "custom"


class WebhookStatus(str, Enum):
    """Webhook delivery status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class APIIntegration:
    """REST API integration configuration."""
    
    integration_id: str
    name: str
    base_url: str
    authentication: Dict[str, Any] = field(default_factory=dict)
    default_headers: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 30
    rate_limit: Optional[Dict[str, Any]] = None
    retry_config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.retry_config:
            self.retry_config = {
                "max_attempts": 3,
                "backoff_multiplier": 2,
                "initial_delay_seconds": 1
            }


@dataclass
class WebhookEndpoint:
    """Webhook endpoint configuration."""
    
    endpoint_id: str
    name: str
    url: str
    events: List[str]  # Events that trigger this webhook
    secret: Optional[str] = None  # For signature verification
    headers: Dict[str, str] = field(default_factory=dict)
    active: bool = True
    retry_config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.retry_config:
            self.retry_config = {
                "max_attempts": 5,
                "backoff_multiplier": 2,
                "initial_delay_seconds": 2
            }


@dataclass
class WebhookDelivery:
    """Webhook delivery record."""
    
    delivery_id: str
    endpoint_id: str
    event_type: str
    payload: Dict[str, Any]
    status: WebhookStatus = WebhookStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    attempts: int = 0
    last_attempt_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    error_message: Optional[str] = None
    next_retry_at: Optional[datetime] = None


@dataclass
class IntegrationMarketplaceEntry:
    """Marketplace entry for third-party integrations."""
    
    entry_id: str
    name: str
    description: str
    category: str
    vendor: str
    version: str
    integration_type: IntegrationType
    configuration_schema: Dict[str, Any]
    installation_instructions: str
    pricing_model: str = "free"
    supported_events: List[str] = field(default_factory=list)
    ratings: Dict[str, Any] = field(default_factory=dict)
    downloads: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class WebhookManager:
    """Manages webhook deliveries and retries."""
    
    def __init__(self):
        self.endpoints: Dict[str, WebhookEndpoint] = {}
        self.deliveries: Dict[str, WebhookDelivery] = {}
        self.delivery_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
    
    def register_webhook(self, endpoint: WebhookEndpoint) -> None:
        """Register a webhook endpoint."""
        self.endpoints[endpoint.endpoint_id] = endpoint
        logger.info(f"Registered webhook endpoint: {endpoint.name} ({endpoint.url})")
    
    def unregister_webhook(self, endpoint_id: str) -> bool:
        """Unregister a webhook endpoint."""
        if endpoint_id in self.endpoints:
            del self.endpoints[endpoint_id]
            logger.info(f"Unregistered webhook endpoint: {endpoint_id}")
            return True
        return False
    
    async def trigger_webhook_event(self, event_type: str, payload: Dict[str, Any]) -> List[str]:
        """Trigger webhook event for all matching endpoints."""
        delivery_ids = []
        
        for endpoint in self.endpoints.values():
            if not endpoint.active or event_type not in endpoint.events:
                continue
            
            delivery_id = str(uuid4())
            delivery = WebhookDelivery(
                delivery_id=delivery_id,
                endpoint_id=endpoint.endpoint_id,
                event_type=event_type,
                payload=payload
            )
            
            self.deliveries[delivery_id] = delivery
            await self.delivery_queue.put(delivery_id)
            delivery_ids.append(delivery_id)
        
        logger.info(f"Triggered webhook event '{event_type}' for {len(delivery_ids)} endpoints")
        return delivery_ids
    
    async def start_delivery_worker(self) -> None:
        """Start the webhook delivery worker."""
        if self._worker_task and not self._worker_task.done():
            return
        
        self._worker_task = asyncio.create_task(self._delivery_worker())
        logger.info("Started webhook delivery worker")
    
    async def stop_delivery_worker(self) -> None:
        """Stop the webhook delivery worker."""
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped webhook delivery worker")
    
    async def _delivery_worker(self) -> None:
        """Worker that processes webhook deliveries."""
        while True:
            try:
                delivery_id = await self.delivery_queue.get()
                delivery = self.deliveries.get(delivery_id)
                
                if delivery:
                    await self._deliver_webhook(delivery)
                
                self.delivery_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in webhook delivery worker: {e}")
    
    async def _deliver_webhook(self, delivery: WebhookDelivery) -> None:
        """Deliver a single webhook."""
        endpoint = self.endpoints.get(delivery.endpoint_id)
        if not endpoint:
            delivery.status = WebhookStatus.FAILED
            delivery.error_message = "Endpoint not found"
            return
        
        delivery.status = WebhookStatus.PENDING
        delivery.attempts += 1
        delivery.last_attempt_at = datetime.now()
        
        try:
            # Prepare payload
            webhook_payload = {
                "event_type": delivery.event_type,
                "delivery_id": delivery.delivery_id,
                "timestamp": delivery.created_at.isoformat(),
                "data": delivery.payload
            }
            
            # Prepare headers
            headers = endpoint.headers.copy()
            headers.update({
                "Content-Type": "application/json",
                "User-Agent": "Aurum-Webhook/1.0",
                "X-Aurum-Event": delivery.event_type,
                "X-Aurum-Delivery": delivery.delivery_id
            })
            
            # Add signature if secret is configured
            if endpoint.secret:
                signature = self._generate_signature(
                    json.dumps(webhook_payload, sort_keys=True),
                    endpoint.secret
                )
                headers["X-Aurum-Signature"] = signature
            
            # Simulate HTTP request (in production, use aiohttp)
            await asyncio.sleep(0.1)  # Simulate network delay
            
            # Mock successful delivery
            delivery.status = WebhookStatus.DELIVERED
            delivery.delivered_at = datetime.now()
            delivery.response_status = 200
            delivery.response_body = "OK"
            
            logger.info(f"Webhook delivered successfully: {delivery.delivery_id}")
            
        except Exception as e:
            delivery.status = WebhookStatus.FAILED
            delivery.error_message = str(e)
            
            # Schedule retry if configured
            if delivery.attempts < endpoint.retry_config["max_attempts"]:
                retry_delay = self._calculate_retry_delay(delivery.attempts, endpoint.retry_config)
                delivery.next_retry_at = datetime.now() + timedelta(seconds=retry_delay)
                delivery.status = WebhookStatus.RETRYING
                
                # Re-queue for retry
                await asyncio.sleep(retry_delay)
                await self.delivery_queue.put(delivery.delivery_id)
                
                logger.info(f"Webhook delivery failed, retry scheduled: {delivery.delivery_id}")
            else:
                logger.error(f"Webhook delivery failed permanently: {delivery.delivery_id} - {e}")
    
    def _generate_signature(self, payload: str, secret: str) -> str:
        """Generate HMAC signature for webhook payload."""
        signature = hmac.new(
            secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()
        return f"sha256={signature}"
    
    def _calculate_retry_delay(self, attempt: int, retry_config: Dict[str, Any]) -> int:
        """Calculate retry delay using exponential backoff."""
        initial_delay = retry_config.get("initial_delay_seconds", 2)
        multiplier = retry_config.get("backoff_multiplier", 2)
        max_delay = retry_config.get("max_delay_seconds", 300)  # 5 minutes
        
        delay = initial_delay * (multiplier ** (attempt - 1))
        return min(delay, max_delay)
    
    def get_delivery_status(self, delivery_id: str) -> Optional[WebhookDelivery]:
        """Get delivery status by ID."""
        return self.deliveries.get(delivery_id)
    
    def get_endpoint_deliveries(self, endpoint_id: str) -> List[WebhookDelivery]:
        """Get all deliveries for an endpoint."""
        return [d for d in self.deliveries.values() if d.endpoint_id == endpoint_id]
    
    def get_failed_deliveries(self) -> List[WebhookDelivery]:
        """Get all failed deliveries."""
        return [d for d in self.deliveries.values() if d.status == WebhookStatus.FAILED]


class IntegrationManager:
    """Manages external system integrations and marketplace."""
    
    def __init__(self):
        self.api_integrations: Dict[str, APIIntegration] = {}
        self.marketplace: Dict[str, IntegrationMarketplaceEntry] = {}
        self.installed_integrations: Dict[str, Dict[str, Any]] = {}
        self.webhook_manager = WebhookManager()
        self._initialize_marketplace()
    
    def _initialize_marketplace(self) -> None:
        """Initialize marketplace with sample integrations."""
        sample_entries = [
            IntegrationMarketplaceEntry(
                entry_id="slack-integration",
                name="Slack Notifications",
                description="Send workflow notifications to Slack channels",
                category="Communication",
                vendor="Aurum Labs",
                version="1.0.0",
                integration_type=IntegrationType.WEBHOOK,
                configuration_schema={
                    "webhook_url": {"type": "string", "required": True},
                    "channel": {"type": "string", "required": True},
                    "username": {"type": "string", "default": "Aurum Bot"}
                },
                installation_instructions="Configure your Slack webhook URL and channel",
                supported_events=["workflow_completed", "workflow_failed", "data_quality_alert"]
            ),
            IntegrationMarketplaceEntry(
                entry_id="email-integration",
                name="Email Notifications",
                description="Send email notifications for workflow events",
                category="Communication",
                vendor="Aurum Labs",
                version="1.2.0",
                integration_type=IntegrationType.EMAIL,
                configuration_schema={
                    "smtp_server": {"type": "string", "required": True},
                    "smtp_port": {"type": "integer", "default": 587},
                    "username": {"type": "string", "required": True},
                    "password": {"type": "string", "required": True, "sensitive": True}
                },
                installation_instructions="Configure SMTP settings for email delivery",
                supported_events=["workflow_completed", "workflow_failed", "compliance_alert"]
            ),
            IntegrationMarketplaceEntry(
                entry_id="salesforce-integration",
                name="Salesforce CRM",
                description="Sync data with Salesforce CRM system",
                category="CRM",
                vendor="Third Party",
                version="2.1.0",
                integration_type=IntegrationType.REST_API,
                configuration_schema={
                    "instance_url": {"type": "string", "required": True},
                    "client_id": {"type": "string", "required": True},
                    "client_secret": {"type": "string", "required": True, "sensitive": True},
                    "username": {"type": "string", "required": True},
                    "password": {"type": "string", "required": True, "sensitive": True}
                },
                installation_instructions="Configure OAuth credentials for Salesforce API access",
                pricing_model="subscription",
                supported_events=["data_sync", "lead_created", "opportunity_updated"]
            )
        ]
        
        for entry in sample_entries:
            self.marketplace[entry.entry_id] = entry
    
    def register_api_integration(self, integration: APIIntegration) -> None:
        """Register an API integration."""
        self.api_integrations[integration.integration_id] = integration
        logger.info(f"Registered API integration: {integration.name}")
    
    def get_marketplace_entries(self, category: Optional[str] = None) -> List[IntegrationMarketplaceEntry]:
        """Get marketplace entries, optionally filtered by category."""
        entries = list(self.marketplace.values())
        if category:
            entries = [e for e in entries if e.category.lower() == category.lower()]
        return sorted(entries, key=lambda x: (x.downloads, x.name), reverse=True)
    
    def install_integration(self, entry_id: str, configuration: Dict[str, Any],
                          tenant_id: Optional[str] = None) -> str:
        """Install an integration from the marketplace."""
        entry = self.marketplace.get(entry_id)
        if not entry:
            raise ValueError(f"Integration not found in marketplace: {entry_id}")
        
        # Validate configuration against schema
        self._validate_configuration(configuration, entry.configuration_schema)
        
        installation_id = str(uuid4())
        installation = {
            "installation_id": installation_id,
            "entry_id": entry_id,
            "tenant_id": tenant_id,
            "configuration": configuration,
            "installed_at": datetime.now(),
            "version": entry.version,
            "active": True
        }
        
        self.installed_integrations[installation_id] = installation
        
        # Update download count
        entry.downloads += 1
        
        # Set up integration based on type
        if entry.integration_type == IntegrationType.WEBHOOK:
            self._setup_webhook_integration(installation, entry)
        elif entry.integration_type == IntegrationType.REST_API:
            self._setup_api_integration(installation, entry)
        
        logger.info(f"Installed integration: {entry.name} ({installation_id})")
        return installation_id
    
    def _validate_configuration(self, config: Dict[str, Any], schema: Dict[str, Any]) -> None:
        """Validate configuration against schema."""
        for field_name, field_schema in schema.items():
            if field_schema.get("required", False) and field_name not in config:
                raise ValueError(f"Required field missing: {field_name}")
            
            if field_name in config:
                field_type = field_schema.get("type")
                value = config[field_name]
                
                if field_type == "string" and not isinstance(value, str):
                    raise ValueError(f"Field {field_name} must be a string")
                elif field_type == "integer" and not isinstance(value, int):
                    raise ValueError(f"Field {field_name} must be an integer")
    
    def _setup_webhook_integration(self, installation: Dict[str, Any], 
                                 entry: IntegrationMarketplaceEntry) -> None:
        """Set up webhook-based integration."""
        config = installation["configuration"]
        
        endpoint = WebhookEndpoint(
            endpoint_id=f"integration_{installation['installation_id']}",
            name=f"{entry.name} Integration",
            url=config.get("webhook_url", ""),
            events=entry.supported_events
        )
        
        self.webhook_manager.register_webhook(endpoint)
    
    def _setup_api_integration(self, installation: Dict[str, Any],
                             entry: IntegrationMarketplaceEntry) -> None:
        """Set up API-based integration."""
        config = installation["configuration"]
        
        api_integration = APIIntegration(
            integration_id=f"integration_{installation['installation_id']}",
            name=f"{entry.name} Integration",
            base_url=config.get("base_url", config.get("instance_url", "")),
            authentication=self._extract_auth_config(config),
            timeout_seconds=config.get("timeout_seconds", 30)
        )
        
        self.register_api_integration(api_integration)
    
    def _extract_auth_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract authentication configuration."""
        auth_config = {}
        
        # OAuth2
        if "client_id" in config and "client_secret" in config:
            auth_config.update({
                "type": "oauth2",
                "client_id": config["client_id"],
                "client_secret": config["client_secret"]
            })
        
        # Basic auth
        if "username" in config and "password" in config:
            auth_config.update({
                "username": config["username"],
                "password": config["password"]
            })
        
        # API key
        if "api_key" in config:
            auth_config.update({
                "type": "api_key",
                "api_key": config["api_key"]
            })
        
        return auth_config
    
    def uninstall_integration(self, installation_id: str) -> bool:
        """Uninstall an integration."""
        installation = self.installed_integrations.get(installation_id)
        if not installation:
            return False
        
        # Clean up webhook if applicable
        webhook_endpoint_id = f"integration_{installation_id}"
        self.webhook_manager.unregister_webhook(webhook_endpoint_id)
        
        # Clean up API integration if applicable
        api_integration_id = f"integration_{installation_id}"
        if api_integration_id in self.api_integrations:
            del self.api_integrations[api_integration_id]
        
        del self.installed_integrations[installation_id]
        logger.info(f"Uninstalled integration: {installation_id}")
        return True
    
    def get_installed_integrations(self, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get installed integrations for a tenant."""
        installations = list(self.installed_integrations.values())
        if tenant_id:
            installations = [i for i in installations if i.get("tenant_id") == tenant_id]
        return installations
    
    async def trigger_integration_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Trigger an event for all relevant integrations."""
        # Trigger webhooks
        await self.webhook_manager.trigger_webhook_event(event_type, payload)
        
        # Trigger other integration types as needed
        logger.info(f"Triggered integration event: {event_type}")
    
    def get_integration_analytics(self) -> Dict[str, Any]:
        """Get analytics about integrations usage."""
        total_marketplace = len(self.marketplace)
        total_installed = len(self.installed_integrations)
        
        # Category breakdown
        categories = {}
        for entry in self.marketplace.values():
            categories[entry.category] = categories.get(entry.category, 0) + 1
        
        # Most popular integrations
        popular = sorted(
            self.marketplace.values(),
            key=lambda x: x.downloads,
            reverse=True
        )[:5]
        
        return {
            "total_marketplace_entries": total_marketplace,
            "total_installed_integrations": total_installed,
            "categories": categories,
            "most_popular": [{"name": e.name, "downloads": e.downloads} for e in popular],
            "webhook_endpoints": len(self.webhook_manager.endpoints),
            "api_integrations": len(self.api_integrations)
        }


# Global integration manager instance
integration_manager = IntegrationManager()