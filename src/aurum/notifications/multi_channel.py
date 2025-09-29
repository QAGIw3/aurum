"""Multi-channel notification sending primitives.

This module provides the abstraction layer between notification routing/
templating and the physical delivery providers. It encapsulates per-channel
rate limiting, retry behaviour, and publishing of delivery status events onto
Kafka via the existing :class:`aurum.events.streaming.EventBus` contract.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence, Tuple, TYPE_CHECKING

from aurum.events.streaming import EventBus, EventEnvelope
from aurum.logging import LogLevel, StructuredLogger, create_logger

if TYPE_CHECKING:  # pragma: no cover - used for type hints only
    from .intelligent_routing import RouteDecision, RoutePlan


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


class NotificationPriority(str, Enum):
    """Supported notification priorities."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class NotificationChannel(str, Enum):
    """Channels supported by the dispatcher."""

    EMAIL = "email"
    SMS = "sms"
    SLACK = "slack"
    TEAMS = "teams"
    PUSH_WEB = "push_web"
    PUSH_MOBILE = "push_mobile"


ChannelKey = Tuple[str, NotificationChannel]


@dataclass(slots=True)
class NotificationDestination:
    """Target recipient for a notification."""

    recipient_id: str
    address: Optional[str] = None
    channels: Sequence[NotificationChannel] = field(default_factory=list)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Notification:
    """Notification payload provided by the orchestration layer."""

    notification_id: str
    tenant_id: str
    priority: NotificationPriority
    template_id: str
    destinations: Sequence[NotificationDestination]
    channel_content: Mapping[NotificationChannel, Mapping[str, Any]]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    deduplication_key: Optional[str] = None
    schedule_at: Optional[datetime] = None

    def content_for(self, channel: NotificationChannel) -> Mapping[str, Any]:
        """Return pre-rendered content for the requested channel."""
        if channel not in self.channel_content:
            return {}
        return self.channel_content[channel]


class DeliveryStatus(str, Enum):
    """Delivery state emitted for downstream analytics."""

    QUEUED = "queued"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEFERRED = "deferred"
    SUPPRESSED = "suppressed"


@dataclass(slots=True)
class DeliveryAttempt:
    """In-flight delivery attempt metadata."""

    attempt_id: str
    notification: Notification
    destination: NotificationDestination
    channel: NotificationChannel
    attempt_number: int
    status: DeliveryStatus = DeliveryStatus.QUEUED
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    provider_message_id: Optional[str] = None
    queued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None


@dataclass(slots=True)
class DeliveryResult:
    """Provider response surfaced to the dispatcher."""

    attempt: DeliveryAttempt
    status: DeliveryStatus
    provider_message_id: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def successful(self) -> bool:
        return self.status in {DeliveryStatus.SENT, DeliveryStatus.DELIVERED}


# ---------------------------------------------------------------------------
# Provider abstractions
# ---------------------------------------------------------------------------


class NotificationProviderError(RuntimeError):
    """Raised when a provider fails to send a message."""


class NotificationProvider:
    """Base class for channel providers."""

    channel: NotificationChannel

    def __init__(self, *, enabled: bool = True, logger: Optional[StructuredLogger] = None) -> None:
        self._enabled = enabled
        self._logger = logger or create_logger(f"notifications.provider.{self.channel.value}")

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def send(
        self,
        notification: Notification,
        destination: NotificationDestination,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        """Send a notification for a single destination."""
        raise NotImplementedError

    async def send_batch(
        self,
        batch: Sequence[Tuple[Notification, NotificationDestination, Mapping[str, Any]]],
    ) -> Sequence[DeliveryResult]:
        """Optional batch API; defaults to individual sends."""
        results = []
        for notification, destination, content in batch:
            result = await self.send(notification, destination, content)
            results.append(result)
        return results


class EmailSMTPProvider(NotificationProvider):
    """Simple SMTP provider wrapper.

    The implementation intentionally keeps side-effects minimal. It logs the
    delivery and pretends success when SMTP credentials are not configured so
    local development and tests remain deterministic.
    """

    channel = NotificationChannel.EMAIL

    def __init__(
        self,
        *,
        from_address: str,
        smtp_host: Optional[str] = None,
        smtp_port: int = 25,
        username: Optional[str] = None,
        password: Optional[str] = None,
        enabled: bool = True,
    ) -> None:
        super().__init__(enabled=enabled)
        self._from_address = from_address
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._username = username
        self._password = password

    async def send(
        self,
        notification: Notification,
        destination: NotificationDestination,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        attempt = DeliveryAttempt(
            attempt_id=str(uuid.uuid4()),
            notification=notification,
            destination=destination,
            channel=self.channel,
            attempt_number=1,
        )
        if not self.enabled:
            attempt.status = DeliveryStatus.SUPPRESSED
            attempt.completed_at = datetime.now(timezone.utc)
            return DeliveryResult(attempt=attempt, status=DeliveryStatus.SUPPRESSED)

        subject = content.get("subject", "Aurum notification")
        body = content.get("body", "")
        recipient = destination.address or destination.metadata.get("email")
        if recipient is None:
            attempt.status = DeliveryStatus.FAILED
            attempt.completed_at = datetime.now(timezone.utc)
            attempt.error_code = "missing_recipient"
            attempt.error_message = "Email recipient not provided"
            return DeliveryResult(
                attempt=attempt,
                status=DeliveryStatus.FAILED,
                error_code=attempt.error_code,
                error_message=attempt.error_message,
            )

        # The actual SMTP exchange is omitted. We log the intent instead.
        self._logger.log(
            LogLevel.INFO,
            "Dispatching email notification",
            event_type="notification_email_send",
            metadata={
                "notification_id": notification.notification_id,
                "recipient": recipient,
                "subject": subject,
            },
        )

        attempt.status = DeliveryStatus.SENT
        attempt.completed_at = datetime.now(timezone.utc)
        return DeliveryResult(attempt=attempt, status=DeliveryStatus.SENT)


class SlackWebhookProvider(NotificationProvider):
    """Slack webhook provider stub."""

    channel = NotificationChannel.SLACK

    def __init__(self, *, webhook_url: Optional[str], enabled: bool = True) -> None:
        super().__init__(enabled=enabled)
        self._webhook_url = webhook_url

    async def send(
        self,
        notification: Notification,
        destination: NotificationDestination,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        attempt = DeliveryAttempt(
            attempt_id=str(uuid.uuid4()),
            notification=notification,
            destination=destination,
            channel=self.channel,
            attempt_number=1,
        )
        if not self.enabled or not self._webhook_url:
            attempt.status = DeliveryStatus.SUPPRESSED
            attempt.completed_at = datetime.now(timezone.utc)
            return DeliveryResult(attempt=attempt, status=DeliveryStatus.SUPPRESSED)

        message = content.get("text") or content.get("blocks")
        if message is None:
            attempt.status = DeliveryStatus.FAILED
            attempt.completed_at = datetime.now(timezone.utc)
            attempt.error_code = "missing_payload"
            attempt.error_message = "Slack payload missing text or blocks"
            return DeliveryResult(
                attempt=attempt,
                status=DeliveryStatus.FAILED,
                error_code=attempt.error_code,
                error_message=attempt.error_message,
            )

        self._logger.log(
            LogLevel.INFO,
            "Dispatching Slack notification",
            event_type="notification_slack_send",
            metadata={
                "notification_id": notification.notification_id,
                "recipient": destination.recipient_id,
            },
        )
        attempt.status = DeliveryStatus.SENT
        attempt.completed_at = datetime.now(timezone.utc)
        return DeliveryResult(attempt=attempt, status=DeliveryStatus.SENT)


class TeamsWebhookProvider(NotificationProvider):
    """Microsoft Teams webhook provider stub."""

    channel = NotificationChannel.TEAMS

    def __init__(self, *, webhook_url: Optional[str], enabled: bool = True) -> None:
        super().__init__(enabled=enabled)
        self._webhook_url = webhook_url

    async def send(
        self,
        notification: Notification,
        destination: NotificationDestination,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        attempt = DeliveryAttempt(
            attempt_id=str(uuid.uuid4()),
            notification=notification,
            destination=destination,
            channel=self.channel,
            attempt_number=1,
        )
        if not self.enabled or not self._webhook_url:
            attempt.status = DeliveryStatus.SUPPRESSED
            attempt.completed_at = datetime.now(timezone.utc)
            return DeliveryResult(attempt=attempt, status=DeliveryStatus.SUPPRESSED)

        self._logger.log(
            LogLevel.INFO,
            "Dispatching Teams notification",
            event_type="notification_teams_send",
            metadata={
                "notification_id": notification.notification_id,
                "recipient": destination.recipient_id,
            },
        )
        attempt.status = DeliveryStatus.SENT
        attempt.completed_at = datetime.now(timezone.utc)
        return DeliveryResult(attempt=attempt, status=DeliveryStatus.SENT)


class SMSProvider(NotificationProvider):
    """SMS provider facade (Twilio-style)."""

    channel = NotificationChannel.SMS

    def __init__(self, *, from_number: Optional[str], enabled: bool = False) -> None:
        super().__init__(enabled=enabled)
        self._from_number = from_number

    async def send(
        self,
        notification: Notification,
        destination: NotificationDestination,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        attempt = DeliveryAttempt(
            attempt_id=str(uuid.uuid4()),
            notification=notification,
            destination=destination,
            channel=self.channel,
            attempt_number=1,
        )
        if not self.enabled or not self._from_number:
            attempt.status = DeliveryStatus.SUPPRESSED
            attempt.completed_at = datetime.now(timezone.utc)
            return DeliveryResult(attempt=attempt, status=DeliveryStatus.SUPPRESSED)

        message = content.get("text")
        phone = destination.address or destination.metadata.get("phone")
        if message is None or phone is None:
            attempt.status = DeliveryStatus.FAILED
            attempt.completed_at = datetime.now(timezone.utc)
            attempt.error_code = "missing_payload"
            attempt.error_message = "SMS payload missing text or recipient"
            return DeliveryResult(
                attempt=attempt,
                status=DeliveryStatus.FAILED,
                error_code=attempt.error_code,
                error_message=attempt.error_message,
            )

        self._logger.log(
            LogLevel.INFO,
            "Dispatching SMS notification",
            event_type="notification_sms_send",
            metadata={
                "notification_id": notification.notification_id,
                "recipient": phone,
            },
        )
        attempt.status = DeliveryStatus.SENT
        attempt.completed_at = datetime.now(timezone.utc)
        return DeliveryResult(attempt=attempt, status=DeliveryStatus.SENT)


class PushWebProvider(NotificationProvider):
    """Web push provider (stub)."""

    channel = NotificationChannel.PUSH_WEB

    async def send(
        self,
        notification: Notification,
        destination: NotificationDestination,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        attempt = DeliveryAttempt(
            attempt_id=str(uuid.uuid4()),
            notification=notification,
            destination=destination,
            channel=self.channel,
            attempt_number=1,
        )
        if not self.enabled:
            attempt.status = DeliveryStatus.SUPPRESSED
            attempt.completed_at = datetime.now(timezone.utc)
            return DeliveryResult(attempt=attempt, status=DeliveryStatus.SUPPRESSED)

        self._logger.log(
            LogLevel.INFO,
            "Dispatching web push notification",
            event_type="notification_push_web_send",
            metadata={
                "notification_id": notification.notification_id,
                "recipient": destination.recipient_id,
            },
        )
        attempt.status = DeliveryStatus.SENT
        attempt.completed_at = datetime.now(timezone.utc)
        return DeliveryResult(attempt=attempt, status=DeliveryStatus.SENT)


class PushMobileProvider(NotificationProvider):
    """Mobile push provider (stub)."""

    channel = NotificationChannel.PUSH_MOBILE

    async def send(
        self,
        notification: Notification,
        destination: NotificationDestination,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        attempt = DeliveryAttempt(
            attempt_id=str(uuid.uuid4()),
            notification=notification,
            destination=destination,
            channel=self.channel,
            attempt_number=1,
        )
        if not self.enabled:
            attempt.status = DeliveryStatus.SUPPRESSED
            attempt.completed_at = datetime.now(timezone.utc)
            return DeliveryResult(attempt=attempt, status=DeliveryStatus.SUPPRESSED)

        self._logger.log(
            LogLevel.INFO,
            "Dispatching mobile push notification",
            event_type="notification_push_mobile_send",
            metadata={
                "notification_id": notification.notification_id,
                "recipient": destination.recipient_id,
            },
        )
        attempt.status = DeliveryStatus.SENT
        attempt.completed_at = datetime.now(timezone.utc)
        return DeliveryResult(attempt=attempt, status=DeliveryStatus.SENT)


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _TokenBucketState:
    tokens: float
    last_refill: float


class NotificationRateLimiter:
    """Simple per-channel token bucket rate limiter."""

    def __init__(
        self,
        *,
        global_per_minute: int,
        global_burst: int,
        channel_limits: Mapping[NotificationChannel, Mapping[str, int]],
    ) -> None:
        self._global_limit = (global_per_minute, global_burst)
        self._channel_limits: Dict[NotificationChannel, Tuple[int, int]] = {
            channel: (
                int(config.get("per_minute", global_per_minute)),
                int(config.get("burst", global_burst)),
            )
            for channel, config in channel_limits.items()
        }
        self._state: Dict[str, _TokenBucketState] = {}
        self._lock = asyncio.Lock()

    async def allow(self, key: str, channel: NotificationChannel, tokens: int = 1) -> Tuple[bool, int]:
        async with self._lock:
            allowed, remaining = self._consume(self._global_key(), self._global_limit, tokens)
            if not allowed:
                return False, remaining
            allowed, remaining = self._consume(self._channel_key(channel), self._channel_limits.get(channel, self._global_limit), tokens)
            if not allowed:
                return False, remaining
            allowed, remaining = self._consume(key, self._channel_limits.get(channel, self._global_limit), tokens)
            return allowed, remaining

    def _consume(self, key: str, limit: Tuple[int, int], tokens: int) -> Tuple[bool, int]:
        per_minute, burst = limit
        state = self._state.get(key)
        now = datetime.now(timezone.utc).timestamp()
        if state is None:
            state = _TokenBucketState(tokens=float(per_minute), last_refill=now)
            self._state[key] = state
        else:
            elapsed = max(0.0, now - state.last_refill)
            refill_rate = per_minute / 60.0
            state.tokens = min(float(per_minute + burst), state.tokens + (elapsed * refill_rate))
            state.last_refill = now
        if state.tokens >= tokens:
            state.tokens -= tokens
            return True, int(state.tokens)
        return False, int(state.tokens)

    @staticmethod
    def _global_key() -> str:
        return "notifications:global"

    @staticmethod
    def _channel_key(channel: NotificationChannel) -> str:
        return f"notifications:channel:{channel.value}"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class MultiChannelDispatcher:
    """Dispatch notifications across multiple providers."""

    def __init__(
        self,
        providers: Mapping[NotificationChannel, NotificationProvider],
        event_bus: EventBus,
        *,
        rate_limiter: Optional[NotificationRateLimiter] = None,
        delivery_topic: str = "aurum.notifications.delivery.v1",
        analytics_topic: str = "aurum.notifications.analytics.v1",
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        self._providers = dict(providers)
        self._event_bus = event_bus
        self._rate_limiter = rate_limiter
        self._delivery_topic = delivery_topic
        self._analytics_topic = analytics_topic
        self._max_attempts = max(1, int(max_attempts))
        self._backoff_seconds = max(0.1, float(backoff_seconds))
        self._logger = create_logger("notifications.dispatcher")

    @classmethod
    def from_config(
        cls,
        event_bus: EventBus,
        *,
        config_dir: Path | str = Path("config/notifications"),
        rate_limiter: Optional[NotificationRateLimiter] = None,
    ) -> "MultiChannelDispatcher":
        config_dir = Path(config_dir)
        channels_config = cls._load_json(config_dir / "channels.json")
        rate_config = cls._load_json(config_dir / "rate_limits.json")

        providers: Dict[NotificationChannel, NotificationProvider] = {
            NotificationChannel.EMAIL: EmailSMTPProvider(
                from_address=channels_config.get("email", {}).get("from_address", "alerts@example.com"),
                smtp_host=None,
                enabled=channels_config.get("email", {}).get("enabled", True),
            ),
            NotificationChannel.SLACK: SlackWebhookProvider(
                webhook_url=channels_config.get("slack", {}).get("webhook_url"),
                enabled=channels_config.get("slack", {}).get("enabled", False),
            ),
            NotificationChannel.TEAMS: TeamsWebhookProvider(
                webhook_url=channels_config.get("teams", {}).get("webhook_url"),
                enabled=channels_config.get("teams", {}).get("enabled", False),
            ),
            NotificationChannel.SMS: SMSProvider(
                from_number=channels_config.get("sms", {}).get("from_number"),
                enabled=channels_config.get("sms", {}).get("enabled", False),
            ),
            NotificationChannel.PUSH_WEB: PushWebProvider(enabled=channels_config.get("push_web", {}).get("enabled", False)),
            NotificationChannel.PUSH_MOBILE: PushMobileProvider(enabled=channels_config.get("push_mobile", {}).get("enabled", False)),
        }

        rl = rate_limiter
        if rl is None and rate_config:
            normalised: Dict[NotificationChannel, Mapping[str, int]] = {}
            for channel_key, value in rate_config.get("channel_limits", {}).items():
                try:
                    channel_enum = NotificationChannel(channel_key)
                except ValueError:
                    channel_enum = NotificationChannel[channel_key.upper()]
                normalised[channel_enum] = value
            rl = NotificationRateLimiter(
                global_per_minute=int(rate_config.get("global", {}).get("per_minute", 120)),
                global_burst=int(rate_config.get("global", {}).get("burst", 30)),
                channel_limits=normalised,
            )

        return cls(providers=providers, event_bus=event_bus, rate_limiter=rl)

    async def dispatch(self, notification: Notification, plan: Optional["RoutePlan"] = None) -> Sequence[DeliveryResult]:
        """Dispatch notification across all chosen channels."""
        decisions: Iterable["RouteDecision"]
        if plan is not None:
            decisions = plan.decisions
        else:
            decisions = self._fallback_decisions(notification)

        results: list[DeliveryResult] = []
        for decision in decisions:
            dest = decision.destination
            channel = decision.channel
            provider = self._providers.get(channel)
            if provider is None:
                self._logger.log(
                    LogLevel.WARN,
                    "Provider missing for channel",
                    event_type="notification_provider_missing",
                    metadata={
                        "channel": channel.value,
                        "notification_id": notification.notification_id,
                    },
                )
                continue
            if not provider.enabled:
                attempt = DeliveryAttempt(
                    attempt_id=str(uuid.uuid4()),
                    notification=notification,
                    destination=dest,
                    channel=channel,
                    attempt_number=decision.attempt or 1,
                    status=DeliveryStatus.SUPPRESSED,
                    completed_at=datetime.now(timezone.utc),
                )
                result = DeliveryResult(attempt=attempt, status=DeliveryStatus.SUPPRESSED)
                await self._publish_delivery(result)
                results.append(result)
                continue

            rate_key = self._rate_key(notification, dest, channel)
            if self._rate_limiter is not None:
                allowed, _ = await self._rate_limiter.allow(rate_key, channel)
                if not allowed:
                    attempt = DeliveryAttempt(
                        attempt_id=str(uuid.uuid4()),
                        notification=notification,
                        destination=dest,
                        channel=channel,
                        attempt_number=decision.attempt or 1,
                        status=DeliveryStatus.DEFERRED,
                        completed_at=datetime.now(timezone.utc),
                        error_code="rate_limited",
                        error_message="Rate limit exceeded for channel",
                    )
                    result = DeliveryResult(
                        attempt=attempt,
                        status=DeliveryStatus.DEFERRED,
                        error_code=attempt.error_code,
                        error_message=attempt.error_message,
                    )
                    await self._publish_delivery(result)
                    results.append(result)
                    continue

            content = decision.content or notification.content_for(channel)
            attempt_number = decision.attempt or 1
            attempt = DeliveryAttempt(
                attempt_id=str(uuid.uuid4()),
                notification=notification,
                destination=dest,
                channel=channel,
                attempt_number=attempt_number,
            )

            result = await self._deliver_with_retry(provider, attempt, content)
            await self._publish_delivery(result)
            results.append(result)
        return results

    async def _deliver_with_retry(
        self,
        provider: NotificationProvider,
        attempt: DeliveryAttempt,
        content: Mapping[str, Any],
    ) -> DeliveryResult:
        current_attempt = 0
        error: Optional[Exception] = None
        while current_attempt < self._max_attempts:
            current_attempt += 1
            attempt.attempt_number = current_attempt
            try:
                result = await provider.send(attempt.notification, attempt.destination, content)
                result.attempt.attempt_number = current_attempt
                return result
            except Exception as exc:  # pragma: no cover - defensive guard
                error = exc
                self._logger.log(
                    LogLevel.ERROR,
                    "Provider send failed",
                    event_type="notification_delivery_error",
                    metadata={
                        "notification_id": attempt.notification.notification_id,
                        "channel": attempt.channel.value,
                        "error": str(exc),
                    },
                )
                await asyncio.sleep(self._backoff_seconds * current_attempt)
        attempt.status = DeliveryStatus.FAILED
        attempt.completed_at = datetime.now(timezone.utc)
        attempt.error_code = "provider_error"
        attempt.error_message = str(error) if error else "unknown"
        return DeliveryResult(
            attempt=attempt,
            status=DeliveryStatus.FAILED,
            error_code=attempt.error_code,
            error_message=attempt.error_message,
        )

    async def _publish_delivery(self, result: DeliveryResult) -> None:
        payload = {
            "id": result.attempt.attempt_id,
            "dispatch_id": result.attempt.notification.notification_id,
            "tenant_id": result.attempt.notification.tenant_id,
            "recipient_id": result.attempt.destination.recipient_id,
            "channel": result.attempt.channel.value,
            "status": result.status.value,
            "attempt": result.attempt.attempt_number,
            "provider_message_id": result.provider_message_id,
            "error_code": result.error_code,
            "error_message": result.error_message,
            "queued_at": int(result.attempt.queued_at.timestamp() * 1_000_000),
            "completed_at": int(result.attempt.completed_at.timestamp() * 1_000_000)
            if result.attempt.completed_at else None,
            "metadata": {str(k): str(v) for k, v in (result.metadata or {}).items()},
        }
        envelope = EventEnvelope(
            topic=self._delivery_topic,
            payload=payload,
            key=result.attempt.notification.notification_id,
            headers={
                "notification_id": result.attempt.notification.notification_id,
                "channel": result.attempt.channel.value,
                "tenant_id": result.attempt.notification.tenant_id,
            },
            schema_subject="aurum.notifications.delivery.v1-value",
            schema_version=1,
        )
        await self._event_bus.publish(envelope)

    def _fallback_decisions(self, notification: Notification) -> Iterable["RouteDecision"]:
        from .intelligent_routing import RouteDecision  # Local import to avoid cycle

        for destination in notification.destinations:
            channels = destination.channels or list(NotificationChannel)
            for channel in channels:
                yield RouteDecision(
                    destination=destination,
                    channel=channel,
                    content=notification.content_for(channel),
                )

    @staticmethod
    def _rate_key(
        notification: Notification,
        destination: NotificationDestination,
        channel: NotificationChannel,
    ) -> str:
        return f"notif:{notification.tenant_id}:{destination.recipient_id}:{channel.value}"

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)


__all__ = [
    "ChannelKey",
    "DeliveryAttempt",
    "DeliveryResult",
    "DeliveryStatus",
    "EmailSMTPProvider",
    "MultiChannelDispatcher",
    "Notification",
    "NotificationChannel",
    "NotificationDestination",
    "NotificationPriority",
    "NotificationProvider",
    "NotificationProviderError",
    "NotificationRateLimiter",
    "PushMobileProvider",
    "PushWebProvider",
    "SlackWebhookProvider",
    "SMSProvider",
    "TeamsWebhookProvider",
]
