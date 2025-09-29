"""Notification routing and escalation logic."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence, Tuple

from aurum.logging import LogLevel, create_logger

from .multi_channel import Notification, NotificationChannel, NotificationDestination, NotificationPriority


class Severity(str, Enum):
    """Severity levels recognised by the routing engine."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass(slots=True)
class QuietHours:
    """Quiet hours configuration for a tenant or user."""

    start: time
    end: time
    timezone: timezone = timezone.utc

    def next_window_end(self, reference: datetime) -> datetime:
        """Return the datetime when the quiet window ends relative to reference."""
        localized = reference.astimezone(self.timezone)
        window_end = localized.replace(hour=self.end.hour, minute=self.end.minute, second=0, microsecond=0)
        if localized.time() > self.end:
            window_end = window_end + timedelta(days=1)
        return window_end.astimezone(timezone.utc)

    def is_active(self, reference: datetime) -> bool:
        localized = reference.astimezone(self.timezone)
        start_dt = localized.replace(hour=self.start.hour, minute=self.start.minute, second=0, microsecond=0)
        end_dt = localized.replace(hour=self.end.hour, minute=self.end.minute, second=0, microsecond=0)
        if self.start < self.end:
            return start_dt <= localized < end_dt
        # Overnight case
        if localized >= start_dt or localized < end_dt:
            return True
        return False


@dataclass(slots=True)
class RoutingPreferences:
    """Per-recipient channel preferences."""

    enabled_channels: Sequence[NotificationChannel] = field(default_factory=list)
    muted: bool = False
    quiet_hours: Optional[QuietHours] = None
    escalation_opt_in: bool = True


@dataclass(slots=True)
class EscalationStep:
    """Single escalation step definition."""

    delay: timedelta
    channels: Sequence[NotificationChannel]


@dataclass(slots=True)
class EscalationPlan:
    """Defines the escalation path for a notification."""

    steps: Sequence[EscalationStep] = field(default_factory=tuple)

    def total_duration(self) -> timedelta:
        return sum((step.delay for step in self.steps), timedelta())


@dataclass(slots=True)
class RoutingPolicy:
    """Tenant-wide routing defaults."""

    severity_channels: Mapping[Severity, Sequence[NotificationChannel]]
    quiet_hours: Optional[QuietHours] = None
    escalation: Mapping[Severity, EscalationPlan] = field(default_factory=dict)
    cooldown_seconds: int = 120


@dataclass(slots=True)
class RoutingContext:
    """Context in which routing decisions are made."""

    severity: Severity
    event_time: datetime
    fingerprint: Optional[str] = None
    requires_ack: bool = False
    ack_timeout: Optional[timedelta] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    on_call_channels: Sequence[NotificationChannel] = field(default_factory=tuple)


@dataclass(slots=True)
class RouteDecision:
    """Single decision to deliver via channel to destination."""

    destination: NotificationDestination
    channel: NotificationChannel
    content: Mapping[str, Any]
    delay_until: Optional[datetime] = None
    attempt: Optional[int] = None


@dataclass(slots=True)
class RoutePlan:
    """Aggregate routing plan produced by the engine."""

    decisions: Tuple[RouteDecision, ...]
    suppressed: bool = False
    reason: Optional[str] = None

    def pending_immediately(self) -> Tuple[RouteDecision, ...]:
        return tuple(d for d in self.decisions if d.delay_until is None)


@dataclass(slots=True)
class EscalationOutcome:
    """Represents escalation scheduling derived from a plan."""

    next_escalation_at: Optional[datetime]
    remaining_steps: int


class RoutingEngine:
    """Main orchestration entry point for routing decisions."""

    def __init__(
        self,
        policy: RoutingPolicy,
        *,
        suppression_ttl: timedelta = timedelta(minutes=5),
    ) -> None:
        self._policy = policy
        self._suppression_ttl = suppression_ttl
        self._suppression_index: MutableMapping[str, datetime] = {}
        self._lock = asyncio.Lock()
        self._logger = create_logger("notifications.routing")

    @classmethod
    def from_config(cls, config_path: Path | str = Path("config/notifications/routing_defaults.json")) -> "RoutingEngine":
        import json

        path = Path(config_path)
        if not path.exists():
            default_policy = RoutingPolicy(
                severity_channels={
                    Severity.LOW: (NotificationChannel.EMAIL,),
                    Severity.MEDIUM: (NotificationChannel.EMAIL,),
                    Severity.HIGH: (NotificationChannel.EMAIL, NotificationChannel.SLACK),
                    Severity.CRITICAL: (
                        NotificationChannel.EMAIL,
                        NotificationChannel.SLACK,
                        NotificationChannel.SMS,
                    ),
                },
            )
            return cls(default_policy)

        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        quiet_cfg = data.get("quiet_hours")
        quiet = None
        if quiet_cfg:
            quiet = QuietHours(
                start=time.fromisoformat(quiet_cfg.get("start", "22:00")),
                end=time.fromisoformat(quiet_cfg.get("end", "07:00")),
            )

        channels_cfg = data.get("escalation", {})
        default_channel_values = tuple(NotificationChannel(ch) for ch in data.get("default_channels", ["email"]))
        severity_channels: Dict[Severity, Sequence[NotificationChannel]] = {}
        for severity in Severity:
            configured = channels_cfg.get(severity.value)
            if configured:
                values = tuple(NotificationChannel(ch) for ch in configured)
            else:
                values = default_channel_values
            severity_channels[severity] = values

        escalation_cfg = data.get("escalation_plan", {})
        escalation_map: Dict[Severity, EscalationPlan] = {}
        for severity, steps_cfg in escalation_cfg.items():
            try:
                sev_enum = Severity(severity)
            except ValueError:
                continue
            steps: list[EscalationStep] = []
            for step_data in steps_cfg:
                delay = timedelta(seconds=int(step_data.get("delay_seconds", 300)))
                channels = tuple(NotificationChannel(ch) for ch in step_data.get("channels", []))
                if not channels:
                    continue
                steps.append(EscalationStep(delay=delay, channels=channels))
            if steps:
                escalation_map[sev_enum] = EscalationPlan(tuple(steps))

        if Severity.CRITICAL not in escalation_map:
            escalation_map[Severity.CRITICAL] = EscalationPlan(
                (
                    EscalationStep(delay=timedelta(minutes=5), channels=(NotificationChannel.SMS,)),
                    EscalationStep(delay=timedelta(minutes=5), channels=(NotificationChannel.TEAMS,)),
                )
            )

        policy = RoutingPolicy(
            severity_channels=severity_channels,
            quiet_hours=quiet,
            escalation=escalation_map,
            cooldown_seconds=data.get("cooldown_seconds", 120),
        )
        return cls(policy)

    async def build_plan(
        self,
        notification: Notification,
        context: RoutingContext,
        preferences: Mapping[str, RoutingPreferences],
    ) -> RoutePlan:
        if await self._is_suppressed(notification, context):
            return RoutePlan(decisions=tuple(), suppressed=True, reason="cooldown_active")

        base_channels = self._policy.severity_channels.get(context.severity, (NotificationChannel.EMAIL,))
        decisions: list[RouteDecision] = []
        for destination in notification.destinations:
            prefs = preferences.get(destination.recipient_id, RoutingPreferences())
            if prefs.muted:
                continue
            chosen_channels = list(base_channels)
            if prefs.enabled_channels:
                chosen_channels = [ch for ch in chosen_channels if ch in prefs.enabled_channels]
            if context.on_call_channels:
                chosen_channels.extend(ch for ch in context.on_call_channels if ch not in chosen_channels)

            delay_until = None
            quiet = prefs.quiet_hours or self._policy.quiet_hours
            if quiet and quiet.is_active(context.event_time):
                delay_until = quiet.next_window_end(context.event_time)

            for channel in chosen_channels:
                decisions.append(
                    RouteDecision(
                        destination=destination,
                        channel=channel,
                        content=notification.content_for(channel),
                        delay_until=delay_until,
                        attempt=1,
                    )
                )

            if context.requires_ack and prefs.escalation_opt_in:
                decisions.extend(self._build_escalation_steps(destination, notification, context, prefs))

        sorted_decisions = tuple(sorted(decisions, key=lambda d: d.delay_until or context.event_time))
        await self._record_dispatch(notification, context)
        return RoutePlan(decisions=sorted_decisions)

    async def _is_suppressed(self, notification: Notification, context: RoutingContext) -> bool:
        dedup_key = notification.deduplication_key or context.fingerprint
        if dedup_key is None:
            return False
        async with self._lock:
            expires = self._suppression_index.get(dedup_key)
            if expires and expires > datetime.now(timezone.utc):
                return True
        return False

    async def _record_dispatch(self, notification: Notification, context: RoutingContext) -> None:
        dedup_key = notification.deduplication_key or context.fingerprint
        if dedup_key is None:
            return
        async with self._lock:
            self._suppression_index[dedup_key] = datetime.now(timezone.utc) + self._suppression_ttl

    def _build_escalation_steps(
        self,
        destination: NotificationDestination,
        notification: Notification,
        context: RoutingContext,
        prefs: RoutingPreferences,
    ) -> Sequence[RouteDecision]:
        plan = self._policy.escalation.get(context.severity)
        if plan is None or not plan.steps:
            return []
        decisions: list[RouteDecision] = []
        cursor = context.event_time + (context.ack_timeout or timedelta(minutes=5))
        attempt = 2
        for step in plan.steps:
            cursor = cursor + step.delay
            for channel in step.channels:
                decisions.append(
                    RouteDecision(
                        destination=destination,
                        channel=channel,
                        delay_until=cursor,
                        content=notification.content_for(channel),
                        attempt=attempt,
                    )
                )
            attempt += 1
        return decisions

    def analyse_escalation(self, plan: RoutePlan, now: Optional[datetime] = None) -> EscalationOutcome:
        now = now or datetime.now(timezone.utc)
        future_steps = [d for d in plan.decisions if d.delay_until and d.delay_until > now]
        if not future_steps:
            return EscalationOutcome(next_escalation_at=None, remaining_steps=0)
        next_step = min(future_steps, key=lambda d: d.delay_until)
        remaining = len({d.delay_until for d in future_steps})
        return EscalationOutcome(next_escalation_at=next_step.delay_until, remaining_steps=remaining)


__all__ = [
    "EscalationOutcome",
    "EscalationPlan",
    "EscalationStep",
    "RoutingPreferences",
    "QuietHours",
    "RoutePlan",
    "RouteDecision",
    "RoutingContext",
    "RoutingEngine",
    "RoutingPolicy",
    "Severity",
]
