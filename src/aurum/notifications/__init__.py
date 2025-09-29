"""Notification service primitives and orchestration."""

from .multi_channel import (
    ChannelKey,
    DeliveryAttempt,
    DeliveryResult,
    DeliveryStatus,
    Notification,
    NotificationChannel,
    NotificationDestination,
    NotificationPriority,
    NotificationProvider,
    MultiChannelDispatcher,
)
from .intelligent_routing import (
    EscalationOutcome,
    EscalationPlan,
    QuietHours,
    RouteDecision,
    RoutePlan,
    RoutingContext,
    RoutingPolicy,
    RoutingPreferences,
    RoutingEngine,
)
from .templates import NotificationTemplate, TemplateRegistry, TemplateRenderError

__all__ = [
    "ChannelKey",
    "DeliveryAttempt",
    "DeliveryResult",
    "DeliveryStatus",
    "Notification",
    "NotificationChannel",
    "NotificationDestination",
    "NotificationPriority",
    "NotificationProvider",
    "MultiChannelDispatcher",
    "EscalationOutcome",
    "EscalationPlan",
    "QuietHours",
    "RouteDecision",
    "RoutePlan",
    "RoutingContext",
    "RoutingPolicy",
    "RoutingPreferences",
    "RoutingEngine",
    "NotificationTemplate",
    "TemplateRegistry",
    "TemplateRenderError",
]
