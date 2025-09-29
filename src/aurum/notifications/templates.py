"""Notification templating utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from string import Template
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple

from aurum.logging import LogLevel, create_logger

from .multi_channel import NotificationChannel


class TemplateRenderError(RuntimeError):
    """Raised when templating fails."""


@dataclass(slots=True)
class NotificationTemplate:
    """Represents a notification template with channel variants."""

    template_id: str
    channel_variants: Mapping[NotificationChannel, Mapping[str, str]]
    locale: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class TemplateRegistry:
    """Registry for notification templates supporting locale fallbacks."""

    def __init__(self) -> None:
        self._templates: MutableMapping[Tuple[str, Optional[str]], NotificationTemplate] = {}
        self._logger = create_logger("notifications.templates")

    def register(self, template: NotificationTemplate) -> None:
        key = (template.template_id, template.locale)
        self._templates[key] = template
        self._logger.log(
            LogLevel.DEBUG,
            "Registered notification template",
            event_type="notification_template_register",
            template_id=template.template_id,
            locale=template.locale,
        )

    def get(self, template_id: str, locale: Optional[str] = None) -> Optional[NotificationTemplate]:
        key = (template_id, locale)
        if key in self._templates:
            return self._templates[key]
        fallback = (template_id, None)
        return self._templates.get(fallback)

    def render(
        self,
        template_id: str,
        channel: NotificationChannel,
        data: Mapping[str, Any],
        *,
        locale: Optional[str] = None,
    ) -> Mapping[str, Any]:
        template = self.get(template_id, locale)
        if template is None:
            raise TemplateRenderError(f"Template {template_id} not registered")

        variant = template.channel_variants.get(channel)
        if variant is None:
            raise TemplateRenderError(
                f"Template {template_id} missing channel variant {channel.value}"
            )

        rendered: Dict[str, Any] = {}
        for key, raw_value in variant.items():
            if not isinstance(raw_value, str):
                rendered[key] = raw_value
                continue
            rendered[key] = self._render_string(raw_value, data)
        return rendered

    def load_directory(self, directory: Path | str) -> int:
        directory = Path(directory)
        if not directory.exists():
            return 0
        count = 0
        for path in directory.glob("*.json"):
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            template_id = payload["template_id"]
            locale = payload.get("locale")
            variants = {}
            for channel_key, channel_payload in payload.get("channels", {}).items():
                try:
                    channel_enum = NotificationChannel(channel_key)
                except ValueError:
                    channel_enum = NotificationChannel[channel_key.upper()]
                variants[channel_enum] = channel_payload
            template = NotificationTemplate(
                template_id=template_id,
                locale=locale,
                channel_variants=variants,
                metadata=payload.get("metadata", {}),
            )
            self.register(template)
            count += 1
        return count

    def _render_string(self, raw_value: str, data: Mapping[str, Any]) -> str:
        try:
            return Template(raw_value).safe_substitute(**data)
        except Exception as exc:  # pragma: no cover - template syntax issues
            raise TemplateRenderError(str(exc)) from exc


__all__ = ["NotificationTemplate", "TemplateRegistry", "TemplateRenderError"]
