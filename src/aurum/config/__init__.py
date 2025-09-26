"""Public configuration facade for legacy imports."""

from __future__ import annotations

from aurum.core.settings import AurumSettings, configure_settings, get_settings

try:
    settings = get_settings()
except RuntimeError:
    # Tests frequently import ``aurum.config.settings`` without bootstrapping the
    # configuration system first.  Fall back to an ephemeral default instance so
    # those imports keep working while still allowing applications to override
    # the configured settings explicitly when needed.
    settings = AurumSettings()
    configure_settings(settings)

__all__ = ["settings", "AurumSettings", "configure_settings", "get_settings"]
