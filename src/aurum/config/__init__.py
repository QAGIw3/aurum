"""Public configuration facade for legacy imports."""

from __future__ import annotations

try:
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

except ImportError:
    # Fallback for when core settings are not available
    from aurum.config.consolidated_loader import MockAurumSettings
    AurumSettings = MockAurumSettings
    settings = AurumSettings()

    def configure_settings(s):
        pass

    def get_settings():
        return settings

__all__ = ["settings", "AurumSettings", "configure_settings", "get_settings"]
