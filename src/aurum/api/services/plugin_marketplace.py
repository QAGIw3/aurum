"""Services shim for Plugin Marketplace.

Provides a stable import path for tests and callers expecting
`aurum.api.services.plugin_marketplace` while the implementation
lives under `aurum.api.v2.plugin_marketplace`.
"""

from __future__ import annotations

from ..v2.plugin_marketplace import (
    PluginMarketplaceService,
    get_plugin_marketplace_service,
)

__all__ = [
    "PluginMarketplaceService",
    "get_plugin_marketplace_service",
]

