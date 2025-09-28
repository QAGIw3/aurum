"""Provider-specific collectors with lazy imports to avoid heavy deps at import time.

This module exposes provider classes and loader functions via lazy attribute
resolution to keep optional dependencies (like `aiohttp`) from being required
unless the corresponding provider is actually used.
"""

from __future__ import annotations

import importlib
from typing import Any

__all__ = [
    # EIA
    "EiaApiClient",
    "EiaCollector",
    "EiaDatasetConfig",
    "load_eia_dataset_configs",
    # FRED
    "FredApiClient",
    "FredCollector",
    "FredDatasetConfig",
    "load_fred_dataset_configs",
    # NOAA
    "DailyQuota",
    "NoaaApiClient",
    "NoaaCollector",
    "NoaaDatasetConfig",
    "NoaaRateLimiter",
    "load_noaa_dataset_configs",
    # World Bank
    "WorldBankApiClient",
    "WorldBankCollector",
    "WorldBankDatasetConfig",
    "load_worldbank_dataset_configs",
    # MISO (optional)
    "MisoApiClient",
    "MisoCollector",
    "MisoDatasetConfig",
    "load_miso_dataset_configs",
    # ISO-NE (optional, requires aiohttp)
    "IsoNeApiClient",
    "IsoNeCollector",
    "IsoNeDatasetConfig",
    "load_isone_dataset_configs",
]


_ATTR_TO_MODULE: dict[str, tuple[str, str]] = {
    # EIA
    "EiaApiClient": (".eia", "EiaApiClient"),
    "EiaCollector": (".eia", "EiaCollector"),
    "EiaDatasetConfig": (".eia", "EiaDatasetConfig"),
    "load_eia_dataset_configs": (".eia", "load_eia_dataset_configs"),
    # FRED
    "FredApiClient": (".fred", "FredApiClient"),
    "FredCollector": (".fred", "FredCollector"),
    "FredDatasetConfig": (".fred", "FredDatasetConfig"),
    "load_fred_dataset_configs": (".fred", "load_fred_dataset_configs"),
    # NOAA
    "DailyQuota": (".noaa", "DailyQuota"),
    "NoaaApiClient": (".noaa", "NoaaApiClient"),
    "NoaaCollector": (".noaa", "NoaaCollector"),
    "NoaaDatasetConfig": (".noaa", "NoaaDatasetConfig"),
    "NoaaRateLimiter": (".noaa", "NoaaRateLimiter"),
    "load_noaa_dataset_configs": (".noaa", "load_noaa_dataset_configs"),
    # World Bank
    "WorldBankApiClient": (".worldbank", "WorldBankApiClient"),
    "WorldBankCollector": (".worldbank", "WorldBankCollector"),
    "WorldBankDatasetConfig": (".worldbank", "WorldBankDatasetConfig"),
    "load_worldbank_dataset_configs": (".worldbank", "load_worldbank_dataset_configs"),
    # MISO
    "MisoApiClient": (".miso", "MisoApiClient"),
    "MisoCollector": (".miso", "MisoCollector"),
    "MisoDatasetConfig": (".miso", "MisoDatasetConfig"),
    "load_miso_dataset_configs": (".miso", "load_miso_dataset_configs"),
    # ISO-NE (optional)
    "IsoNeApiClient": (".isone", "IsoNeApiClient"),
    "IsoNeCollector": (".isone", "IsoNeCollector"),
    "IsoNeDatasetConfig": (".isone", "IsoNeDatasetConfig"),
    "load_isone_dataset_configs": (".isone", "load_isone_dataset_configs"),
}


def __getattr__(name: str) -> Any:
    if name in _ATTR_TO_MODULE:
        module_name, attr = _ATTR_TO_MODULE[name]
        module = importlib.import_module(module_name, package=__name__)
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
