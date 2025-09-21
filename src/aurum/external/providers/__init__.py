"""Provider-specific collectors built atop the external framework."""

from .eia import (
    EiaApiClient,
    EiaCollector,
    EiaDatasetConfig,
    load_eia_dataset_configs,
)
from .fred import (
    FredApiClient,
    FredCollector,
    FredDatasetConfig,
    load_fred_dataset_configs,
)
from .noaa import (
    DailyQuota,
    NoaaApiClient,
    NoaaCollector,
    NoaaDatasetConfig,
    NoaaRateLimiter,
    load_noaa_dataset_configs,
)
from .worldbank import (
    WorldBankApiClient,
    WorldBankCollector,
    WorldBankDatasetConfig,
    load_worldbank_dataset_configs,
)

__all__ = [
    "DailyQuota",
    "EiaApiClient",
    "EiaCollector",
    "EiaDatasetConfig",
    "FredApiClient",
    "FredCollector",
    "FredDatasetConfig",
    "NoaaApiClient",
    "NoaaCollector",
    "NoaaDatasetConfig",
    "NoaaRateLimiter",
    "WorldBankApiClient",
    "WorldBankCollector",
    "WorldBankDatasetConfig",
    "load_eia_dataset_configs",
    "load_fred_dataset_configs",
    "load_noaa_dataset_configs",
    "load_worldbank_dataset_configs",
]
