"""Utilities for integrating Drought.gov datasets into Aurum."""

from .catalog import (
    DroughtCatalog,
    RasterIndexDataset,
    VectorLayer,
    FoundationalBucket,
    RegionSet,
    load_catalog,
)
from .fetcher import HttpAsset, AssetDownloadResult, AssetFetcher
from .zonal import ZonalStatisticsEngine

__all__ = [
    "DroughtCatalog",
    "RasterIndexDataset",
    "VectorLayer",
    "FoundationalBucket",
    "RegionSet",
    "load_catalog",
    "HttpAsset",
    "AssetDownloadResult",
    "AssetFetcher",
    "ZonalStatisticsEngine",
]
