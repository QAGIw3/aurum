"""Aurum storage repositories: timescale, postgres, trino, clickhouse."""

from .ports import SeriesRepository, MetadataRepository, AnalyticRepository, ReadOnlyRepository, CacheRepository
from .timescale import TimescaleSeriesRepo
from .postgres import PostgresMetaRepo
from .trino import TrinoAnalyticRepo
from .redis_cache import RedisCacheRepo

__all__ = [
    # Interfaces
    "SeriesRepository",
    "MetadataRepository", 
    "AnalyticRepository",
    "ReadOnlyRepository",
    "CacheRepository",
    # Implementations
    "TimescaleSeriesRepo",
    "PostgresMetaRepo",
    "TrinoAnalyticRepo",
    "RedisCacheRepo",
]