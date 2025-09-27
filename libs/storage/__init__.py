"""Aurum storage repositories: timescale, postgres, trino, clickhouse."""

from .ports import SeriesRepository, MetadataRepository, AnalyticRepository, ReadOnlyRepository
from .timescale import TimescaleSeriesRepo
from .postgres import PostgresMetaRepo
from .trino import TrinoAnalyticRepo

__all__ = [
    # Interfaces
    "SeriesRepository",
    "MetadataRepository", 
    "AnalyticRepository",
    "ReadOnlyRepository",
    # Implementations
    "TimescaleSeriesRepo",
    "PostgresMetaRepo",
    "TrinoAnalyticRepo",
]