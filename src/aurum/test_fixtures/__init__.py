"""Test fixtures and golden files for data ingestion validation."""

from __future__ import annotations

from .fixture_generator import (
    FixtureGenerator,
    DataSourceFixture,
    TestCase,
    FixtureConfig
)
from .golden_file_manager import GoldenFileManager, GoldenFile
from .test_validator import TestValidator, ValidationResult
from .synthetic_data import (
    SyntheticDataGenerator,
    EIATestDataGenerator,
    FREDTestDataGenerator,
    CPITestDataGenerator,
    NOAAWeatherDataGenerator,
    ISOTestDataGenerator
)

__all__ = [
    "FixtureGenerator",
    "DataSourceFixture",
    "TestCase",
    "FixtureConfig",
    "GoldenFileManager",
    "GoldenFile",
    "TestValidator",
    "ValidationResult",
    "SyntheticDataGenerator",
    "EIATestDataGenerator",
    "FREDTestDataGenerator",
    "CPITestDataGenerator",
    "NOAAWeatherDataGenerator",
    "ISOTestDataGenerator"
]
