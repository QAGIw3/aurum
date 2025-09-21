"""Async parser framework with improved error handling and performance."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ..telemetry.context import get_request_id
from .exceptions import ParserException, ValidationException, ProcessingException


@dataclass
class ParserResult:
    """Result of parsing operation."""
    data: pd.DataFrame
    metadata: Dict[str, Any]
    errors: List[str]
    warnings: List[str]
    processing_time: float


@dataclass
class ParserMetrics:
    """Parser performance metrics."""
    rows_processed: int = 0
    files_processed: int = 0
    total_processing_time: float = 0.0
    error_count: int = 0
    warning_count: int = 0


class AsyncParser(ABC):
    """Abstract base class for async parsers."""

    def __init__(self, name: str, max_workers: int = 4):
        self.name = name
        self.logger = logging.getLogger(f"aurum.parsers.{name}")
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"parser-{name}"
        )
        self.metrics = ParserMetrics()

    async def parse_async(self, path: str, asof: date) -> ParserResult:
        """Parse file asynchronously with metrics tracking."""
        start_time = asyncio.get_event_loop().time()
        request_id = get_request_id()

        try:
            self.logger.info(f"Starting parse of {path} for {asof}", extra={"request_id": request_id})

            # Run parsing in thread pool to avoid blocking
            result = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._parse_sync,
                path,
                asof
            )

            processing_time = asyncio.get_event_loop().time() - start_time

            # Update metrics
            self.metrics.files_processed += 1
            self.metrics.rows_processed += len(result.data)
            self.metrics.total_processing_time += processing_time
            self.metrics.error_count += len(result.errors)
            self.metrics.warning_count += len(result.warnings)

            self.logger.info(
                f"Completed parse of {path}: {len(result.data)} rows, "
                f"{len(result.errors)} errors, {processing_time".2f"}s",
                extra={"request_id": request_id}
            )

            return result

        except Exception as exc:
            processing_time = asyncio.get_event_loop().time() - start_time
            self.metrics.error_count += 1

            self.logger.error(
                f"Failed to parse {path}: {exc}",
                extra={"request_id": request_id, "exc_info": True}
            )

            raise ParserException(
                parser=self.name,
                file_path=path,
                message=f"Parse failed: {exc}",
                request_id=request_id
            ) from exc

    @abstractmethod
    def _parse_sync(self, path: str, asof: date) -> ParserResult:
        """Synchronous parsing implementation."""
        pass

    async def get_metrics(self) -> ParserMetrics:
        """Get parser performance metrics."""
        return self.metrics

    async def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self.metrics = ParserMetrics()


class ParserRegistry:
    """Registry for parser implementations."""

    def __init__(self):
        self._parsers: Dict[str, AsyncParser] = {}
        self._metrics: Dict[str, ParserMetrics] = {}

    def register(self, vendor: str, parser: AsyncParser) -> None:
        """Register a parser for a vendor."""
        self._parsers[vendor] = parser
        self.logger.info(f"Registered parser for vendor: {vendor}")

    async def parse(self, vendor: str, path: str, asof: date) -> ParserResult:
        """Parse file using registered parser."""
        if vendor not in self._parsers:
            raise ValidationException(
                field="vendor",
                message=f"No parser registered for vendor '{vendor}'",
                request_id=get_request_id()
            )

        parser = self._parsers[vendor]
        return await parser.parse_async(path, asof)

    async def get_all_metrics(self) -> Dict[str, ParserMetrics]:
        """Get metrics for all parsers."""
        metrics = {}
        for vendor, parser in self._parsers.items():
            metrics[vendor] = await parser.get_metrics()
        return metrics

    async def reset_all_metrics(self) -> None:
        """Reset metrics for all parsers."""
        for parser in self._parsers.values():
            await parser.reset_metrics()


class ParserValidator:
    """Validates parser results against schemas."""

    def __init__(self):
        self.validation_rules = self._load_validation_rules()

    def _load_validation_rules(self) -> Dict[str, Any]:
        """Load validation rules for different data types."""
        return {
            "required_columns": [
                "asof_date", "iso", "market", "location", "mid",
                "tenor_label", "price_type", "curve_key"
            ],
            "data_types": {
                "asof_date": "datetime64[ns]",
                "mid": "float64",
                "bid": "float64",
                "ask": "float64",
            },
            "business_rules": {
                "non_negative_prices": lambda df: (df[["mid", "bid", "ask"]] >= 0).all().all(),
                "valid_price_types": lambda df: df["price_type"].isin(["MID", "BID", "ASK", "LAST"]).all(),
                "required_iso": lambda df: df["iso"].notna().all(),
            }
        }

    async def validate(self, result: ParserResult) -> List[str]:
        """Validate parser result and return list of issues."""
        issues = []

        # Check required columns
        missing_columns = set(self.validation_rules["required_columns"]) - set(result.data.columns)
        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")

        # Check data types
        for column, expected_type in self.validation_rules["data_types"].items():
            if column in result.data.columns:
                try:
                    result.data[column] = result.data[column].astype(expected_type)
                except Exception as exc:
                    issues.append(f"Column {column} type conversion failed: {exc}")

        # Check business rules
        for rule_name, rule_func in self.validation_rules["business_rules"].items():
            try:
                if not rule_func(result.data):
                    issues.append(f"Business rule failed: {rule_name}")
            except Exception as exc:
                issues.append(f"Business rule error {rule_name}: {exc}")

        # Check for common data quality issues
        if result.data.empty:
            issues.append("No data rows found")
        else:
            # Check for excessive null values
            null_percentages = result.data.isnull().mean()
            high_null_columns = null_percentages[null_percentages > 0.5]
            if not high_null_columns.empty:
                issues.append(f"High null percentages in columns: {list(high_null_columns.index)}")

            # Check for duplicates
            duplicates = result.data.duplicated().sum()
            if duplicates > 0:
                issues.append(f"Found {duplicates} duplicate rows")

        return issues


class ParserPipeline:
    """Complete parsing pipeline with validation and enrichment."""

    def __init__(self):
        self.registry = ParserRegistry()
        self.validator = ParserValidator()
        self._setup_default_parsers()

    def _setup_default_parsers(self) -> None:
        """Setup default parser implementations."""
        # This would register the async versions of the existing parsers
        # For now, we'll create async wrappers around existing parsers
        pass

    async def process_file(
        self,
        file_path: str,
        asof: date,
        vendor: Optional[str] = None
    ) -> ParserResult:
        """Process a file through the complete parsing pipeline."""
        request_id = get_request_id()

        # Detect vendor if not provided
        if vendor is None:
            vendor = self._detect_vendor_from_path(file_path)

        # Parse file
        result = await self.registry.parse(vendor, file_path, asof)

        # Validate result
        validation_issues = await self.validator.validate(result)
        result.errors.extend(validation_issues)

        # Enrich metadata
        result.metadata.update({
            "vendor_detected": vendor,
            "file_path": file_path,
            "asof_date": asof.isoformat(),
            "validation_issues": len(validation_issues),
            "request_id": request_id,
        })

        return result

    def _detect_vendor_from_path(self, file_path: str) -> str:
        """Detect vendor from file path."""
        path = Path(file_path)
        name = path.name.upper()

        vendor_hints = {
            "_PW_": "pw",
            "_EUGP_": "eugp",
            "_RP_": "rp",
            "_SIMPLE_": "simple",
        }

        for marker, vendor in vendor_hints.items():
            if marker in name:
                return vendor

        raise ValidationException(
            field="file_path",
            message=f"Could not detect vendor from filename '{path.name}'",
            request_id=get_request_id()
        )

    async def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            "parser_metrics": await self.registry.get_all_metrics(),
            "request_id": get_request_id(),
        }


# Global instances
_parser_pipeline = ParserPipeline()


def get_parser_pipeline() -> ParserPipeline:
    """Get the global parser pipeline instance."""
    return _parser_pipeline
