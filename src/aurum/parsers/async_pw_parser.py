"""Async wrapper for PW vendor parser with enhanced error handling."""

from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List

import pandas as pd

from .async_parser import AsyncParser, ParserResult
from .exceptions import FileFormatException, DataQualityException


class AsyncPWParser(AsyncParser):
    """Async wrapper for PW parser with enhanced features."""

    def __init__(self):
        super().__init__("pw")
        self._sync_parser = None

    def _parse_sync(self, path: str, asof: date) -> ParserResult:
        """Synchronous PW parsing with enhanced error handling."""
        errors = []
        warnings = []
        metadata = {}

        try:
            # Import sync parser dynamically to avoid circular imports
            if self._sync_parser is None:
                from .vendor_curves.parse_pw import parse as sync_parse
                self._sync_parser = sync_parse

            # Parse the file
            data = self._sync_parser(path, asof)

            if data.empty:
                errors.append("No data found in file")
                return ParserResult(
                    data=data,
                    metadata=metadata,
                    errors=errors,
                    warnings=warnings,
                    processing_time=0.0
                )

            # Validate data quality
            quality_issues = self._validate_data_quality(data)
            warnings.extend(quality_issues)

            # Enrich metadata
            metadata.update({
                "vendor": "PW",
                "file_type": "Excel",
                "rows_parsed": len(data),
                "columns": list(data.columns),
                "asof_date": asof.isoformat(),
            })

            return ParserResult(
                data=data,
                metadata=metadata,
                errors=errors,
                warnings=warnings,
                processing_time=0.0  # Would be set by async wrapper
            )

        except Exception as exc:
            errors.append(str(exc))
            # Return empty DataFrame with errors
            return ParserResult(
                data=pd.DataFrame(),
                metadata=metadata,
                errors=errors,
                warnings=warnings,
                processing_time=0.0
            )

    def _validate_data_quality(self, data: pd.DataFrame) -> List[str]:
        """Validate data quality and return list of issues."""
        issues = []

        # Check for required columns
        required_columns = ["iso", "market", "location", "mid", "asof_date"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")

        # Check for null values in critical fields
        if "mid" in data.columns:
            null_mids = data["mid"].isnull().sum()
            if null_mids > 0:
                issues.append(f"Found {null_mids} null MID values")

        # Check for negative prices
        if "mid" in data.columns:
            negative_prices = (data["mid"] < 0).sum()
            if negative_prices > 0:
                issues.append(f"Found {negative_prices} negative price values")

        # Check for data consistency
        if "iso" in data.columns and "market" in data.columns:
            inconsistent_records = data[
                (data["iso"].isna()) & (data["market"].notna()) |
                (data["iso"].notna()) & (data["market"].isna())
            ]
            if not inconsistent_records.empty:
                issues.append(f"Found {len(inconsistent_records)} records with inconsistent ISO/market data")

        return issues

    async def parse_with_validation(self, path: str, asof: date) -> ParserResult:
        """Parse file with enhanced validation and quality checks."""
        result = await self.parse_async(path, asof)

        # Additional async validation
        validation_issues = await self._async_validate(result.data)
        result.errors.extend(validation_issues)

        # If too many errors, raise exception
        if len(result.errors) > len(result.data) * 0.5:  # More than 50% errors
            raise DataQualityException(
                parser=self.name,
                file_path=path,
                quality_issues=result.errors,
                request_id=self.logger.parent  # Would need proper request ID
            )

        return result

    async def _async_validate(self, data: pd.DataFrame) -> List[str]:
        """Perform async validation operations."""
        issues = []

        # Check for duplicates asynchronously
        if not data.empty:
            # This could be done in parallel with other operations
            duplicates = data.duplicated().sum()
            if duplicates > 0:
                issues.append(f"Found {duplicates} duplicate records")

        return issues
