"""Data processing improvements with streaming and validation."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncGenerator, Dict, List, Optional
from dataclasses import dataclass

from pydantic import BaseModel, Field, validator

from ..core.models import AurumBaseModel
from ..telemetry.context import get_request_id


@dataclass
class DataQualityMetrics:
    """Data quality metrics for a dataset."""
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    null_value_count: int = 0
    duplicate_count: int = 0
    processing_time: float = 0.0
    schema_compliance: float = 1.0

    def get_validity_rate(self) -> float:
        """Get the validity rate as a percentage."""
        return self.valid_rows / max(self.total_rows, 1)

    def get_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)."""
        if self.total_rows == 0:
            return 100.0

        validity_weight = 0.4
        schema_weight = 0.3
        null_weight = 0.2
        duplicate_weight = 0.1

        validity_score = self.get_validity_rate() * 100
        schema_score = self.schema_compliance * 100
        null_score = (1 - self.null_value_count / max(self.total_rows * 10, 1)) * 100
        duplicate_score = (1 - self.duplicate_count / max(self.total_rows, 1)) * 100

        return (
            validity_score * validity_weight +
            schema_score * schema_weight +
            null_score * null_weight +
            duplicate_score * duplicate_weight
        )


class DataValidator:
    """Validates data against schemas and business rules."""

    def __init__(self):
        self.validation_rules = self._load_validation_rules()

    def _load_validation_rules(self) -> Dict[str, Any]:
        """Load validation rules for different data types."""
        return {
            "curve_observation": {
                "required_fields": ["asof_date", "iso", "market", "location", "mid"],
                "numeric_fields": ["mid", "bid", "ask"],
                "date_fields": ["asof_date", "contract_month"],
                "string_fields": ["iso", "market", "location", "tenor_label"],
            },
            "metadata": {
                "required_fields": ["name", "value"],
                "string_fields": ["name", "value", "description"],
            }
        }

    async def validate_data(
        self,
        data: List[Dict[str, Any]],
        data_type: str
    ) -> Tuple[List[Dict[str, Any]], DataQualityMetrics]:
        """Validate data and return quality metrics."""
        start_time = time.time()

        if data_type not in self.validation_rules:
            raise ValueError(f"Unknown data type: {data_type}")

        rules = self.validation_rules[data_type]
        metrics = DataQualityMetrics()
        metrics.total_rows = len(data)

        valid_data = []
        invalid_data = []

        for row in data:
            try:
                # Check required fields
                for field in rules["required_fields"]:
                    if field not in row or row[field] is None:
                        metrics.invalid_rows += 1
                        invalid_data.append(row)
                        continue

                # Validate field types
                await self._validate_field_types(row, rules)

                # Check for null values
                null_count = sum(1 for v in row.values() if v is None)
                metrics.null_value_count += null_count

                # Check for duplicates (simplified)
                if row in valid_data:
                    metrics.duplicate_count += 1
                    continue

                valid_data.append(row)
                metrics.valid_rows += 1

            except Exception:
                metrics.invalid_rows += 1
                invalid_data.append(row)

        metrics.processing_time = time.time() - start_time
        metrics.schema_compliance = metrics.valid_rows / max(metrics.total_rows, 1)

        return valid_data, metrics

    async def _validate_field_types(self, row: Dict[str, Any], rules: Dict[str, Any]) -> None:
        """Validate field types according to rules."""
        # This is a simplified validation - in practice would be more comprehensive
        for field in rules.get("numeric_fields", []):
            if field in row and row[field] is not None:
                try:
                    float(row[field])
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid numeric value for field {field}")

        for field in rules.get("date_fields", []):
            if field in row and row[field] is not None:
                # Basic date validation
                if isinstance(row[field], str):
                    # Would validate date format here
                    pass


class DataStreamProcessor:
    """Process large datasets with streaming and validation."""

    def __init__(self, validator: DataValidator, chunk_size: int = 1000):
        self.validator = validator
        self.chunk_size = chunk_size
        self._processing_stats = {}

    async def process_stream(
        self,
        data_stream: AsyncGenerator[List[Dict[str, Any]], None],
        data_type: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Process a stream of data with validation and metrics."""
        request_id = get_request_id()

        async for chunk in data_stream:
            start_time = time.time()

            # Validate chunk
            valid_data, metrics = await self.validator.validate_data(chunk, data_type)

            processing_time = time.time() - start_time

            # Record statistics
            self._processing_stats[request_id] = self._processing_stats.get(request_id, {})
            self._processing_stats[request_id]["chunks_processed"] = \
                self._processing_stats[request_id].get("chunks_processed", 0) + 1
            self._processing_stats[request_id]["total_rows"] = \
                self._processing_stats[request_id].get("total_rows", 0) + len(chunk)
            self._processing_stats[request_id]["valid_rows"] = \
                self._processing_stats[request_id].get("valid_rows", 0) + len(valid_data)

            # Yield valid data
            for row in valid_data:
                yield row

    async def get_processing_stats(self, request_id: str) -> Dict[str, Any]:
        """Get processing statistics for a request."""
        return self._processing_stats.get(request_id, {})

    async def clear_stats(self, request_id: str) -> None:
        """Clear processing statistics for a request."""
        self._processing_stats.pop(request_id, None)


class DataEnricher:
    """Enrich data with additional metadata and derived fields."""

    def __init__(self):
        self.enrichment_rules = self._load_enrichment_rules()

    def _load_enrichment_rules(self) -> Dict[str, Any]:
        """Load enrichment rules."""
        return {
            "curve_observation": {
                "derived_fields": {
                    "curve_key": lambda row: f"{row.get('iso', '')}_{row.get('market', '')}_{row.get('location', '')}",
                    "price_spread": lambda row: (row.get('ask', 0) or 0) - (row.get('bid', 0) or 0),
                    "has_bid_ask": lambda row: row.get('bid') is not None and row.get('ask') is not None,
                },
                "metadata": {
                    "data_source": "aurum_api",
                    "enriched_at": lambda: time.time(),
                    "enriched_by": "DataEnricher",
                }
            }
        }

    async def enrich_data(
        self,
        data: List[Dict[str, Any]],
        data_type: str
    ) -> List[Dict[str, Any]]:
        """Enrich data with derived fields and metadata."""
        if data_type not in self.enrichment_rules:
            return data

        rules = self.enrichment_rules[data_type]
        enriched_data = []

        for row in data:
            enriched_row = row.copy()

            # Add derived fields
            for field_name, field_func in rules.get("derived_fields", {}).items():
                try:
                    enriched_row[field_name] = field_func(row)
                except Exception:
                    enriched_row[field_name] = None

            # Add metadata
            for field_name, field_value in rules.get("metadata", {}).items():
                if callable(field_value):
                    enriched_row[field_name] = field_value()
                else:
                    enriched_row[field_name] = field_value

            enriched_data.append(enriched_row)

        return enriched_data


class DataPipeline:
    """Complete data processing pipeline with streaming, validation, and enrichment."""

    def __init__(self):
        self.validator = DataValidator()
        self.enricher = DataEnricher()
        self.processor = DataStreamProcessor(self.validator)

    async def process_dataset(
        self,
        data_stream: AsyncGenerator[List[Dict[str, Any]], None],
        data_type: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Process a complete dataset through the pipeline."""
        async for enriched_row in self.processor.process_stream(data_stream, data_type):
            yield enriched_row

    async def get_pipeline_stats(self, request_id: str) -> Dict[str, Any]:
        """Get pipeline processing statistics."""
        return await self.processor.get_processing_stats(request_id)

    async def validate_and_enrich_batch(
        self,
        data: List[Dict[str, Any]],
        data_type: str
    ) -> Tuple[List[Dict[str, Any]], DataQualityMetrics]:
        """Validate and enrich a batch of data."""
        # Validate
        valid_data, metrics = await self.validator.validate_data(data, data_type)

        # Enrich
        enriched_data = await self.enricher.enrich_data(valid_data, data_type)

        return enriched_data, metrics


class DataQualityReporter:
    """Generate reports on data quality."""

    def __init__(self):
        self.reports = []

    async def generate_report(
        self,
        metrics: DataQualityMetrics,
        data_type: str,
        request_id: str
    ) -> Dict[str, Any]:
        """Generate a data quality report."""
        report = {
            "data_type": data_type,
            "request_id": request_id,
            "timestamp": time.time(),
            "metrics": {
                "total_rows": metrics.total_rows,
                "valid_rows": metrics.valid_rows,
                "invalid_rows": metrics.invalid_rows,
                "validity_rate": metrics.get_validity_rate(),
                "quality_score": metrics.get_quality_score(),
                "processing_time": metrics.processing_time,
                "schema_compliance": metrics.schema_compliance,
            },
            "recommendations": self._generate_recommendations(metrics),
        }

        self.reports.append(report)
        return report

    def _generate_recommendations(self, metrics: DataQualityMetrics) -> List[str]:
        """Generate recommendations based on quality metrics."""
        recommendations = []

        validity_rate = metrics.get_validity_rate()
        if validity_rate < 0.95:
            recommendations.append("High invalid data rate detected - review data sources")

        quality_score = metrics.get_quality_score()
        if quality_score < 80:
            recommendations.append("Overall data quality is poor - consider data cleaning")

        if metrics.null_value_count > metrics.total_rows * 0.1:
            recommendations.append("High number of null values - consider data imputation")

        if metrics.duplicate_count > 0:
            recommendations.append("Duplicate records found - implement deduplication")

        return recommendations or ["Data quality is acceptable"]

    async def get_recent_reports(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent quality reports."""
        return self.reports[-limit:]


# Global instances
_data_pipeline = DataPipeline()
_data_quality_reporter = DataQualityReporter()


def get_data_pipeline() -> DataPipeline:
    """Get the global data processing pipeline."""
    return _data_pipeline


def get_data_quality_reporter() -> DataQualityReporter:
    """Get the global data quality reporter."""
    return _data_quality_reporter
