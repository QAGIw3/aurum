"""Cost estimation for data ingestion pipelines."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from .profiler import DatasetProfile, PerformanceMetrics


@dataclass
class CostModel:
    """Cost model for data ingestion."""

    # API costs (per call)
    api_call_cost_usd: Dict[str, float] = field(default_factory=lambda: {
        "eia": 0.001,      # EIA API calls
        "fred": 0.0005,    # FRED API calls
        "cpi": 0.0005,     # CPI API calls (FRED)
        "noaa": 0.0001,    # NOAA API calls
        "iso": 0.002       # ISO data access
    })

    # Data transfer costs (per GB)
    data_transfer_cost_usd_per_gb: Dict[str, float] = field(default_factory=lambda: {
        "inbound": 0.00,   # Usually free inbound
        "outbound": 0.09,  # AWS outbound data transfer
        "internal": 0.01   # Internal data transfer
    })

    # Compute costs (per hour)
    compute_cost_usd_per_hour: Dict[str, float] = field(default_factory=lambda: {
        "seatunnel": 0.05,  # SeaTunnel processing
        "airflow": 0.03,    # Airflow orchestration
        "kafka": 0.02       # Kafka streaming
    })

    # Storage costs (per GB per month)
    storage_cost_usd_per_gb_month: Dict[str, float] = field(default_factory=lambda: {
        "s3": 0.023,        # S3 Standard storage
        "postgres": 0.125,  # RDS PostgreSQL
        "kafka": 0.05       # Kafka storage
    })

    # Additional multipliers
    error_retry_cost_multiplier: float = 1.5  # Extra cost for retries due to errors
    peak_hour_multiplier: float = 1.2        # Higher costs during peak hours
    data_quality_cost_multiplier: float = 1.1 # Extra cost for data quality issues

    def get_api_cost(self, data_source: str) -> float:
        """Get API call cost for a data source.

        Args:
            data_source: Data source identifier

        Returns:
            Cost per API call in USD
        """
        return self.api_call_cost_usd.get(data_source, 0.001)

    def get_compute_cost(self, component: str) -> float:
        """Get compute cost for a component.

        Args:
            component: Component name

        Returns:
            Cost per hour in USD
        """
        return self.compute_cost_usd_per_hour.get(component, 0.05)


@dataclass
class CostEstimate:
    """Cost estimate for data ingestion."""

    dataset_name: str
    data_source: str
    time_period: str  # daily, monthly, yearly

    # Costs breakdown
    api_costs_usd: float = 0.0
    compute_costs_usd: float = 0.0
    storage_costs_usd: float = 0.0
    data_transfer_costs_usd: float = 0.0
    total_costs_usd: float = 0.0

    # Cost factors
    error_retry_multiplier: float = 1.0
    data_quality_multiplier: float = 1.0
    peak_hour_multiplier: float = 1.0

    # Volume metrics
    estimated_api_calls: int = 0
    estimated_data_volume_gb: float = 0.0
    estimated_compute_hours: float = 0.0

    # Efficiency metrics
    cost_per_record_usd: float = 0.0
    cost_per_gb_usd: float = 0.0
    records_per_usd: float = 0.0

    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "dataset_name": self.dataset_name,
            "data_source": self.data_source,
            "time_period": self.time_period,
            "costs_breakdown": {
                "api_costs_usd": self.api_costs_usd,
                "compute_costs_usd": self.compute_costs_usd,
                "storage_costs_usd": self.storage_costs_usd,
                "data_transfer_costs_usd": self.data_transfer_costs_usd,
                "total_costs_usd": self.total_costs_usd
            },
            "cost_factors": {
                "error_retry_multiplier": self.error_retry_multiplier,
                "data_quality_multiplier": self.data_quality_multiplier,
                "peak_hour_multiplier": self.peak_hour_multiplier
            },
            "volume_metrics": {
                "estimated_api_calls": self.estimated_api_calls,
                "estimated_data_volume_gb": self.estimated_data_volume_gb,
                "estimated_compute_hours": self.estimated_compute_hours
            },
            "efficiency_metrics": {
                "cost_per_record_usd": self.cost_per_record_usd,
                "cost_per_gb_usd": self.cost_per_gb_usd,
                "records_per_usd": self.records_per_usd
            },
            "generated_at": self.generated_at
        }


@dataclass
class DataSourceCosts:
    """Cost breakdown by data source."""

    data_source: str
    datasets: Dict[str, CostEstimate] = field(default_factory=dict)
    total_costs_usd: float = 0.0
    average_cost_per_dataset_usd: float = 0.0
    cost_efficiency_score: float = 0.0  # Higher is better

    def add_dataset_cost(self, dataset_name: str, cost_estimate: CostEstimate) -> None:
        """Add cost estimate for a dataset.

        Args:
            dataset_name: Name of the dataset
            cost_estimate: Cost estimate for the dataset
        """
        self.datasets[dataset_name] = cost_estimate
        self._recalculate_totals()

    def _recalculate_totals(self) -> None:
        """Recalculate total and average costs."""
        if not self.datasets:
            self.total_costs_usd = 0.0
            self.average_cost_per_dataset_usd = 0.0
            return

        total_costs = sum(cost.total_costs_usd for cost in self.datasets.values())
        self.total_costs_usd = total_costs
        self.average_cost_per_dataset_usd = total_costs / len(self.datasets)

        # Calculate efficiency score (records per dollar)
        total_records = sum(
            cost.records_per_usd * cost.total_costs_usd
            for cost in self.datasets.values()
        )

        if total_costs > 0:
            self.cost_efficiency_score = total_records / total_costs
        else:
            self.cost_efficiency_score = 0.0

    def get_cost_summary(self) -> Dict[str, Any]:
        """Get cost summary for this data source.

        Returns:
            Cost summary dictionary
        """
        return {
            "data_source": self.data_source,
            "total_costs_usd": self.total_costs_usd,
            "average_cost_per_dataset_usd": self.average_cost_per_dataset_usd,
            "cost_efficiency_score": self.cost_efficiency_score,
            "dataset_count": len(self.datasets),
            "datasets": {
                name: cost.to_dict() for name, cost in self.datasets.items()
            }
        }


class CostEstimator:
    """Estimate costs for data ingestion based on performance metrics."""

    def __init__(self, cost_model: Optional[CostModel] = None):
        """Initialize cost estimator.

        Args:
            cost_model: Optional cost model, uses default if not provided
        """
        self.cost_model = cost_model or CostModel()

    def estimate_dataset_cost(
        self,
        profile: DatasetProfile,
        time_period: str = "monthly",
        include_multipliers: bool = True
    ) -> CostEstimate:
        """Estimate cost for a dataset based on its profile.

        Args:
            profile: Dataset profile
            time_period: Time period for estimation (daily, monthly, yearly)
            include_multipliers: Whether to include cost multipliers

        Returns:
            Cost estimate for the dataset
        """
        if not profile.metrics:
            return CostEstimate(
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                time_period=time_period
            )

        avg_metrics = profile.get_average_metrics()
        if not avg_metrics:
            return CostEstimate(
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                time_period=time_period
            )

        # Calculate base costs
        api_costs = self._calculate_api_costs(profile, avg_metrics)
        compute_costs = self._calculate_compute_costs(profile, avg_metrics)
        storage_costs = self._calculate_storage_costs(profile, avg_metrics)
        data_transfer_costs = self._calculate_data_transfer_costs(profile, avg_metrics)

        # Calculate multipliers
        error_retry_multiplier = 1.0
        data_quality_multiplier = 1.0
        peak_hour_multiplier = 1.0

        if include_multipliers:
            efficiency = avg_metrics.calculate_efficiency()
            error_retry_multiplier = (
                1.0 + (avg_metrics.retry_count / max(avg_metrics.api_call_count, 1)) *
                (self.cost_model.error_retry_cost_multiplier - 1.0)
            )
            data_quality_multiplier = (
                1.0 + (1.0 - efficiency.get("data_quality_score", 1.0)) *
                (self.cost_model.data_quality_cost_multiplier - 1.0)
            )
            peak_hour_multiplier = self.cost_model.peak_hour_multiplier

        # Apply multipliers
        api_costs *= error_retry_multiplier * data_quality_multiplier * peak_hour_multiplier
        compute_costs *= data_quality_multiplier * peak_hour_multiplier

        total_costs = api_costs + compute_costs + storage_costs + data_transfer_costs

        # Calculate volume metrics
        api_calls_per_period = self._scale_to_period(avg_metrics.api_call_count, time_period)
        data_volume_per_period = self._scale_to_period(avg_metrics.byte_count / (1024**3), time_period)  # GB
        compute_hours_per_period = self._scale_to_period(avg_metrics.processing_time_seconds / 3600, time_period)  # Hours

        # Calculate efficiency metrics
        cost_per_record = total_costs / max(avg_metrics.record_count, 1)
        cost_per_gb = total_costs / max(data_volume_per_period, 0.001)
        records_per_usd = avg_metrics.record_count / max(total_costs, 0.001)

        return CostEstimate(
            dataset_name=profile.dataset_name,
            data_source=profile.data_source,
            time_period=time_period,
            api_costs_usd=api_costs,
            compute_costs_usd=compute_costs,
            storage_costs_usd=storage_costs,
            data_transfer_costs_usd=data_transfer_costs,
            total_costs_usd=total_costs,
            error_retry_multiplier=error_retry_multiplier,
            data_quality_multiplier=data_quality_multiplier,
            peak_hour_multiplier=peak_hour_multiplier,
            estimated_api_calls=int(api_calls_per_period),
            estimated_data_volume_gb=data_volume_per_period,
            estimated_compute_hours=compute_hours_per_period,
            cost_per_record_usd=cost_per_record,
            cost_per_gb_usd=cost_per_gb,
            records_per_usd=records_per_usd
        )

    def _calculate_api_costs(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> float:
        """Calculate API costs.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            API costs in USD
        """
        api_cost_per_call = self.cost_model.get_api_cost(profile.data_source)
        return metrics.api_call_count * api_cost_per_call

    def _calculate_compute_costs(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> float:
        """Calculate compute costs.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            Compute costs in USD
        """
        # Estimate compute hours based on processing time
        compute_hours = metrics.processing_time_seconds / 3600  # Convert to hours

        # Use SeaTunnel cost as default, can be refined per data source
        compute_cost_per_hour = self.cost_model.get_compute_cost("seatunnel")

        return compute_hours * compute_cost_per_hour

    def _calculate_storage_costs(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> float:
        """Calculate storage costs.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            Storage costs in USD
        """
        # This is a rough estimate - in practice would need actual storage tracking
        # Estimate based on data volume processed
        data_volume_gb = metrics.byte_count / (1024**3)

        # Use Kafka storage cost as default for streaming data
        storage_cost_per_gb_month = self.cost_model.storage_cost_usd_per_gb_month["kafka"]

        # Convert to per-run cost (assuming data is stored temporarily)
        return data_volume_gb * storage_cost_per_gb_month / 30  # Per day approximation

    def _calculate_data_transfer_costs(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> float:
        """Calculate data transfer costs.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            Data transfer costs in USD
        """
        # Estimate data transfer based on processed bytes
        data_volume_gb = metrics.byte_count / (1024**3)

        # Use internal data transfer cost (lower than outbound)
        transfer_cost_per_gb = self.cost_model.data_transfer_cost_usd_per_gb["internal"]

        return data_volume_gb * transfer_cost_per_gb

    def _scale_to_period(self, value: float, time_period: str) -> float:
        """Scale a value to the specified time period.

        Args:
            value: Value to scale
            time_period: Time period (daily, monthly, yearly)

        Returns:
            Scaled value
        """
        if time_period == "daily":
            return value
        elif time_period == "monthly":
            return value * 30  # Approximate month
        elif time_period == "yearly":
            return value * 365  # Approximate year
        else:
            return value

    def estimate_data_source_costs(
        self,
        profiles: Dict[str, DatasetProfile],
        time_period: str = "monthly"
    ) -> Dict[str, DataSourceCosts]:
        """Estimate costs for all datasets by data source.

        Args:
            profiles: Dictionary of dataset profiles by name
            time_period: Time period for estimation

        Returns:
            Dictionary of data source costs by data source
        """
        data_source_costs: Dict[str, DataSourceCosts] = {}

        for dataset_name, profile in profiles.items():
            if not profile.data_source:
                continue

            # Initialize data source costs if not exists
            if profile.data_source not in data_source_costs:
                data_source_costs[profile.data_source] = DataSourceCosts(
                    data_source=profile.data_source
                )

            # Estimate costs for this dataset
            cost_estimate = self.estimate_dataset_cost(profile, time_period)

            # Add to data source costs
            data_source_costs[profile.data_source].add_dataset_cost(
                dataset_name, cost_estimate
            )

        return data_source_costs

    def generate_cost_report(
        self,
        profiles: Dict[str, DatasetProfile],
        time_period: str = "monthly"
    ) -> Dict[str, Any]:
        """Generate comprehensive cost report.

        Args:
            profiles: Dictionary of dataset profiles by name
            time_period: Time period for estimation

        Returns:
            Comprehensive cost report
        """
        data_source_costs = self.estimate_data_source_costs(profiles, time_period)

        # Calculate totals across all data sources
        total_costs = 0.0
        total_datasets = 0
        cost_efficiency_scores = []

        for ds_costs in data_source_costs.values():
            total_costs += ds_costs.total_costs_usd
            total_datasets += len(ds_costs.datasets)
            cost_efficiency_scores.append(ds_costs.cost_efficiency_score)

        average_cost_per_dataset = total_costs / max(total_datasets, 1)
        overall_efficiency_score = (
            sum(cost_efficiency_scores) / max(len(cost_efficiency_scores), 1)
        )

        return {
            "generated_at": datetime.now().isoformat(),
            "time_period": time_period,
            "summary": {
                "total_costs_usd": total_costs,
                "total_datasets": total_datasets,
                "average_cost_per_dataset_usd": average_cost_per_dataset,
                "overall_cost_efficiency_score": overall_efficiency_score,
                "data_sources_analyzed": len(data_source_costs)
            },
            "data_source_breakdown": {
                ds: costs.get_cost_summary()
                for ds, costs in data_source_costs.items()
            },
            "cost_model_used": {
                "api_costs": self.cost_model.api_call_cost_usd,
                "compute_costs": self.cost_model.compute_cost_usd_per_hour,
                "storage_costs": self.cost_model.storage_cost_usd_per_gb_month,
                "data_transfer_costs": self.cost_model.data_transfer_cost_usd_per_gb
            }
        }
