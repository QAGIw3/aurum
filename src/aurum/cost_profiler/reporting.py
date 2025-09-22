"""Reporting for cost profiler results."""

from __future__ import annotations

import json
import csv
from io import StringIO
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, TextIO
from dataclasses import dataclass, field

from .profiler import DatasetProfile
from .performance_analyzer import AnalysisResult
from .cost_estimator import CostEstimate, DataSourceCosts


@dataclass
class ReportConfig:
    """Configuration for report generation."""

    include_costs: bool = True
    include_performance_analysis: bool = True
    include_raw_metrics: bool = False
    include_recommendations: bool = True
    cost_time_period: str = "monthly"  # daily, monthly, yearly
    output_format: str = "json"  # json, csv, markdown
    include_charts: bool = True
    max_datasets: Optional[int] = None
    filter_data_sources: List[str] = field(default_factory=list)


@dataclass
class ProfileReport:
    """Comprehensive profile report."""

    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    config: ReportConfig = field(default_factory=ReportConfig)
    datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)
    analysis_results: Dict[str, Any] = field(default_factory=dict)
    cost_estimates: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "generated_at": self.generated_at,
            "config": {
                "include_costs": self.config.include_costs,
                "include_performance_analysis": self.config.include_performance_analysis,
                "include_raw_metrics": self.config.include_raw_metrics,
                "include_recommendations": self.config.include_recommendations,
                "cost_time_period": self.config.cost_time_period,
                "output_format": self.config.output_format,
                "max_datasets": self.config.max_datasets,
                "filter_data_sources": self.config.filter_data_sources
            },
            "datasets": self.datasets,
            "summary": self.summary,
            "analysis_results": self.analysis_results,
            "cost_estimates": self.cost_estimates
        }

    def to_json(self) -> str:
        """Convert to JSON string.

        Returns:
            JSON representation
        """
        return json.dumps(self.to_dict(), indent=2, default=str)

    def to_csv(self) -> str:
        """Convert to CSV format.

        Returns:
            CSV representation
        """
        output = StringIO()

        # Write summary first
        writer = csv.writer(output)
        writer.writerow(["Report Summary"])
        writer.writerow(["Generated At", self.generated_at])

        for key, value in self.summary.items():
            if isinstance(value, dict):
                writer.writerow([f"Summary {key}", json.dumps(value)])
            else:
                writer.writerow([f"Summary {key}", value])

        writer.writerow([])

        # Write datasets
        if self.datasets:
            writer.writerow(["Dataset Performance"])

            # Get all unique keys from datasets
            all_keys = set()
            for dataset_data in self.datasets.values():
                all_keys.update(dataset_data.keys())

            # Write header
            header = ["Dataset Name", "Data Source"] + sorted(all_keys)
            writer.writerow(header)

            # Write dataset rows
            for dataset_name, dataset_data in self.datasets.items():
                row = [dataset_name, dataset_data.get("data_source", "")]
                for key in sorted(all_keys):
                    value = dataset_data.get(key, "")
                    if isinstance(value, dict):
                        row.append(json.dumps(value))
                    else:
                        row.append(value)
                writer.writerow(row)

        return output.getvalue()

    def to_markdown(self) -> str:
        """Convert to Markdown format.

        Returns:
            Markdown representation
        """
        md = f"# Data Ingestion Performance Report\n\n"
        md += f"**Generated:** {self.generated_at}\n\n"

        # Summary section
        if self.summary:
            md += "## Summary\n\n"
            for key, value in self.summary.items():
                if isinstance(value, dict):
                    md += f"**{key}:**\n"
                    for sub_key, sub_value in value.items():
                        md += f"- {sub_key}: {sub_value}\n"
                    md += "\n"
                else:
                    md += f"**{key}:** {value}\n\n"

        # Datasets section
        if self.datasets:
            md += "## Dataset Performance\n\n"

            # Get all unique keys from datasets
            all_keys = set()
            for dataset_data in self.datasets.values():
                all_keys.update(dataset_data.keys())

            # Write header
            header = ["Dataset Name", "Data Source"] + sorted(all_keys)
            md += "| " + " | ".join(header) + " |\n"
            md += "| " + " | ".join(["---"] * len(header)) + " |\n"

            # Write dataset rows
            for dataset_name, dataset_data in self.datasets.items():
                row = [dataset_name, dataset_data.get("data_source", "")]
                for key in sorted(all_keys):
                    value = dataset_data.get(key, "")
                    if isinstance(value, dict):
                        row.append(f"`{json.dumps(value)}`")
                    else:
                        row.append(str(value))
                md += "| " + " | ".join(row) + " |\n"

            md += "\n"

        # Analysis results section
        if self.analysis_results and self.analysis_results.get("performance_issues"):
            md += "## Performance Issues\n\n"
            issues = self.analysis_results["performance_issues"]

            if issues:
                md += "| Dataset | Issue Type | Severity | Description |\n"
                md += "| --- | --- | --- | --- |\n"

                for issue in issues:
                    md += f"| {issue.get('dataset_name', '')} | {issue.get('issue_type', '')} | {issue.get('severity', '')} | {issue.get('description', '')} |\n"

                md += "\n"

        # Cost estimates section
        if self.cost_estimates and self.cost_estimates.get("data_source_breakdown"):
            md += "## Cost Estimates\n\n"
            breakdown = self.cost_estimates["data_source_breakdown"]

            md += "| Data Source | Total Cost (USD) | Avg Cost per Dataset | Efficiency Score |\n"
            md += "| --- | --- | --- | --- |\n"

            for ds_name, ds_data in breakdown.items():
                md += f"| {ds_name} | ${ds_data.get('total_costs_usd', 0):.2f} | ${ds_data.get('average_cost_per_dataset_usd', 0):.2f} | {ds_data.get('cost_efficiency_score', 0):.2f} |\n"

            md += "\n"

        return md


@dataclass
class CostReport:
    """Specialized cost report."""

    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    time_period: str = "monthly"
    total_costs_usd: float = 0.0
    data_source_costs: Dict[str, DataSourceCosts] = field(default_factory=dict)
    cost_breakdown: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "generated_at": self.generated_at,
            "time_period": self.time_period,
            "total_costs_usd": self.total_costs_usd,
            "data_source_costs": {
                ds: costs.get_cost_summary()
                for ds, costs in self.data_source_costs.items()
            },
            "cost_breakdown": self.cost_breakdown,
            "recommendations": self.recommendations
        }

    def to_markdown(self) -> str:
        """Convert to Markdown format.

        Returns:
            Markdown representation
        """
        md = f"# Data Ingestion Cost Report\n\n"
        md += f"**Generated:** {self.generated_at}\n"
        md += f"**Time Period:** {self.time_period}\n\n"

        md += f"## Total Costs\n\n"
        md += f"**Total Cost:** ${self.total_costs_usd:.2f} USD\n\n"

        if self.data_source_costs:
            md += "## Data Source Breakdown\n\n"
            md += "| Data Source | Cost (USD) | Datasets | Avg Cost/Dataset | Efficiency Score |\n"
            md += "| --- | --- | --- | --- | --- |\n"

            for ds_name, ds_costs in self.data_source_costs.items():
                summary = ds_costs.get_cost_summary()
                md += f"| {ds_name} | ${summary['total_costs_usd']:.2f} | {summary['dataset_count']} | ${summary['average_cost_per_dataset_usd']:.2f} | {summary['cost_efficiency_score']:.2f} |\n"

            md += "\n"

        if self.recommendations:
            md += "## Cost Optimization Recommendations\n\n"
            for i, rec in enumerate(self.recommendations, 1):
                md += f"{i}. {rec}\n"

            md += "\n"

        return md


class ProfileReporter:
    """Generate reports from profiling data."""

    def __init__(self, config: Optional[ReportConfig] = None):
        """Initialize profile reporter.

        Args:
            config: Optional report configuration
        """
        self.config = config or ReportConfig()

    def generate_profile_report(
        self,
        profiles: Dict[str, DatasetProfile],
        analysis_results: Optional[Dict[str, AnalysisResult]] = None,
        cost_estimator = None
    ) -> ProfileReport:
        """Generate comprehensive profile report.

        Args:
            profiles: Dictionary of dataset profiles by name
            analysis_results: Optional analysis results by dataset
            cost_estimator: Optional cost estimator for cost calculations

        Returns:
            Comprehensive profile report
        """
        report = ProfileReport(config=self.config)

        # Filter profiles if needed
        filtered_profiles = self._filter_profiles(profiles)

        # Generate dataset summaries
        for dataset_name, profile in filtered_profiles.items():
            if profile.metrics:
                avg_metrics = profile.get_average_metrics()
                if avg_metrics:
                    summary = profile.get_performance_summary()
                    report.datasets[dataset_name] = summary

        # Calculate overall summary
        report.summary = self._calculate_overall_summary(filtered_profiles)

        # Add analysis results
        if self.config.include_performance_analysis and analysis_results:
            report.analysis_results = {
                "performance_issues": [],
                "optimization_recommendations": []
            }

            for dataset_name, result in analysis_results.items():
                if dataset_name in filtered_profiles:
                    report.analysis_results["performance_issues"].extend([
                        issue.to_dict() for issue in result.performance_issues
                    ])
                    report.analysis_results["optimization_recommendations"].extend([
                        rec.to_dict() for rec in result.optimization_recommendations
                    ])

        # Add cost estimates
        if self.config.include_costs and cost_estimator:
            report.cost_estimates = cost_estimator.generate_cost_report(
                filtered_profiles,
                self.config.cost_time_period
            )

        return report

    def _filter_profiles(self, profiles: Dict[str, DatasetProfile]) -> Dict[str, DatasetProfile]:
        """Filter profiles based on configuration.

        Args:
            profiles: Profiles to filter

        Returns:
            Filtered profiles
        """
        filtered = profiles.copy()

        # Filter by data sources
        if self.config.filter_data_sources:
            filtered = {
                name: profile for name, profile in filtered.items()
                if profile.data_source in self.config.filter_data_sources
            }

        # Limit number of datasets
        if self.config.max_datasets and len(filtered) > self.config.max_datasets:
            # Sort by most recent and take top N
            sorted_profiles = sorted(
                filtered.items(),
                key=lambda x: x[1].created_at if hasattr(x[1], 'created_at') else '',
                reverse=True
            )
            filtered = dict(sorted_profiles[:self.config.max_datasets])

        return filtered

    def _calculate_overall_summary(self, profiles: Dict[str, DatasetProfile]) -> Dict[str, Any]:
        """Calculate overall summary statistics.

        Args:
            profiles: Dataset profiles

        Returns:
            Overall summary statistics
        """
        if not profiles:
            return {}

        total_datasets = len(profiles)
        data_sources = set(profile.data_source for profile in profiles.values())
        total_runs = sum(len(profile.metrics) for profile in profiles.values())

        # Calculate average metrics
        all_metrics = []
        for profile in profiles.values():
            avg_metrics = profile.get_average_metrics()
            if avg_metrics:
                all_metrics.append(avg_metrics)

        if not all_metrics:
            return {
                "total_datasets": total_datasets,
                "data_sources": list(data_sources),
                "total_runs": total_runs
            }

        # Aggregate metrics
        total_records = sum(m.record_count for m in all_metrics)
        total_bytes = sum(m.byte_count for m in all_metrics)
        total_api_calls = sum(m.api_call_count for m in all_metrics)
        total_errors = sum(m.error_count for m in all_metrics)

        avg_throughput = sum(m.calculate_throughput()["rows_per_second"] for m in all_metrics) / len(all_metrics)
        avg_error_rate = sum(m.calculate_efficiency()["error_rate"] for m in all_metrics) / len(all_metrics)

        return {
            "total_datasets": total_datasets,
            "data_sources": list(data_sources),
            "total_runs": total_runs,
            "total_records_processed": total_records,
            "total_bytes_processed": total_bytes,
            "total_api_calls": total_api_calls,
            "total_errors": total_errors,
            "average_throughput_rows_per_second": avg_throughput,
            "average_error_rate": avg_error_rate,
            "average_records_per_dataset": total_records / total_datasets
        }

    def save_report(self, report: ProfileReport, file_path: Path) -> bool:
        """Save report to file.

        Args:
            report: Report to save
            file_path: Path to save report

        Returns:
            True if saved successfully
        """
        try:
            if self.config.output_format == "json":
                with open(file_path, 'w') as f:
                    f.write(report.to_json())
            elif self.config.output_format == "csv":
                with open(file_path, 'w', newline='') as f:
                    f.write(report.to_csv())
            elif self.config.output_format == "markdown":
                with open(file_path, 'w') as f:
                    f.write(report.to_markdown())
            else:
                raise ValueError(f"Unsupported output format: {self.config.output_format}")

            return True

        except Exception as e:
            # Log error but don't raise
            return False

    def generate_cost_report(
        self,
        profiles: Dict[str, DatasetProfile],
        cost_estimator
    ) -> CostReport:
        """Generate specialized cost report.

        Args:
            profiles: Dataset profiles
            cost_estimator: Cost estimator instance

        Returns:
            Cost report
        """
        cost_data = cost_estimator.generate_cost_report(profiles, self.config.cost_time_period)

        # Extract data source costs
        data_source_costs = {}
        for ds_name, ds_data in cost_data.get("data_source_breakdown", {}).items():
            data_source_costs[ds_name] = DataSourceCosts(
                data_source=ds_name,
                datasets={},  # Would need to populate from detailed data
                total_costs_usd=ds_data.get("total_costs_usd", 0.0),
                average_cost_per_dataset_usd=ds_data.get("average_cost_per_dataset_usd", 0.0),
                cost_efficiency_score=ds_data.get("cost_efficiency_score", 0.0)
            )

        # Generate cost optimization recommendations
        recommendations = self._generate_cost_recommendations(cost_data)

        return CostReport(
            time_period=self.config.cost_time_period,
            total_costs_usd=cost_data.get("summary", {}).get("total_costs_usd", 0.0),
            data_source_costs=data_source_costs,
            cost_breakdown=cost_data,
            recommendations=recommendations
        )

    def _generate_cost_recommendations(self, cost_data: Dict[str, Any]) -> List[str]:
        """Generate cost optimization recommendations.

        Args:
            cost_data: Cost analysis data

        Returns:
            List of recommendation strings
        """
        recommendations = []
        summary = cost_data.get("summary", {})

        # Check total costs
        total_costs = summary.get("total_costs_usd", 0)
        if total_costs > 1000:  # $1000 threshold
            recommendations.append(
                f"High total costs (${total_costs:.0f}) - consider optimizing API usage and batch sizes"
            )

        # Check cost efficiency
        efficiency_score = summary.get("overall_cost_efficiency_score", 0)
        if efficiency_score < 100:  # Less than 100 records per dollar
            recommendations.append(
                f"Low cost efficiency ({efficiency_score:.0f} records per USD) - review error rates and throughput"
            )

        # Check per-data-source costs
        breakdown = cost_data.get("data_source_breakdown", {})
        for ds_name, ds_data in breakdown.items():
            ds_cost = ds_data.get("total_costs_usd", 0)
            if ds_cost > 200:  # $200 per data source threshold
                recommendations.append(
                    f"High costs for {ds_name} (${ds_cost:.0f}) - consider reducing API calls or switching to bulk endpoints"
                )

        if not recommendations:
            recommendations.append("No significant cost optimization opportunities identified")

        return recommendations
