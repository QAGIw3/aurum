"""Performance analysis for cost profiling data."""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum

from .profiler import DatasetProfile, PerformanceMetrics


class PerformanceIssueType(str, Enum):
    """Types of performance issues."""

    LOW_THROUGHPUT = "LOW_THROUGHPUT"
    HIGH_ERROR_RATE = "HIGH_ERROR_RATE"
    HIGH_RETRY_RATE = "HIGH_RETRY_RATE"
    HIGH_MEMORY_USAGE = "HIGH_MEMORY_USAGE"
    HIGH_CPU_USAGE = "HIGH_CPU_USAGE"
    SLOW_API_RESPONSE = "SLOW_API_RESPONSE"
    INEFFICIENT_PROCESSING = "INEFFICIENT_PROCESSING"
    COST_INEFFICIENCY = "COST_INEFFICIENCY"


class OptimizationCategory(str, Enum):
    """Categories of optimization recommendations."""

    THROUGHPUT = "THROUGHPUT"
    RELIABILITY = "RELIABILITY"
    COST = "COST"
    RESOURCE_USAGE = "RESOURCE_USAGE"
    CONFIGURATION = "CONFIGURATION"


@dataclass
class PerformanceIssue:
    """A performance issue identified by analysis."""

    issue_type: PerformanceIssueType
    dataset_name: str
    data_source: str
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    current_value: float
    threshold_value: float
    impact_score: float = 0.0
    affected_metrics: List[str] = field(default_factory=list)
    recommendation_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "issue_type": self.issue_type.value,
            "dataset_name": self.dataset_name,
            "data_source": self.data_source,
            "severity": self.severity,
            "description": self.description,
            "current_value": self.current_value,
            "threshold_value": self.threshold_value,
            "impact_score": self.impact_score,
            "affected_metrics": self.affected_metrics,
            "recommendation_ids": self.recommendation_ids
        }


@dataclass
class OptimizationRecommendation:
    """An optimization recommendation."""

    recommendation_id: str
    category: OptimizationCategory
    title: str
    description: str
    expected_benefit: str
    implementation_effort: str  # LOW, MEDIUM, HIGH
    affected_datasets: List[str] = field(default_factory=list)
    technical_details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "recommendation_id": self.recommendation_id,
            "category": self.category.value,
            "title": self.title,
            "description": self.description,
            "expected_benefit": self.expected_benefit,
            "implementation_effort": self.implementation_effort,
            "affected_datasets": self.affected_datasets,
            "technical_details": self.technical_details
        }


@dataclass
class AnalysisResult:
    """Result of performance analysis."""

    analysis_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    datasets_analyzed: int = 0
    total_issues_found: int = 0
    issues_by_severity: Dict[str, int] = field(default_factory=dict)
    issues_by_type: Dict[str, int] = field(default_factory=dict)
    performance_issues: List[PerformanceIssue] = field(default_factory=list)
    optimization_recommendations: List[OptimizationRecommendation] = field(default_factory=list)
    summary_metrics: Dict[str, Any] = field(default_factory=dict)

    def get_issues_by_severity(self, severity: str) -> List[PerformanceIssue]:
        """Get issues by severity level.

        Args:
            severity: Severity level to filter by

        Returns:
            List of issues with the specified severity
        """
        return [issue for issue in self.performance_issues if issue.severity == severity]

    def get_recommendations_by_category(self, category: OptimizationCategory) -> List[OptimizationRecommendation]:
        """Get recommendations by category.

        Args:
            category: Category to filter by

        Returns:
            List of recommendations in the specified category
        """
        return [rec for rec in self.optimization_recommendations if rec.category == category]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "analysis_timestamp": self.analysis_timestamp,
            "datasets_analyzed": self.datasets_analyzed,
            "total_issues_found": self.total_issues_found,
            "issues_by_severity": self.issues_by_severity,
            "issues_by_type": self.issues_by_type,
            "performance_issues": [issue.to_dict() for issue in self.performance_issues],
            "optimization_recommendations": [rec.to_dict() for rec in self.optimization_recommendations],
            "summary_metrics": self.summary_metrics
        }


class PerformanceAnalyzer:
    """Analyze performance metrics and provide optimization recommendations."""

    def __init__(self, throughput_thresholds: Dict[str, float] = None):
        """Initialize performance analyzer.

        Args:
            throughput_thresholds: Thresholds for throughput warnings by data source
        """
        self.throughput_thresholds = throughput_thresholds or {
            "eia": 50.0,
            "fred": 100.0,
            "cpi": 80.0,
            "noaa": 30.0,
            "iso": 25.0
        }

        self.error_rate_threshold = 0.05  # 5%
        self.retry_rate_threshold = 0.1   # 10%
        self.memory_threshold_mb = 500    # 500MB
        self.api_latency_threshold = 10.0 # 10 seconds

    def analyze_dataset_profile(
        self,
        profile: DatasetProfile,
        analysis_config: Optional[Dict[str, Any]] = None
    ) -> AnalysisResult:
        """Analyze a single dataset profile.

        Args:
            profile: Dataset profile to analyze
            analysis_config: Optional analysis configuration

        Returns:
            Analysis result
        """
        result = AnalysisResult()
        result.datasets_analyzed = 1

        if not profile.metrics:
            return result

        avg_metrics = profile.get_average_metrics()
        if not avg_metrics:
            return result

        # Analyze throughput
        throughput_issues = self._analyze_throughput(profile, avg_metrics)
        result.performance_issues.extend(throughput_issues)

        # Analyze error rates
        error_issues = self._analyze_error_rates(profile, avg_metrics)
        result.performance_issues.extend(error_issues)

        # Analyze resource usage
        resource_issues = self._analyze_resource_usage(profile, avg_metrics)
        result.performance_issues.extend(resource_issues)

        # Analyze API performance
        api_issues = self._analyze_api_performance(profile, avg_metrics)
        result.performance_issues.extend(api_issues)

        # Update issue counts
        for issue in result.performance_issues:
            result.total_issues_found += 1
            result.issues_by_severity[issue.severity] = (
                result.issues_by_severity.get(issue.severity, 0) + 1
            )
            result.issues_by_type[issue.issue_type.value] = (
                result.issues_by_type.get(issue.issue_type.value, 0) + 1
            )

        # Generate recommendations
        result.optimization_recommendations = self._generate_recommendations(
            profile, result.performance_issues
        )

        # Calculate summary metrics
        result.summary_metrics = self._calculate_summary_metrics(profile, avg_metrics)

        return result

    def _analyze_throughput(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> List[PerformanceIssue]:
        """Analyze throughput performance.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            List of throughput-related issues
        """
        issues = []
        throughput = metrics.calculate_throughput()

        # Get threshold for this data source
        threshold = self.throughput_thresholds.get(
            profile.data_source,
            self.throughput_thresholds["eia"]  # Default threshold
        )

        if throughput["rows_per_second"] < threshold:
            severity = "HIGH" if throughput["rows_per_second"] < threshold * 0.5 else "MEDIUM"

            issues.append(PerformanceIssue(
                issue_type=PerformanceIssueType.LOW_THROUGHPUT,
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                severity=severity,
                description=f"Low throughput: {throughput['rows_per_second']:.1f} rows/s",
                current_value=throughput["rows_per_second"],
                threshold_value=threshold,
                affected_metrics=["throughput_rows_per_second", "processing_time_seconds"],
                impact_score=1.0 - (throughput["rows_per_second"] / threshold)
            ))

        return issues

    def _analyze_error_rates(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> List[PerformanceIssue]:
        """Analyze error rates.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            List of error-related issues
        """
        issues = []
        efficiency = metrics.calculate_efficiency()

        # Check error rate
        if efficiency["error_rate"] > self.error_rate_threshold:
            severity = "CRITICAL" if efficiency["error_rate"] > 0.2 else "HIGH"

            issues.append(PerformanceIssue(
                issue_type=PerformanceIssueType.HIGH_ERROR_RATE,
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                severity=severity,
                description=f"High error rate: {efficiency['error_rate']:.2%}",
                current_value=efficiency["error_rate"],
                threshold_value=self.error_rate_threshold,
                affected_metrics=["error_rate", "api_call_count"],
                impact_score=efficiency["error_rate"] / self.error_rate_threshold
            ))

        # Check retry rate
        if efficiency["retry_rate"] > self.retry_rate_threshold:
            severity = "HIGH" if efficiency["retry_rate"] > 0.2 else "MEDIUM"

            issues.append(PerformanceIssue(
                issue_type=PerformanceIssueType.HIGH_RETRY_RATE,
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                severity=severity,
                description=f"High retry rate: {efficiency['retry_rate']:.2%}",
                current_value=efficiency["retry_rate"],
                threshold_value=self.retry_rate_threshold,
                affected_metrics=["retry_rate", "api_call_count"],
                impact_score=efficiency["retry_rate"] / self.retry_rate_threshold
            ))

        return issues

    def _analyze_resource_usage(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> List[PerformanceIssue]:
        """Analyze resource usage.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            List of resource-related issues
        """
        issues = []

        # Check memory usage
        if metrics.memory_peak_mb > self.memory_threshold_mb:
            severity = "HIGH" if metrics.memory_peak_mb > self.memory_threshold_mb * 2 else "MEDIUM"

            issues.append(PerformanceIssue(
                issue_type=PerformanceIssueType.HIGH_MEMORY_USAGE,
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                severity=severity,
                description=f"High memory usage: {metrics.memory_peak_mb:.1f} MB",
                current_value=metrics.memory_peak_mb,
                threshold_value=self.memory_threshold_mb,
                affected_metrics=["memory_peak_mb"],
                impact_score=metrics.memory_peak_mb / self.memory_threshold_mb
            ))

        # Check CPU usage
        if metrics.cpu_peak_percent > 80:  # 80% CPU usage
            severity = "HIGH" if metrics.cpu_peak_percent > 95 else "MEDIUM"

            issues.append(PerformanceIssue(
                issue_type=PerformanceIssueType.HIGH_CPU_USAGE,
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                severity=severity,
                description=f"High CPU usage: {metrics.cpu_peak_percent:.1f}%",
                current_value=metrics.cpu_peak_percent,
                threshold_value=80.0,
                affected_metrics=["cpu_peak_percent"],
                impact_score=metrics.cpu_peak_percent / 100.0
            ))

        return issues

    def _analyze_api_performance(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> List[PerformanceIssue]:
        """Analyze API performance.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            List of API-related issues
        """
        issues = []

        # Calculate average API call time
        if metrics.api_call_count > 0:
            avg_api_time = metrics.processing_time_seconds / metrics.api_call_count

            if avg_api_time > self.api_latency_threshold:
                severity = "HIGH" if avg_api_time > self.api_latency_threshold * 2 else "MEDIUM"

                issues.append(PerformanceIssue(
                    issue_type=PerformanceIssueType.SLOW_API_RESPONSE,
                    dataset_name=profile.dataset_name,
                    data_source=profile.data_source,
                    severity=severity,
                    description=f"Slow API response: {avg_api_time:.2f}s average",
                    current_value=avg_api_time,
                    threshold_value=self.api_latency_threshold,
                    affected_metrics=["processing_time_seconds", "api_call_count"],
                    impact_score=avg_api_time / self.api_latency_threshold
                ))

        return issues

    def _generate_recommendations(
        self,
        profile: DatasetProfile,
        issues: List[PerformanceIssue]
    ) -> List[OptimizationRecommendation]:
        """Generate optimization recommendations based on issues.

        Args:
            profile: Dataset profile
            issues: Performance issues found

        Returns:
            List of optimization recommendations
        """
        recommendations = []
        issue_types = {issue.issue_type for issue in issues}

        # Throughput optimization recommendations
        if PerformanceIssueType.LOW_THROUGHPUT in issue_types:
            recommendations.append(OptimizationRecommendation(
                recommendation_id=f"throughput_opt_{profile.dataset_name}",
                category=OptimizationCategory.THROUGHPUT,
                title="Increase Batch Size",
                description=f"Increase batch size for {profile.data_source} to improve throughput",
                expected_benefit="2-5x throughput improvement",
                implementation_effort="LOW",
                affected_datasets=[profile.dataset_name],
                technical_details={
                    "data_source": profile.data_source,
                    "current_throughput": profile.get_average_metrics().calculate_throughput()["rows_per_second"],
                    "suggested_batch_size": "1000-5000"
                }
            ))

        # Error rate recommendations
        if PerformanceIssueType.HIGH_ERROR_RATE in issue_types:
            recommendations.append(OptimizationRecommendation(
                recommendation_id=f"error_handling_{profile.dataset_name}",
                category=OptimizationCategory.RELIABILITY,
                title="Improve Error Handling",
                description=f"Add better error handling and retry logic for {profile.data_source}",
                expected_benefit="50-90% reduction in error rate",
                implementation_effort="MEDIUM",
                affected_datasets=[profile.dataset_name],
                technical_details={
                    "data_source": profile.data_source,
                    "error_rate": profile.get_average_metrics().calculate_efficiency()["error_rate"]
                }
            ))

        # Retry rate recommendations
        if PerformanceIssueType.HIGH_RETRY_RATE in issue_types:
            recommendations.append(OptimizationRecommendation(
                recommendation_id=f"retry_optimization_{profile.dataset_name}",
                category=OptimizationCategory.RELIABILITY,
                title="Optimize Retry Strategy",
                description=f"Implement exponential backoff for {profile.data_source} retries",
                expected_benefit="30-70% reduction in retry rate",
                implementation_effort="MEDIUM",
                affected_datasets=[profile.dataset_name],
                technical_details={
                    "data_source": profile.data_source,
                    "retry_rate": profile.get_average_metrics().calculate_efficiency()["retry_rate"]
                }
            ))

        # Memory optimization recommendations
        if PerformanceIssueType.HIGH_MEMORY_USAGE in issue_types:
            recommendations.append(OptimizationRecommendation(
                recommendation_id=f"memory_optimization_{profile.dataset_name}",
                category=OptimizationCategory.RESOURCE_USAGE,
                title="Optimize Memory Usage",
                description=f"Implement streaming or chunked processing for {profile.data_source}",
                expected_benefit="50-80% reduction in memory usage",
                implementation_effort="HIGH",
                affected_datasets=[profile.dataset_name],
                technical_details={
                    "data_source": profile.data_source,
                    "memory_usage_mb": profile.get_average_metrics().memory_peak_mb
                }
            ))

        # API performance recommendations
        if PerformanceIssueType.SLOW_API_RESPONSE in issue_types:
            recommendations.append(OptimizationRecommendation(
                recommendation_id=f"api_optimization_{profile.dataset_name}",
                category=OptimizationCategory.THROUGHPUT,
                title="Optimize API Calls",
                description=f"Implement connection pooling and request optimization for {profile.data_source}",
                expected_benefit="2-10x improvement in API response time",
                implementation_effort="MEDIUM",
                affected_datasets=[profile.dataset_name],
                technical_details={
                    "data_source": profile.data_source,
                    "avg_api_time": profile.get_average_metrics().processing_time_seconds / max(profile.get_average_metrics().api_call_count, 1)
                }
            ))

        return recommendations

    def _calculate_summary_metrics(self, profile: DatasetProfile, metrics: PerformanceMetrics) -> Dict[str, Any]:
        """Calculate summary metrics for analysis.

        Args:
            profile: Dataset profile
            metrics: Performance metrics

        Returns:
            Summary metrics dictionary
        """
        throughput = metrics.calculate_throughput()
        efficiency = metrics.calculate_efficiency()

        return {
            "dataset_name": profile.dataset_name,
            "data_source": profile.data_source,
            "run_count": len(profile.metrics),
            "average_throughput_rows_per_second": throughput["rows_per_second"],
            "average_throughput_bytes_per_second": throughput["bytes_per_second"],
            "average_error_rate": efficiency["error_rate"],
            "average_retry_rate": efficiency["retry_rate"],
            "average_memory_usage_mb": metrics.memory_peak_mb,
            "average_cpu_usage_percent": metrics.cpu_peak_percent,
            "total_records_processed": metrics.record_count,
            "total_api_calls": metrics.api_call_count,
            "data_quality_score": efficiency.get("data_quality_score", 1.0)
        }
