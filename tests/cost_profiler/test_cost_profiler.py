"""Tests for cost profiler system."""

import pytest
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from aurum.cost_profiler import (
    CostProfiler,
    ProfilerConfig,
    DatasetProfile,
    PerformanceMetrics,
    MetricsCollector,
    CollectionConfig,
    PerformanceAnalyzer,
    CostEstimator,
    CostModel,
    ProfileReporter,
    ReportConfig
)


class TestPerformanceMetrics:
    """Test PerformanceMetrics functionality."""

    def test_calculate_throughput(self):
        """Test throughput calculation."""
        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            byte_count=1024000,  # 1MB
            processing_time_seconds=10.0,
            api_call_count=50
        )

        throughput = metrics.calculate_throughput()

        assert throughput["rows_per_second"] == 100.0
        assert throughput["bytes_per_second"] == 102400.0

    def test_calculate_efficiency(self):
        """Test efficiency calculation."""
        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            api_call_count=50,
            retry_count=5,
            error_count=2
        )

        efficiency = metrics.calculate_efficiency()

        assert efficiency["error_rate"] == 0.04  # 2/50
        assert efficiency["retry_rate"] == 0.1   # 5/50
        assert efficiency["records_per_api_call"] == 20.0  # 1000/50

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            processing_time_seconds=10.0
        )

        data = metrics.to_dict()

        assert data["dataset_name"] == "test_dataset"
        assert data["data_source"] == "eia"
        assert data["record_count"] == 1000
        assert "throughput" in data
        assert "efficiency" in data


class TestDatasetProfile:
    """Test DatasetProfile functionality."""

    def test_add_metrics(self):
        """Test adding metrics to profile."""
        profile = DatasetProfile(
            dataset_name="test_dataset",
            data_source="eia",
            profile_id="test_123"
        )

        metrics1 = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            processing_time_seconds=10.0
        )

        metrics2 = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=2000,
            processing_time_seconds=15.0
        )

        profile.add_metrics(metrics1)
        profile.add_metrics(metrics2)

        assert len(profile.metrics) == 2
        assert profile.baseline_metrics == metrics2  # Should be better metrics

    def test_get_average_metrics(self):
        """Test getting average metrics."""
        profile = DatasetProfile(
            dataset_name="test_dataset",
            data_source="eia",
            profile_id="test_123"
        )

        metrics1 = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            processing_time_seconds=10.0
        )

        metrics2 = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=2000,
            processing_time_seconds=20.0
        )

        profile.add_metrics(metrics1)
        profile.add_metrics(metrics2)

        avg_metrics = profile.get_average_metrics()

        assert avg_metrics is not None
        assert avg_metrics.record_count == 3000  # (1000 + 2000) / 2
        assert avg_metrics.processing_time_seconds == 15.0  # (10 + 20) / 2

    def test_get_performance_summary(self):
        """Test getting performance summary."""
        profile = DatasetProfile(
            dataset_name="test_dataset",
            data_source="eia",
            profile_id="test_123"
        )

        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            processing_time_seconds=10.0,
            api_call_count=50
        )

        profile.add_metrics(metrics)

        summary = profile.get_performance_summary()

        assert summary["dataset_name"] == "test_dataset"
        assert summary["data_source"] == "eia"
        assert summary["run_count"] == 1
        assert "average_metrics" in summary


class TestCostProfiler:
    """Test CostProfiler functionality."""

    def test_initialization(self):
        """Test profiler initialization."""
        config = ProfilerConfig(
            collection_interval_seconds=1.0,
            enable_memory_monitoring=True
        )

        profiler = CostProfiler(config)

        assert profiler.config == config
        assert len(profiler.dataset_profiles) == 0
        assert len(profiler.active_profiling_sessions) == 0

    def test_start_profiling(self):
        """Test starting a profiling session."""
        profiler = CostProfiler(ProfilerConfig())

        session_id = profiler.start_profiling("test_dataset", "eia")

        assert session_id in profiler.active_profiling_sessions
        assert profiler.active_profiling_sessions[session_id] == "test_dataset"
        assert "test_dataset" in profiler.dataset_profiles

    def test_end_profiling(self):
        """Test ending a profiling session."""
        profiler = CostProfiler(ProfilerConfig())

        session_id = profiler.start_profiling("test_dataset", "eia")
        profile = profiler.end_profiling(session_id)

        assert session_id not in profiler.active_profiling_sessions
        assert profile is not None
        assert profile.dataset_name == "test_dataset"

    def test_record_metrics(self):
        """Test recording metrics."""
        profiler = CostProfiler(ProfilerConfig())

        session_id = profiler.start_profiling("test_dataset", "eia")

        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            processing_time_seconds=10.0
        )

        profiler.record_metrics(session_id, metrics)

        assert "test_dataset" in profiler.dataset_profiles
        assert len(profiler.dataset_profiles["test_dataset"].metrics) == 1

    def test_analyze_performance_issues(self):
        """Test performance issue analysis."""
        profiler = CostProfiler(ProfilerConfig())

        # Add a dataset with low throughput
        session_id = profiler.start_profiling("test_dataset", "eia")

        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=100,
            processing_time_seconds=10.0,  # 10 rows/second (very low)
            api_call_count=10
        )

        profiler.record_metrics(session_id, metrics)
        profiler.end_profiling(session_id)

        issues = profiler.analyze_performance_issues()

        assert len(issues) > 0
        assert any(issue["issue_type"] == "LOW_THROUGHPUT" for issue in issues)


class TestMetricsCollector:
    """Test MetricsCollector functionality."""

    def test_initialization(self):
        """Test collector initialization."""
        config = CollectionConfig(
            collection_interval_seconds=1.0,
            enable_system_metrics=True
        )

        collector = MetricsCollector(config)

        assert collector.config == config
        assert not collector.is_collecting

    def test_record_api_call(self):
        """Test recording API calls."""
        config = CollectionConfig()
        collector = MetricsCollector(config)

        collector.start_collection("test_dataset", "eia")

        collector.record_api_call("https://api.example.com", 1.0, True)
        collector.record_api_call("https://api.example.com", 2.0, False)

        metrics = collector.get_current_metrics()

        assert metrics.api_call_count == 2
        assert metrics.error_count == 1
        assert metrics.processing_time_seconds == 3.0

    def test_record_data_processing(self):
        """Test recording data processing."""
        config = CollectionConfig()
        collector = MetricsCollector(config)

        collector.start_collection("test_dataset", "eia")

        collector.record_data_processing(1000, 1024000, 10.0)

        metrics = collector.get_current_metrics()

        assert metrics.record_count == 1000
        assert metrics.byte_count == 1024000
        assert metrics.processing_time_seconds == 10.0

    def test_get_metrics_summary(self):
        """Test getting metrics summary."""
        config = CollectionConfig()
        collector = MetricsCollector(config)

        collector.start_collection("test_dataset", "eia")

        collector.record_api_call("https://api.example.com", 1.0, True)
        collector.record_data_processing(1000, 1024000, 10.0)

        summary = collector.get_metrics_summary()

        assert summary["is_collecting"] is True
        assert summary["total_samples"] > 0
        assert "current_session" in summary


class TestPerformanceAnalyzer:
    """Test PerformanceAnalyzer functionality."""

    def test_initialization(self):
        """Test analyzer initialization."""
        thresholds = {"eia": 50.0, "fred": 100.0}
        analyzer = PerformanceAnalyzer(thresholds)

        assert analyzer.throughput_thresholds == thresholds
        assert analyzer.error_rate_threshold == 0.05

    def test_analyze_dataset_profile(self):
        """Test analyzing dataset profile."""
        analyzer = PerformanceAnalyzer()

        profile = DatasetProfile(
            dataset_name="test_dataset",
            data_source="eia",
            profile_id="test_123"
        )

        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            processing_time_seconds=100.0,  # 10 rows/second (low)
            api_call_count=50,
            error_count=5  # 10% error rate (high)
        )

        profile.add_metrics(metrics)

        result = analyzer.analyze_dataset_profile(profile)

        assert result.datasets_analyzed == 1
        assert len(result.performance_issues) > 0

        # Should detect both low throughput and high error rate
        issue_types = [issue.issue_type.value for issue in result.performance_issues]
        assert "LOW_THROUGHPUT" in issue_types
        assert "HIGH_ERROR_RATE" in issue_types


class TestCostEstimator:
    """Test CostEstimator functionality."""

    def test_initialization(self):
        """Test estimator initialization."""
        cost_model = CostModel()
        estimator = CostEstimator(cost_model)

        assert estimator.cost_model == cost_model

    def test_estimate_dataset_cost(self):
        """Test cost estimation for dataset."""
        cost_model = CostModel()
        estimator = CostEstimator(cost_model)

        profile = DatasetProfile(
            dataset_name="test_dataset",
            data_source="eia",
            profile_id="test_123"
        )

        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            byte_count=1024000,  # 1MB
            processing_time_seconds=10.0,
            api_call_count=50
        )

        profile.add_metrics(metrics)

        cost_estimate = estimator.estimate_dataset_cost(profile, "monthly")

        assert cost_estimate.dataset_name == "test_dataset"
        assert cost_estimate.data_source == "eia"
        assert cost_estimate.time_period == "monthly"
        assert cost_estimate.total_costs_usd > 0
        assert cost_estimate.api_costs_usd > 0
        assert cost_estimate.compute_costs_usd > 0

    def test_estimate_data_source_costs(self):
        """Test estimating costs for multiple datasets."""
        cost_model = CostModel()
        estimator = CostEstimator(cost_model)

        profiles = {
            "dataset1": DatasetProfile(
                dataset_name="dataset1",
                data_source="eia",
                profile_id="test_123"
            ),
            "dataset2": DatasetProfile(
                dataset_name="dataset2",
                data_source="fred",
                profile_id="test_456"
            )
        }

        # Add metrics to profiles
        for profile in profiles.values():
            metrics = PerformanceMetrics(
                dataset_name=profile.dataset_name,
                data_source=profile.data_source,
                record_count=1000,
                processing_time_seconds=10.0,
                api_call_count=50
            )
            profile.add_metrics(metrics)

        data_source_costs = estimator.estimate_data_source_costs(profiles, "monthly")

        assert "eia" in data_source_costs
        assert "fred" in data_source_costs
        assert data_source_costs["eia"].total_costs_usd > 0
        assert data_source_costs["fred"].total_costs_usd > 0


class TestProfileReporter:
    """Test ProfileReporter functionality."""

    def test_initialization(self):
        """Test reporter initialization."""
        config = ReportConfig(
            include_costs=True,
            output_format="json"
        )

        reporter = ProfileReporter(config)

        assert reporter.config == config

    def test_generate_profile_report(self):
        """Test generating profile report."""
        config = ReportConfig()
        reporter = ProfileReporter(config)

        profiles = {
            "test_dataset": DatasetProfile(
                dataset_name="test_dataset",
                data_source="eia",
                profile_id="test_123"
            )
        }

        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            processing_time_seconds=10.0,
            api_call_count=50
        )

        profiles["test_dataset"].add_metrics(metrics)

        report = reporter.generate_profile_report(profiles)

        assert report.generated_at is not None
        assert report.datasets is not None
        assert "test_dataset" in report.datasets

    def test_save_report(self):
        """Test saving report to file."""
        config = ReportConfig(output_format="json")
        reporter = ProfileReporter(config)

        report = ProfileReport()
        report.datasets["test"] = {"test": "data"}

        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = Path(tmp_dir) / "test_report.json"

            success = reporter.save_report(report, file_path)

            assert success
            assert file_path.exists()

            # Verify content
            with open(file_path, 'r') as f:
                content = f.read()
                assert "test" in content
                assert "data" in content


class TestIntegration:
    """Integration tests for cost profiler system."""

    def test_full_profiling_workflow(self):
        """Test complete profiling workflow."""
        # Initialize components
        profiler_config = ProfilerConfig()
        profiler = CostProfiler(profiler_config)

        analyzer = PerformanceAnalyzer()
        cost_model = CostModel()
        estimator = CostEstimator(cost_model)

        # Start profiling
        session_id = profiler.start_profiling("test_dataset", "eia")

        # Record metrics
        metrics = PerformanceMetrics(
            dataset_name="test_dataset",
            data_source="eia",
            record_count=1000,
            byte_count=1024000,
            processing_time_seconds=10.0,
            api_call_count=50,
            retry_count=2,
            error_count=1
        )

        profiler.record_metrics(session_id, metrics)

        # End profiling
        profile = profiler.end_profiling(session_id)

        assert profile is not None
        assert len(profile.metrics) == 1

        # Analyze performance
        analysis_result = analyzer.analyze_dataset_profile(profile)

        assert analysis_result.datasets_analyzed == 1
        assert len(analysis_result.performance_issues) >= 0  # May or may not have issues

        # Estimate costs
        cost_estimate = estimator.estimate_dataset_cost(profile, "monthly")

        assert cost_estimate.dataset_name == "test_dataset"
        assert cost_estimate.total_costs_usd > 0

    def test_report_generation(self):
        """Test report generation workflow."""
        # Create test data
        profiles = {
            "dataset1": DatasetProfile(
                dataset_name="dataset1",
                data_source="eia",
                profile_id="test_1"
            ),
            "dataset2": DatasetProfile(
                dataset_name="dataset2",
                data_source="fred",
                profile_id="test_2"
            )
        }

        for name, profile in profiles.items():
            metrics = PerformanceMetrics(
                dataset_name=name,
                data_source=profile.data_source,
                record_count=1000,
                processing_time_seconds=10.0,
                api_call_count=50
            )
            profile.add_metrics(metrics)

        # Generate report
        reporter_config = ReportConfig(
            include_costs=True,
            include_performance_analysis=True,
            output_format="json"
        )

        reporter = ProfileReporter(reporter_config)
        cost_estimator = CostEstimator()

        report = reporter.generate_profile_report(profiles, cost_estimator=cost_estimator)

        assert report.generated_at is not None
        assert len(report.datasets) == 2
        assert report.summary is not None
        assert report.cost_estimates is not None

        # Test different output formats
        json_output = report.to_json()
        assert '"test_dataset"' in json_output

        csv_output = report.to_csv()
        assert "test_dataset" in csv_output

        markdown_output = report.to_markdown()
        assert "# Data Ingestion Performance Report" in markdown_output


if __name__ == "__main__":
    pytest.main([__file__])
