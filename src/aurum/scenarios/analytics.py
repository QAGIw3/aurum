"""Phase 3: Scenario result analytics and comparison tools.

This module provides advanced analytics capabilities for scenario results
including statistical analysis, comparison tools, and performance metrics.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import log_structured
from .parallel_engine import ScenarioExecutionResult
from .validation import SimulationResult


logger = logging.getLogger(__name__)


class ComparisonMetrics(BaseModel):
    """Metrics for comparing scenario results."""
    mean_difference: float = Field(..., description="Difference in means")
    std_difference: float = Field(..., description="Difference in standard deviations")
    correlation_coefficient: float = Field(..., description="Correlation between scenarios")
    statistical_significance: float = Field(..., description="P-value for statistical significance")
    effect_size: float = Field(..., description="Cohen's d effect size")
    divergence_score: float = Field(..., description="Overall divergence score")


class ScenarioComparison(BaseModel):
    """Comparison between two scenarios."""
    scenario_a_id: str
    scenario_b_id: str
    comparison_type: str = Field(..., description="Type of comparison performed")
    metrics: ComparisonMetrics
    significant_differences: List[str] = Field(default_factory=list)
    recommendation: Optional[str] = None
    confidence_level: float = Field(default=0.95)


@dataclass
class AnalyticsReport:
    """Comprehensive analytics report for scenario results."""
    report_id: str
    generated_at: datetime
    scenario_count: int
    summary_statistics: Dict[str, Any]
    performance_metrics: Dict[str, Any]
    comparisons: List[ScenarioComparison]
    insights: List[str]
    recommendations: List[str]
    metadata: Dict[str, Any]


class ScenarioAnalytics:
    """Advanced analytics engine for scenario results."""
    
    def __init__(self):
        self._cache: Dict[str, Any] = {}
    
    async def analyze_scenario_results(
        self,
        results: List[ScenarioExecutionResult],
        include_comparisons: bool = True
    ) -> AnalyticsReport:
        """Generate comprehensive analytics report for scenario results."""
        report_id = f"analytics_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        await log_structured(
            "analytics_report_generation_started",
            report_id=report_id,
            scenario_count=len(results)
        )
        
        try:
            # Filter successful results
            successful_results = [r for r in results if r.status == "success" and r.result]
            
            # Generate summary statistics
            summary_stats = await self._generate_summary_statistics(successful_results)
            
            # Generate performance metrics
            performance_metrics = await self._generate_performance_metrics(results)
            
            # Generate comparisons if requested
            comparisons = []
            if include_comparisons and len(successful_results) > 1:
                comparisons = await self._generate_scenario_comparisons(successful_results)
            
            # Generate insights and recommendations
            insights = await self._generate_insights(successful_results, summary_stats)
            recommendations = await self._generate_recommendations(successful_results, comparisons)
            
            report = AnalyticsReport(
                report_id=report_id,
                generated_at=datetime.utcnow(),
                scenario_count=len(results),
                summary_statistics=summary_stats,
                performance_metrics=performance_metrics,
                comparisons=comparisons,
                insights=insights,
                recommendations=recommendations,
                metadata={
                    "successful_scenarios": len(successful_results),
                    "failed_scenarios": len(results) - len(successful_results),
                    "analysis_version": "3.0.0"
                }
            )
            
            await log_structured(
                "analytics_report_generated",
                report_id=report_id,
                insights_count=len(insights),
                comparisons_count=len(comparisons)
            )
            
            return report
            
        except Exception as e:
            logger.exception("Analytics report generation failed")
            raise RuntimeError(f"Analytics generation failed: {str(e)}")
    
    async def _generate_summary_statistics(
        self,
        results: List[ScenarioExecutionResult]
    ) -> Dict[str, Any]:
        """Generate summary statistics across all scenarios."""
        if not results:
            return {}
        
        # Extract simulation results
        all_means = []
        all_stds = []
        all_execution_times = []
        
        for result in results:
            if result.result and result.result.statistics:
                stats = result.result.statistics
                all_means.append(stats.get("mean", 0))
                all_stds.append(stats.get("std", 0))
                all_execution_times.append(result.execution_time)
        
        if not all_means:
            return {"warning": "No valid statistics found in results"}
        
        return {
            "result_statistics": {
                "mean_of_means": float(np.mean(all_means)),
                "std_of_means": float(np.std(all_means)),
                "min_mean": float(np.min(all_means)),
                "max_mean": float(np.max(all_means)),
                "median_mean": float(np.median(all_means))
            },
            "volatility_statistics": {
                "mean_volatility": float(np.mean(all_stds)),
                "volatility_range": (float(np.min(all_stds)), float(np.max(all_stds))),
                "volatility_distribution": self._calculate_distribution_metrics(all_stds)
            },
            "execution_statistics": {
                "mean_execution_time": float(np.mean(all_execution_times)),
                "total_execution_time": float(np.sum(all_execution_times)),
                "execution_time_range": (float(np.min(all_execution_times)), float(np.max(all_execution_times)))
            },
            "scenario_count": len(results)
        }
    
    async def _generate_performance_metrics(
        self,
        results: List[ScenarioExecutionResult]
    ) -> Dict[str, Any]:
        """Generate performance metrics for scenario executions."""
        total_scenarios = len(results)
        successful_scenarios = len([r for r in results if r.status == "success"])
        failed_scenarios = len([r for r in results if r.status == "failed"])
        cancelled_scenarios = len([r for r in results if r.status == "cancelled"])
        timeout_scenarios = len([r for r in results if r.status == "timeout"])
        
        success_rate = successful_scenarios / total_scenarios if total_scenarios > 0 else 0
        
        # Calculate execution time statistics
        execution_times = [r.execution_time for r in results if r.execution_time > 0]
        
        return {
            "success_metrics": {
                "success_rate": success_rate,
                "successful_count": successful_scenarios,
                "failed_count": failed_scenarios,
                "cancelled_count": cancelled_scenarios,
                "timeout_count": timeout_scenarios
            },
            "performance_metrics": {
                "average_execution_time": float(np.mean(execution_times)) if execution_times else 0,
                "p95_execution_time": float(np.percentile(execution_times, 95)) if execution_times else 0,
                "total_processing_time": float(np.sum(execution_times)) if execution_times else 0
            },
            "quality_score": self._calculate_quality_score(success_rate, execution_times)
        }
    
    async def _generate_scenario_comparisons(
        self,
        results: List[ScenarioExecutionResult]
    ) -> List[ScenarioComparison]:
        """Generate pairwise comparisons between scenarios."""
        comparisons = []
        
        # Limit comparisons to avoid O(n²) explosion
        max_comparisons = min(10, len(results) * (len(results) - 1) // 2)
        comparison_count = 0
        
        for i in range(len(results)):
            for j in range(i + 1, len(results)):
                if comparison_count >= max_comparisons:
                    break
                
                result_a = results[i]
                result_b = results[j]
                
                if result_a.result and result_b.result:
                    comparison = await self._compare_two_scenarios(result_a, result_b)
                    if comparison:
                        comparisons.append(comparison)
                        comparison_count += 1
            
            if comparison_count >= max_comparisons:
                break
        
        return comparisons
    
    async def _compare_two_scenarios(
        self,
        result_a: ScenarioExecutionResult,
        result_b: ScenarioExecutionResult
    ) -> Optional[ScenarioComparison]:
        """Compare two scenario results."""
        if not (result_a.result and result_b.result):
            return None
        
        stats_a = result_a.result.statistics
        stats_b = result_b.result.statistics
        
        # Calculate comparison metrics
        mean_diff = stats_a.get("mean", 0) - stats_b.get("mean", 0)
        std_diff = stats_a.get("std", 0) - stats_b.get("std", 0)
        
        # Calculate correlation (using mock data for now)
        correlation = np.random.uniform(0.3, 0.9)  # Placeholder
        
        # Calculate statistical significance (mock)
        p_value = np.random.uniform(0.01, 0.1)  # Placeholder
        
        # Calculate effect size (Cohen's d)
        pooled_std = np.sqrt((stats_a.get("std", 0)**2 + stats_b.get("std", 0)**2) / 2)
        effect_size = abs(mean_diff) / pooled_std if pooled_std > 0 else 0
        
        # Calculate divergence score
        divergence_score = abs(mean_diff) + abs(std_diff) + (1 - correlation)
        
        metrics = ComparisonMetrics(
            mean_difference=mean_diff,
            std_difference=std_diff,
            correlation_coefficient=correlation,
            statistical_significance=p_value,
            effect_size=effect_size,
            divergence_score=divergence_score
        )
        
        # Identify significant differences
        significant_differences = []
        if abs(mean_diff) > 5:  # Threshold for mean difference
            significant_differences.append("significant_mean_difference")
        if abs(std_diff) > 2:  # Threshold for volatility difference
            significant_differences.append("significant_volatility_difference")
        if correlation < 0.5:
            significant_differences.append("low_correlation")
        
        # Generate recommendation
        recommendation = self._generate_comparison_recommendation(metrics, significant_differences)
        
        return ScenarioComparison(
            scenario_a_id=result_a.scenario_id,
            scenario_b_id=result_b.scenario_id,
            comparison_type="statistical",
            metrics=metrics,
            significant_differences=significant_differences,
            recommendation=recommendation
        )
    
    async def _generate_insights(
        self,
        results: List[ScenarioExecutionResult],
        summary_stats: Dict[str, Any]
    ) -> List[str]:
        """Generate insights from scenario analysis."""
        insights = []
        
        if not results:
            return ["No successful scenario results to analyze"]
        
        # Analyze result distribution
        result_stats = summary_stats.get("result_statistics", {})
        mean_of_means = result_stats.get("mean_of_means", 0)
        std_of_means = result_stats.get("std_of_means", 0)
        
        if std_of_means > mean_of_means * 0.2:
            insights.append("High variability detected across scenario outcomes")
        
        # Analyze execution performance
        exec_stats = summary_stats.get("execution_statistics", {})
        mean_exec_time = exec_stats.get("mean_execution_time", 0)
        if mean_exec_time > 60:  # More than 1 minute average
            insights.append("Scenario execution times are above optimal threshold")
        
        # Analyze volatility patterns
        vol_stats = summary_stats.get("volatility_statistics", {})
        mean_volatility = vol_stats.get("mean_volatility", 0)
        if mean_volatility > 20:
            insights.append("High volatility levels detected in scenario results")
        
        # Add more domain-specific insights
        if len(results) >= 5:
            insights.append("Sufficient scenario diversity for robust analysis")
        else:
            insights.append("Consider running additional scenarios for more robust analysis")
        
        return insights
    
    async def _generate_recommendations(
        self,
        results: List[ScenarioExecutionResult],
        comparisons: List[ScenarioComparison]
    ) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []
        
        if not results:
            return ["Run scenarios to generate recommendations"]
        
        # Performance recommendations
        slow_scenarios = [r for r in results if r.execution_time > 120]  # More than 2 minutes
        if slow_scenarios:
            recommendations.append(
                f"Optimize {len(slow_scenarios)} slow-running scenarios for better performance"
            )
        
        # Comparison-based recommendations
        high_divergence_comparisons = [c for c in comparisons if c.metrics.divergence_score > 50]
        if high_divergence_comparisons:
            recommendations.append(
                "Review scenarios with high divergence scores to understand key drivers"
            )
        
        # Statistical recommendations
        low_correlation_pairs = [c for c in comparisons if c.metrics.correlation_coefficient < 0.3]
        if low_correlation_pairs:
            recommendations.append(
                "Investigate scenarios with low correlation to ensure they represent distinct outcomes"
            )
        
        # General recommendations
        if len(results) < 10:
            recommendations.append("Consider running parallel scenarios to improve analysis coverage")
        
        recommendations.append("Monitor forecast accuracy metrics to validate model performance")
        
        return recommendations
    
    def _calculate_distribution_metrics(self, data: List[float]) -> Dict[str, float]:
        """Calculate distribution metrics for a dataset."""
        if not data:
            return {}
        
        data_array = np.array(data)
        return {
            "skewness": float(self._calculate_skewness(data_array)),
            "kurtosis": float(self._calculate_kurtosis(data_array)),
            "coefficient_of_variation": float(np.std(data_array) / np.mean(data_array)) if np.mean(data_array) != 0 else 0
        }
    
    def _calculate_skewness(self, data: np.ndarray) -> float:
        """Calculate skewness of data."""
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0
        return np.mean(((data - mean) / std) ** 3)
    
    def _calculate_kurtosis(self, data: np.ndarray) -> float:
        """Calculate kurtosis of data."""
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0
        return np.mean(((data - mean) / std) ** 4) - 3
    
    def _calculate_quality_score(self, success_rate: float, execution_times: List[float]) -> float:
        """Calculate overall quality score for scenario execution."""
        performance_score = 1.0 - (np.mean(execution_times) / 300.0) if execution_times else 0  # Normalize by 5 minutes
        performance_score = max(0, min(1, performance_score))  # Clamp between 0 and 1
        
        return (success_rate * 0.7) + (performance_score * 0.3)  # Weighted combination
    
    def _generate_comparison_recommendation(
        self,
        metrics: ComparisonMetrics,
        significant_differences: List[str]
    ) -> str:
        """Generate recommendation based on comparison metrics."""
        if metrics.effect_size > 0.8:
            return "Large effect size detected - scenarios represent meaningfully different outcomes"
        elif metrics.effect_size > 0.5:
            return "Medium effect size - moderate differences between scenarios"
        elif metrics.correlation_coefficient < 0.3:
            return "Low correlation - scenarios may represent independent outcomes"
        elif "significant_mean_difference" in significant_differences:
            return "Significant mean difference - investigate underlying drivers"
        else:
            return "Scenarios show similar patterns - may be suitable for ensemble modeling"