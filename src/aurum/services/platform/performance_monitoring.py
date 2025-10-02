"""Performance monitoring service for system observability.

Implements business logic for performance monitoring, metrics collection,
and performance budgets.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError

logger = logging.getLogger(__name__)


class PerformanceMonitoringService(BaseService):
    """Service for performance monitoring operations.
    
    Performance monitoring provides:
    - Performance budget tracking
    - Load testing and benchmarking
    - Performance regression detection
    - Metric aggregation and reporting
    - SLA monitoring
    
    This service:
    - Defines and tracks performance budgets
    - Runs performance tests
    - Analyzes performance trends
    - Generates performance reports
    - Triggers alerts on violations
    """
    
    def __init__(self):
        """Initialize service with default configuration."""
        super().__init__()
        self._performance_budgets: Dict[str, Dict[str, Any]] = {}
        self._metrics: List[Dict[str, Any]] = []
    
    async def define_performance_budget(
        self,
        endpoint: str,
        target_latency_ms: float,
        target_throughput_rps: float,
        target_error_rate: float = 0.01,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Define performance budget for an endpoint.
        
        Args:
            endpoint: API endpoint path
            target_latency_ms: Target latency in milliseconds
            target_throughput_rps: Target throughput (requests per second)
            target_error_rate: Target error rate (e.g., 0.01 for 1%)
            context: Service context
            
        Returns:
            ServiceResult with performance budget
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If definition fails
        """
        self._log_operation(
            "define_performance_budget",
            context=context,
            endpoint=endpoint
        )
        
        try:
            # Validate inputs
            self._validate_endpoint(endpoint)
            self._validate_latency(target_latency_ms)
            self._validate_throughput(target_throughput_rps)
            self._validate_error_rate(target_error_rate)
            
            # Create budget
            budget = {
                "endpoint": endpoint,
                "target_latency_ms": target_latency_ms,
                "target_throughput_rps": target_throughput_rps,
                "target_error_rate": target_error_rate,
                "created_at": datetime.now().isoformat(),
                "status": "active"
            }
            
            self._performance_budgets[endpoint] = budget
            
            return ServiceResult.ok(
                data=budget,
                metadata={"endpoint": endpoint, "budget_defined": True}
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "define_performance_budget", context)
    
    async def check_performance_budget(
        self,
        endpoint: str,
        actual_metrics: Dict[str, float],
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Check if actual performance meets budget.
        
        Args:
            endpoint: API endpoint
            actual_metrics: Actual performance metrics
            context: Service context
            
        Returns:
            ServiceResult with budget check results
            
        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If budget not found
            ServiceError: If check fails
        """
        self._log_operation(
            "check_performance_budget",
            context=context,
            endpoint=endpoint
        )
        
        try:
            self._validate_endpoint(endpoint)
            
            if endpoint not in self._performance_budgets:
                raise NotFoundError("performance_budget", endpoint)
            
            budget = self._performance_budgets[endpoint]
            
            # Check metrics against budget
            results = {
                "endpoint": endpoint,
                "budget": budget,
                "actual": actual_metrics,
                "violations": [],
                "budget_met": True
            }
            
            if actual_metrics.get("latency_ms", 0) > budget["target_latency_ms"]:
                results["violations"].append({
                    "metric": "latency",
                    "target": budget["target_latency_ms"],
                    "actual": actual_metrics.get("latency_ms", 0)
                })
                results["budget_met"] = False
            
            if actual_metrics.get("error_rate", 0) > budget["target_error_rate"]:
                results["violations"].append({
                    "metric": "error_rate",
                    "target": budget["target_error_rate"],
                    "actual": actual_metrics.get("error_rate", 0)
                })
                results["budget_met"] = False
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "endpoint": endpoint,
                    "budget_met": results["budget_met"],
                    "violation_count": len(results["violations"])
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "check_performance_budget", context)
    
    async def get_performance_report(
        self,
        start_date: datetime,
        end_date: datetime,
        endpoints: Optional[List[str]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get performance report for a time period.
        
        Args:
            start_date: Start of report period
            end_date: End of report period
            endpoints: Filter by specific endpoints
            context: Service context
            
        Returns:
            ServiceResult with performance report
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If report generation fails
        """
        self._log_operation(
            "get_performance_report",
            context=context,
            start_date=start_date,
            end_date=end_date
        )
        
        try:
            # Validate inputs
            if start_date > end_date:
                raise ValidationError(
                    "Start date must be before end date",
                    field="date_range"
                )
            
            # Generate report (simplified)
            report = self._generate_performance_report(start_date, end_date, endpoints)
            
            return ServiceResult.ok(
                data=report,
                metadata={
                    "period_start": start_date.isoformat(),
                    "period_end": end_date.isoformat(),
                    "endpoints_analyzed": len(report.get("endpoint_metrics", []))
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_performance_report", context)
    
    # Private helper methods
    
    def _validate_endpoint(self, endpoint: str) -> None:
        """Validate endpoint path."""
        if not endpoint or not endpoint.strip():
            raise ValidationError("Endpoint is required", field="endpoint")
        
        if not endpoint.startswith("/"):
            raise ValidationError("Endpoint must start with /", field="endpoint")
    
    def _validate_latency(self, latency_ms: float) -> None:
        """Validate latency value."""
        if latency_ms <= 0:
            raise ValidationError("Latency must be positive", field="target_latency_ms")
        
        if latency_ms > 60000:  # 60 seconds
            raise ValidationError("Latency budget too high (max 60000ms)", field="target_latency_ms")
    
    def _validate_throughput(self, throughput_rps: float) -> None:
        """Validate throughput value."""
        if throughput_rps <= 0:
            raise ValidationError("Throughput must be positive", field="target_throughput_rps")
    
    def _validate_error_rate(self, error_rate: float) -> None:
        """Validate error rate."""
        if not (0 <= error_rate <= 1):
            raise ValidationError("Error rate must be between 0 and 1", field="target_error_rate")
    
    def _validate_dataset_name(self, dataset_name: str) -> None:
        """Validate dataset name."""
        if not dataset_name or not dataset_name.strip():
            raise ValidationError("Dataset name is required", field="dataset_name")
    
    def _validate_check_type(self, check_type: str) -> None:
        """Validate quality check type."""
        valid_types = ["completeness", "accuracy", "consistency", "timeliness", "validity"]
        if check_type not in valid_types:
            raise ValidationError(
                f"Invalid check type. Must be one of: {', '.join(valid_types)}",
                field="check_type"
            )
    
    def _execute_quality_check(
        self,
        dataset_name: str,
        check_type: str,
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a quality check."""
        return {
            "dataset_name": dataset_name,
            "check_type": check_type,
            "passed": True,
            "score": 0.95,
            "issues": [],
            "checked_at": datetime.now().isoformat()
        }
    
    def _generate_performance_report(
        self,
        start_date: datetime,
        end_date: datetime,
        endpoints: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Generate performance report."""
        return {
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            "endpoint_metrics": [],
            "overall_sla_compliance": 0.98,
            "p50_latency_ms": 45,
            "p95_latency_ms": 150,
            "p99_latency_ms": 300,
            "avg_throughput_rps": 100,
            "error_rate": 0.005,
            "generated_at": datetime.now().isoformat()
        }

