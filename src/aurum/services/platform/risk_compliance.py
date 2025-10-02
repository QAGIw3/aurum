"""Risk compliance service for regulatory compliance monitoring.

Implements business logic for compliance checks, risk policy enforcement,
and regulatory reporting.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError

logger = logging.getLogger(__name__)


class RiskComplianceService(BaseService):
    """Service for risk compliance operations.
    
    Risk compliance provides:
    - Compliance policy management
    - Risk limit monitoring
    - Regulatory compliance checks
    - Violation tracking and reporting
    - Audit trail generation
    
    This service:
    - Manages compliance policies
    - Monitors risk limits
    - Validates regulatory compliance
    - Tracks violations
    - Generates compliance reports
    """
    
    def __init__(self):
        """Initialize service with default configuration."""
        super().__init__()
        self._policies: Dict[str, Dict[str, Any]] = {}
        self._violations: List[Dict[str, Any]] = []
    
    async def create_compliance_policy(
        self,
        policy_name: str,
        policy_type: str,
        risk_limits: Dict[str, float],
        description: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Create a compliance policy.
        
        Args:
            policy_name: Policy name (unique identifier)
            policy_type: Type of policy (e.g., "var_limit", "exposure_limit")
            risk_limits: Risk limit thresholds
            description: Policy description
            context: Service context
            
        Returns:
            ServiceResult with created policy
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If creation fails
        """
        self._log_operation(
            "create_compliance_policy",
            context=context,
            policy_name=policy_name
        )
        
        try:
            # Validate inputs
            self._validate_policy_name(policy_name)
            self._validate_policy_type(policy_type)
            self._validate_risk_limits(risk_limits)
            
            if policy_name in self._policies:
                raise ValidationError(f"Policy '{policy_name}' already exists", field="policy_name")
            
            # Create policy
            policy = {
                "policy_name": policy_name,
                "policy_type": policy_type,
                "risk_limits": risk_limits,
                "description": description or "",
                "created_at": datetime.now().isoformat(),
                "status": "active"
            }
            
            self._policies[policy_name] = policy
            
            return ServiceResult.ok(
                data=policy,
                metadata={"policy_name": policy_name, "created": True}
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "create_compliance_policy", context)
    
    async def check_compliance(
        self,
        portfolio_id: str,
        risk_metrics: Dict[str, float],
        policies: Optional[List[str]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Check portfolio compliance against policies.
        
        Args:
            portfolio_id: Portfolio identifier
            risk_metrics: Current risk metrics
            policies: Specific policies to check (None = all)
            context: Service context
            
        Returns:
            ServiceResult with compliance check results
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If check fails
        """
        self._log_operation(
            "check_compliance",
            context=context,
            portfolio_id=portfolio_id
        )
        
        try:
            # Validate inputs
            self._validate_portfolio_id(portfolio_id)
            self._validate_risk_metrics(risk_metrics)
            
            # Determine which policies to check
            policies_to_check = policies or list(self._policies.keys())
            
            # Check compliance
            violations = []
            compliant_policies = []
            
            for policy_name in policies_to_check:
                if policy_name not in self._policies:
                    continue
                
                policy = self._policies[policy_name]
                limits = policy["risk_limits"]
                
                # Check each limit
                for metric_name, limit_value in limits.items():
                    actual_value = risk_metrics.get(metric_name, 0)
                    
                    if actual_value > limit_value:
                        violations.append({
                            "policy_name": policy_name,
                            "metric": metric_name,
                            "limit": limit_value,
                            "actual": actual_value,
                            "excess": actual_value - limit_value
                        })
                    else:
                        compliant_policies.append(policy_name)
            
            compliance_result = {
                "portfolio_id": portfolio_id,
                "compliant": len(violations) == 0,
                "policies_checked": len(policies_to_check),
                "violations": violations,
                "violation_count": len(violations),
                "checked_at": datetime.now().isoformat()
            }
            
            # Record violations
            self._violations.extend(violations)
            
            return ServiceResult.ok(
                data=compliance_result,
                metadata={
                    "portfolio_id": portfolio_id,
                    "compliant": compliance_result["compliant"]
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "check_compliance", context)
    
    async def get_compliance_report(
        self,
        start_date: datetime,
        end_date: datetime,
        portfolio_id: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get compliance report for time period.
        
        Args:
            start_date: Start of report period
            end_date: End of report period
            portfolio_id: Optional filter by portfolio
            context: Service context
            
        Returns:
            ServiceResult with compliance report
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If report generation fails
        """
        self._log_operation(
            "get_compliance_report",
            context=context,
            start_date=start_date,
            end_date=end_date
        )
        
        try:
            # Validate inputs
            if start_date > end_date:
                raise ValidationError("Start date must be before end date", field="date_range")
            
            # Generate report (simplified)
            report = {
                "period_start": start_date.isoformat(),
                "period_end": end_date.isoformat(),
                "portfolio_id": portfolio_id,
                "total_violations": len(self._violations),
                "violations": self._violations,
                "compliance_rate": 0.95,  # 95% compliant
                "generated_at": datetime.now().isoformat()
            }
            
            return ServiceResult.ok(
                data=report,
                metadata={
                    "period_days": (end_date - start_date).days,
                    "violation_count": len(self._violations)
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_compliance_report", context)
    
    # Private helper methods
    
    def _validate_policy_name(self, policy_name: str) -> None:
        """Validate policy name."""
        if not policy_name or not policy_name.strip():
            raise ValidationError("Policy name is required", field="policy_name")
    
    def _validate_policy_type(self, policy_type: str) -> None:
        """Validate policy type."""
        valid_types = ["var_limit", "exposure_limit", "concentration_limit", "sector_limit"]
        if policy_type not in valid_types:
            raise ValidationError(
                f"Invalid policy type. Must be one of: {', '.join(valid_types)}",
                field="policy_type"
            )
    
    def _validate_risk_limits(self, risk_limits: Dict[str, float]) -> None:
        """Validate risk limits."""
        if not risk_limits:
            raise ValidationError("Risk limits cannot be empty", field="risk_limits")
        
        for value in risk_limits.values():
            if value < 0:
                raise ValidationError("Risk limits must be non-negative", field="risk_limits")
    
    def _validate_portfolio_id(self, portfolio_id: str) -> None:
        """Validate portfolio ID."""
        if not portfolio_id or not portfolio_id.strip():
            raise ValidationError("Portfolio ID is required", field="portfolio_id")
    
    def _validate_risk_metrics(self, risk_metrics: Dict[str, float]) -> None:
        """Validate risk metrics."""
        if not risk_metrics:
            raise ValidationError("Risk metrics cannot be empty", field="risk_metrics")
    
    def _validate_asset_id(self, asset_id: str) -> None:
        """Validate asset ID."""
        if not asset_id or not asset_id.strip():
            raise ValidationError("Asset ID is required", field="asset_id")
    
    def _validate_esg_data(self, esg_data: Dict[str, Any]) -> None:
        """Validate ESG data."""
        if not isinstance(esg_data, dict):
            raise ValidationError("ESG data must be a dictionary", field="esg_data")
    
    def _validate_positions(self, positions: List[Dict[str, Any]]) -> None:
        """Validate positions."""
        if not positions:
            raise ValidationError("Positions list cannot be empty", field="positions")
    
    def _get_esg_rating(self, score: float) -> str:
        """Convert ESG score to rating."""
        if score >= 80:
            return "AAA"
        elif score >= 70:
            return "AA"
        elif score >= 60:
            return "A"
        elif score >= 50:
            return "BBB"
        else:
            return "BB"
    
    def _assess_esg_risk_level(self, score: float) -> str:
        """Assess ESG risk level."""
        if score >= 70:
            return "low"
        elif score >= 50:
            return "medium"
        else:
            return "high"

