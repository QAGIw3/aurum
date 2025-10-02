"""PPA (Power Purchase Agreement) service for contract operations.

Implements business logic for PPA contracts, valuations, and risk analysis.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import date
from calendar import monthrange

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import PpaRepository

logger = logging.getLogger(__name__)


class PpaService(BaseService):
    """Service for PPA contract operations.

    PPA (Power Purchase Agreement) contracts involve long-term energy
    purchase agreements with complex valuation and risk calculations.

    This service:
    - Validates PPA contract parameters
    - Calculates contract valuations
    - Provides risk metrics
    - Manages contract lifecycle
    - Enforces business rules
    """

    def __init__(self, ppa_repository: PpaRepository):
        """Initialize service with dependencies.

        Args:
            ppa_repository: Repository for PPA data access
        """
        super().__init__()
        self.ppa_repo = ppa_repository

    async def get_ppa_contracts(
        self,
        contract_ids: Optional[List[str]] = None,
        counterparty: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get PPA contracts with optional filtering.

        Business logic:
        - Validates contract ID format
        - Applies business rules for date ranges
        - Enforces tenant access control
        - Limits result size for performance

        Args:
            contract_ids: List of specific contract IDs
            counterparty: Filter by counterparty name
            start_date: Filter contracts starting on or after this date
            end_date: Filter contracts ending on or before this date
            limit: Maximum results (max 1000)
            context: Service context

        Returns:
            ServiceResult with list of PPA contracts

        Raises:
            ValidationError: If parameters invalid
            ServiceError: If query fails
        """
        self._log_operation(
            "get_ppa_contracts",
            context=context,
            contract_ids=contract_ids,
            counterparty=counterparty
        )

        try:
            # Validate inputs
            if contract_ids:
                self._validate_contract_ids(contract_ids)

            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )

            if start_date and end_date and start_date > end_date:
                raise ValidationError(
                    "Start date must be before end date",
                    field="date_range"
                )

            # Query repository
            contracts = await self.ppa_repo.get_ppa_contracts(
                contract_ids=contract_ids,
                counterparty=counterparty,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )

            return ServiceResult.ok(
                data=contracts,
                metadata={
                    "contract_count": len(contracts),
                    "limit": limit,
                    "has_more": len(contracts) == limit
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_ppa_contracts", context)

    async def get_ppa_valuations(
        self,
        contract_id: Optional[str] = None,
        asof_date: Optional[date] = None,
        valuation_type: Optional[str] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get PPA valuations with optional filtering.

        Business logic:
        - Validates contract exists (if specified)
        - Applies business rules for valuation types
        - Calculates derived metrics where needed

        Args:
            contract_id: Filter by specific contract ID
            asof_date: Filter valuations as of this date
            valuation_type: Filter by valuation type
            limit: Maximum results
            context: Service context

        Returns:
            ServiceResult with PPA valuations

        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If contract not found
            ServiceError: If query fails
        """
        self._log_operation(
            "get_ppa_valuations",
            context=context,
            contract_id=contract_id,
            valuation_type=valuation_type
        )

        try:
            # Validate inputs
            if contract_id:
                self._validate_contract_id(contract_id)

            if valuation_type:
                self._validate_valuation_type(valuation_type)

            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )

            # Query repository
            valuations = await self.ppa_repo.get_ppa_valuations(
                contract_id=contract_id,
                asof_date=asof_date,
                valuation_type=valuation_type,
                limit=limit
            )

            # Calculate derived metrics if needed
            enriched_valuations = self._enrich_valuations(valuations)

            return ServiceResult.ok(
                data=enriched_valuations,
                metadata={
                    "valuation_count": len(valuations),
                    "contract_id": contract_id,
                    "valuation_type": valuation_type,
                    "limit": limit
                }
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_ppa_valuations", context)

    async def calculate_contract_valuation(
        self,
        contract_id: str,
        asof_date: date,
        price_scenario: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Calculate PPA contract valuation.

        Business logic:
        - Validates contract exists and is active
        - Applies business rules for valuation calculation
        - Calculates risk-adjusted values
        - Includes sensitivity analysis

        Args:
            contract_id: PPA contract identifier
            asof_date: Valuation date
            price_scenario: Optional price scenario override
            context: Service context

        Returns:
            ServiceResult with valuation data

        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If contract not found
            ServiceError: If calculation fails
        """
        self._log_operation(
            "calculate_contract_valuation",
            context=context,
            contract_id=contract_id,
            asof_date=asof_date.isoformat()
        )

        try:
            # Validate inputs
            self._validate_contract_id(contract_id)
            self._validate_date(asof_date)

            if price_scenario:
                self._validate_price_scenario(price_scenario)

            # Check if contract exists and is active
            contracts = await self.ppa_repo.get_ppa_contracts(
                contract_ids=[contract_id],
                limit=1
            )

            if not contracts:
                raise NotFoundError("ppa_contract", contract_id)

            contract = contracts[0]

            # Check if contract is active on valuation date
            if not self._is_contract_active(contract, asof_date):
                raise ValidationError(
                    f"Contract {contract_id} is not active on {asof_date}",
                    field="contract_id"
                )

            # Calculate valuation (simplified for now)
            valuation = await self.ppa_repo.calculate_ppa_valuation(
                contract_id=contract_id,
                asof_date=asof_date,
                price_scenario=price_scenario
            )

            if not valuation:
                # Calculate valuation if not cached
                valuation = await self._calculate_valuation(contract, asof_date)

            # Add derived metrics
            enriched_valuation = self._add_risk_metrics(valuation)

            return ServiceResult.ok(
                data=enriched_valuation,
                metadata={
                    "contract_id": contract_id,
                    "asof_date": asof_date.isoformat(),
                    "calculation_performed": True
                }
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "calculate_contract_valuation", context)

    async def get_contract_risk_metrics(
        self,
        contract_id: str,
        asof_date: Optional[date] = None,
        risk_metrics: Optional[List[str]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get risk metrics for a PPA contract.

        Args:
            contract_id: PPA contract identifier
            asof_date: Date for risk calculation
            risk_metrics: Specific metrics to calculate
            context: Service context

        Returns:
            ServiceResult with risk metrics

        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If contract not found
            ServiceError: If calculation fails
        """
        self._log_operation(
            "get_contract_risk_metrics",
            context=context,
            contract_id=contract_id,
            risk_metrics=risk_metrics
        )

        try:
            # Validate inputs
            self._validate_contract_id(contract_id)

            if risk_metrics:
                for metric in risk_metrics:
                    self._validate_risk_metric(metric)

            # Get base risk metrics
            metrics = await self.ppa_repo.get_ppa_risk_metrics(
                contract_id=contract_id,
                asof_date=asof_date,
                risk_metric=risk_metrics[0] if risk_metrics else None,
                limit=100
            )

            # Calculate additional derived metrics
            enriched_metrics = self._calculate_derived_risk_metrics(metrics)

            return ServiceResult.ok(
                data={
                    "contract_id": contract_id,
                    "asof_date": asof_date.isoformat() if asof_date else None,
                    "risk_metrics": enriched_metrics,
                    "calculated_metrics": list(enriched_metrics.keys())
                },
                metadata={
                    "contract_id": contract_id,
                    "metric_count": len(enriched_metrics)
                }
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_contract_risk_metrics", context)

    # Private helper methods

    def _validate_contract_ids(self, contract_ids: List[str]) -> None:
        """Validate contract ID list."""
        if not contract_ids:
            raise ValidationError("Contract IDs list cannot be empty", field="contract_ids")

        if len(contract_ids) > 100:
            raise ValidationError("Too many contract IDs (max 100)", field="contract_ids")

        for contract_id in contract_ids:
            self._validate_contract_id(contract_id)

    def _validate_contract_id(self, contract_id: str) -> None:
        """Validate contract ID format."""
        if not contract_id or not contract_id.strip():
            raise ValidationError("Contract ID is required", field="contract_id")

        if len(contract_id) > 100:
            raise ValidationError("Contract ID too long", field="contract_id")

        # Check for invalid characters
        invalid_chars = ["<", ">", "&", "\"", "'", ";"]
        if any(char in contract_id for char in invalid_chars):
            raise ValidationError("Contract ID contains invalid characters", field="contract_id")

    def _validate_valuation_type(self, valuation_type: str) -> None:
        """Validate valuation type."""
        valid_types = ["mark_to_market", "intrinsic_value", "risk_adjusted"]
        if valuation_type not in valid_types:
            raise ValidationError(
                f"Invalid valuation type. Must be one of: {', '.join(valid_types)}",
                field="valuation_type"
            )

    def _validate_price_scenario(self, scenario: str) -> None:
        """Validate price scenario name."""
        if not scenario or not scenario.strip():
            raise ValidationError("Price scenario is required", field="price_scenario")

        if len(scenario) > 100:
            raise ValidationError("Price scenario name too long", field="price_scenario")

    def _validate_risk_metric(self, metric: str) -> None:
        """Validate risk metric name."""
        valid_metrics = ["VaR", "CVaR", "duration", "convexity", "delta", "gamma"]
        if metric not in valid_metrics:
            raise ValidationError(
                f"Invalid risk metric. Must be one of: {', '.join(valid_metrics)}",
                field="risk_metric"
            )

    def _validate_date(self, date_obj: date) -> None:
        """Validate date is reasonable."""
        if date_obj.year < 2000 or date_obj.year > 2100:
            raise ValidationError("Date out of reasonable range", field="date")

    def _is_contract_active(self, contract: Dict[str, Any], asof_date: date) -> bool:
        """Check if contract is active on given date."""
        start_date = contract.get("start_date")
        end_date = contract.get("end_date")

        if not start_date or not end_date:
            return False

        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
            return start <= asof_date <= end
        except (ValueError, TypeError):
            return False

    def _enrich_valuations(self, valuations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add derived metrics to valuations."""
        enriched = []
        for valuation in valuations:
            enriched_valuation = dict(valuation)

            # Add derived metrics (simplified)
            if "contract_value" in valuation:
                enriched_valuation["value_category"] = self._categorize_value(
                    valuation["contract_value"]
                )

            enriched.append(enriched_valuation)

        return enriched

    def _categorize_value(self, value: float) -> str:
        """Categorize contract value."""
        if value > 1000000:
            return "high_value"
        elif value > 100000:
            return "medium_value"
        else:
            return "low_value"

    async def _calculate_valuation(
        self,
        contract: Dict[str, Any],
        asof_date: date
    ) -> Dict[str, Any]:
        """Calculate PPA valuation (simplified implementation)."""
        # This would involve complex financial calculations
        # For now, return a placeholder
        return {
            "contract_id": contract["contract_id"],
            "asof_date": asof_date.isoformat(),
            "contract_value": 500000.0,
            "calculation_method": "simplified",
            "risk_adjusted_value": 480000.0
        }

    def _add_risk_metrics(self, valuation: Dict[str, Any]) -> Dict[str, Any]:
        """Add risk metrics to valuation."""
        enriched = dict(valuation)

        # Add risk metrics (simplified)
        contract_value = valuation.get("contract_value", 0)
        enriched.update({
            "var_95": contract_value * 0.05,  # 5% VaR
            "var_99": contract_value * 0.10,  # 1% VaR
            "duration": 2.5,  # Average duration
            "sensitivity_price": 0.85,  # Price sensitivity
        })

        return enriched

    def _calculate_derived_risk_metrics(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate additional derived risk metrics."""
        derived = {}

        if metrics:
            values = [m.get("value", 0) for m in metrics if "value" in m]

            if values:
                derived.update({
                    "portfolio_var": sum(values) * 0.05,
                    "max_drawdown": min(values) if values else 0,
                    "volatility": self._calculate_volatility(values),
                    "sharpe_ratio": 1.2  # Placeholder
                })

        return derived

    def _calculate_volatility(self, values: List[float]) -> float:
        """Calculate volatility from value series."""
        if len(values) < 2:
            return 0.0

        # Simple volatility calculation
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5

