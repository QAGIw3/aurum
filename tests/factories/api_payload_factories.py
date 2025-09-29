"""Factory classes for API request/response payloads."""

import factory
from faker import Faker
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

fake = Faker()


class ApiPayloadFactory:
    """Factory for creating various API payloads."""

    @staticmethod
    def create_scenario_payload(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a scenario creation payload."""
        payload = {
            "name": fake.sentence(nb_words=4),
            "description": fake.paragraph(),
            "scenario_type": fake.random_element([
                "monte_carlo", "forecasting", "stress_test", "sensitivity_analysis"
            ]),
            "assumptions": [
                {"type": "market_growth", "value": fake.pyfloat(min_value=-0.1, max_value=0.2)},
                {"type": "discount_rate", "value": fake.pyfloat(min_value=0.05, max_value=0.15)},
            ],
            "parameters": {
                "forecast_period_months": fake.random_int(min=6, max=36),
                "confidence_interval": fake.random_element([0.90, 0.95, 0.99]),
                "num_simulations": fake.random_int(min=100, max=10000),
            },
            "metadata": {
                "tags": [fake.word() for _ in range(fake.random_int(min=1, max=5))],
                "priority": fake.random_element(["low", "medium", "high", "critical"]),
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_scenario_run_payload(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a scenario run payload."""
        payload = {
            "scenario_id": str(fake.uuid4()),
            "parameters": {
                "curve_data": {
                    "historical_prices": [fake.pyfloat(min_value=10, max_value=100) for _ in range(50)],
                    "volatility": fake.pyfloat(min_value=0.1, max_value=0.5),
                },
                "market_assumptions": {
                    "interest_rates": [fake.pyfloat(min_value=0.01, max_value=0.08) for _ in range(12)],
                    "inflation_rate": fake.pyfloat(min_value=0.01, max_value=0.05),
                },
            },
            "run_options": {
                "async": fake.boolean(),
                "priority": fake.random_element(["low", "normal", "high"]),
                "timeout_minutes": fake.random_int(min=5, max=120),
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_curve_payload(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a curve creation payload."""
        payload = {
            "name": f"Curve-{fake.word().capitalize()}",
            "curve_type": fake.random_element([
                "forward_curve", "volatility_surface", "correlation_matrix",
                "price_curve", "yield_curve", "spread_curve"
            ]),
            "commodity": fake.random_element([
                "power", "gas", "oil", "coal", "carbon", "renewables"
            ]),
            "region": fake.random_element([
                "ERCOT", "PJM", "MISO", "CAISO", "NYISO", "ISO-NE"
            ]),
            "data_points": [
                {
                    "timestamp": (datetime.now() + timedelta(days=i)).isoformat(),
                    "value": fake.pyfloat(min_value=10, max_value=200),
                    "confidence": fake.pyfloat(min_value=0.8, max_value=1.0),
                }
                for i in range(fake.random_int(min=10, max=100))
            ],
            "metadata": {
                "source": fake.company(),
                "quality_score": fake.pyfloat(min_value=0.5, max_value=1.0),
                "version": fake.random_int(min=1, max=10),
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_user_payload(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a user creation payload."""
        payload = {
            "username": fake.user_name(),
            "email": fake.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "roles": fake.random_elements(
                elements=["admin", "analyst", "viewer", "developer"],
                length=fake.random_int(min=1, max=3),
                unique=True
            ),
            "preferences": {
                "theme": fake.random_element(["light", "dark", "auto"]),
                "timezone": fake.timezone(),
                "language": fake.random_element(["en", "es", "fr", "de"]),
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_tenant_payload(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a tenant creation payload."""
        payload = {
            "name": fake.company(),
            "description": fake.catch_phrase(),
            "settings": {
                "max_users": fake.random_int(min=5, max=100),
                "max_scenarios": fake.random_int(min=10, max=1000),
                "feature_flags": fake.random_elements(
                    elements=[
                        "advanced_analytics", "real_time_data", "api_access",
                        "custom_models", "bulk_operations"
                    ],
                    length=fake.random_int(min=1, max=5),
                    unique=True
                ),
            },
            "subscription": {
                "plan": fake.random_element(["starter", "professional", "enterprise"]),
                "billing_cycle": fake.random_element(["monthly", "annual"]),
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_search_payload(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a search/filter payload."""
        payload = {
            "query": fake.sentence(nb_words=3),
            "filters": {
                "date_range": {
                    "start": (datetime.now() - timedelta(days=30)).isoformat(),
                    "end": datetime.now().isoformat(),
                },
                "status": fake.random_elements(
                    elements=["active", "completed", "failed", "pending"],
                    length=fake.random_int(min=1, max=3),
                    unique=True
                ),
                "tags": [fake.word() for _ in range(fake.random_int(min=1, max=3))],
            },
            "pagination": {
                "page": fake.random_int(min=1, max=10),
                "page_size": fake.random_int(min=10, max=100),
                "sort_by": fake.random_element(["created_at", "name", "status"]),
                "sort_order": fake.random_element(["asc", "desc"]),
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_bulk_operation_payload(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a bulk operation payload."""
        payload = {
            "operation": fake.random_element([
                "delete", "update", "archive", "export", "duplicate"
            ]),
            "resource_type": fake.random_element([
                "scenarios", "curves", "users", "tenants"
            ]),
            "resource_ids": [str(fake.uuid4()) for _ in range(fake.random_int(min=2, max=20))],
            "parameters": {
                "new_status": fake.random_element(["active", "archived", "deleted"]),
                "tags_to_add": [fake.word() for _ in range(fake.random_int(min=1, max=3))],
                "tags_to_remove": [fake.word() for _ in range(fake.random_int(min=0, max=2))],
            },
            "options": {
                "dry_run": fake.boolean(),
                "async": fake.boolean(),
                "notify_on_completion": fake.boolean(),
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_error_response(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create an error response payload."""
        payload = {
            "error": {
                "type": fake.random_element([
                    "validation_error", "authentication_error", "authorization_error",
                    "not_found_error", "rate_limit_error", "internal_error"
                ]),
                "code": fake.random_element([
                    "INVALID_REQUEST", "UNAUTHORIZED", "FORBIDDEN", "NOT_FOUND",
                    "RATE_LIMIT_EXCEEDED", "INTERNAL_ERROR", "SERVICE_UNAVAILABLE"
                ]),
                "message": fake.sentence(),
                "details": {
                    "field_errors": [
                        {
                            "field": fake.word(),
                            "message": fake.sentence(),
                            "code": fake.word().upper(),
                        }
                        for _ in range(fake.random_int(min=0, max=3))
                    ],
                    "context": {
                        "request_id": str(fake.uuid4()),
                        "timestamp": datetime.now().isoformat(),
                        "path": f"/api/v1/{fake.word()}",
                        "method": fake.random_element(["GET", "POST", "PUT", "DELETE"]),
                    }
                }
            }
        }

        if overrides:
            payload.update(overrides)

        return payload

    @staticmethod
    def create_success_response(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a success response payload."""
        payload = {
            "data": {
                "id": str(fake.uuid4()),
                "created_at": fake.date_time_this_year().isoformat(),
                "updated_at": fake.date_time_this_year().isoformat(),
            },
            "meta": {
                "request_id": str(fake.uuid4()),
                "processing_time_ms": fake.random_int(min=10, max=1000),
                "version": "1.0.0",
            }
        }

        if overrides:
            payload.update(overrides)

        return payload
