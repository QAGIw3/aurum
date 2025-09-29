"""Factory classes for scenario-related test data."""

import factory
from faker import Faker
from typing import Dict, Any, List, Optional

fake = Faker()


class ScenarioFactory(factory.Factory):
    """Factory for creating scenario test data."""

    class Meta:
        model = dict

    name = factory.LazyAttribute(lambda _: fake.sentence(nb_words=4))
    description = factory.LazyAttribute(lambda _: fake.paragraph())
    scenario_type = factory.Iterator(["monte_carlo", "forecasting", "stress_test", "sensitivity_analysis"])

    @factory.post_generation
    def assumptions(self, create, extracted, **kwargs):
        """Generate scenario assumptions."""
        if not create:
            return

        if extracted is None:
            # Generate default assumptions
            assumptions = [
                {"type": "market_growth", "value": fake.pyfloat(min_value=-0.1, max_value=0.2)},
                {"type": "discount_rate", "value": fake.pyfloat(min_value=0.05, max_value=0.15)},
                {"type": "volatility", "value": fake.pyfloat(min_value=0.1, max_value=0.5)},
            ]
        else:
            assumptions = extracted

        return assumptions

    @factory.post_generation
    def parameters(self, create, extracted, **kwargs):
        """Generate scenario parameters."""
        if not create:
            return

        if extracted is None:
            # Generate default parameters
            parameters = {
                "forecast_period_months": fake.random_int(min=6, max=36),
                "confidence_interval": fake.random_element([0.90, 0.95, 0.99]),
                "num_simulations": fake.random_int(min=100, max=10000),
                "risk_free_rate": fake.pyfloat(min_value=0.02, max_value=0.08),
            }
        else:
            parameters = extracted

        return parameters

    @factory.post_generation
    def metadata(self, create, extracted, **kwargs):
        """Generate scenario metadata."""
        if not create:
            return

        if extracted is None:
            metadata = {
                "created_by": fake.user_name(),
                "created_at": fake.date_time_this_year(),
                "version": fake.random_int(min=1, max=10),
                "tags": [fake.word() for _ in range(fake.random_int(min=1, max=5))],
            }
        else:
            metadata = extracted

        return metadata


class ScenarioRunFactory(factory.Factory):
    """Factory for creating scenario run test data."""

    class Meta:
        model = dict

    scenario_id = factory.LazyAttribute(lambda _: fake.uuid4())
    run_id = factory.LazyAttribute(lambda _: fake.uuid4())
    status = factory.Iterator(["pending", "running", "completed", "failed", "cancelled"])

    @factory.post_generation
    def inputs(self, create, extracted, **kwargs):
        """Generate scenario run inputs."""
        if not create:
            return

        if extracted is None:
            inputs = {
                "curve_data": {
                    "historical_prices": [fake.pyfloat(min_value=10, max_value=100) for _ in range(100)],
                    "volatility": fake.pyfloat(min_value=0.1, max_value=0.5),
                },
                "market_data": {
                    "interest_rates": [fake.pyfloat(min_value=0.01, max_value=0.08) for _ in range(12)],
                    "inflation_rate": fake.pyfloat(min_value=0.01, max_value=0.05),
                },
                "assumptions": {
                    "growth_rate": fake.pyfloat(min_value=-0.05, max_value=0.15),
                    "discount_rate": fake.pyfloat(min_value=0.05, max_value=0.12),
                },
            }
        else:
            inputs = extracted

        return inputs

    @factory.post_generation
    def outputs(self, create, extracted, **kwargs):
        """Generate scenario run outputs."""
        if not create:
            return

        if extracted is None:
            outputs = {
                "results": {
                    "expected_return": fake.pyfloat(min_value=-0.1, max_value=0.3),
                    "risk_metrics": {
                        "volatility": fake.pyfloat(min_value=0.05, max_value=0.3),
                        "var_95": fake.pyfloat(min_value=0.01, max_value=0.2),
                        "sharpe_ratio": fake.pyfloat(min_value=-1, max_value=3),
                    },
                    "confidence_intervals": {
                        "lower_95": fake.pyfloat(min_value=-0.2, max_value=0.1),
                        "upper_95": fake.pyfloat(min_value=0.1, max_value=0.4),
                    },
                },
                "performance_metrics": {
                    "computation_time": fake.pyfloat(min_value=0.1, max_value=10.0),
                    "memory_usage": fake.random_int(min=100, max=2000),
                },
            }
        else:
            outputs = extracted

        return outputs

    @factory.post_generation
    def metadata(self, create, extracted, **kwargs):
        """Generate scenario run metadata."""
        if not create:
            return

        if extracted is None:
            metadata = {
                "started_at": fake.date_time_this_year(),
                "completed_at": factory.LazyAttribute(
                    lambda obj: fake.date_time_between(
                        start_date=obj.metadata["started_at"] if hasattr(obj, 'metadata') and obj.metadata else fake.date_time_this_year(),
                        end_date="+1h"
                    ) if fake.boolean() else None
                ),
                "duration_seconds": fake.random_int(min=1, max=3600),
                "worker_id": fake.uuid4(),
                "error_message": factory.LazyAttribute(
                    lambda _: fake.sentence() if fake.boolean(chance=20) else None
                ),
            }
        else:
            metadata = extracted

        return metadata


class ScenarioComparisonFactory(factory.Factory):
    """Factory for creating scenario comparison test data."""

    class Meta:
        model = dict

    comparison_id = factory.LazyAttribute(lambda _: fake.uuid4())
    scenario_ids = factory.LazyAttribute(
        lambda _: [str(fake.uuid4()) for _ in range(fake.random_int(min=2, max=5))]
    )
    comparison_type = factory.Iterator(["baseline_vs_scenario", "scenario_vs_scenario", "sensitivity_analysis"])

    @factory.post_generation
    def metrics(self, create, extracted, **kwargs):
        """Generate comparison metrics."""
        if not create:
            return

        if extracted is None:
            metrics = {
                "performance_comparison": {
                    "baseline_return": fake.pyfloat(min_value=0.05, max_value=0.15),
                    "scenario_return": fake.pyfloat(min_value=0.03, max_value=0.18),
                    "return_difference": fake.pyfloat(min_value=-0.1, max_value=0.1),
                    "risk_adjusted_return": fake.pyfloat(min_value=-0.5, max_value=2.0),
                },
                "risk_comparison": {
                    "baseline_volatility": fake.pyfloat(min_value=0.1, max_value=0.3),
                    "scenario_volatility": fake.pyfloat(min_value=0.08, max_value=0.35),
                    "max_drawdown_baseline": fake.pyfloat(min_value=-0.3, max_value=-0.05),
                    "max_drawdown_scenario": fake.pyfloat(min_value=-0.4, max_value=-0.03),
                },
                "statistical_tests": {
                    "t_test_p_value": fake.pyfloat(min_value=0.001, max_value=0.999),
                    "ks_test_p_value": fake.pyfloat(min_value=0.001, max_value=0.999),
                    "significant_difference": fake.boolean(),
                },
            }
        else:
            metrics = extracted

        return metrics
