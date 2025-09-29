"""Factory classes for curve-related test data."""

import factory
from faker import Faker
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import random

fake = Faker()


class CurveFactory(factory.Factory):
    """Factory for creating curve test data."""

    class Meta:
        model = dict

    curve_id = factory.LazyAttribute(lambda _: fake.uuid4())
    name = factory.LazyAttribute(lambda _: f"Curve-{fake.word().capitalize()}")
    curve_type = factory.Iterator([
        "forward_curve", "volatility_surface", "correlation_matrix",
        "price_curve", "yield_curve", "spread_curve"
    ])
    commodity = factory.Iterator([
        "power", "gas", "oil", "coal", "carbon", "renewables"
    ])

    @factory.post_generation
    def metadata(self, create, extracted, **kwargs):
        """Generate curve metadata."""
        if not create:
            return

        if extracted is None:
            metadata = {
                "created_at": fake.date_time_this_year(),
                "updated_at": fake.date_time_this_year(),
                "version": fake.random_int(min=1, max=10),
                "source": fake.company(),
                "quality_score": fake.pyfloat(min_value=0.5, max_value=1.0),
                "tags": [fake.word() for _ in range(fake.random_int(min=1, max=5))],
            }
        else:
            metadata = extracted

        return metadata

    @factory.post_generation
    def data_points(self, create, extracted, **kwargs):
        """Generate curve data points."""
        if not create:
            return

        if extracted is None:
            # Generate time series data points
            num_points = fake.random_int(min=10, max=100)
            base_date = fake.date_time_this_year()

            data_points = []
            for i in range(num_points):
                point_date = base_date + timedelta(days=i * 7)  # Weekly points
                data_points.append({
                    "timestamp": point_date.isoformat(),
                    "value": fake.pyfloat(min_value=10, max_value=200),
                    "confidence": fake.pyfloat(min_value=0.8, max_value=1.0),
                })

        else:
            data_points = extracted

        return data_points


class CurveDataFactory(factory.Factory):
    """Factory for creating detailed curve data."""

    class Meta:
        model = dict

    curve_id = factory.LazyAttribute(lambda _: fake.uuid4())
    data_type = factory.Iterator(["prices", "volumes", "volatilities", "correlations"])

    @factory.post_generation
    def time_series(self, create, extracted, **kwargs):
        """Generate time series data."""
        if not create:
            return

        if extracted is None:
            # Generate realistic time series data
            num_points = fake.random_int(min=50, max=500)
            start_date = fake.date_time_this_year()
            frequency = fake.random_element(["hourly", "daily", "weekly", "monthly"])

            time_series = []
            current_date = start_date

            for i in range(num_points):
                if frequency == "hourly":
                    current_date += timedelta(hours=1)
                elif frequency == "daily":
                    current_date += timedelta(days=1)
                elif frequency == "weekly":
                    current_date += timedelta(weeks=1)
                else:  # monthly
                    current_date += timedelta(days=30)

                # Generate realistic price/volatility data
                if "price" in self.data_type.lower():
                    base_value = fake.pyfloat(min_value=50, max_value=150)
                    # Add some trend and noise
                    trend = (i / num_points) * fake.pyfloat(min_value=-20, max_value=30)
                    noise = fake.pyfloat(min_value=-5, max_value=5)
                    value = base_value + trend + noise

                elif "volatilit" in self.data_type.lower():
                    value = fake.pyfloat(min_value=0.1, max_value=0.8)

                else:
                    value = fake.pyfloat(min_value=0.0, max_value=1.0)

                time_series.append({
                    "timestamp": current_date.isoformat(),
                    "value": max(0, value),  # Ensure non-negative values
                    "quality_flag": fake.random_element(["good", "suspect", "bad"]),
                })

        else:
            time_series = extracted

        return time_series

    @factory.post_generation
    def statistics(self, create, extracted, **kwargs):
        """Generate curve statistics."""
        if not create:
            return

        if extracted is None:
            statistics = {
                "mean": fake.pyfloat(min_value=50, max_value=150),
                "median": fake.pyfloat(min_value=50, max_value=150),
                "std_dev": fake.pyfloat(min_value=5, max_value=30),
                "min": fake.pyfloat(min_value=20, max_value=100),
                "max": fake.pyfloat(min_value=100, max_value=200),
                "count": fake.random_int(min=50, max=500),
                "missing_count": fake.random_int(min=0, max=10),
            }
        else:
            statistics = extracted

        return statistics


class CurveComparisonFactory(factory.Factory):
    """Factory for creating curve comparison test data."""

    class Meta:
        model = dict

    comparison_id = factory.LazyAttribute(lambda _: fake.uuid4())
    curve_ids = factory.LazyAttribute(
        lambda _: [str(fake.uuid4()) for _ in range(fake.random_int(min=2, max=4))]
    )
    comparison_type = factory.Iterator(["price_comparison", "shape_analysis", "volatility_comparison"])

    @factory.post_generation
    def metrics(self, create, extracted, **kwargs):
        """Generate comparison metrics."""
        if not create:
            return

        if extracted is None:
            metrics = {
                "correlation": fake.pyfloat(min_value=-1.0, max_value=1.0),
                "mean_absolute_error": fake.pyfloat(min_value=0.0, max_value=10.0),
                "root_mean_square_error": fake.pyfloat(min_value=0.0, max_value=15.0),
                "max_difference": fake.pyfloat(min_value=0.0, max_value=50.0),
                "similarity_score": fake.pyfloat(min_value=0.0, max_value=1.0),
            }
        else:
            metrics = extracted

        return metrics


class MarketDataFactory(factory.Factory):
    """Factory for creating market data test fixtures."""

    class Meta:
        model = dict

    data_type = factory.Iterator([
        "spot_prices", "forward_prices", "option_prices", "futures_prices"
    ])
    commodity = factory.Iterator([
        "power", "gas", "oil", "coal", "carbon", "renewables"
    ])
    region = factory.Iterator([
        "ERCOT", "PJM", "MISO", "CAISO", "NYISO", "ISO-NE"
    ])

    @factory.post_generation
    def market_data(self, create, extracted, **kwargs):
        """Generate market data points."""
        if not create:
            return

        if extracted is None:
            # Generate market data for different time periods
            periods = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]
            market_data = {}

            for period in periods:
                # Generate daily prices for each period
                prices = []
                for day in range(1, 29):  # ~28-31 days
                    date = f"{period}-{day:02d}"
                    prices.append({
                        "date": date,
                        "price": fake.pyfloat(min_value=20, max_value=200),
                        "volume": fake.random_int(min=1000, max=100000),
                        "high": fake.pyfloat(min_value=25, max_value=220),
                        "low": fake.pyfloat(min_value=15, max_value=180),
                    })

                market_data[period] = prices

        else:
            market_data = extracted

        return market_data
