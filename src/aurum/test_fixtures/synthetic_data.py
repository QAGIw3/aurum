"""Synthetic data generators for test fixtures."""

from __future__ import annotations

import json
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from pathlib import Path


@dataclass
class SyntheticDataConfig:
    """Configuration for synthetic data generation."""

    record_count: int = 100
    date_range_days: int = 30
    include_null_values: bool = True
    null_probability: float = 0.1
    include_invalid_values: bool = True
    invalid_probability: float = 0.05
    seed: Optional[int] = None


class SyntheticDataGenerator(ABC):
    """Base class for synthetic data generators."""

    def __init__(self, config: SyntheticDataConfig):
        """Initialize synthetic data generator.

        Args:
            config: Configuration for data generation
        """
        self.config = config
        if config.seed is not None:
            random.seed(config.seed)

    @abstractmethod
    def generate_records(self) -> List[Dict[str, Any]]:
        """Generate synthetic data records.

        Returns:
            List of synthetic data records
        """
        pass

    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Get Avro schema for the data.

        Returns:
            Avro schema definition
        """
        pass

    def _generate_date_range(self, start_date: str = None) -> List[str]:
        """Generate a range of dates.

        Args:
            start_date: Start date (defaults to 30 days ago)

        Returns:
            List of date strings in YYYY-MM-DD format
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=self.config.date_range_days)).strftime("%Y-%m-%d")

        start = datetime.strptime(start_date, "%Y-%m-%d")
        dates = []

        for i in range(self.config.record_count):
            date = start + timedelta(days=i)
            dates.append(date.strftime("%Y-%m-%d"))

        return dates

    def _add_null_values(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add null values to records based on configuration.

        Args:
            records: Original records

        Returns:
            Records with null values added
        """
        if not self.config.include_null_values:
            return records

        result = []
        for record in records:
            new_record = record.copy()

            # Randomly set some fields to null
            for key in record.keys():
                if random.random() < self.config.null_probability:
                    new_record[key] = None

            result.append(new_record)

        return result

    def _add_invalid_values(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add invalid values to records based on configuration.

        Args:
            records: Original records

        Returns:
            Records with invalid values added
        """
        if not self.config.include_invalid_values:
            return records

        result = []
        for record in records:
            new_record = record.copy()

            # Randomly corrupt some fields
            for key, value in record.items():
                if random.random() < self.config.invalid_probability:
                    new_record[key] = self._generate_invalid_value(key, value)

            result.append(new_record)

        return result

    def _generate_invalid_value(self, field_name: str, original_value: Any) -> Any:
        """Generate an invalid value for a field.

        Args:
            field_name: Name of the field
            original_value: Original field value

        Returns:
            Invalid value for the field
        """
        if isinstance(original_value, str):
            # Corrupt string values
            if "email" in field_name.lower():
                return "invalid-email-format"
            elif "date" in field_name.lower():
                return "invalid-date"
            else:
                return "INVALID_" + str(original_value)

        elif isinstance(original_value, (int, float)):
            # Corrupt numeric values
            if "age" in field_name.lower() or "count" in field_name.lower():
                return -999  # Invalid negative age/count
            else:
                return 999999999  # Invalid large number

        elif isinstance(original_value, bool):
            # Flip boolean values
            return not original_value

        else:
            return None  # Null as invalid value


class EIATestDataGenerator(SyntheticDataGenerator):
    """Synthetic data generator for EIA test data."""

    def generate_records(self) -> List[Dict[str, Any]]:
        """Generate synthetic EIA data records.

        Returns:
            List of synthetic EIA records
        """
        dates = self._generate_date_range()
        records = []

        for i, date in enumerate(dates):
            record = {
                "series_id": f"EIA_TEST_{i:03d}",
                "period": date,
                "value": round(random.uniform(10, 1000), 2),
                "units": random.choice(["MWh", "MW", "BTU", "Gallons"]),
                "area": random.choice(["CA", "TX", "NY", "FL", "IL"]),
                "sector": random.choice(["RESIDENTIAL", "COMMERCIAL", "INDUSTRIAL", "TRANSPORTATION"]),
                "source": "EIA",
                "ingested_at": datetime.now().isoformat()
            }
            records.append(record)

        # Add null and invalid values
        records = self._add_null_values(records)
        records = self._add_invalid_values(records)

        return records

    def get_schema(self) -> Dict[str, Any]:
        """Get Avro schema for EIA data.

        Returns:
            Avro schema definition
        """
        return {
            "type": "record",
            "name": "EiaSeriesRecord",
            "namespace": "aurum.eia",
            "fields": [
                {"name": "series_id", "type": "string"},
                {"name": "period", "type": "string"},
                {"name": "value", "type": ["null", "double"], "default": None},
                {"name": "units", "type": ["null", "string"], "default": None},
                {"name": "area", "type": ["null", "string"], "default": None},
                {"name": "sector", "type": ["null", "string"], "default": None},
                {"name": "source", "type": "string"},
                {"name": "ingested_at", "type": "string"}
            ]
        }


class FREDTestDataGenerator(SyntheticDataGenerator):
    """Synthetic data generator for FRED test data."""

    def generate_records(self) -> List[Dict[str, Any]]:
        """Generate synthetic FRED data records.

        Returns:
            List of synthetic FRED records
        """
        dates = self._generate_date_range()
        records = []

        for i, date in enumerate(dates):
            record = {
                "series_id": random.choice(["GDP", "UNRATE", "FEDFUNDS", "CPIAUCSL"]),
                "date": date,
                "value": round(random.uniform(-5, 20), 4),
                "units": random.choice(["Percent", "Billions of Dollars", "Index"]),
                "seasonal_adjustment": random.choice(["SA", "NSA"]),
                "frequency": random.choice(["M", "Q", "A"]),
                "title": "Test Economic Indicator",
                "source": "FRED",
                "ingested_at": datetime.now().isoformat()
            }
            records.append(record)

        # Add null and invalid values
        records = self._add_null_values(records)
        records = self._add_invalid_values(records)

        return records

    def get_schema(self) -> Dict[str, Any]:
        """Get Avro schema for FRED data.

        Returns:
            Avro schema definition
        """
        return {
            "type": "record",
            "name": "FredSeriesRecord",
            "namespace": "aurum.fred",
            "fields": [
                {"name": "series_id", "type": "string"},
                {"name": "date", "type": "string"},
                {"name": "value", "type": ["null", "double"], "default": None},
                {"name": "units", "type": ["null", "string"], "default": None},
                {"name": "seasonal_adjustment", "type": ["null", "string"], "default": None},
                {"name": "frequency", "type": ["null", "string"], "default": None},
                {"name": "title", "type": ["null", "string"], "default": None},
                {"name": "source", "type": "string"},
                {"name": "ingested_at", "type": "string"}
            ]
        }


class CPITestDataGenerator(SyntheticDataGenerator):
    """Synthetic data generator for CPI test data."""

    def generate_records(self) -> List[Dict[str, Any]]:
        """Generate synthetic CPI data records.

        Returns:
            List of synthetic CPI records
        """
        dates = self._generate_date_range()
        records = []

        for i, date in enumerate(dates):
            record = {
                "series_id": random.choice(["CPIAUCSL", "CPILFESL", "CPITRNSL"]),
                "date": date,
                "value": round(random.uniform(200, 300), 2),
                "units": "Index",
                "seasonal_adjustment": random.choice(["SA", "NSA"]),
                "area": "US",
                "source": "FRED",
                "ingested_at": datetime.now().isoformat()
            }
            records.append(record)

        # Add null and invalid values
        records = self._add_null_values(records)
        records = self._add_invalid_values(records)

        return records

    def get_schema(self) -> Dict[str, Any]:
        """Get Avro schema for CPI data.

        Returns:
            Avro schema definition
        """
        return {
            "type": "record",
            "name": "CpiSeriesRecord",
            "namespace": "aurum.cpi",
            "fields": [
                {"name": "series_id", "type": "string"},
                {"name": "date", "type": "string"},
                {"name": "value", "type": ["null", "double"], "default": None},
                {"name": "units", "type": ["null", "string"], "default": None},
                {"name": "seasonal_adjustment", "type": ["null", "string"], "default": None},
                {"name": "area", "type": ["null", "string"], "default": None},
                {"name": "source", "type": "string"},
                {"name": "ingested_at", "type": "string"}
            ]
        }


class NOAAWeatherDataGenerator(SyntheticDataGenerator):
    """Synthetic data generator for NOAA weather test data."""

    def generate_records(self) -> List[Dict[str, Any]]:
        """Generate synthetic NOAA weather data records.

        Returns:
            List of synthetic NOAA weather records
        """
        dates = self._generate_date_range()
        records = []

        for i, date in enumerate(dates):
            record = {
                "station_id": f"USW{random.randint(10000, 99999):05d}",
                "date": date,
                "temperature_max": round(random.uniform(-10, 40), 1),
                "temperature_min": round(random.uniform(-20, 30), 1),
                "precipitation": round(random.uniform(0, 10), 2),
                "snowfall": round(random.uniform(0, 20), 1),
                "wind_speed": round(random.uniform(0, 50), 1),
                "data_quality": random.choice([" ", "o", "s", "x"]),
                "source": "NOAA",
                "ingested_at": datetime.now().isoformat()
            }
            records.append(record)

        # Add null and invalid values
        records = self._add_null_values(records)
        records = self._add_invalid_values(records)

        return records

    def get_schema(self) -> Dict[str, Any]:
        """Get Avro schema for NOAA weather data.

        Returns:
            Avro schema definition
        """
        return {
            "type": "record",
            "name": "NoaaWeatherRecord",
            "namespace": "aurum.noaa",
            "fields": [
                {"name": "station_id", "type": "string"},
                {"name": "date", "type": "string"},
                {"name": "temperature_max", "type": ["null", "double"], "default": None},
                {"name": "temperature_min", "type": ["null", "double"], "default": None},
                {"name": "precipitation", "type": ["null", "double"], "default": None},
                {"name": "snowfall", "type": ["null", "double"], "default": None},
                {"name": "wind_speed", "type": ["null", "double"], "default": None},
                {"name": "data_quality", "type": ["null", "string"], "default": None},
                {"name": "source", "type": "string"},
                {"name": "ingested_at", "type": "string"}
            ]
        }


class ISOTestDataGenerator(SyntheticDataGenerator):
    """Synthetic data generator for ISO test data."""

    def generate_records(self) -> List[Dict[str, Any]]:
        """Generate synthetic ISO data records.

        Returns:
            List of synthetic ISO records
        """
        dates = self._generate_date_range()
        records = []

        for i, date in enumerate(dates):
            record = {
                "iso": random.choice(["NYISO", "PJM", "CAISO", "MISO"]),
                "datetime": f"{date}T{i%24"02d"}:00:00",
                "lmp": round(random.uniform(10, 200), 2),
                "load": random.randint(1000, 50000),
                "generation": random.randint(8000, 45000),
                "node": f"NODE_{i%100"03d"}",
                "zone": random.choice(["ZONE_A", "ZONE_B", "ZONE_C"]),
                "source": "ISO",
                "ingested_at": datetime.now().isoformat()
            }
            records.append(record)

        # Add null and invalid values
        records = self._add_null_values(records)
        records = self._add_invalid_values(records)

        return records

    def get_schema(self) -> Dict[str, Any]:
        """Get Avro schema for ISO data.

        Returns:
            Avro schema definition
        """
        return {
            "type": "record",
            "name": "IsoLmpRecord",
            "namespace": "aurum.iso",
            "fields": [
                {"name": "iso", "type": "string"},
                {"name": "datetime", "type": "string"},
                {"name": "lmp", "type": ["null", "double"], "default": None},
                {"name": "load", "type": ["null", "int"], "default": None},
                {"name": "generation", "type": ["null", "int"], "default": None},
                {"name": "node", "type": ["null", "string"], "default": None},
                {"name": "zone", "type": ["null", "string"], "default": None},
                {"name": "source", "type": "string"},
                {"name": "ingested_at", "type": "string"}
            ]
        }


# Factory function for creating generators
def create_data_generator(data_source: str, config: SyntheticDataConfig) -> SyntheticDataGenerator:
    """Create a data generator for the specified source.

    Args:
        data_source: Name of the data source (eia, fred, cpi, noaa, iso)
        config: Configuration for data generation

    Returns:
        Appropriate data generator instance
    """
    generators = {
        "eia": EIATestDataGenerator,
        "fred": FREDTestDataGenerator,
        "cpi": CPITestDataGenerator,
        "noaa": NOAAWeatherDataGenerator,
        "iso": ISOTestDataGenerator
    }

    if data_source not in generators:
        raise ValueError(f"Unknown data source: {data_source}")

    return generators[data_source](config)
