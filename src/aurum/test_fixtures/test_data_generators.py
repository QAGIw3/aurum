"""Test data generators for ISO data ingestion testing."""

import random
import string
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from ..external.adapters.base import IsoDataType


@dataclass
class TestDataConfig:
    """Configuration for test data generation."""
    record_count: int = 100
    time_range_hours: int = 24
    price_range: tuple[float, float] = (-100, 1000)
    load_range: tuple[float, float] = (1000, 50000)
    generation_range: tuple[float, float] = (0, 1000)
    location_count: int = 10


class TestDataGenerator:
    """Base class for generating test data."""

    def __init__(self, config: TestDataConfig = None):
        self.config = config or TestDataConfig()
        self._random = random.Random(42)  # Seed for reproducible results

    def generate_timestamp_range(self, start_time: datetime = None) -> List[datetime]:
        """Generate a range of timestamps."""
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=self.config.time_range_hours)

        timestamps = []
        for i in range(self.config.record_count):
            hours_offset = i * (self.config.time_range_hours / self.config.record_count)
            timestamp = start_time + timedelta(hours=hours_offset)
            timestamps.append(timestamp)

        return timestamps

    def generate_location_id(self, prefix: str = "LOC") -> str:
        """Generate a location ID."""
        return f"{prefix}{self._random.randint(1, self.config.location_count):03d}"

    def generate_price(self) -> float:
        """Generate a realistic price."""
        base_price = self._random.uniform(*self.config.price_range)
        # Add some realistic variation
        variation = self._random.uniform(-10, 10)
        return round(base_price + variation, 2)

    def generate_load(self) -> float:
        """Generate a realistic load value."""
        base_load = self._random.uniform(*self.config.load_range)
        # Add some realistic variation
        variation = self._random.uniform(-1000, 1000)
        return round(base_load + variation, 2)

    def generate_id(self, length: int = 8) -> str:
        """Generate a random ID."""
        return ''.join(self._random.choices(string.ascii_uppercase + string.digits, k=length))


class CaisoTestDataGenerator(TestDataGenerator):
    """Test data generator for CAISO."""

    def generate_lmp_data(self, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate CAISO LMP test data."""
        timestamps = self.generate_timestamp_range(start_time)
        locations = [f"TH_NP15_GEN-APND_{i:03d}" for i in range(1, 6)]
        locations += [f"TH_SP15_GEN-APND_{i:03d}" for i in range(1, 6)]

        data = []
        for timestamp in timestamps:
            for location in locations:
                zone = "NP15" if "NP15" in location else "SP15"

                # Generate correlated prices (NP15 typically lower than SP15)
                base_price = 40.0 if zone == "NP15" else 50.0
                price_variation = self._random.uniform(-5, 5)
                price_total = round(base_price + price_variation, 2)

                record = {
                    "INTERVAL_START_GMT": timestamp.isoformat() + "Z",
                    "INTERVAL_END_GMT": (timestamp + timedelta(hours=1)).isoformat() + "Z",
                    "PNODE_ID": location,
                    "PNODE_NAME": location,
                    "TAC_ZONE": zone,
                    "MARKET_RUN_ID": self._random.choice(["DAM", "RTM"]),
                    "LMP_CONGESTION": round(price_total * 0.05, 2),
                    "LMP_LOSS": round(price_total * 0.02, 2),
                    "LMP_ENERGY": round(price_total * 0.93, 2),
                    "OASIS_RUN_ID": self.generate_id()
                }
                data.append(record)

        return data[:self.config.record_count]

    def generate_load_data(self, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate CAISO load test data."""
        timestamps = self.generate_timestamp_range(start_time)
        zones = ["CAISO", "SP15", "NP15", "ZP26"]

        data = []
        for timestamp in timestamps:
            for zone in zones:
                # Generate realistic load patterns
                hour = timestamp.hour
                base_load = 25000 if zone == "CAISO" else 8000

                # Peak hours have higher load
                if 6 <= hour <= 20:
                    load_multiplier = 1.2
                else:
                    load_multiplier = 0.8

                load = base_load * load_multiplier * (0.9 + self._random.uniform(-0.1, 0.1))
                load = round(load, 2)

                record = {
                    "INTERVAL_START_GMT": timestamp.isoformat() + "Z",
                    "INTERVAL_END_GMT": (timestamp + timedelta(hours=1)).isoformat() + "Z",
                    "TAC_ZONE": zone,
                    "MARKET_RUN_ID": "DAM",
                    "MW": load
                }
                data.append(record)

        return data[:self.config.record_count]


class MisoTestDataGenerator(TestDataGenerator):
    """Test data generator for MISO."""

    def generate_lmp_data(self, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate MISO LMP test data."""
        timestamps = self.generate_timestamp_range(start_time)
        zones = ["ALTW", "CIN", "DEOK", "DPL", "MEC"]

        data = []
        for timestamp in timestamps:
            for zone in zones:
                # MISO prices are typically lower than CAISO
                lmp = self.generate_price() * 0.8
                congestion = round(lmp * 0.1, 2)
                losses = round(lmp * 0.05, 2)

                record = {
                    "timestamp": timestamp.isoformat(),
                    "node_id": f"{zone}.{zone}",
                    "node_name": zone,
                    "zone": zone,
                    "market": self._random.choice(["DA", "RT"]),
                    "lmp": lmp,
                    "congestion": congestion,
                    "losses": losses
                }
                data.append(record)

        return data[:self.config.record_count]

    def generate_load_data(self, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate MISO load test data."""
        timestamps = self.generate_timestamp_range(start_time)
        zones = ["ALTW", "CIN", "DEOK", "DPL", "MEC"]

        data = []
        for timestamp in timestamps:
            for zone in zones:
                load = self._random.uniform(2000, 8000)
                load = round(load, 2)

                record = {
                    "timestamp": timestamp.isoformat(),
                    "zone": zone,
                    "load": load
                }
                data.append(record)

        return data[:self.config.record_count]


class PjmTestDataGenerator(TestDataGenerator):
    """Test data generator for PJM."""

    def generate_lmp_data(self, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate PJM LMP test data."""
        timestamps = self.generate_timestamp_range(start_time)
        hubs = ["WESTERN_HUB", "EASTERN_HUB", "PJM_WEST", "PJM_EAST"]

        data = []
        for timestamp in timestamps:
            for hub in hubs:
                lmp = self.generate_price() * 1.1  # PJM prices typically higher
                mcpc = round(lmp * 0.95, 2)
                mcl = round(lmp * 0.03, 2)
                mcc = round(lmp * 0.02, 2)

                record = {
                    "datetime_beginning_ept": timestamp.isoformat(),
                    "datetime_beginning_utc": timestamp.isoformat(),
                    "pnode_id": hub,
                    "pnode_name": hub,
                    "total_lmp_da": lmp,
                    "system_energy_price_da": mcpc,
                    "congestion_price_da": mcc,
                    "marginal_loss_price_da": mcl
                }
                data.append(record)

        return data[:self.config.record_count]


class ErcotTestDataGenerator(TestDataGenerator):
    """Test data generator for ERCOT."""

    def generate_spp_data(self, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate ERCOT SPP test data."""
        timestamps = self.generate_timestamp_range(start_time)
        settlement_points = ["HB_HOUSTON", "HB_NORTH", "HB_SOUTH", "LZ_AEN", "LZ_CPS"]

        data = []
        for timestamp in timestamps:
            for settlement_point in settlement_points:
                spp = self.generate_price() * 0.9  # ERCOT prices typically lower

                record = {
                    "settlement_point": settlement_point,
                    "settlement_point_name": settlement_point,
                    "spp": spp,
                    "mcpc": round(spp * 0.98, 2),
                    "mcl": round(spp * 0.01, 2),
                    "mcc": round(spp * 0.01, 2),
                    "timestamp": timestamp.isoformat()
                }
                data.append(record)

        return data[:self.config.record_count]


class SppTestDataGenerator(TestDataGenerator):
    """Test data generator for SPP."""

    def generate_lmp_data(self, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate SPP LMP test data."""
        timestamps = self.generate_timestamp_range(start_time)
        locations = ["SETTLEMENT_LOCATION_001", "HUB001", "ZONE001", "SETTLEMENT_LOCATION_002"]

        data = []
        for timestamp in timestamps:
            for location in locations:
                lmp = self.generate_price() * 1.05  # SPP prices moderate

                record = {
                    "settlement_location": location,
                    "settlement_location_name": location,
                    "lmp": lmp,
                    "lmp_congestion": round(lmp * 0.08, 2),
                    "lmp_loss": round(lmp * 0.03, 2),
                    "lmp_energy": round(lmp * 0.89, 2),
                    "timestamp": timestamp.isoformat(),
                    "market_time": timestamp.isoformat()
                }
                data.append(record)

        return data[:self.config.record_count]


# Factory function to get the appropriate generator
def get_test_data_generator(iso_code: str) -> TestDataGenerator:
    """Get the appropriate test data generator for an ISO."""
    generators = {
        "CAISO": CaisoTestDataGenerator,
        "MISO": MisoTestDataGenerator,
        "PJM": PjmTestDataGenerator,
        "ERCOT": ErcotTestDataGenerator,
        "SPP": SppTestDataGenerator
    }

    generator_class = generators.get(iso_code.upper())
    if generator_class:
        return generator_class()

    return TestDataGenerator()


# Convenience functions for generating test data
def generate_caiso_lmp_data(count: int = 50) -> List[Dict[str, Any]]:
    """Generate CAISO LMP test data."""
    generator = CaisoTestDataGenerator(TestDataConfig(record_count=count))
    return generator.generate_lmp_data()


def generate_miso_load_data(count: int = 30) -> List[Dict[str, Any]]:
    """Generate MISO load test data."""
    generator = MisoTestDataGenerator(TestDataConfig(record_count=count))
    return generator.generate_load_data()


def generate_pjm_generation_data(count: int = 40) -> List[Dict[str, Any]]:
    """Generate PJM generation test data."""
    generator = PjmTestDataGenerator(TestDataConfig(record_count=count))
    return generator.generate_lmp_data()


def generate_ercot_spp_data(count: int = 60) -> List[Dict[str, Any]]:
    """Generate ERCOT SPP test data."""
    generator = ErcotTestDataGenerator(TestDataConfig(record_count=count))
    return generator.generate_spp_data()


def generate_spp_interchange_data(count: int = 25) -> List[Dict[str, Any]]:
    """Generate SPP interchange test data."""
    generator = SppTestDataGenerator(TestDataConfig(record_count=count))
    return generator.generate_lmp_data()
