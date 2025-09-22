#!/usr/bin/env python3
"""NOAA Weather Station Manager - Enhanced Coverage Management.

This script provides comprehensive tools for managing NOAA weather station configurations,
including station discovery, data type analysis, and configuration management.
"""

import os
import sys
import json
import csv
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, asdict
from enum import Enum

class NOAADataType(Enum):
    """NOAA weather data types."""
    TEMPERATURE = "temperature"
    PRECIPITATION = "precipitation"
    WIND = "wind"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"
    SOLAR_RADIATION = "solar_radiation"
    EVAPORATION = "evaporation"
    SOIL_TEMPERATURE = "soil_temperature"
    VISIBILITY = "visibility"
    DEGREE_DAYS = "degree_days"
    PERCENTILES = "percentiles"
    STATISTICS = "statistics"
    NORMALS = "normals"

@dataclass
class NOAAStation:
    """NOAA weather station information."""
    id: str
    name: str
    latitude: float
    longitude: float
    elevation: float
    elevation_unit: str
    data_coverage: float
    mindate: str
    maxdate: str
    available_data_types: List[str]
    region: str
    state: str
    dataset_compatibility: Set[str]

    def to_dict(self) -> Dict:
        """Convert station to dictionary."""
        return asdict(self)

@dataclass
class NOAAConfiguration:
    """NOAA dataset configuration."""
    dataset_id: str
    dataset: str
    name: str
    description: str
    frequency: str
    schedule: str
    stations: List[str]
    datatypes: List[str]
    window_days: int
    topic: str
    enabled: bool
    quality_checks: Dict
    coverage_info: Dict

class NOAAStationManager:
    """Manager for NOAA weather stations and configurations."""

    def __init__(self, config_dir: str = "/Users/mstudio/dev/aurum/config"):
        self.config_dir = Path(config_dir)
        self.stations_file = self.config_dir / "noaa_stations_expanded.csv"
        self.config_file = self.config_dir / "noaa_ingest_datasets_expanded.json"

        # Comprehensive NOAA station database
        self.station_database = self._load_station_database()

    def _load_station_database(self) -> Dict[str, NOAAStation]:
        """Load or create comprehensive NOAA station database."""
        if self.stations_file.exists():
            return self._load_stations_from_csv()
        else:
            return self._create_comprehensive_station_database()

    def _create_comprehensive_station_database(self) -> Dict[str, NOAAStation]:
        """Create comprehensive NOAA station database with expanded coverage."""

        stations = {}

        # Major US cities and regions with high-quality weather stations
        station_data = [
            # Northeast
            ("GHCND:USW00094728", "NEW YORK CENTRAL PARK", 40.7829, -73.9654, 42.7, "m", 1.0, "1869-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD", "DP01", "DP05", "DP10", "DX32", "DX70", "DX90", "EMNT", "EMXT", "EMXP", "MNTM", "MXTM"], "Northeast", "NY", {"ghcnd_daily", "ghcnd_hourly", "gsom", "normals"}),
            ("GHCND:USW00014739", "BOSTON LOGAN INTERNATIONAL AIRPORT", 42.3656, -71.0096, 20.1, "m", 0.98, "1936-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Northeast", "MA", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00013781", "PHILADELPHIA INTERNATIONAL AIRPORT", 39.8729, -75.2408, 18.9, "m", 0.97, "1940-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB"], "Northeast", "PA", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),

            # Midwest
            ("GHCND:USW00023174", "CHICAGO O'HARE INTERNATIONAL AIRPORT", 41.9786, -87.9048, 205.4, "m", 1.0, "1962-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD", "DP01", "DP05", "DP10", "DX32", "DX70", "DX90", "EMNT", "EMXT", "EMXP", "MNTM", "MXTM"], "Midwest", "IL", {"ghcnd_daily", "ghcnd_hourly", "gsom", "normals"}),
            ("GHCND:USW00014922", "MINNEAPOLIS-ST. PAUL INTERNATIONAL AIRPORT", 44.8831, -93.2289, 255.0, "m", 0.99, "1938-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Midwest", "MN", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00094846", "DETROIT METROPOLITAN AIRPORT", 42.2124, -83.3534, 202.1, "m", 0.98, "1929-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Midwest", "MI", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),

            # Southeast
            ("GHCND:USW00013874", "ATLANTA HARTSFIELD INTERNATIONAL AIRPORT", 33.6407, -84.4269, 308.2, "m", 0.99, "1930-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD", "DP01", "DP05", "DP10", "DX32", "DX70", "DX90", "EMNT", "EMXT", "EMXP", "MNTM", "MXTM"], "Southeast", "GA", {"ghcnd_daily", "ghcnd_hourly", "gsom", "normals"}),
            ("GHCND:USW00012839", "MIAMI INTERNATIONAL AIRPORT", 25.7959, -80.2870, 4.3, "m", 0.98, "1948-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Southeast", "FL", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00013881", "CHARLOTTE DOUGLAS INTERNATIONAL AIRPORT", 35.2140, -80.9431, 228.3, "m", 0.97, "1948-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Southeast", "NC", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),

            # Southwest
            ("GHCND:USW00023188", "DENVER INTERNATIONAL AIRPORT", 39.8561, -104.6737, 1655.3, "m", 0.99, "1995-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD", "DP01", "DP05", "DP10", "DX32", "DX70", "DX90", "EMNT", "EMXT", "EMXP", "MNTM", "MXTM"], "Southwest", "CO", {"ghcnd_daily", "ghcnd_hourly", "gsom", "normals"}),
            ("GHCND:USW00023183", "PHOENIX SKY HARBOR INTERNATIONAL AIRPORT", 33.4343, -112.0117, 335.6, "m", 0.98, "1933-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Southwest", "AZ", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00023044", "LAS VEGAS MCCARRAN INTERNATIONAL AIRPORT", 36.0719, -115.1634, 664.5, "m", 0.97, "1948-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Southwest", "NV", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),

            # West Coast
            ("GHCND:USW00012842", "SEATTLE-TACOMA INTERNATIONAL AIRPORT", 47.4502, -122.3088, 113.1, "m", 0.99, "1948-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD", "DP01", "DP05", "DP10", "DX32", "DX70", "DX90", "EMNT", "EMXT", "EMXP", "MNTM", "MXTM"], "West", "WA", {"ghcnd_daily", "ghcnd_hourly", "gsom", "normals"}),
            ("GHCND:USW00023234", "SAN FRANCISCO INTERNATIONAL AIRPORT", 37.6188, -122.3750, 2.7, "m", 0.98, "1927-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "West", "CA", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00023174", "LOS ANGELES INTERNATIONAL AIRPORT", 33.9425, -118.4081, 38.1, "m", 0.99, "1944-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "West", "CA", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),

            # Additional comprehensive coverage
            ("GHCND:USW00014898", "DALLAS/FT. WORTH INTERNATIONAL AIRPORT", 32.8968, -97.0379, 182.3, "m", 0.98, "1973-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "South", "TX", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00014933", "HOUSTON INTERCONTINENTAL AIRPORT", 29.9844, -95.3414, 29.9, "m", 0.97, "1969-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "South", "TX", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00093230", "SALT LAKE CITY INTERNATIONAL AIRPORT", 40.7884, -111.9778, 1287.7, "m", 0.96, "1928-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Mountain", "UT", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00003927", "PORTLAND INTERNATIONAL AIRPORT", 45.5887, -122.5975, 11.9, "m", 0.98, "1938-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Pacific Northwest", "OR", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
            ("GHCND:USW00012960", "BALTIMORE-WASHINGTON INTERNATIONAL AIRPORT", 39.1754, -76.6684, 44.8, "m", 0.97, "1950-01-01", "2024-09-22", ["TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5", "AWND", "RHAV", "PSUN", "TSUN", "EVAP", "TSOIL", "VISIB", "CLDD", "HTDD"], "Mid-Atlantic", "MD", {"ghcnd_daily", "ghcnd_hourly", "gsom"}),
        ]

        for station_id, name, lat, lon, elev, elev_unit, coverage, mindate, maxdate, data_types, region, state, datasets in station_data:
            stations[station_id] = NOAAStation(
                id=station_id,
                name=name,
                latitude=lat,
                longitude=lon,
                elevation=elev,
                elevation_unit=elev_unit,
                data_coverage=coverage,
                mindate=mindate,
                maxdate=maxdate,
                available_data_types=data_types,
                region=region,
                state=state,
                dataset_compatibility=datasets
            )

        return stations

    def _load_stations_from_csv(self) -> Dict[str, NOAAStation]:
        """Load stations from CSV file."""
        stations = {}
        if not self.stations_file.exists():
            return self._create_comprehensive_station_database()

        with open(self.stations_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stations[row['id']] = NOAAStation(
                    id=row['id'],
                    name=row['name'],
                    latitude=float(row['latitude']),
                    longitude=float(row['longitude']),
                    elevation=float(row['elevation']),
                    elevation_unit=row['elevation_unit'],
                    data_coverage=float(row['data_coverage']),
                    mindate=row['mindate'],
                    maxdate=row['maxdate'],
                    available_data_types=row['available_data_types'].split(','),
                    region=row['region'],
                    state=row['state'],
                    dataset_compatibility=set(row['dataset_compatibility'].split(','))
                )

        return stations

    def save_stations_to_csv(self):
        """Save stations to CSV file."""
        with open(self.stations_file, 'w', newline='') as f:
            fieldnames = ['id', 'name', 'latitude', 'longitude', 'elevation', 'elevation_unit',
                         'data_coverage', 'mindate', 'maxdate', 'available_data_types',
                         'region', 'state', 'dataset_compatibility']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for station in self.station_database.values():
                writer.writerow({
                    'id': station.id,
                    'name': station.name,
                    'latitude': station.latitude,
                    'longitude': station.longitude,
                    'elevation': station.elevation,
                    'elevation_unit': station.elevation_unit,
                    'data_coverage': station.data_coverage,
                    'mindate': station.mindate,
                    'maxdate': station.maxdate,
                    'available_data_types': ','.join(station.available_data_types),
                    'region': station.region,
                    'state': station.state,
                    'dataset_compatibility': ','.join(station.dataset_compatibility)
                })

    def get_stations_by_region(self, region: str) -> List[NOAAStation]:
        """Get stations in a specific region."""
        return [s for s in self.station_database.values() if s.region == region]

    def get_stations_by_dataset(self, dataset: str) -> List[NOAAStation]:
        """Get stations compatible with a specific dataset."""
        return [s for s in self.station_database.values() if dataset in s.dataset_compatibility]

    def get_stations_by_data_types(self, required_types: List[str]) -> List[NOAAStation]:
        """Get stations that provide all required data types."""
        return [s for s in self.station_database.values()
                if all(dt in s.available_data_types for dt in required_types)]

    def generate_configuration(self) -> Dict:
        """Generate comprehensive NOAA configuration."""
        config = {
            "metadata": {
                "description": "NOAA Weather Data Ingestion Configuration - Comprehensive Coverage",
                "version": "3.0.0",
                "last_updated": datetime.now().isoformat(),
                "source": "NOAA Station Manager",
                "total_stations": len(self.station_database),
                "regions_covered": list(set(s.region for s in self.station_database.values())),
                "data_types_available": list(set(dt for s in self.station_database.values() for dt in s.available_data_types))
            },
            "datasets": []
        }

        # Generate configurations for each dataset type
        dataset_configs = {
            "ghcnd_daily": {
                "dataset": "GHCND",
                "name": "Global Historical Climatology Network - Daily",
                "description": "Daily weather observations - Comprehensive Coverage",
                "frequency": "daily",
                "schedule": "0 6 * * *",
                "window_days": 1,
                "topic": "aurum.ref.noaa.weather.ghcnd.daily.v1",
                "quality_checks": {
                    "null_value_threshold": 0.1,
                    "outlier_detection": True,
                    "range_validation": {
                        "temperature": [-50, 60],
                        "precipitation": [0, 500],
                        "wind_speed": [0, 100],
                        "humidity": [0, 100],
                        "pressure": [800, 1100],
                        "solar_radiation": [0, 1000]
                    }
                }
            },
            "ghcnd_hourly": {
                "dataset": "GHCND",
                "name": "Global Historical Climatology Network - Hourly",
                "description": "Hourly weather observations - Comprehensive Coverage",
                "frequency": "hourly",
                "schedule": "0 */6 * * *",
                "window_hours": 1,
                "window_days": 0,
                "topic": "aurum.ref.noaa.weather.ghcnd.hourly.v1",
                "quality_checks": {
                    "null_value_threshold": 0.05,
                    "outlier_detection": True,
                    "range_validation": {
                        "temperature": [-50, 60],
                        "precipitation": [0, 200],
                        "wind_speed": [0, 50],
                        "humidity": [0, 100],
                        "pressure": [800, 1100],
                        "solar_radiation": [0, 1000]
                    }
                }
            },
            "gsom_monthly": {
                "dataset": "GSOM",
                "name": "Global Summary of the Month",
                "description": "Monthly weather summaries - Comprehensive Coverage",
                "frequency": "monthly",
                "schedule": "0 8 1 * *",
                "window_days": 30,
                "topic": "aurum.ref.noaa.weather.gsom.monthly.v1",
                "quality_checks": {
                    "null_value_threshold": 0.15,
                    "outlier_detection": True,
                    "range_validation": {
                        "temperature": [-60, 70],
                        "precipitation": [0, 1000],
                        "wind_speed": [0, 150],
                        "humidity": [0, 100],
                        "pressure": [800, 1100],
                        "solar_radiation": [0, 1200]
                    }
                }
            },
            "normals_daily": {
                "dataset": "NORMAL_DLY",
                "name": "Normals - Daily",
                "description": "30-year climate normals - Comprehensive Coverage",
                "frequency": "daily",
                "schedule": "0 2 * * *",
                "window_days": 1,
                "topic": "aurum.ref.noaa.weather.normals.daily.v1",
                "quality_checks": {
                    "null_value_threshold": 0.05,
                    "outlier_detection": False,
                    "range_validation": {
                        "temperature": [-40, 50],
                        "precipitation": [0, 300]
                    }
                }
            }
        }

        for dataset_id, base_config in dataset_configs.items():
            compatible_stations = self.get_stations_by_dataset(dataset_id)

            if compatible_stations:
                # Sort by data coverage and select top stations
                sorted_stations = sorted(compatible_stations, key=lambda x: x.data_coverage, reverse=True)
                selected_stations = sorted_stations[:50]  # Limit to 50 stations per dataset

                # Get all unique data types
                all_data_types = set()
                for station in selected_stations:
                    all_data_types.update(station.available_data_types)

                dataset_config = base_config.copy()
                dataset_config.update({
                    "dataset_id": dataset_id,
                    "stations": [s.id for s in selected_stations],
                    "datatypes": sorted(list(all_data_types)),
                    "enabled": True,
                    "backfill_enabled": dataset_id != "ghcnd_hourly",  # Enable backfill for most datasets
                    "coverage_info": {
                        "total_available": len(compatible_stations),
                        "selected": len(selected_stations),
                        "average_data_coverage": sum(s.data_coverage for s in selected_stations) / len(selected_stations),
                        "regions_covered": list(set(s.region for s in selected_stations)),
                        "states_covered": list(set(s.state for s in selected_stations))
                    }
                })

                config["datasets"].append(dataset_config)

        return config

    def save_configuration(self):
        """Save comprehensive configuration to file."""
        config = self.generate_configuration()

        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"✅ Comprehensive configuration saved to: {self.config_file}")

    def print_summary(self):
        """Print summary of station coverage."""
        print("🌦️  NOAA Weather Station Coverage Summary")
        print("=" * 50)

        total_stations = len(self.station_database)
        regions = set(s.region for s in self.station_database.values())
        states = set(s.state for s in self.station_database.values())

        print(f"📊 Total Stations: {total_stations}")
        print(f"🌍 Regions Covered: {len(regions)} - {', '.join(sorted(regions))}")
        print(f"🏛️  States Covered: {len(states)} - {', '.join(sorted(states))}")

        # Data types coverage
        all_data_types = set()
        for station in self.station_database.values():
            all_data_types.update(station.available_data_types)

        print(f"📈 Data Types Available: {len(all_data_types)}")
        print(f"   Temperature: {len([dt for dt in all_data_types if dt in ['TMAX', 'TMIN', 'TAVG']])}")
        print(f"   Precipitation: {len([dt for dt in all_data_types if dt in ['PRCP', 'SNOW', 'SNWD']])}")
        print(f"   Wind: {len([dt for dt in all_data_types if dt in ['WSF2', 'WSF5', 'AWND']])}")
        print(f"   Other: {len([dt for dt in all_data_types if dt not in ['TMAX', 'TMIN', 'TAVG', 'PRCP', 'SNOW', 'SNWD', 'WSF2', 'WSF5', 'AWND']])}")

        # Dataset compatibility
        dataset_counts = {}
        for station in self.station_database.values():
            for dataset in station.dataset_compatibility:
                dataset_counts[dataset] = dataset_counts.get(dataset, 0) + 1

        print(f"\n🔧 Dataset Compatibility:")
        for dataset, count in sorted(dataset_counts.items()):
            print(f"   {dataset}: {count} stations")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="NOAA Weather Station Manager")
    parser.add_argument("--action", choices=["summary", "config", "save"], default="summary",
                       help="Action to perform")
    parser.add_argument("--config-dir", default="/Users/mstudio/dev/aurum/config",
                       help="Configuration directory")

    args = parser.parse_args()

    manager = NOAAStationManager(args.config_dir)

    if args.action == "summary":
        manager.print_summary()
    elif args.action == "config":
        config = manager.generate_configuration()
        print(json.dumps(config, indent=2))
    elif args.action == "save":
        manager.save_stations_to_csv()
        manager.save_configuration()

if __name__ == "__main__":
    main()
