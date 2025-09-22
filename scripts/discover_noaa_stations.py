#!/usr/bin/env python3
"""Script to discover NOAA weather stations and their available data types."""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple
from pathlib import Path

# NOAA API Configuration
NOAA_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
NOAA_TOKEN = os.getenv("NOAA_API_TOKEN", "")

# US States and major cities for comprehensive coverage
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# Common weather data types available in NOAA GHCND
WEATHER_DATA_TYPES = [
    # Temperature
    "TMAX", "TMIN", "TAVG",  # Maximum, Minimum, Average Temperature
    # Precipitation
    "PRCP",  # Precipitation
    "SNOW", "SNWD",  # Snowfall, Snow Depth
    # Wind
    "WSF2", "WSF5", "WSFM", "AWND",  # Wind Speed (2min, 5min, max, average)
    # Humidity
    "RHAV",  # Relative Humidity
    # Pressure
    "PSUN",  # Pressure
    # Solar Radiation
    "TSUN",  # Sunshine Duration
    # Evaporation
    "EVAP",  # Evaporation
    # Soil Temperature
    "TSOIL",  # Soil Temperature
    # Visibility
    "VISIB",  # Visibility
    # Cloud Cover
    "CLDD",  # Cooling Degree Days
    "HTDD",  # Heating Degree Days
]

# Climate Normals data types
NORMALS_DATA_TYPES = [
    "DLY-TMAX-NORMAL", "DLY-TMIN-NORMAL", "DLY-TAVG-NORMAL",
    "DLY-PRCP-NORMAL", "DLY-SNOW-NORMAL",
    "DLY-SNWD-NORMAL", "DLY-AWND-NORMAL"
]

def make_noaa_request(endpoint: str, params: Dict = None) -> Dict:
    """Make a request to NOAA API with proper error handling."""
    headers = {"token": NOAA_TOKEN} if NOAA_TOKEN else {}

    try:
        response = requests.get(
            f"{NOAA_BASE_URL}/{endpoint}",
            headers=headers,
            params=params or {},
            timeout=30
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400 and "Token parameter is required" in response.text:
            print("⚠️  NOAA API token required for this endpoint")
            return {"results": []}
        else:
            print(f"❌ API error {response.status_code}: {response.text}")
            return {"results": []}

    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
        return {"results": []}

def get_available_datasets() -> List[Dict]:
    """Get list of available NOAA datasets."""
    print("📊 Discovering available NOAA datasets...")

    data = make_noaa_request("datasets")
    return data.get("results", [])

def get_stations_for_location(location_id: str, dataset: str = "GHCND") -> List[Dict]:
    """Get stations for a specific location."""
    params = {
        "datasetid": dataset,
        "locationid": location_id,
        "limit": 1000
    }

    data = make_noaa_request("stations", params)
    return data.get("results", [])

def get_station_data_types(station_id: str, dataset: str = "GHCND") -> List[str]:
    """Get available data types for a specific station."""
    params = {
        "datasetid": dataset,
        "stationid": station_id,
        "startdate": "2023-01-01",
        "enddate": "2023-12-31",
        "limit": 10
    }

    data = make_noaa_request("data", params)
    results = data.get("results", [])

    data_types = set()
    for record in results:
        if "datatype" in record:
            data_types.add(record["datatype"])

    return list(data_types)

def discover_high_quality_stations() -> Dict[str, List[Dict]]:
    """Discover high-quality weather stations across the US."""
    print("🗺️  Discovering high-quality weather stations...")

    discovered_stations = {
        "ghcnd_daily": [],
        "ghcnd_hourly": [],
        "gsom": [],
        "normals": []
    }

    # Get available datasets first
    datasets = get_available_datasets()
    print(f"✅ Found {len(datasets)} available datasets")

    # Search for stations in major US cities and states
    search_locations = []

    # Add state locations
    for state in US_STATES:
        search_locations.append(f"FIPS:{state}")

    # Add major city locations (using FIPS codes)
    major_cities = [
        "FIPS:36061",  # New York County, NY
        "FIPS:17031",  # Cook County, IL (Chicago)
        "FIPS:06075",  # San Francisco County, CA
        "FIPS:48201",  # Harris County, TX (Houston)
        "FIPS:12086",  # Miami-Dade County, FL
        "FIPS:53033",  # King County, WA (Seattle)
        "FIPS:4013",   # Maricopa County, AZ (Phoenix)
        "FIPS:6037",   # Los Angeles County, CA
        "FIPS:42101",  # Philadelphia County, PA
        "FIPS:22071",  # Orleans Parish, LA (New Orleans)
    ]

    search_locations.extend(major_cities)

    print(f"🔍 Searching {len(search_locations)} locations for weather stations...")

    for location in search_locations:
        stations = get_stations_for_location(location, "GHCND")

        for station in stations:
            # Filter for high-quality stations
            if (station.get("datacoverage", 0) > 0.9 and  # High data coverage
                station.get("maxdate", "") >= "2023" and  # Recent data
                station.get("mindate", "") <= "2010"):    # Historical data

                # Get available data types
                data_types = get_station_data_types(station["id"], "GHCND")

                # Categorize station based on available data types
                station_info = {
                    "id": station["id"],
                    "name": station["name"],
                    "latitude": station["latitude"],
                    "longitude": station["longitude"],
                    "elevation": station["elevation"],
                    "elevationUnit": station["elevationUnit"],
                    "datacoverage": station["datacoverage"],
                    "mindate": station["mindate"],
                    "maxdate": station["maxdate"],
                    "available_data_types": data_types,
                    "location": location
                }

                # Categorize by dataset compatibility
                if "TMAX" in data_types and "TMIN" in data_types:
                    if len(data_types) >= 5:  # Good variety of data
                        discovered_stations["ghcnd_daily"].append(station_info)
                        if "TMAX" in data_types and len(data_types) >= 3:  # Hourly capable
                            discovered_stations["ghcnd_hourly"].append(station_info)

                if "TAVG" in data_types:
                    discovered_stations["gsom"].append(station_info)

                if any(dt.startswith("DLY-") for dt in data_types):
                    discovered_stations["normals"].append(station_info)

    # Remove duplicates
    for category in discovered_stations:
        seen_ids = set()
        unique_stations = []
        for station in discovered_stations[category]:
            if station["id"] not in seen_ids:
                seen_ids.add(station["id"])
                unique_stations.append(station)
        discovered_stations[category] = unique_stations

    return discovered_stations

def generate_expanded_configuration(discovered_stations: Dict[str, List[Dict]]) -> Dict:
    """Generate expanded NOAA configuration based on discovered stations."""

    # Get current configuration as base
    config_path = Path(__file__).parent.parent / "config" / "noaa_ingest_datasets.json"
    with open(config_path) as f:
        base_config = json.load(f)

    # Expand each dataset
    expanded_datasets = []

    for dataset_config in base_config["datasets"]:
        dataset_id = dataset_config["dataset_id"]
        stations = discovered_stations.get(dataset_id, [])

        if stations:
            # Select top stations by data coverage
            sorted_stations = sorted(stations, key=lambda x: x["datacoverage"], reverse=True)

            # Take top 50 stations or all if less than 50
            selected_stations = sorted_stations[:50]

            expanded_config = dataset_config.copy()
            expanded_config["stations"] = [s["id"] for s in selected_stations]
            expanded_config["expanded_coverage"] = {
                "total_discovered": len(stations),
                "selected": len(selected_stations),
                "average_data_coverage": sum(s["datacoverage"] for s in selected_stations) / len(selected_stations),
                "regions_covered": len(set(s["location"] for s in selected_stations))
            }

            # Expand data types based on discovered types
            all_data_types = set()
            for station in selected_stations:
                all_data_types.update(station["available_data_types"])

            expanded_config["datatypes"] = sorted(list(all_data_types))
            expanded_datasets.append(expanded_config)

    # Update metadata
    base_config["datasets"] = expanded_datasets
    base_config["metadata"]["last_updated"] = datetime.now().isoformat()
    base_config["metadata"]["version"] = "2.0.0"
    base_config["metadata"]["expansion_info"] = {
        "total_stations_discovered": sum(len(stations) for stations in discovered_stations.values()),
        "datasets_expanded": len(expanded_datasets),
        "expansion_date": datetime.now().isoformat()
    }

    return base_config

def create_station_summary(discovered_stations: Dict[str, List[Dict]]) -> Dict:
    """Create a summary of discovered stations."""

    summary = {
        "total_stations": sum(len(stations) for stations in discovered_stations.values()),
        "by_dataset": {},
        "by_region": {},
        "data_types_coverage": {},
        "quality_metrics": {}
    }

    for dataset, stations in discovered_stations.items():
        summary["by_dataset"][dataset] = len(stations)

        # Analyze data types coverage
        all_data_types = set()
        for station in stations:
            all_data_types.update(station["available_data_types"])

        summary["data_types_coverage"][dataset] = sorted(list(all_data_types))

        # Quality metrics
        if stations:
            avg_coverage = sum(s["datacoverage"] for s in stations) / len(stations)
            summary["quality_metrics"][dataset] = {
                "average_data_coverage": avg_coverage,
                "min_coverage": min(s["datacoverage"] for s in stations),
                "max_coverage": max(s["datacoverage"] for s in stations)
            }

    return summary

def main():
    """Main discovery function."""
    print("🌦️  NOAA Weather Station Discovery Tool")
    print("=" * 50)

    if not NOAA_TOKEN:
        print("⚠️  NOAA_API_TOKEN not set. Results will be limited.")
        print("   Set it with: export NOAA_API_TOKEN='your_token'")
        print()

    # Discover stations
    discovered_stations = discover_high_quality_stations()

    # Create summary
    summary = create_station_summary(discovered_stations)

    print("\n📊 Discovery Summary:")
    print(f"   Total stations discovered: {summary['total_stations']}")
    for dataset, count in summary["by_dataset"].items():
        print(f"   {dataset}: {count} stations")

    print("\n🧪 Data Types Coverage:")
    for dataset, data_types in summary["data_types_coverage"].items():
        print(f"   {dataset}: {len(data_types)} data types - {', '.join(data_types[:5])}...")

    print("\n📈 Quality Metrics:")
    for dataset, metrics in summary["quality_metrics"].items():
        print(f"   {dataset}: {metrics['average_data_coverage']:.2f} avg coverage")

    # Generate expanded configuration
    print("\n🔧 Generating expanded configuration...")
    expanded_config = generate_expanded_configuration(discovered_stations)

    # Save expanded configuration
    output_path = Path(__file__).parent.parent / "config" / "noaa_ingest_datasets_expanded.json"
    with open(output_path, "w") as f:
        json.dump(expanded_config, f, indent=2)

    print(f"✅ Expanded configuration saved to: {output_path}")

    # Save discovery summary
    summary_path = Path(__file__).parent.parent / "config" / "noaa_station_discovery_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"✅ Discovery summary saved to: {summary_path}")

    print("\n🎯 Next Steps:")
    print("   1. Review the expanded configuration")
    print("   2. Update main configuration file if desired")
    print("   3. Test with a subset of new stations")
    print("   4. Update monitoring thresholds if needed")

if __name__ == "__main__":
    main()
