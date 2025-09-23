#!/usr/bin/env python3
"""Mock external data provider for sandbox testing."""

from __future__ import annotations

import json
import os
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
import uvicorn

app = FastAPI(title="Mock External Data Provider")

# Configuration
PROVIDER_NAME = os.getenv("PROVIDER_NAME", "unknown")
MOCK_DATA_FILE = os.getenv("MOCK_DATA_FILE", "/app/testdata/mock_data.json")
MOCK_RESPONSE_DELAY = float(os.getenv("MOCK_RESPONSE_DELAY", "0.1"))


def load_mock_data() -> Dict[str, Any]:
    """Load mock data from file."""
    try:
        with open(MOCK_DATA_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        # Generate sample data if file doesn't exist
        return generate_sample_data()


def generate_sample_data() -> Dict[str, Any]:
    """Generate sample mock data."""
    base_time = datetime.now() - timedelta(days=7)

    # Generate time series data
    timeseries_data = []
    for i in range(1000):
        timestamp = (base_time + timedelta(hours=i)).isoformat()
        timeseries_data.append({
            "timestamp": timestamp,
            "value": round(random.uniform(10, 100), 2),
            "series_id": f"mock_series_{i % 10}",
            "metadata": {"quality": "good", "source": PROVIDER_NAME}
        })

    # Generate catalog data
    catalog_data = []
    for i in range(50):
        catalog_data.append({
            "series_id": f"mock_series_{i}",
            "name": f"Mock Series {i}",
            "description": f"Sample data series {i} for {PROVIDER_NAME}",
            "frequency": "hourly",
            "units": "USD/MWh",
            "start_date": base_time.isoformat(),
            "end_date": datetime.now().isoformat()
        })

    return {
        "timeseries": timeseries_data,
        "catalog": catalog_data,
        "metadata": {
            "provider": PROVIDER_NAME,
            "generated_at": datetime.now().isoformat(),
            "data_points": len(timeseries_data),
            "series_count": len(catalog_data)
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "provider": PROVIDER_NAME,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/catalog")
async def get_catalog():
    """Get catalog of available data series."""
    data = load_mock_data()

    # Add response delay for realism
    if MOCK_RESPONSE_DELAY > 0:
        await asyncio.sleep(MOCK_RESPONSE_DELAY)

    return {
        "provider": PROVIDER_NAME,
        "catalog": data["catalog"],
        "metadata": data["metadata"]
    }


@app.get("/data/{series_id}")
async def get_series_data(
    series_id: str,
    start_date: str = None,
    end_date: str = None,
    limit: int = 1000
):
    """Get time series data for a specific series."""
    data = load_mock_data()

    # Filter data for the requested series
    series_data = [item for item in data["timeseries"] if item["series_id"] == series_id]

    # Apply date filters if provided
    if start_date:
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        series_data = [item for item in series_data if datetime.fromisoformat(item["timestamp"].replace('Z', '+00:00')) >= start_dt]

    if end_date:
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        series_data = [item for item in series_data if datetime.fromisoformat(item["timestamp"].replace('Z', '+00:00')) <= end_dt]

    # Apply limit
    series_data = series_data[:limit]

    # Add response delay for realism
    if MOCK_RESPONSE_DELAY > 0:
        await asyncio.sleep(MOCK_RESPONSE_DELAY * len(series_data) / 1000)

    return {
        "provider": PROVIDER_NAME,
        "series_id": series_id,
        "data": series_data,
        "count": len(series_data),
        "metadata": data["metadata"]
    }


@app.get("/bulk/{data_type}")
async def get_bulk_data(
    data_type: str,
    start_date: str = None,
    end_date: str = None
):
    """Get bulk data for testing."""
    data = load_mock_data()

    if data_type == "catalog":
        bulk_data = data["catalog"]
    elif data_type == "timeseries":
        bulk_data = data["timeseries"]
    else:
        raise HTTPException(status_code=400, detail=f"Unknown data type: {data_type}")

    # Add response delay for realism
    if MOCK_RESPONSE_DELAY > 0:
        await asyncio.sleep(MOCK_RESPONSE_DELAY * len(bulk_data) / 1000)

    return {
        "provider": PROVIDER_NAME,
        "data_type": data_type,
        "data": bulk_data,
        "count": len(bulk_data),
        "metadata": data["metadata"]
    }


@app.get("/rate-limit-test")
async def rate_limit_test():
    """Endpoint to test rate limiting behavior."""
    # Simulate occasional rate limiting
    if random.random() < 0.05:  # 5% chance
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    # Add variable delay
    delay = MOCK_RESPONSE_DELAY + random.uniform(0, 0.1)
    await asyncio.sleep(delay)

    return {
        "provider": PROVIDER_NAME,
        "message": "Rate limit test passed",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/error-test")
async def error_test():
    """Endpoint to test error handling."""
    # Simulate occasional errors
    if random.random() < 0.1:  # 10% chance
        raise HTTPException(status_code=500, detail="Internal server error")

    return {
        "provider": PROVIDER_NAME,
        "message": "Error test passed",
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    import asyncio

    # Determine port based on provider
    port_map = {
        "caiso": 8001,
        "eia": 8002,
        "fred": 8003
    }

    port = port_map.get(PROVIDER_NAME, 8000)

    print(f"Starting mock {PROVIDER_NAME} provider on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
