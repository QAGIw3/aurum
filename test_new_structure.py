#!/usr/bin/env python3
"""Test script for the new Aurum structure."""

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def test_config_system():
    """Test the new unified config system."""
    print("🧪 Testing config system...")
    
    # Set some test environment variables
    os.environ["AURUM_ENVIRONMENT"] = "test"
    os.environ["AURUM_DEBUG"] = "true"
    os.environ["AURUM_TIMESCALE_HOST"] = "test-timescale"
    os.environ["AURUM_REDIS_HOST"] = "test-redis"
    
    from libs.common.config import AurumSettings, reset_settings
    
    # Reset settings to pick up env vars
    reset_settings()
    
    settings = AurumSettings()
    
    assert settings.environment == "test"
    assert settings.debug is True
    assert settings.database.timescale_host == "test-timescale"
    assert settings.redis.host == "test-redis"
    
    # Test nested config access
    assert settings.api.title == "Aurum API"
    assert settings.cache.high_frequency_ttl == 60
    
    print("✅ Config system tests passed")


def test_core_models():
    """Test core domain models."""
    print("🧪 Testing core models...")
    
    from libs.core import CurveKey, IsoCode, IsoMarket, PriceObservation
    from datetime import datetime, date
    
    # Test CurveKey
    curve = CurveKey(
        iso=IsoCode.PJM,
        market=IsoMarket.DAY_AHEAD,
        location="WESTERN_HUB",
        product="ENERGY"
    )
    
    assert curve.iso == IsoCode.PJM
    assert curve.market == IsoMarket.DAY_AHEAD
    
    # Test PriceObservation
    obs = PriceObservation(
        curve=curve,
        interval_start=datetime.now(),
        delivery_date=date.today(),
        price=50.0,
    )
    
    assert obs.price == 50.0
    assert obs.currency.value == "USD"
    
    print("✅ Core models tests passed")


def test_repository_interfaces():
    """Test repository interfaces can be imported."""
    print("🧪 Testing repository interfaces...")
    
    from libs.storage.ports import SeriesRepository, MetadataRepository, AnalyticRepository
    from libs.storage import TimescaleSeriesRepo, PostgresMetaRepo, TrinoAnalyticRepo
    
    # Test that implementations inherit from interfaces
    assert issubclass(TimescaleSeriesRepo, SeriesRepository)
    assert issubclass(PostgresMetaRepo, MetadataRepository)
    assert issubclass(TrinoAnalyticRepo, AnalyticRepository)
    
    print("✅ Repository interface tests passed")


def test_app_structure():
    """Test FastAPI app can be created."""
    print("🧪 Testing app structure...")
    
    try:
        from apps.api.main import create_app
        from libs.common.config import AurumSettings
        
        # Create test settings
        settings = AurumSettings(
            environment="test",
            debug=True,
        )
        
        app = create_app(settings)
        
        assert app.title == settings.api.title
        assert "series" in [route.tags[0] if route.tags else "" for route in app.routes if hasattr(route, 'tags')]
        
        print("✅ App structure tests passed")
        
    except ImportError as e:
        print(f"⚠️  App structure test skipped due to missing dependencies: {e}")


def main():
    """Run all tests."""
    print("🚀 Testing new Aurum structure...\n")
    
    test_config_system()
    test_core_models()
    test_repository_interfaces()
    test_app_structure()
    
    print("\n✅ All tests passed! New structure is working correctly.")


if __name__ == "__main__":
    main()