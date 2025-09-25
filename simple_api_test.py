#!/usr/bin/env python3
"""Simple test to isolate API creation issues."""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_simple_import():
    """Test basic imports."""
    try:
        from aurum.core import AurumSettings
        print("✅ AurumSettings imported successfully")

        settings = AurumSettings.from_env()
        print(f"✅ Settings created: {settings.api.title}")

        return settings
    except Exception as e:
        print(f"❌ Settings import failed: {e}")
        return None

if __name__ == "__main__":
    print("=== Simple API Test ===")
    test_simple_import()
