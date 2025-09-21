#!/usr/bin/env python3
"""Test script to verify NOAA job setup and Vault integration."""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

def test_noaa_templates() -> bool:
    """Test that NOAA templates exist and are properly configured."""
    template_dir = REPO_ROOT / "seatunnel" / "jobs" / "templates"

    required_templates = [
        "noaa_ghcnd_to_kafka.conf.tmpl",
        "noaa_ghcnd_daily_to_kafka.conf.tmpl",
        "noaa_ghcnd_hourly_to_kafka.conf.tmpl"
    ]

    missing_templates = []
    for template in required_templates:
        template_path = template_dir / template
        if not template_path.exists():
            missing_templates.append(template)
        else:
            # Check for sliding window support
            content = template_path.read_text()
            if "SLIDING_HOURS" not in content:
                print(f"⚠️ {template} may not have sliding window support")
            if "VAULT" not in content:
                print(f"⚠️ {template} may not have Vault token injection")

    if missing_templates:
        print("❌ Missing NOAA templates:", missing_templates)
        return False

    print("✅ All NOAA templates found")
    return True

def test_vault_scripts() -> bool:
    """Test that Vault management scripts exist."""
    script_dir = REPO_ROOT / "scripts" / "secrets"

    required_scripts = [
        "noaa_vault_manager.py",
        "pull_noaa_env.py"
    ]

    missing_scripts = []
    for script in required_scripts:
        script_path = script_dir / script
        if not script_path.exists():
            missing_scripts.append(script)

    if missing_scripts:
        print("❌ Missing Vault scripts:", missing_scripts)
        return False

    print("✅ All Vault management scripts found")
    return True

def test_airflow_dag() -> bool:
    """Test that Airflow DAG includes NOAA jobs."""
    dag_file = REPO_ROOT / "airflow" / "dags" / "ingest_public_feeds.py"
    if not dag_file.exists():
        print("❌ Airflow DAG file not found")
        return False

    content = dag_file.read_text()

    # Check for new NOAA sources
    if "noaa_ghcnd_daily" not in content:
        print("❌ NOAA daily job not found in DAG")
        return False

    if "noaa_ghcnd_hourly" not in content:
        print("❌ NOAA hourly job not found in DAG")
        return False

    # Check for sliding window configuration
    if "SLIDING_HOURS" not in content:
        print("⚠️ Sliding window configuration may be missing from DAG")

    # Check for Vault mappings
    if "aurum/noaa:token" not in content:
        print("⚠️ NOAA Vault token mapping may be missing from DAG")

    print("✅ Airflow DAG includes NOAA jobs with proper configuration")
    return True

def main() -> int:
    print("Testing NOAA job setup...")

    tests = [
        ("NOAA Templates", test_noaa_templates),
        ("Vault Scripts", test_vault_scripts),
        ("Airflow DAG", test_airflow_dag),
    ]

    all_passed = True
    for test_name, test_func in tests:
        print(f"\n🧪 Testing {test_name}...")
        try:
            if not test_func():
                all_passed = False
        except Exception as e:
            print(f"❌ {test_name} test failed: {e}")
            all_passed = False

    if all_passed:
        print("\n✅ All NOAA job setup tests passed!")
        print("\nNext steps:")
        print("1. Initialize Vault with NOAA secrets: python3 scripts/secrets/noaa_vault_manager.py --init")
        print("2. Set NOAA API token: python3 scripts/secrets/noaa_vault_manager.py --set 'your-token-here'")
        print("3. Test dry-run: python3 scripts/seatunnel/dry_run_renderer.py --template noaa_ghcnd_daily_to_kafka")
        print("4. Deploy to Airflow and test with: airflow dags test ingest_public_feeds")
        return 0
    else:
        print("\n❌ Some NOAA job setup tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
