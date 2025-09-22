#!/usr/bin/env python3
"""Test script to validate NOAA DAGs and configurations."""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def test_noaa_dag_imports():
    """Test that NOAA DAGs can be imported without errors."""
    print("🧪 Testing NOAA DAG imports...")

    try:
        # Test main NOAA ingestion DAG
        from airflow.dags.noaa_ingest_dag import (
            noaa_dag,
            NOAA_DATASETS,
            get_noaa_config,
            validate_noaa_config,
            update_noaa_watermark
        )
        print("✅ noaa_ingest_dag imported successfully")
        print(f"   Created {len(NOAA_DATASETS)} dataset configurations")

        # Test NOAA monitoring DAG
        from airflow.dags.noaa_monitoring_dag import (
            monitoring_dag,
            daily_monitoring_dag,
            NOAA_MONITORING_CONFIG
        )
        print("✅ noaa_monitoring_dag imported successfully")
        print(f"   Monitoring configured for {len(NOAA_MONITORING_CONFIG['datasets'])} datasets")

        return True

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_noaa_configuration():
    """Test NOAA configuration loading."""
    print("\n🧪 Testing NOAA configuration...")

    try:
        import json

        config_path = Path(__file__).parent.parent / "config" / "noaa_ingest_datasets.json"
        with open(config_path) as f:
            config = json.load(f)

        print("✅ NOAA configuration loaded successfully")
        print(f"   Version: {config.get('metadata', {}).get('version', 'unknown')}")
        print(f"   Datasets configured: {len(config.get('datasets', []))}")

        # Validate required fields
        required_fields = ['dataset_id', 'dataset', 'stations', 'datatypes', 'frequency']
        for dataset in config.get('datasets', []):
            missing_fields = [field for field in required_fields if field not in dataset]
            if missing_fields:
                print(f"❌ Dataset {dataset.get('dataset_id', 'unknown')} missing fields: {missing_fields}")
                return False

        return True

    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def test_seatunnel_templates():
    """Test SeaTunnel template existence."""
    print("\n🧪 Testing SeaTunnel templates...")

    try:
        templates_dir = Path(__file__).parent.parent / "seatunnel" / "jobs" / "templates"
        required_templates = [
            "noaa_weather_enhanced.conf.tmpl",
            "noaa_kafka_to_timescale_enhanced.conf.tmpl"
        ]

        for template in required_templates:
            template_path = templates_dir / template
            if template_path.exists():
                print(f"✅ Template {template} found")
            else:
                print(f"❌ Template {template} missing")
                return False

        return True

    except Exception as e:
        print(f"❌ Template check error: {e}")
        return False

def test_dag_syntax():
    """Test DAG syntax by compiling all files."""
    print("\n🧪 Testing DAG syntax...")

    try:
        import subprocess

        dags_dir = Path(__file__).parent.parent / "airflow" / "dags"
        noaa_dags = [
            "noaa_ingest_dag.py",
            "noaa_monitoring_dag.py"
        ]

        for dag_file in noaa_dags:
            dag_path = dags_dir / dag_file
            if dag_path.exists():
                result = subprocess.run(
                    [sys.executable, "-m", "py_compile", str(dag_path)],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    print(f"✅ {dag_file} compiles successfully")
                else:
                    print(f"❌ {dag_file} compilation failed: {result.stderr}")
                    return False
            else:
                print(f"❌ {dag_file} not found")
                return False

        return True

    except Exception as e:
        print(f"❌ Syntax check error: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting NOAA DAG validation tests...\n")

    tests = [
        ("DAG Imports", test_noaa_dag_imports),
        ("Configuration", test_noaa_configuration),
        ("SeaTunnel Templates", test_seatunnel_templates),
        ("DAG Syntax", test_dag_syntax),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running {test_name}...")
        print('='*50)
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test_name} failed with exception: {e}")
            results.append(False)

    print(f"\n{'='*50}")
    print("📊 TEST SUMMARY")
    print('='*50)

    passed = sum(results)
    total = len(results)

    for i, (test_name, _) in enumerate(tests):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"{status} {test_name}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! NOAA workflow is ready.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
