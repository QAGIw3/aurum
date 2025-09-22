#!/usr/bin/env python3
"""Simple test script for NOAA DAGs validation."""

import sys
import os
import subprocess
from pathlib import Path

def test_dag_imports():
    """Test that NOAA DAGs can be imported without external dependencies."""
    print("🧪 Testing NOAA DAG imports...")

    try:
        # Test main NOAA ingestion DAG
        print("   Testing noaa_ingest_dag.py...")
        result = subprocess.run(
            [sys.executable, "-c", "import airflow.dags.noaa_ingest_dag"],
            capture_output=True,
            text=True,
            cwd="/Users/mstudio/dev/aurum"
        )

        if result.returncode == 0:
            print("   ✅ noaa_ingest_dag imported successfully")
        else:
            print(f"   ❌ Import failed: {result.stderr}")
            return False

        # Test NOAA monitoring DAG
        print("   Testing noaa_monitoring_dag.py...")
        result = subprocess.run(
            [sys.executable, "-c", "import airflow.dags.noaa_monitoring_dag"],
            capture_output=True,
            text=True,
            cwd="/Users/mstudio/dev/aurum"
        )

        if result.returncode == 0:
            print("   ✅ noaa_monitoring_dag imported successfully")
        else:
            print(f"   ❌ Import failed: {result.stderr}")
            return False

        return True

    except Exception as e:
        print(f"❌ Import test failed: {e}")
        return False

def test_dag_syntax():
    """Test DAG syntax by compiling all files."""
    print("\n🧪 Testing DAG syntax...")

    try:
        dags_dir = Path("/Users/mstudio/dev/aurum/airflow/dags")
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
                    print(f"   ✅ {dag_file} compiles successfully")
                else:
                    print(f"   ❌ {dag_file} compilation failed: {result.stderr}")
                    return False
            else:
                print(f"   ❌ {dag_file} not found")
                return False

        return True

    except Exception as e:
        print(f"❌ Syntax check error: {e}")
        return False

def test_configuration():
    """Test NOAA configuration loading."""
    print("\n🧪 Testing NOAA configuration...")

    try:
        import json

        config_path = Path("/Users/mstudio/dev/aurum/config/noaa_ingest_datasets.json")
        with open(config_path) as f:
            config = json.load(f)

        print("   ✅ NOAA configuration loaded successfully")
        print(f"      Version: {config.get('metadata', {}).get('version', 'unknown')}")
        print(f"      Datasets configured: {len(config.get('datasets', []))}")

        # Validate required fields
        required_fields = ['dataset_id', 'dataset', 'stations', 'datatypes', 'frequency']
        for dataset in config.get('datasets', []):
            missing_fields = [field for field in required_fields if field not in dataset]
            if missing_fields:
                print(f"   ❌ Dataset {dataset.get('dataset_id', 'unknown')} missing fields: {missing_fields}")
                return False

        return True

    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def test_seatunnel_templates():
    """Test SeaTunnel template existence."""
    print("\n🧪 Testing SeaTunnel templates...")

    try:
        templates_dir = Path("/Users/mstudio/dev/aurum/seatunnel/jobs/templates")
        required_templates = [
            "noaa_weather_enhanced.conf.tmpl",
            "noaa_kafka_to_timescale_enhanced.conf.tmpl"
        ]

        for template in required_templates:
            template_path = templates_dir / template
            if template_path.exists():
                print(f"   ✅ Template {template} found")
            else:
                print(f"   ❌ Template {template} missing")
                return False

        return True

    except Exception as e:
        print(f"❌ Template check error: {e}")
        return False

def run_simple_test():
    """Run simple NOAA DAG validation tests."""
    print("🚀 NOAA DAG Simple Validation Test")
    print("=" * 50)

    tests = [
        ("DAG Imports", test_dag_imports),
        ("Configuration", test_configuration),
        ("SeaTunnel Templates", test_seatunnel_templates),
        ("DAG Syntax", test_dag_syntax),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*20}")
        print(f"Running {test_name}...")
        print('='*20)

        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test_name} failed with exception: {e}")
            results.append(False)

    # Summary
    print("\n" + "="*50)
    print("📊 TEST SUMMARY")
    print("="*50)

    passed = sum(results)
    total = len(results)

    for i, (test_name, _) in enumerate(tests):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"{status} {test_name}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! NOAA workflow is ready for deployment.")
        print("\n📋 Next Steps:")
        print("  1. Set NOAA_API_TOKEN environment variable")
        print("  2. Start Kafka, Schema Registry, and TimescaleDB services")
        print("  3. Run deployment script: ./scripts/deploy_noaa_workflow.sh")
        print("  4. Test DAG execution in Airflow UI")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please fix the issues above.")
        return 1

def main():
    """Main function."""
    return run_simple_test()

if __name__ == "__main__":
    sys.exit(main())
