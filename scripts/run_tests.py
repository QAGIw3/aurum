#!/usr/bin/env python3
"""Comprehensive test runner for Aurum refactored services."""

import asyncio
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional


class TestRunner:
    """Comprehensive test runner for all test types."""

    def __init__(self):
        self.repo_root = Path(__file__).parent.parent
        self.test_results: Dict[str, Dict[str, Any]] = {}

    def run_unit_tests(self) -> Dict[str, Any]:
        """Run unit tests."""
        print("🧪 Running Unit Tests...")

        result = {
            "type": "unit",
            "passed": 0,
            "failed": 0,
            "total": 0,
            "duration": 0.0,
            "output": ""
        }

        try:
            start_time = time.time()

            # Run pytest on unit tests
            cmd = [
                sys.executable, "-m", "pytest",
                "tests/unit/",
                "-v",
                "--tb=short",
                "--durations=10"
            ]

            process = subprocess.run(
                cmd,
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )

            result["duration"] = time.time() - start_time
            result["output"] = process.stdout + process.stderr

            if process.returncode == 0:
                result["passed"] = 1  # Simplified for demo
                result["total"] = 1
                print(f"  ✅ Unit Tests: PASSED ({result['duration']:.1f}s)")
            else:
                result["failed"] = 1
                result["total"] = 1
                print(f"  ❌ Unit Tests: FAILED ({result['duration']:.1f}s)")

        except subprocess.TimeoutExpired:
            result["failed"] = 1
            result["output"] = "Unit tests timed out"
            print("  ⏰ Unit Tests: TIMEOUT")
        except Exception as e:
            result["failed"] = 1
            result["output"] = f"Error running unit tests: {e}"
            print(f"  ⚠️  Unit Tests: ERROR - {e}")

        self.test_results["unit"] = result
        return result

    def run_integration_tests(self) -> Dict[str, Any]:
        """Run integration tests."""
        print("🔗 Running Integration Tests...")

        result = {
            "type": "integration",
            "passed": 0,
            "failed": 0,
            "total": 0,
            "duration": 0.0,
            "output": ""
        }

        try:
            start_time = time.time()

            # Run pytest on integration tests
            cmd = [
                sys.executable, "-m", "pytest",
                "tests/integration/",
                "-v",
                "--tb=short",
                "--durations=10"
            ]

            process = subprocess.run(
                cmd,
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout for integration tests
            )

            result["duration"] = time.time() - start_time
            result["output"] = process.stdout + process.stderr

            if process.returncode == 0:
                result["passed"] = 1  # Simplified for demo
                result["total"] = 1
                print(f"  ✅ Integration Tests: PASSED ({result['duration']:.1f}s)")
            else:
                result["failed"] = 1
                result["total"] = 1
                print(f"  ❌ Integration Tests: FAILED ({result['duration']:.1f}s)")

        except subprocess.TimeoutExpired:
            result["failed"] = 1
            result["output"] = "Integration tests timed out"
            print("  ⏰ Integration Tests: TIMEOUT")
        except Exception as e:
            result["failed"] = 1
            result["output"] = f"Error running integration tests: {e}"
            print(f"  ⚠️  Integration Tests: ERROR - {e}")

        self.test_results["integration"] = result
        return result

    def run_contract_tests(self) -> Dict[str, Any]:
        """Run contract tests."""
        print("📋 Running Contract Tests...")

        result = {
            "type": "contract",
            "passed": 0,
            "failed": 0,
            "total": 0,
            "duration": 0.0,
            "output": ""
        }

        try:
            start_time = time.time()

            # Run pytest on contract tests
            cmd = [
                sys.executable, "-m", "pytest",
                "tests/contract/",
                "-v",
                "--tb=short"
            ]

            process = subprocess.run(
                cmd,
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=300
            )

            result["duration"] = time.time() - start_time
            result["output"] = process.stdout + process.stderr

            if process.returncode == 0:
                result["passed"] = 1  # Simplified for demo
                result["total"] = 1
                print(f"  ✅ Contract Tests: PASSED ({result['duration']:.1f}s)")
            else:
                result["failed"] = 1
                result["total"] = 1
                print(f"  ❌ Contract Tests: FAILED ({result['duration']:.1f}s)")

        except subprocess.TimeoutExpired:
            result["failed"] = 1
            result["output"] = "Contract tests timed out"
            print("  ⏰ Contract Tests: TIMEOUT")
        except Exception as e:
            result["failed"] = 1
            result["output"] = f"Error running contract tests: {e}"
            print(f"  ⚠️  Contract Tests: ERROR - {e}")

        self.test_results["contract"] = result
        return result

    def run_performance_tests(self) -> Dict[str, Any]:
        """Run performance tests."""
        print("⚡ Running Performance Tests...")

        result = {
            "type": "performance",
            "passed": 0,
            "failed": 0,
            "total": 0,
            "duration": 0.0,
            "output": ""
        }

        try:
            start_time = time.time()

            # For demo, we'll run a simple performance check
            # In real implementation, would run load tests, benchmark tests, etc.

            # Simulate performance testing by running a simple script
            performance_script = self.repo_root / "demo_enhanced_di_container.py"

            if performance_script.exists():
                cmd = [sys.executable, str(performance_script)]

                process = subprocess.run(
                    cmd,
                    cwd=self.repo_root,
                    capture_output=True,
                    text=True,
                    timeout=120
                )

                result["duration"] = time.time() - start_time
                result["output"] = process.stdout + process.stderr

                if process.returncode == 0:
                    result["passed"] = 1
                    result["total"] = 1
                    print(f"  ✅ Performance Tests: PASSED ({result['duration']:.1f}s)")
                else:
                    result["failed"] = 1
                    result["total"] = 1
                    print(f"  ❌ Performance Tests: FAILED ({result['duration']:.1f}s)")
            else:
                result["failed"] = 1
                result["output"] = "Performance test script not found"
                print("  ❌ Performance Tests: SCRIPT NOT FOUND")

        except subprocess.TimeoutExpired:
            result["failed"] = 1
            result["output"] = "Performance tests timed out"
            print("  ⏰ Performance Tests: TIMEOUT")
        except Exception as e:
            result["failed"] = 1
            result["output"] = f"Error running performance tests: {e}"
            print(f"  ⚠️  Performance Tests: ERROR - {e}")

        self.test_results["performance"] = result
        return result

    def run_quality_gates(self) -> Dict[str, Any]:
        """Run quality gates."""
        print("🏗️  Running Quality Gates...")

        result = {
            "type": "quality_gates",
            "passed": 0,
            "failed": 0,
            "total": 0,
            "duration": 0.0,
            "output": ""
        }

        try:
            start_time = time.time()

            # Run quality gates script
            quality_script = self.repo_root / "scripts" / "quality_gates.py"

            if quality_script.exists():
                cmd = [sys.executable, str(quality_script)]

                process = subprocess.run(
                    cmd,
                    cwd=self.repo_root,
                    capture_output=True,
                    text=True,
                    timeout=60
                )

                result["duration"] = time.time() - start_time
                result["output"] = process.stdout + process.stderr

                if process.returncode == 0:
                    result["passed"] = 1
                    result["total"] = 1
                    print(f"  ✅ Quality Gates: PASSED ({result['duration']:.1f}s)")
                else:
                    result["failed"] = 1
                    result["total"] = 1
                    print(f"  ❌ Quality Gates: FAILED ({result['duration']:.1f}s)")
            else:
                result["failed"] = 1
                result["output"] = "Quality gates script not found"
                print("  ❌ Quality Gates: SCRIPT NOT FOUND")

        except subprocess.TimeoutExpired:
            result["failed"] = 1
            result["output"] = "Quality gates timed out"
            print("  ⏰ Quality Gates: TIMEOUT")
        except Exception as e:
            result["failed"] = 1
            result["output"] = f"Error running quality gates: {e}"
            print(f"  ⚠️  Quality Gates: ERROR - {e}")

        self.test_results["quality_gates"] = result
        return result

    def run_all_tests(self) -> bool:
        """Run all test types."""
        print("🚀 Running Comprehensive Test Suite")
        print("=" * 60)

        test_types = [
            ("Unit Tests", self.run_unit_tests),
            ("Integration Tests", self.run_integration_tests),
            ("Contract Tests", self.run_contract_tests),
            ("Performance Tests", self.run_performance_tests),
            ("Quality Gates", self.run_quality_gates),
        ]

        all_passed = True
        total_duration = 0.0

        for test_name, test_func in test_types:
            print(f"\n🔍 {test_name}...")
            test_result = test_func()
            total_duration += test_result["duration"]

            if test_result["failed"] > 0:
                all_passed = False

        # Print summary
        print(f"\n{'='*60}")
        print("📊 TEST SUMMARY")
        print("=" * 60)

        total_passed = sum(r["passed"] for r in self.test_results.values())
        total_failed = sum(r["failed"] for r in self.test_results.values())
        total_tests = sum(r["total"] for r in self.test_results.values())

        print(f"✅ Passed: {total_passed}")
        print(f"❌ Failed: {total_failed}")
        print(f"📈 Success Rate: {(total_passed / max(total_tests, 1)) * 100:.1f}%")
        print(f"⏱️  Total Duration: {total_duration:.1f}s")

        for test_type, result in self.test_results.items():
            status = "✅" if result["passed"] > result["failed"] else "❌"
            print(f"  {status} {test_type.title()}: {result['passed']}/{result['total']} passed ({result['duration']:.1f}s)")

        return all_passed

    def get_test_report(self) -> str:
        """Generate a detailed test report."""
        if not self.test_results:
            return "No tests have been run yet."

        report = ["📋 COMPREHENSIVE TEST REPORT"]
        report.append("=" * 50)

        total_passed = 0
        total_failed = 0
        total_duration = 0.0

        for test_type, result in self.test_results.items():
            passed = result["passed"]
            failed = result["failed"]
            duration = result["duration"]

            total_passed += passed
            total_failed += failed
            total_duration += duration

            status = "✅ PASSED" if passed >= failed else "❌ FAILED"
            report.append(f"\n{test_type.upper()}: {status}")
            report.append(f"  Tests: {passed + failed}")
            report.append(f"  Passed: {passed}")
            report.append(f"  Failed: {failed}")
            report.append(f"  Duration: {duration:.1f}s")

            if result["output"]:
                # Add a snippet of output for failed tests
                if failed > 0:
                    lines = result["output"].split('\n')[:10]  # First 10 lines
                    report.append("  Output:")
                    for line in lines:
                        if line.strip():
                            report.append(f"    {line}")

        report.append(f"\n{'='*50}")
        report.append("OVERALL RESULTS:")
        report.append(f"  Total Tests: {total_passed + total_failed}")
        report.append(f"  Total Passed: {total_passed}")
        report.append(f"  Total Failed: {total_failed}")
        report.append(f"  Success Rate: {(total_passed / max(total_passed + total_failed, 1)) * 100:.1f}%")
        report.append(f"  Total Duration: {total_duration:.1f}s")

        return "\n".join(report)


def main():
    """Run comprehensive test suite."""
    runner = TestRunner()

    print("🧪 Aurum Comprehensive Test Runner")
    print("=" * 60)

    success = runner.run_all_tests()

    if success:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed.")
        print("\n" + runner.get_test_report())
        return 1


if __name__ == "__main__":
    sys.exit(main())
