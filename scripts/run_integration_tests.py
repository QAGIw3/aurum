#!/usr/bin/env python3
"""Integration test runner for Aurum platform.

This script orchestrates the full integration test suite including:
- Docker compose setup/teardown
- Service health checks
- API endpoint testing
- RLS and tenancy validation
- Data pipeline verification
- Cleanup operations
"""

import asyncio
import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

import yaml


class IntegrationTestRunner:
    """Orchestrates integration tests for the Aurum platform."""

    def __init__(
        self,
        compose_file: str = "docker-compose.integration.yml",
        project_name: str = "aurum-integration-test"
    ):
        self.compose_file = compose_file
        self.project_name = project_name
        self.base_dir = Path(__file__).parent.parent

    def run_command(self, cmd: List[str], check: bool = True, **kwargs) -> subprocess.CompletedProcess:
        """Run a shell command."""
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, **kwargs)

        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, cmd)

        return result

    def start_services(self) -> None:
        """Start all services using docker-compose."""
        print("Starting integration test services...")

        cmd = [
            "docker-compose",
            "-f", str(self.base_dir / self.compose_file),
            "-p", self.project_name,
            "up",
            "-d",
            "--wait"
        ]

        self.run_command(cmd)
        print("Services started successfully")

    def stop_services(self) -> None:
        """Stop all services."""
        print("Stopping integration test services...")

        cmd = [
            "docker-compose",
            "-f", str(self.base_dir / self.compose_file),
            "-p", self.project_name,
            "down",
            "-v"
        ]

        try:
            self.run_command(cmd)
            print("Services stopped successfully")
        except Exception as e:
            print(f"Warning: Error stopping services: {e}")

    def check_service_health(self) -> bool:
        """Check if all services are healthy."""
        print("Checking service health...")

        services = [
            ("postgres", "localhost", 15432),
            ("redis", "localhost", 16379),
            ("kafka", "localhost", 9092),
            ("schema-registry", "localhost", 8081),
            ("trino", "localhost", 8080),
            ("api", "localhost", 8000),
        ]

        all_healthy = True

        for service_name, host, port in services:
            try:
                # Use appropriate health check for each service
                if service_name == "postgres":
                    cmd = ["pg_isready", "-h", host, "-p", str(port), "-U", "aurum_test"]
                    result = subprocess.run(cmd, capture_output=True, check=True)
                    healthy = result.returncode == 0
                elif service_name == "redis":
                    cmd = ["redis-cli", "-h", host, "-p", str(port), "ping"]
                    result = subprocess.run(cmd, capture_output=True, check=True)
                    healthy = b"PONG" in result.stdout
                elif service_name == "kafka":
                    cmd = ["kafka-broker-api-versions", "--bootstrap-server", f"{host}:{port}"]
                    result = subprocess.run(cmd, capture_output=True)
                    healthy = result.returncode == 0
                elif service_name == "api":
                    cmd = ["curl", "-f", "http://localhost:8000/health"]
                    result = subprocess.run(cmd, capture_output=True)
                    healthy = result.returncode == 0
                else:
                    # Generic HTTP health check
                    cmd = ["curl", "-f", f"http://{host}:{port}/health"]
                    result = subprocess.run(cmd, capture_output=True)
                    healthy = result.returncode == 0

                status = "✓" if healthy else "✗"
                print(f"  {service_name}: {status}")
                all_healthy = all_healthy and healthy

            except Exception as e:
                print(f"  {service_name}: ✗ (Error: {e})")
                all_healthy = False

        return all_healthy

    def wait_for_services(self, timeout: int = 300) -> bool:
        """Wait for all services to become healthy."""
        print(f"Waiting for services to be healthy (timeout: {timeout}s)...")

        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.check_service_health():
                print("All services are healthy!")
                return True

            print("Services not ready yet, waiting...")
            time.sleep(10)

        print("Timeout waiting for services to be healthy")
        return False

    def run_tests(self, test_args: List[str]) -> int:
        """Run the test suite."""
        print("Running integration tests...")

        cmd = [
            sys.executable, "-m", "pytest",
            "tests/integration/",
            "-v",
            "--tb=short",
            "--junitxml=test-results.xml",
            "--cov=src/aurum",
            "--cov-report=xml",
            "--cov-report=term-missing"
        ] + test_args

        result = self.run_command(cmd, check=False)

        if result.returncode == 0:
            print("All tests passed!")
        else:
            print("Some tests failed!")

        return result.returncode

    def setup_test_data(self) -> None:
        """Set up test data in the database."""
        print("Setting up test data...")

        # Run database migrations
        cmd = [
            "docker-compose",
            "-f", str(self.base_dir / self.compose_file),
            "-p", self.project_name,
            "exec", "-T", "postgres",
            "psql", "-U", "aurum_test", "-d", "aurum_test",
            "-f", "/fixtures/setup_test_data.sql"
        ]

        try:
            self.run_command(cmd)
            print("Test data setup completed")
        except Exception as e:
            print(f"Warning: Test data setup failed: {e}")

    def collect_logs(self) -> None:
        """Collect logs from all services."""
        print("Collecting service logs...")

        cmd = [
            "docker-compose",
            "-f", str(self.base_dir / self.compose_file),
            "-p", self.project_name,
            "logs"
        ]

        try:
            result = self.run_command(cmd, capture_output=True)
            log_file = self.base_dir / "integration_test_logs.txt"
            with open(log_file, "w") as f:
                f.write(result.stdout.decode())
            print(f"Logs collected to: {log_file}")
        except Exception as e:
            print(f"Warning: Log collection failed: {e}")

    def run_full_test_suite(self, test_args: List[str] = None) -> int:
        """Run the complete integration test suite."""
        if test_args is None:
            test_args = []

        try:
            print("Starting integration test suite...")

            # Start services
            self.start_services()

            # Wait for services to be ready
            if not self.wait_for_services():
                raise RuntimeError("Services failed to become healthy")

            # Set up test data
            self.setup_test_data()

            # Run tests
            test_exit_code = self.run_tests(test_args)

            # Collect logs
            self.collect_logs()

            return test_exit_code

        except Exception as e:
            print(f"Integration test suite failed: {e}")
            self.collect_logs()
            return 1

        finally:
            # Always stop services
            self.stop_services()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run Aurum integration tests")
    parser.add_argument(
        "--compose-file",
        default="docker-compose.integration.yml",
        help="Docker compose file to use"
    )
    parser.add_argument(
        "--project-name",
        default="aurum-integration-test",
        help="Docker compose project name"
    )
    parser.add_argument(
        "--keep-services",
        action="store_true",
        help="Keep services running after tests"
    )
    parser.add_argument(
        "test_args",
        nargs="*",
        help="Additional arguments to pass to pytest"
    )

    args = parser.parse_args()

    runner = IntegrationTestRunner(
        compose_file=args.compose_file,
        project_name=args.project_name
    )

    try:
        exit_code = runner.run_full_test_suite(args.test_args)

        if not args.keep_services:
            runner.stop_services()

        sys.exit(exit_code)

    except KeyboardInterrupt:
        print("\nTest suite interrupted by user")
        runner.collect_logs()
        runner.stop_services()
        sys.exit(130)

    except Exception as e:
        print(f"Test suite failed: {e}")
        runner.collect_logs()
        runner.stop_services()
        sys.exit(1)


if __name__ == "__main__":
    main()
