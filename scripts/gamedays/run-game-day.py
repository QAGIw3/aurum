#!/usr/bin/env python3
"""Game day simulation script for testing system resilience."""

import json
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any


def run_command(command: str, capture_output: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    try:
        return subprocess.run(
            command,
            shell=True,
            capture_output=capture_output,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {command}")
        print(f"Error: {e.stderr}")
        return e


def log_event(event_type: str, message: str, details: Dict[str, Any] = None):
    """Log a game day event."""
    timestamp = datetime.utcnow().isoformat()
    event = {
        "timestamp": timestamp,
        "event_type": event_type,
        "message": message,
        "details": details or {}
    }

    print(f"[{timestamp}] {event_type}: {message}")
    if details:
        print(f"  Details: {json.dumps(details, indent=2)}")

    return event


def simulate_chaos_experiment(experiment_name: str, description: str) -> Dict[str, Any]:
    """Simulate a chaos engineering experiment."""
    log_event("CHAOS_START", f"Starting chaos experiment: {experiment_name}", {"description": description})

    # Simulate different types of chaos experiments
    experiments = {
        "pod_kill": {
            "command": "kubectl delete pod -l app.kubernetes.io/name=aurum-api -n aurum-dev --field-selector=status.phase=Running --ignore-not-found=true",
            "description": "Randomly kill API pods to test resilience"
        },
        "cpu_stress": {
            "command": "kubectl exec -n aurum-dev deployment/aurum-api -- sh -c 'dd if=/dev/zero of=/tmp/stress bs=1M count=100'",
            "description": "Induce CPU stress on API pods"
        },
        "network_latency": {
            "command": "kubectl exec -n aurum-dev deployment/aurum-api -- tc qdisc add dev eth0 root netem delay 100ms",
            "description": "Add network latency to API pods"
        },
        "memory_pressure": {
            "command": "kubectl exec -n aurum-dev deployment/aurum-api -- stress --vm 1 --vm-bytes 256M --timeout 30s",
            "description": "Induce memory pressure on API pods"
        }
    }

    if experiment_name not in experiments:
        log_event("ERROR", f"Unknown experiment: {experiment_name}")
        return {"status": "failed", "error": "Unknown experiment"}

    experiment = experiments[experiment_name]

    try:
        # Execute chaos experiment
        result = run_command(experiment["command"])
        log_event("CHAOS_EXECUTED", f"Chaos experiment executed: {experiment_name}")

        # Wait for system to stabilize
        time.sleep(10)

        # Check system health after chaos
        health_check = check_system_health()

        return {
            "status": "completed",
            "experiment": experiment_name,
            "description": experiment["description"],
            "health_after_chaos": health_check
        }

    except Exception as e:
        log_event("CHAOS_FAILED", f"Chaos experiment failed: {experiment_name}", {"error": str(e)})
        return {"status": "failed", "error": str(e)}


def check_system_health() -> Dict[str, Any]:
    """Check overall system health."""
    health = {"timestamp": datetime.utcnow().isoformat()}

    # Check API pods
    try:
        result = run_command("kubectl get pods -l app.kubernetes.io/name=aurum-api -n aurum-dev --no-headers | wc -l")
        api_pods = int(result.stdout.strip())
        health["api_pods"] = api_pods
        health["api_healthy"] = api_pods > 0
    except Exception as e:
        health["api_healthy"] = False
        health["api_error"] = str(e)

    # Check worker pods
    try:
        result = run_command("kubectl get pods -l app.kubernetes.io/name=aurum-worker -n aurum-dev --no-headers | wc -l")
        worker_pods = int(result.stdout.strip())
        health["worker_pods"] = worker_pods
        health["worker_healthy"] = worker_pods > 0
    except Exception as e:
        health["worker_healthy"] = False
        health["worker_error"] = str(e)

    # Check database connectivity
    try:
        result = run_command("kubectl exec -n aurum-dev postgres-primary -- psql -t -c 'SELECT 1'")
        health["database_healthy"] = True
        health["database_response"] = result.stdout.strip()
    except Exception as e:
        health["database_healthy"] = False
        health["database_error"] = str(e)

    # Overall health assessment
    healthy_components = sum([
        health.get("api_healthy", False),
        health.get("worker_healthy", False),
        health.get("database_healthy", False)
    ])

    health["overall_status"] = "healthy" if healthy_components >= 2 else "degraded"
    health["resilience_score"] = (healthy_components / 3) * 100

    return health


def simulate_load_test(duration_seconds: int = 60) -> Dict[str, Any]:
    """Simulate a load test scenario."""
    log_event("LOAD_TEST_START", f"Starting load test for {duration_seconds} seconds")

    # Use hey (HTTP load generator) or curl to generate load
    load_test_results = {}

    try:
        # Generate load using curl (basic implementation)
        start_time = time.time()

        # Simulate concurrent requests
        concurrent_requests = 10
        request_count = 0

        while time.time() - start_time < duration_seconds:
            # Send multiple concurrent requests
            processes = []
            for i in range(concurrent_requests):
                try:
                    proc = subprocess.Popen(
                        ["curl", "-s", "-f", "-w", "%{http_code},%{time_total}\n", "https://api.aurum.com/health"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    processes.append(proc)
                    request_count += 1
                except Exception as e:
                    log_event("LOAD_ERROR", f"Failed to start request {i}", {"error": str(e)})

            # Wait for all requests to complete
            for proc in processes:
                try:
                    stdout, stderr = proc.communicate(timeout=10)
                    if proc.returncode == 0:
                        output = stdout.decode().strip()
                        if "," in output:
                            http_code, response_time = output.split(",")
                            log_event("REQUEST_SUCCESS", f"Request completed: {http_code} in {response_time}s")
                    else:
                        log_event("REQUEST_FAILED", f"Request failed: {proc.returncode}")
                except subprocess.TimeoutExpired:
                    proc.kill()
                    log_event("REQUEST_TIMEOUT", "Request timed out")

        end_time = time.time()
        actual_duration = end_time - start_time

        load_test_results = {
            "status": "completed",
            "duration_seconds": actual_duration,
            "requests_sent": request_count,
            "concurrent_users": concurrent_requests,
            "requests_per_second": request_count / actual_duration if actual_duration > 0 else 0
        }

        log_event("LOAD_TEST_END", f"Load test completed: {request_count} requests in {actual_duration".1f"}s")

    except Exception as e:
        load_test_results = {
            "status": "failed",
            "error": str(e)
        }
        log_event("LOAD_TEST_ERROR", f"Load test failed", {"error": str(e)})

    return load_test_results


def simulate_failure_scenario(scenario_name: str) -> Dict[str, Any]:
    """Simulate various failure scenarios."""
    log_event("FAILURE_START", f"Starting failure scenario: {scenario_name}")

    scenarios = {
        "database_outage": {
            "actions": [
                "kubectl scale deployment postgres-primary --replicas=0 -n aurum-dev",
                "sleep 30",  # Wait for failover
                "kubectl scale deployment postgres-secondary --replicas=1 -n aurum-dev"
            ],
            "validation": "kubectl get pods -l app.kubernetes.io/name=postgres-secondary -n aurum-dev"
        },
        "kafka_outage": {
            "actions": [
                "kubectl scale deployment kafka-primary --replicas=0 -n aurum-dev",
                "sleep 30",
                "kubectl scale deployment kafka-secondary --replicas=1 -n aurum-dev"
            ],
            "validation": "kubectl get pods -l app.kubernetes.io/name=kafka-secondary -n aurum-dev"
        },
        "cache_failure": {
            "actions": [
                "kubectl exec -n aurum-dev redis-primary -- redis-cli SHUTDOWN",
                "sleep 10",
                "kubectl rollout restart deployment redis-secondary -n aurum-dev"
            ],
            "validation": "kubectl get pods -l app.kubernetes.io/name=redis-secondary -n aurum-dev"
        }
    }

    if scenario_name not in scenarios:
        log_event("ERROR", f"Unknown failure scenario: {scenario_name}")
        return {"status": "failed", "error": "Unknown scenario"}

    scenario = scenarios[scenario_name]

    try:
        # Execute failure scenario
        for action in scenario["actions"]:
            if action == "sleep":
                continue
            elif action.startswith("sleep "):
                sleep_time = int(action.split(" ")[1])
                time.sleep(sleep_time)
            else:
                result = run_command(action)
                log_event("FAILURE_ACTION", f"Executed: {action}")

        # Validate recovery
        result = run_command(scenario["validation"])
        log_event("FAILURE_VALIDATION", f"Validation completed for: {scenario_name}")

        return {
            "status": "completed",
            "scenario": scenario_name,
            "validation_result": result.stdout.strip()
        }

    except Exception as e:
        log_event("FAILURE_ERROR", f"Failure scenario failed: {scenario_name}", {"error": str(e)})
        return {"status": "failed", "error": str(e)}


def run_game_day_simulation(game_day_config: Dict[str, Any]) -> Dict[str, Any]:
    """Run a comprehensive game day simulation."""
    log_event("GAME_DAY_START", "Starting game day simulation", {"config": game_day_config})

    game_day_results = {
        "start_time": datetime.utcnow().isoformat(),
        "experiments": [],
        "load_tests": [],
        "failure_scenarios": [],
        "overall_status": "unknown"
    }

    # Run chaos experiments
    chaos_experiments = game_day_config.get("chaos_experiments", ["pod_kill", "cpu_stress"])
    for experiment in chaos_experiments:
        result = simulate_chaos_experiment(experiment, f"Game day chaos: {experiment}")
        game_day_results["experiments"].append(result)

        # Check system health after each experiment
        health = check_system_health()
        log_event("HEALTH_CHECK", f"Health after {experiment}", health)

        if health["overall_status"] == "degraded":
            log_event("WARNING", "System degraded after chaos experiment", {"experiment": experiment})

    # Run load tests
    load_tests = game_day_config.get("load_tests", [{"duration": 60, "concurrent_users": 10}])
    for load_test in load_tests:
        result = simulate_load_test(load_test.get("duration", 60))
        game_day_results["load_tests"].append(result)

    # Run failure scenarios
    failure_scenarios = game_day_config.get("failure_scenarios", ["database_outage"])
    for scenario in failure_scenarios:
        result = simulate_failure_scenario(scenario)
        game_day_results["failure_scenarios"].append(result)

        # Final health check
        final_health = check_system_health()
        log_event("FINAL_HEALTH_CHECK", "Final system health assessment", final_health)

    # Determine overall status
    failed_components = sum([
        1 for exp in game_day_results["experiments"] if exp.get("status") == "failed",
        1 for test in game_day_results["load_tests"] if test.get("status") == "failed",
        1 for scenario in game_day_results["failure_scenarios"] if scenario.get("status") == "failed"
    ])

    if failed_components == 0:
        game_day_results["overall_status"] = "PASSED"
        log_event("GAME_DAY_SUCCESS", "Game day simulation completed successfully")
    elif failed_components <= len(game_day_results["experiments"]) // 2:
        game_day_results["overall_status"] = "PARTIAL"
        log_event("GAME_DAY_PARTIAL", "Game day simulation had some issues", {"failed_components": failed_components})
    else:
        game_day_results["overall_status"] = "FAILED"
        log_event("GAME_DAY_FAILED", "Game day simulation failed", {"failed_components": failed_components})

    game_day_results["end_time"] = datetime.utcnow().isoformat()

    return game_day_results


def generate_game_day_report(game_day_results: Dict[str, Any], report_file: str):
    """Generate a comprehensive game day report."""

    report = f"""
# Game Day Simulation Report
## Generated: {datetime.utcnow().isoformat()}

## Executive Summary
- **Overall Status**: {game_day_results.get('overall_status', 'unknown')}
- **Start Time**: {game_day_results.get('start_time')}
- **End Time**: {game_day_results.get('end_time')}
- **Duration**: {(datetime.fromisoformat(game_day_results['end_time']) - datetime.fromisoformat(game_day_results['start_time'])).total_seconds() / 60".1f"} minutes

## Chaos Experiments
"""

    for i, experiment in enumerate(game_day_results.get('experiments', []), 1):
        report += f"### Experiment {i}: {experiment.get('experiment', 'Unknown')}\n"
        report += f"- **Status**: {experiment.get('status', 'unknown')}\n"
        report += f"- **Description**: {experiment.get('description', 'N/A')}\n"

        health = experiment.get('health_after_chaos', {})
        if health:
            report += f"- **Resilience Score**: {health.get('resilience_score', 0)}%\n"
            report += f"- **System Health**: {health.get('overall_status', 'unknown')}\n"

        report += "\n"

    report += "## Load Tests\n"
    for i, load_test in enumerate(game_day_results.get('load_tests', []), 1):
        report += f"### Load Test {i}\n"
        report += f"- **Status**: {load_test.get('status', 'unknown')}\n"
        report += f"- **Requests Sent**: {load_test.get('requests_sent', 0)}\n"
        report += f"- **Duration**: {load_test.get('duration_seconds', 0)}".1f" seconds\n"
        report += f"- **Requests/Second**: {load_test.get('requests_per_second', 0)".1f"}\n\n"

    report += "## Failure Scenarios\n"
    for i, scenario in enumerate(game_day_results.get('failure_scenarios', []), 1):
        report += f"### Failure Scenario {i}: {scenario.get('scenario', 'Unknown')}\n"
        report += f"- **Status**: {scenario.get('status', 'unknown')}\n"
        report += f"- **Validation**: {scenario.get('validation_result', 'N/A')}\n\n"

    report += "## Recommendations\n"
    if game_day_results.get('overall_status') == 'PASSED':
        report += "- ✅ System demonstrated excellent resilience\n"
        report += "- Continue regular chaos engineering practices\n"
        report += "- Consider increasing failure injection intensity\n"
    elif game_day_results.get('overall_status') == 'PARTIAL':
        report += "- ⚠️ System showed partial resilience with some issues\n"
        report += "- Address identified failure points before next game day\n"
        report += "- Review monitoring and alerting thresholds\n"
    else:
        report += "- ❌ System failed under stress - immediate action required\n"
        report += "- Prioritize fixing critical failure points\n"
        report += "- Schedule follow-up game day after fixes\n"

    report += "- Schedule next game day in 30 days\n"
    report += "- Update runbooks based on findings\n"
    report += "- Review incident response procedures\n"

    with open(report_file, 'w') as f:
        f.write(report)

    print(f"📊 Game day report generated: {report_file}")


def main():
    """Main function to run game day simulation."""
    import argparse

    parser = argparse.ArgumentParser(description="Run game day resilience simulation")
    parser.add_argument("--config", help="Game day configuration file")
    parser.add_argument("--scenario", choices=['basic', 'comprehensive', 'extreme'],
                       default='basic', help="Game day scenario intensity")
    parser.add_argument("--dry-run", action='store_true', help="Show what would be done without executing")
    parser.add_argument("--report-dir", default='/tmp', help="Directory to store game day reports")

    args = parser.parse_args()

    print("🎮 Starting game day simulation...")

    if args.dry_run:
        print("🔍 DRY RUN MODE - No chaos will be injected")
        return 0

    # Define game day scenarios
    scenarios = {
        "basic": {
            "chaos_experiments": ["pod_kill"],
            "load_tests": [{"duration": 30, "concurrent_users": 5}],
            "failure_scenarios": ["database_outage"]
        },
        "comprehensive": {
            "chaos_experiments": ["pod_kill", "cpu_stress", "memory_pressure"],
            "load_tests": [{"duration": 60, "concurrent_users": 10}],
            "failure_scenarios": ["database_outage", "kafka_outage"]
        },
        "extreme": {
            "chaos_experiments": ["pod_kill", "cpu_stress", "memory_pressure", "network_latency"],
            "load_tests": [{"duration": 120, "concurrent_users": 20}],
            "failure_scenarios": ["database_outage", "kafka_outage", "cache_failure"]
        }
    }

    if args.scenario not in scenarios:
        print(f"❌ Unknown scenario: {args.scenario}")
        return 1

    game_day_config = scenarios[args.scenario]

    print(f"🎯 Running {args.scenario} game day scenario...")

    # Run game day simulation
    game_day_results = run_game_day_simulation(game_day_config)

    # Generate report
    report_dir = Path(args.report_dir)
    report_dir.mkdir(exist_ok=True)

    report_file = report_dir / f"game-day-report-{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md"

    generate_game_day_report(game_day_results, str(report_file))

    # Exit with appropriate code
    if game_day_results.get('overall_status') == 'PASSED':
        print("✅ Game day simulation completed successfully")
        return 0
    else:
        print("❌ Game day simulation revealed issues that need attention")
        return 1


if __name__ == "__main__":
    sys.exit(main())
