#!/usr/bin/env python3
"""Validate alert routing configuration and rules."""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any


def load_alert_routing_config(config_file: str) -> Dict[str, Any]:
    """Load alert routing configuration."""
    try:
        with open(config_file) as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading alert routing config {config_file}: {e}")
        return {}


def load_great_expectations_results(ge_reports_dir: str) -> List[Dict[str, Any]]:
    """Load Great Expectations validation results."""
    results = []
    ge_path = Path(ge_reports_dir)

    if not ge_path.exists():
        print(f"⚠️ GE reports directory not found: {ge_reports_dir}")
        return results

    for result_file in ge_path.glob("*.json"):
        try:
            with open(result_file) as f:
                result = json.load(f)
                results.append(result)
        except Exception as e:
            print(f"❌ Error loading GE result {result_file}: {e}")

    return results


def load_contract_test_results(contract_reports_dir: str) -> List[Dict[str, Any]]:
    """Load contract test results."""
    results = []
    contract_path = Path(contract_reports_dir)

    if not contract_path.exists():
        print(f"⚠️ Contract reports directory not found: {contract_reports_dir}")
        return results

    for result_file in contract_path.glob("*.json"):
        try:
            with open(result_file) as f:
                result = json.load(f)
                results.append(result)
        except Exception as e:
            print(f"❌ Error loading contract result {result_file}: {e}")

    return results


def validate_routing_rules(routing_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Validate alert routing rules."""
    print("🔍 Validating alert routing rules...")

    issues = []

    # Check required sections
    required_sections = ["routes", "receivers", "inhibit_rules", "notification_policies"]
    for section in required_sections:
        if section not in routing_config:
            issues.append({
                "type": "missing_section",
                "section": section,
                "severity": "high",
                "description": f"Missing required section: {section}"
            })

    if "routes" not in routing_config:
        return issues

    routes = routing_config["routes"]

    # Validate each route
    for i, route in enumerate(routes):
        route_name = route.get("name", f"route_{i}")

        # Check for matchers
        if "matchers" not in route and "match_re" not in route:
            issues.append({
                "type": "missing_matchers",
                "route": route_name,
                "severity": "medium",
                "description": "Route has no matchers or match_re conditions"
            })

        # Check for receiver
        if "receiver" not in route:
            issues.append({
                "type": "missing_receiver",
                "route": route_name,
                "severity": "high",
                "description": "Route has no receiver specified"
            })
        else:
            # Verify receiver exists
            receiver_name = route["receiver"]
            receivers = routing_config.get("receivers", [])
            receiver_exists = any(r.get("name") == receiver_name for r in receivers)

            if not receiver_exists:
                issues.append({
                    "type": "invalid_receiver",
                    "route": route_name,
                    "receiver": receiver_name,
                    "severity": "high",
                    "description": f"Receiver '{receiver_name}' does not exist"
                })

        # Check for group_by
        if "group_by" not in route:
            issues.append({
                "type": "missing_group_by",
                "route": route_name,
                "severity": "low",
                "description": "Route has no group_by configuration (may cause alert spam)"
            })

        # Check for continue
        if "continue" not in route:
            issues.append({
                "type": "missing_continue",
                "route": route_name,
                "severity": "low",
                "description": "Route has no continue flag (alerts may not reach multiple receivers)"
            })

    return issues


def validate_ge_alert_routing(ge_results: List[Dict[str, Any]], routing_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Validate that GE failures route to appropriate receivers."""
    print("🔍 Validating GE alert routing...")

    issues = []

    if not ge_results:
        print("⚠️ No GE results to validate")
        return issues

    # Extract GE validation failures
    ge_failures = []
    for result in ge_results:
        if result.get("success", True) is False:
            ge_failures.append(result)

    if not ge_failures:
        print("✅ No GE failures to route")
        return issues

    print(f"Found {len(ge_failures)} GE validation failures")

    # Check if routing config can handle GE failures
    routes = routing_config.get("routes", [])

    # Look for GE-specific routes
    ge_routes = [r for r in routes if any(
        "great_expectations" in str(m.get("value", "")) or
        "data_quality" in str(m.get("value", "")) or
        "validation_failure" in str(m.get("value", ""))
        for m in r.get("matchers", [])
    )]

    if not ge_routes:
        issues.append({
            "type": "missing_ge_routes",
            "severity": "high",
            "description": f"Found {len(ge_failures)} GE failures but no routing rules for them",
            "details": "Add routes with matchers for Great Expectations failures"
        })
    else:
        print(f"✅ Found {len(ge_routes)} routing rules for GE failures")

    return issues


def validate_contract_alert_routing(contract_results: List[Dict[str, Any]], routing_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Validate that contract test failures route to appropriate receivers."""
    print("🔍 Validating contract alert routing...")

    issues = []

    if not contract_results:
        print("⚠️ No contract results to validate")
        return issues

    # Extract contract test failures
    contract_failures = []
    for result in contract_results:
        if result.get("passed", True) is False:
            contract_failures.append(result)

    if not contract_failures:
        print("✅ No contract test failures to route")
        return issues

    print(f"Found {len(contract_failures)} contract test failures")

    # Check if routing config can handle contract failures
    routes = routing_config.get("routes", [])

    # Look for contract-specific routes
    contract_routes = [r for r in routes if any(
        "contract" in str(m.get("value", "")) or
        "pact" in str(m.get("value", "")) or
        "consumer" in str(m.get("value", ""))
        for m in r.get("matchers", [])
    )]

    if not contract_routes:
        issues.append({
            "type": "missing_contract_routes",
            "severity": "high",
            "description": f"Found {len(contract_failures)} contract failures but no routing rules for them",
            "details": "Add routes with matchers for contract test failures"
        })
    else:
        print(f"✅ Found {len(contract_routes)} routing rules for contract failures")

    return issues


def validate_notification_policies(routing_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Validate notification policies."""
    print("🔍 Validating notification policies...")

    issues = []

    policies = routing_config.get("notification_policies", [])

    if not policies:
        issues.append({
            "type": "missing_notification_policies",
            "severity": "medium",
            "description": "No notification policies defined",
            "details": "Add notification policies to control alert delivery"
        })
        return issues

    for policy in policies:
        policy_name = policy.get("name", "unknown")

        # Check for required fields
        required_fields = ["name", "receivers", "group_by"]
        for field in required_fields:
            if field not in policy:
                issues.append({
                    "type": "missing_policy_field",
                    "policy": policy_name,
                    "field": field,
                    "severity": "medium",
                    "description": f"Notification policy missing required field: {field}"
                })

        # Check receivers exist
        receivers = policy.get("receivers", [])
        routing_receivers = [r.get("name") for r in routing_config.get("receivers", [])]

        for receiver in receivers:
            if receiver not in routing_receivers:
                issues.append({
                    "type": "invalid_policy_receiver",
                    "policy": policy_name,
                    "receiver": receiver,
                    "severity": "medium",
                    "description": f"Policy references non-existent receiver: {receiver}"
                })

    return issues


def generate_routing_recommendations(issues: List[Dict[str, Any]], routing_config: Dict[str, Any]) -> str:
    """Generate recommendations for fixing routing issues."""
    recommendations = []

    # Group issues by type
    issues_by_type = {}
    for issue in issues:
        issue_type = issue.get("type", "unknown")
        if issue_type not in issues_by_type:
            issues_by_type[issue_type] = []
        issues_by_type[issue_type].append(issue)

    # Generate recommendations
    if "missing_ge_routes" in issues_by_type:
        recommendations.append("""
Add GE-specific routing rules:
```yaml
routes:
  - name: "great-expectations-failures"
    matchers:
      - name: "alertname"
        value: ".*Great Expectations.*"
      - name: "severity"
        value: "warning|critical"
    receiver: "data-quality-team"
    group_by: ["alertname", "datasource"]
    continue: true
```""")

    if "missing_contract_routes" in issues_by_type:
        recommendations.append("""
Add contract-specific routing rules:
```yaml
routes:
  - name: "contract-test-failures"
    matchers:
      - name: "alertname"
        value: ".*Contract.*|.*Pact.*"
      - name: "severity"
        value: "warning|critical"
    receiver: "platform-team"
    group_by: ["alertname", "contract"]
    continue: true
```""")

    if "missing_notification_policies" in issues_by_type:
        recommendations.append("""
Add notification policies to control alert delivery:
```yaml
notification_policies:
  - name: "data-quality-critical"
    receivers: ["data-quality-team", "oncall-platform"]
    group_by: ["alertname"]
    repeat_interval: "4h"
    matchers:
      - name: "severity"
        value: "critical"
```""")

    return "\n\n".join(recommendations)


def main():
    """Main function to validate alert routing."""
    import argparse

    parser = argparse.ArgumentParser(description="Validate alert routing configuration")
    parser.add_argument("--ge-reports", required=True, help="Directory containing GE reports")
    parser.add_argument("--contract-reports", required=True, help="Directory containing contract test reports")
    parser.add_argument("--alert-config", required=True, help="Alert routing configuration file")
    parser.add_argument("--validation-report", required=True, help="Output file for validation report")

    args = parser.parse_args()

    print("🔍 Starting alert routing validation...")

    # Load configuration
    routing_config = load_alert_routing_config(args.alert_config)

    if not routing_config:
        print("❌ No alert routing configuration loaded")
        return 1

    print("✅ Loaded alert routing configuration")

    # Load results
    ge_results = load_great_expectations_results(args.ge_reports)
    contract_results = load_contract_test_results(args.contract_reports)

    print(f"Found {len(ge_results)} GE results and {len(contract_results)} contract results")

    # Validate routing rules
    routing_issues = validate_routing_rules(routing_config)

    # Validate GE routing
    ge_routing_issues = validate_ge_alert_routing(ge_results, routing_config)

    # Validate contract routing
    contract_routing_issues = validate_contract_alert_routing(contract_results, routing_config)

    # Validate notification policies
    policy_issues = validate_notification_policies(routing_config)

    # Combine all issues
    all_issues = routing_issues + ge_routing_issues + contract_routing_issues + policy_issues

    # Generate recommendations
    recommendations = generate_routing_recommendations(all_issues, routing_config)

    # Create validation report
    validation_report = {
        "validation_timestamp": json.dumps(json.loads('{"timestamp": "' + str(Path.cwd()) + '"}'), default=str),
        "total_issues": len(all_issues),
        "issues_by_severity": {
            "high": len([i for i in all_issues if i.get("severity") == "high"]),
            "medium": len([i for i in all_issues if i.get("severity") == "medium"]),
            "low": len([i for i in all_issues if i.get("severity") == "low"])
        },
        "issues": all_issues,
        "recommendations": recommendations,
        "validation_passed": len(all_issues) == 0
    }

    # Write report
    with open(args.validation_report, "w") as f:
        json.dump(validation_report, f, indent=2)

    # Summary
    if all_issues:
        print("❌ Alert routing validation found issues:")
        for issue in all_issues:
            severity = issue.get("severity", "unknown").upper()
            print(f"  [{severity}] {issue.get('description', 'Unknown issue')}")

        if recommendations:
            print("\n📋 Recommendations:")
            print(recommendations)

        return 1
    else:
        print("✅ Alert routing validation passed")
        return 0


if __name__ == "__main__":
    sys.exit(main())
