from __future__ import annotations

"""Feature flag management CLI for interacting with the Aurum API.

This tool offers convenience commands to create, list, update, and evaluate feature flags.
It communicates with the HTTP API and is suitable for ops workflows and quick diagnostics.
"""

import argparse
import json
import os
import sys
from typing import Any, Iterable

import requests

DEFAULT_BASE_URL = os.getenv("AURUM_API_BASE_URL", "http://localhost:8095")


class FeatureCLIError(RuntimeError):
    """Raised when the feature CLI encounters an error."""


def _build_session(token: str | None) -> requests.Session:
    session = requests.Session()
    if token:
        session.headers.update({"Authorization": f"Bearer {token}"})
    session.headers.update({"User-Agent": "aurum-feature-cli/0.1"})
    return session


def _request(
    session: requests.Session,
    *,
    base_url: str,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    resp = session.request(method, url, params=params, json=json_body, timeout=30)
    if resp.status_code >= 400:
        message = resp.text or resp.reason
        raise FeatureCLIError(f"HTTP {resp.status_code}: {message}")
    return resp.json()


def list_features(
    base_url: str = DEFAULT_BASE_URL,
    token: str | None = None,
    status: str | None = None,
    tag: str | None = None,
    page: int = 1,
    limit: int = 50,
) -> None:
    """List feature flags."""
    session = _build_session(token)
    params = {"page": page, "limit": limit}
    if status:
        params["status"] = status
    if tag:
        params["tag"] = tag

    data = _request(session, base_url=base_url, method="GET", path="/v1/admin/features", params=params)
    print(json.dumps(data, indent=2))


def create_feature(
    base_url: str = DEFAULT_BASE_URL,
    token: str | None = None,
    name: str | None = None,
    key: str | None = None,
    description: str = "",
    default_value: bool = False,
    status: str = "disabled",
    tags: list[str] | None = None,
) -> None:
    """Create a new feature flag."""
    if not name or not key:
        print("Error: Both --name and --key are required", file=sys.stderr)
        raise FeatureCLIError("missing_name_or_key")

    session = _build_session(token)
    json_body = {
        "name": name,
        "key": key,
        "description": description,
        "default_value": default_value,
        "status": status,
        "tags": tags or []
    }

    data = _request(session, base_url=base_url, method="POST", path="/v1/admin/features", json_body=json_body)
    print(json.dumps(data, indent=2))


def get_feature(
    base_url: str = DEFAULT_BASE_URL,
    token: str | None = None,
    key: str | None = None,
) -> None:
    """Get details for a specific feature flag."""
    if not key:
        print("Error: --key is required", file=sys.stderr)
        raise FeatureCLIError("missing_key")

    session = _build_session(token)
    data = _request(session, base_url=base_url, method="GET", path=f"/v1/admin/features/{key}")
    print(json.dumps(data, indent=2))


def update_feature_status(
    base_url: str = DEFAULT_BASE_URL,
    token: str | None = None,
    key: str | None = None,
    status: str | None = None,
) -> None:
    """Update the status of a feature flag."""
    if not key or not status:
        print("Error: Both --key and --status are required", file=sys.stderr)
        raise FeatureCLIError("missing_key_or_status")

    session = _build_session(token)
    json_body = {"status": status}

    data = _request(session, base_url=base_url, method="PUT", path=f"/v1/admin/features/{key}/status", json_body=json_body)
    print(json.dumps(data, indent=2))


def evaluate_features(
    base_url: str = DEFAULT_BASE_URL,
    token: str | None = None,
    user_id: str | None = None,
    user_segment: str = "all_users",
    tenant_id: str | None = None,
) -> None:
    """Evaluate all feature flags for a user context."""
    if not user_id:
        print("Error: --user-id is required", file=sys.stderr)
        raise FeatureCLIError("missing_user_id")

    session = _build_session(token)
    params = {"user_id": user_id, "user_segment": user_segment}
    if tenant_id:
        params["tenant_id"] = tenant_id

    data = _request(session, base_url=base_url, method="GET", path="/v1/admin/features/evaluate", params=params)
    print(json.dumps(data, indent=2))


def get_feature_stats(
    base_url: str = DEFAULT_BASE_URL,
    token: str | None = None,
) -> None:
    """Get feature flag system statistics."""
    session = _build_session(token)

    data = _request(session, base_url=base_url, method="GET", path="/v1/admin/features/stats")
    print(json.dumps(data, indent=2))


def main(argv: list[str] | None = None) -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="aurum-feature",
        description="Feature flag management CLI for Aurum"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Base URL for the Aurum API (default: {DEFAULT_BASE_URL})"
    )
    parser.add_argument(
        "--token",
        help="Authentication token (if required)"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # List command
    list_parser = subparsers.add_parser("list", help="List feature flags")
    list_parser.add_argument("--status", help="Filter by status")
    list_parser.add_argument("--tag", help="Filter by tag")
    list_parser.add_argument("--page", type=int, default=1, help="Page number")
    list_parser.add_argument("--limit", type=int, default=50, help="Items per page")

    # Create command
    create_parser = subparsers.add_parser("create", help="Create a new feature flag")
    create_parser.add_argument("--name", required=True, help="Feature flag name")
    create_parser.add_argument("--key", required=True, help="Feature flag key")
    create_parser.add_argument("--description", default="", help="Feature description")
    create_parser.add_argument("--default-value", action="store_true", help="Set default value to true")
    create_parser.add_argument("--status", default="disabled", help="Initial status")
    create_parser.add_argument("--tag", action="append", dest="tags", help="Add a tag (can be used multiple times)")

    # Get command
    get_parser = subparsers.add_parser("get", help="Get feature flag details")
    get_parser.add_argument("--key", required=True, help="Feature flag key")

    # Update status command
    update_parser = subparsers.add_parser("update", help="Update feature flag status")
    update_parser.add_argument("--key", required=True, help="Feature flag key")
    update_parser.add_argument("--status", required=True, help="New status")

    # Evaluate command
    eval_parser = subparsers.add_parser("eval", help="Evaluate features for a user")
    eval_parser.add_argument("--user-id", required=True, help="User ID")
    eval_parser.add_argument("--user-segment", default="all_users", help="User segment")
    eval_parser.add_argument("--tenant-id", help="Tenant ID")

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Get feature statistics")

    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    # Map commands to functions
    command_map = {
        "list": list_features,
        "create": create_feature,
        "get": get_feature,
        "update": update_feature_status,
        "eval": evaluate_features,
        "stats": get_feature_stats,
    }

    func = command_map[args.command]
    try:
        func(
            base_url=args.base_url,
            token=args.token,
            **{k.replace("-", "_"): v for k, v in vars(args).items() if k not in ["base_url", "token", "command"]}
        )
    except FeatureCLIError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
