from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Iterable

import requests

DEFAULT_BASE_URL = "http://localhost:8095"
TENANT_HEADER = "X-Aurum-Tenant"


class ScenarioCLIError(RuntimeError):
    """Raised when the scenario CLI encounters an error."""


def _build_session(token: str | None) -> requests.Session:
    session = requests.Session()
    if token:
        session.headers.update({"Authorization": f"Bearer {token}"})
    session.headers.update({"User-Agent": "aurum-scenario-cli/0.1"})
    return session


def _request(
    session: requests.Session,
    *,
    base_url: str,
    method: str,
    path: str,
    tenant: str | None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    headers: dict[str, str] = {}
    if tenant:
        headers[TENANT_HEADER] = tenant
    resp = session.request(method, url, headers=headers, params=params, json=json_body, timeout=30)
    if resp.status_code >= 400:
        message = resp.text or resp.reason
        raise ScenarioCLIError(f"{method} {url} failed: {resp.status_code} {message}")
    if resp.content:
        return resp.json()
    return {}


def _parse_assumptions(raw_items: Iterable[str]) -> list[dict[str, Any]]:
    assumptions: list[dict[str, Any]] = []
    for raw in raw_items:
        if raw.startswith("@"):
            path = raw[1:]
            data = os.path.expanduser(path)
            with open(data, "r", encoding="utf-8") as fh:
                raw = fh.read()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:  # pragma: no cover - user input path
            raise ScenarioCLIError(f"Failed to parse assumption JSON: {raw}") from exc
        assumptions.append(payload)
    return assumptions


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        formatted = f"{value:.4f}"
        formatted = formatted.rstrip("0").rstrip(".")
        return formatted or "0"
    return str(value)


def _print_table(rows: Iterable[dict[str, Any]], columns: list[tuple[str, str]]) -> None:
    rows = list(rows)
    if not rows:
        print("No rows.")
        return
    headers = [header for _, header in columns]
    string_rows = []
    for row in rows:
        string_rows.append([_stringify(row.get(key)) for key, _ in columns])
    widths = [len(header) for header in headers]
    for string_row in string_rows:
        for idx, cell in enumerate(string_row):
            widths[idx] = max(widths[idx], len(cell))
    header_line = " | ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers))
    separator = "-+-".join("-" * width for width in widths)
    print(header_line)
    print(separator)
    for string_row in string_rows:
        print(" | ".join(string_row[idx].ljust(widths[idx]) for idx in range(len(headers))))


def _print_meta(meta: dict[str, Any] | None) -> None:
    if not meta:
        return
    hints: list[str] = []
    for key in ("next_cursor", "prev_cursor"):
        value = meta.get(key)
        if value:
            hints.append(f"{key}={value}")
    if hints:
        print("\nMeta: " + ", ".join(hints))


def _cmd_list(args: argparse.Namespace, session: requests.Session) -> None:
    payload = _request(
        session,
        base_url=args.base_url,
        method="GET",
        path="/v1/scenarios",
        tenant=args.tenant,
        params={"limit": args.limit},
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    rows = payload.get("data", []) or []
    _print_table(rows, [
        ("scenario_id", "SCENARIO"),
        ("name", "NAME"),
        ("status", "STATUS"),
    ])
    _print_meta(payload.get("meta"))


def _cmd_create(args: argparse.Namespace, session: requests.Session) -> None:
    if not args.tenant:
        raise ScenarioCLIError("--tenant (or AURUM_TENANT_ID) is required for scenario creation")
    assumptions = _parse_assumptions(args.assumption or [])
    body = {
        "tenant_id": args.tenant,
        "name": args.name,
        "description": args.description,
        "assumptions": assumptions,
    }
    payload = _request(
        session,
        base_url=args.base_url,
        method="POST",
        path="/v1/scenarios",
        tenant=args.tenant,
        json_body=body,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


def _cmd_run(args: argparse.Namespace, session: requests.Session) -> None:
    params: dict[str, Any] = {}
    if args.code_version or args.seed is not None:
        params["code_version"] = args.code_version
        if args.seed is not None:
            params["seed"] = args.seed
    payload = _request(
        session,
        base_url=args.base_url,
        method="POST",
        path=f"/v1/scenarios/{args.scenario_id}/run",
        tenant=args.tenant,
        json_body=params or None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


def _cmd_outputs(args: argparse.Namespace, session: requests.Session) -> None:
    params: dict[str, Any] = {"limit": args.limit}
    if args.metric:
        params["metric"] = args.metric
    if args.tenor_type:
        params["tenor_type"] = args.tenor_type
    if args.curve_key:
        params["curve_key"] = args.curve_key
    if args.cursor:
        params["cursor"] = args.cursor
    if args.since_cursor:
        params["since_cursor"] = args.since_cursor
    if args.prev_cursor:
        params["prev_cursor"] = args.prev_cursor
    payload = _request(
        session,
        base_url=args.base_url,
        method="GET",
        path=f"/v1/scenarios/{args.scenario_id}/outputs",
        tenant=args.tenant,
        params=params,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    rows = payload.get("data", []) or []
    _print_table(
        rows,
        [
            ("run_id", "RUN"),
            ("curve_key", "CURVE"),
            ("tenor_label", "TENOR"),
            ("metric", "METRIC"),
            ("value", "VALUE"),
            ("asof_date", "ASOF"),
        ],
    )
    _print_meta(payload.get("meta"))


def _cmd_runs(args: argparse.Namespace, session: requests.Session) -> None:
    params: dict[str, Any] = {"limit": args.limit}
    if args.state:
        params["state"] = args.state
    if args.cursor:
        params["cursor"] = args.cursor
    if args.since_cursor:
        params["since_cursor"] = args.since_cursor
    if args.prev_cursor:
        params["prev_cursor"] = args.prev_cursor
    payload = _request(
        session,
        base_url=args.base_url,
        method="GET",
        path=f"/v1/scenarios/{args.scenario_id}/runs",
        tenant=args.tenant,
        params=params,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    rows = payload.get("data", []) or []
    _print_table(
        rows,
        [
            ("run_id", "RUN"),
            ("state", "STATE"),
            ("code_version", "CODE"),
            ("seed", "SEED"),
        ],
    )
    _print_meta(payload.get("meta"))


def _cmd_cancel(args: argparse.Namespace, session: requests.Session) -> None:
    payload = _request(
        session,
        base_url=args.base_url,
        method="POST",
        path=f"/v1/scenarios/runs/{args.run_id}/cancel",
        tenant=args.tenant,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    data = payload.get("data")
    if data:
        _print_table([data], [("run_id", "RUN"), ("state", "STATE")])
    _print_meta(payload.get("meta"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scenario lifecycle helper for the Aurum API")
    parser.add_argument(
        "--base-url",
        default=os.getenv("AURUM_API_BASE_URL", DEFAULT_BASE_URL),
        help=f"API base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--tenant",
        default=os.getenv("AURUM_TENANT_ID"),
        help="Tenant identifier to send in X-Aurum-Tenant header",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("AURUM_API_TOKEN"),
        help="Bearer token for authorization",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List scenarios for the tenant")
    list_parser.add_argument("--limit", type=int, default=20, help="Maximum scenarios to return")
    list_parser.add_argument("--json", action="store_true", help="Print raw JSON instead of a table")
    list_parser.set_defaults(handler=_cmd_list)

    create_parser = subparsers.add_parser("create", help="Create a new scenario definition")
    create_parser.add_argument("name", help="Scenario name")
    create_parser.add_argument("--description", help="Optional description")
    create_parser.add_argument(
        "--assumption",
        action="append",
        help="Assumption JSON payload or @path to file (repeatable)",
    )
    create_parser.set_defaults(handler=_cmd_create)

    run_parser = subparsers.add_parser("run", help="Trigger a scenario run")
    run_parser.add_argument("scenario_id", help="Scenario identifier to run")
    run_parser.add_argument("--code-version", help="Optional code version tag")
    run_parser.add_argument("--seed", type=int, help="Optional random seed")
    run_parser.set_defaults(handler=_cmd_run)

    runs_parser = subparsers.add_parser("runs", help="List runs for a scenario")
    runs_parser.add_argument("scenario_id", help="Scenario identifier")
    runs_parser.add_argument("--state", help="Optional run state filter")
    runs_parser.add_argument("--limit", type=int, default=20, help="Maximum runs to return")
    runs_parser.add_argument("--cursor", help="Forward pagination cursor")
    runs_parser.add_argument("--since-cursor", dest="since_cursor", help="Alias for --cursor")
    runs_parser.add_argument("--prev-cursor", dest="prev_cursor", help="Cursor to fetch the previous page")
    runs_parser.add_argument("--json", action="store_true", help="Print raw JSON instead of a table")
    runs_parser.set_defaults(handler=_cmd_runs)

    cancel_parser = subparsers.add_parser("cancel", help="Cancel a scenario run")
    cancel_parser.add_argument("run_id", help="Run identifier to cancel")
    cancel_parser.add_argument("--json", action="store_true", help="Print raw JSON instead of a table")
    cancel_parser.set_defaults(handler=_cmd_cancel)

    outputs_parser = subparsers.add_parser("outputs", help="Fetch scenario outputs")
    outputs_parser.add_argument("scenario_id", help="Scenario identifier")
    outputs_parser.add_argument("--limit", type=int, default=20, help="Maximum rows to return")
    outputs_parser.add_argument("--metric", help="Filter by metric name (e.g., mid)")
    outputs_parser.add_argument("--tenor-type", dest="tenor_type", help="Filter by tenor type")
    outputs_parser.add_argument("--curve-key", dest="curve_key", help="Filter by curve key")
    outputs_parser.add_argument("--cursor", help="Forward pagination cursor")
    outputs_parser.add_argument("--since-cursor", dest="since_cursor", help="Alias for --cursor")
    outputs_parser.add_argument("--prev-cursor", dest="prev_cursor", help="Cursor to fetch the previous page")
    outputs_parser.add_argument("--json", action="store_true", help="Print raw JSON instead of a table")
    outputs_parser.set_defaults(handler=_cmd_outputs)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    session = _build_session(args.token)
    try:
        handler = getattr(args, "handler")
    except AttributeError:  # pragma: no cover - defensive
        parser.print_help()
        return 1

    try:
        handler(args, session)
    except ScenarioCLIError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:  # pragma: no cover - network failure
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
