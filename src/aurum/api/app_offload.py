from __future__ import annotations

"""Helpers for building offload predicates from settings.

Extracted from the API app factory to make composition cleaner and testable.
"""

import fnmatch
import logging
import re
from typing import Any, Dict, Optional

from aurum.core import AurumSettings
from aurum.api.rate_limiting.concurrency_middleware import OffloadInstruction

LOGGER = logging.getLogger(__name__)


def build_offload_predicate(settings: AurumSettings):
    """Build a predicate function describing which requests should be offloaded.

    Returns None when offloading is not enabled.
    """

    offload_cfg = getattr(settings, "async_offload", None)
    api_cfg = getattr(settings, "api", None)
    offload_routes = getattr(api_cfg, "offload_routes", None) if api_cfg else None
    if offload_cfg is None or not getattr(offload_cfg, "enabled", False):
        return None

    if not offload_routes:
        return None

    normalized = []
    for entry in offload_routes:
        if not isinstance(entry, dict):
            LOGGER.warning("invalid_offload_route_entry", extra={"entry": entry})
            continue
        path = str(entry.get("path", "/"))
        normalized_path = path.rstrip("/") or "/"
        job_name = str(entry.get("job", "")).strip()
        if not job_name:
            LOGGER.warning("invalid_offload_route_job_missing", extra={"entry": entry})
            continue
        methods = entry.get("methods")
        if methods is not None and not isinstance(methods, (list, tuple)):
            LOGGER.warning("invalid_offload_route_methods", extra={"entry": entry})
            methods = None
        if isinstance(methods, (list, tuple)):
            try:
                methods = tuple(
                    str(item).upper()
                    for item in methods
                    if str(item).strip()
                )
            except Exception:
                LOGGER.warning("invalid_offload_route_methods", extra={"entry": entry})
                continue
        if not methods:
            methods = ("POST",)

        match_type = str(entry.get("match", "exact") or "exact").lower()
        pattern = normalized_path
        compiled_regex = None
        if match_type == "regex":
            try:
                compiled_regex = re.compile(pattern)
            except re.error as exc:
                LOGGER.warning(
                    "invalid_offload_route_regex",
                    extra={"pattern": pattern, "error": str(exc)},
                )
                continue
        elif match_type not in {"exact", "prefix", "glob"}:
            LOGGER.warning(
                "invalid_offload_route_match",
                extra={"match": match_type, "entry": entry},
            )
            match_type = "exact"

        response_headers = entry.get("response_headers")
        if response_headers is not None and not isinstance(response_headers, dict):
            LOGGER.warning("invalid_offload_response_headers", extra={"entry": entry})
            response_headers = None

        normalized.append(
            {
                "pattern": pattern,
                "match": match_type,
                "compiled": compiled_regex,
                "methods": tuple(methods),
                "job": job_name,
                "queue": entry.get("queue"),
                "status_url": entry.get("status_url", "/v1/admin/offload/{task_id}"),
                "response_headers": response_headers,
            }
        )

    if not normalized:
        return None

    def _matches(route: Dict[str, Any], request_path: str) -> bool:
        match_type = route["match"]
        pattern = route["pattern"]
        if match_type == "exact":
            return request_path == pattern
        if match_type == "prefix":
            return request_path.startswith(pattern)
        if match_type == "glob":
            return fnmatch.fnmatch(request_path, pattern)
        if match_type == "regex" and route["compiled"] is not None:
            return bool(route["compiled"].search(request_path))
        return False

    def predicate(request_info: Dict[str, Any]) -> Optional[OffloadInstruction]:
        request_path = str(request_info.get("path", "")).rstrip("/") or "/"
        request_method = str(request_info.get("method", "")).upper()
        for route in normalized:
            allowed_methods = route["methods"]
            if allowed_methods and "*" not in allowed_methods and request_method not in allowed_methods:
                continue
            if not _matches(route, request_path):
                continue

            payload = {
                "path": request_path,
                "method": request_method,
                "headers": request_info.get("headers", {}),
                "query_string": request_info.get("query_string"),
                "client": request_info.get("client"),
            }

            return OffloadInstruction(
                job_name=route["job"],
                payload=payload,
                queue=route.get("queue"),
                status_url=route.get("status_url"),
                response_headers=route.get("response_headers"),
            )
        return None

    return predicate


__all__ = ["build_offload_predicate"]

