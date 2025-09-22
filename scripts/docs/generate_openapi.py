#!/usr/bin/env python3
"""
Generate OpenAPI schema from the FastAPI routes without running the server.

Outputs:
- docs/api/aurum.yaml (for Redoc index)
- docs/api/openapi-spec.yaml (compat)
- docs/api/openapi-spec.json (optional JSON)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def main() -> int:
    # Repo root is the top-level 'aurum' folder that contains src/
    repo_root = Path(__file__).resolve().parents[2]
    src_dir = repo_root / "src"
    docs_api_dir = repo_root / "docs" / "api"
    docs_api_dir.mkdir(parents=True, exist_ok=True)

    # Ensure we can import `aurum` from src/
    sys.path.insert(0, str(src_dir))

    # Avoid heavy imports from aurum.api.__init__
    os.environ.setdefault("AURUM_API_LIGHT_INIT", "1")

    # Stub missing optional modules to keep import light
    import types
    sys.modules.setdefault(
        'aurum.observability.slo_dashboard',
        types.SimpleNamespace(
            get_slo_dashboard_config=lambda: {},
            check_slo_status=lambda: {},
            get_sli_values=lambda: {},
        ),
    )

    # Stub optional third-party modules used by clients
    trino_pkg = types.ModuleType('trino')
    # Mark as package
    setattr(trino_pkg, '__path__', [])
    dbapi_mod = types.ModuleType('trino.dbapi')
    sys.modules.setdefault('trino', trino_pkg)
    sys.modules.setdefault('trino.dbapi', dbapi_mod)

    # Stub OpenTelemetry
    otel_pkg = types.ModuleType('opentelemetry')
    setattr(otel_pkg, '__path__', [])
    otel_trace = types.ModuleType('opentelemetry.trace')
    otel_trace.get_tracer = lambda *args, **kwargs: types.SimpleNamespace(start_as_current_span=lambda *a, **k: types.SimpleNamespace(__aenter__=lambda s: s, __aexit__=lambda s, exc_type, exc, tb: False))
    sys.modules.setdefault('opentelemetry', otel_pkg)
    sys.modules.setdefault('opentelemetry.trace', otel_trace)

    # Stub data access layer to avoid importing heavy dependencies
    ext_dao_mod = types.ModuleType('aurum.data.external_dao')
    class _StubExternalDAO:  # minimal placeholder
        async def get_trino_client(self):
            return types.SimpleNamespace(execute_query=lambda *a, **k: [])
    ext_dao_mod.ExternalDAO = _StubExternalDAO
    sys.modules.setdefault('aurum.data.external_dao', ext_dao_mod)

    # Prefer a minimal import path for the app: build a FastAPI instance and include routers
    from fastapi import FastAPI
    from fastapi.openapi.utils import get_openapi

    # Import the v1 router (comprehensive surface)
    from aurum.api import routes as v1_routes

    app = FastAPI(title="Aurum API", version="1.0.0", description="Aurum Market Intelligence Platform API")
    app.include_router(v1_routes.router)

    # Generate OpenAPI schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description or "",
        routes=app.routes,
    )

    # Write JSON and YAML
    out_json = docs_api_dir / "openapi-spec.json"
    out_yaml_compat = docs_api_dir / "openapi-spec.yaml"

    with out_json.open("w") as f:
        json.dump(schema, f, indent=2)

    try:
        import yaml  # type: ignore
    except Exception as e:
        print(f"warning: pyyaml not available ({e}); skipping YAML outputs")
    else:
        with out_yaml_compat.open("w") as f:
            yaml.safe_dump(schema, f, sort_keys=False)
        print(f"wrote: {out_yaml_compat}")

    print(f"wrote: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
