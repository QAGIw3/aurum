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
    out_yaml_main = docs_api_dir / "aurum.yaml"  # used by docs/api/index.html
    out_yaml_compat = docs_api_dir / "openapi-spec.yaml"

    with out_json.open("w") as f:
        json.dump(schema, f, indent=2)

    try:
        import yaml  # type: ignore
    except Exception as e:
        print(f"warning: pyyaml not available ({e}); skipping YAML outputs")
    else:
        for target in (out_yaml_main, out_yaml_compat):
            with target.open("w") as f:
                yaml.safe_dump(schema, f, sort_keys=False)
        print(f"wrote: {out_yaml_main}")
        print(f"wrote: {out_yaml_compat}")

    print(f"wrote: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
