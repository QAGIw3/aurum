#!/usr/bin/env python3
"""Build API docs and export OpenAPI from the FastAPI app.

Outputs:
- docs/api/openapi.json (generated from running app routes)
- docs/api/api-docs.md (markdown reference)
- docs/api/openapi.generated.yaml (OpenAPI YAML export)
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

os.environ.setdefault("AURUM_API_LIGHT_INIT", "1")
try:
    from aurum.api.app import create_app
    from aurum.api.openapi_generator import OpenAPIGenerator, DocumentationFormat
except Exception as exc:  # Fallback to file-based generation
    create_app = None  # type: ignore
    OpenAPIGenerator = None  # type: ignore
    DocumentationFormat = None  # type: ignore
    _import_error = exc


def _from_openapi_yaml(spec_path: Path) -> dict:
    import yaml
    return yaml.safe_load(spec_path.read_text())


def _write_simple_markdown(schema: dict, md_out: Path) -> None:
    lines = []
    info = schema.get("info", {})
    title = info.get("title", "Aurum API")
    desc = info.get("description", "")
    version = info.get("version", "")
    lines.append(f"# {title}")
    if desc:
        lines.append("")
        lines.append(desc)
    if version:
        lines.append("")
        lines.append(f"Version: `{version}`")
    lines.append("")
    lines.append("## Endpoints")
    paths = schema.get("paths", {})
    for path, methods in sorted(paths.items()):
        for method, op in sorted(methods.items()):
            if not isinstance(op, dict):
                continue
            summary = op.get("summary") or op.get("operationId") or ""
            lines.append(f"- `{method.upper()}` `{path}` - {summary}")
    comps = schema.get("components", {}).get("schemas", {})
    if comps:
        lines.append("")
        lines.append("## Schemas")
        for name in sorted(comps.keys()):
            lines.append(f"- `{name}`")
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text("\n".join(lines) + "\n")


def _write_redoc_html(json_path: Path, html_out: Path) -> None:
    """Write a Redoc HTML wrapper pointing at the given JSON spec.

    The resulting HTML relies on the Redoc CDN and can be served statically.
    """
    rel = os.path.relpath(json_path, html_out.parent)
    html = f"""<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\"/>
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
    <title>Aurum API – OpenAPI Reference</title>
    <link rel=\"icon\" href=\"data:,\"/>
    <style>body {{ margin: 0; padding: 0 }}</style>
    <script src=\"https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js\"></script>
  </head>
  <body>
    <redoc spec-url=\"{rel}\" expand-responses=\"200,201\" hide-download-button></redoc>
  </body>
</html>\n"""
    html_out.parent.mkdir(parents=True, exist_ok=True)
    html_out.write_text(html)


def main() -> None:
    md_out = Path("docs/api/api-docs.md")
    json_out = Path("docs/api/openapi.json")
    yaml_out = Path("docs/api/openapi.generated.yaml")
    yaml_src = Path("docs/api/openapi-spec.yaml")

    if create_app and OpenAPIGenerator:
        try:
            app = create_app()  # May be sync or async
            # Handle async create_app
            if hasattr(app, "__await__"):
                import asyncio
                app = asyncio.run(app)  # type: ignore
            generator = OpenAPIGenerator(app, title=app.title, version=app.version)
            generator.generate_markdown_docs(str(md_out))
            schema = generator.generate_schema()
            json_out.parent.mkdir(parents=True, exist_ok=True)
            json_out.write_text(json.dumps(schema, indent=2))
            yaml_out.parent.mkdir(parents=True, exist_ok=True)
            generator.save_schema(str(yaml_out), format_type=DocumentationFormat.YAML)
            print("✅ API docs written from running app:")
            # Also emit a Redoc HTML viewer
            _write_redoc_html(json_out, Path("docs/api/api-docs.html"))
            _write_redoc_html(json_out, Path("docs/api/index.html"))
        except Exception as exc:
            print(f"⚠️  Falling back to OpenAPI YAML due to app init failure: {exc}")
            schema = None
            # fall through to fallback path below
    if not (create_app and OpenAPIGenerator) or schema is None:
        # Fallback: read existing OpenAPI YAML
        if not yaml_src.exists():
            raise SystemExit(
                f"OpenAPI generation failed ({_import_error}); no fallback spec at {yaml_src}"
            )
        schema = None
        try:
            schema = _from_openapi_yaml(yaml_src)
        except Exception as e:
            print(f"⚠️  Could not parse {yaml_src}: {e}")
        # Always write/generated YAML copy
        yaml_out.parent.mkdir(parents=True, exist_ok=True)
        yaml_out.write_text(yaml_src.read_text())
        # Write JSON and markdown only if parse succeeded
        if schema:
            json_out.parent.mkdir(parents=True, exist_ok=True)
            json_out.write_text(json.dumps(schema, indent=2))
            _write_simple_markdown(schema, md_out)
            # Emit a Redoc HTML viewer against the generated JSON
            _write_redoc_html(json_out, Path("docs/api/api-docs.html"))
            _write_redoc_html(json_out, Path("docs/api/index.html"))
            print("✅ API docs written from OpenAPI spec:")
        else:
            md_out.parent.mkdir(parents=True, exist_ok=True)
            md_out.write_text(
                "# Aurum API\n\nOpenAPI spec available at `docs/api/openapi-spec.yaml`. "
                "Generation fell back to copying YAML due to parse issues.\n"
            )
            print("✅ Copied OpenAPI YAML; wrote stub markdown:")

    print(f" - {md_out}")
    print(f" - {json_out}")
    print(f" - {yaml_out}")


if __name__ == "__main__":
    main()
