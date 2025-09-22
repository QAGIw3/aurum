#!/usr/bin/env python3
"""Build API docs and export OpenAPI from the FastAPI app.

Outputs:
- docs/api/openapi.json (generated from running app routes)
- docs/api/api-docs.md (markdown reference)
- openapi/aurum.generated.yaml (OpenAPI YAML export)
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

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


def main() -> None:
    md_out = Path("docs/api/api-docs.md")
    json_out = Path("docs/api/openapi.json")
    yaml_out = Path("openapi/aurum.generated.yaml")
    yaml_src = Path("openapi/aurum.yaml")

    if create_app and OpenAPIGenerator:
        app = create_app()  # Uses defaults
        generator = OpenAPIGenerator(app, title=app.title, version=app.version)
        generator.generate_markdown_docs(str(md_out))
        schema = generator.generate_schema()
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(schema, indent=2))
        yaml_out.parent.mkdir(parents=True, exist_ok=True)
        generator.save_schema(str(yaml_out), format_type=DocumentationFormat.YAML)
        print("✅ API docs written from running app:")
    else:
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
            print("✅ API docs written from OpenAPI spec:")
        else:
            md_out.parent.mkdir(parents=True, exist_ok=True)
            md_out.write_text(
                "# Aurum API\n\nOpenAPI spec available at `openapi/aurum.yaml`. "
                "Generation fell back to copying YAML due to parse issues.\n"
            )
            print("✅ Copied OpenAPI YAML; wrote stub markdown:")

    print(f" - {md_out}")
    print(f" - {json_out}")
    print(f" - {yaml_out}")


if __name__ == "__main__":
    main()
