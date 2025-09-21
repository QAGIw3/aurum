#!/usr/bin/env python3
"""Generate API documentation from OpenAPI specification."""

import json
import yaml
from pathlib import Path
from typing import Dict, Any
import requests


def load_openapi_spec() -> Dict[str, Any]:
    """Load OpenAPI specification from file."""
    spec_file = Path(__file__).parent.parent / "docs" / "api" / "openapi-spec.yaml"

    with open(spec_file, 'r') as f:
        return yaml.safe_load(f)


def generate_markdown_docs(spec: Dict[str, Any]) -> str:
    """Generate Markdown documentation from OpenAPI spec."""

    info = spec.get('info', {})
    servers = spec.get('servers', [])
    paths = spec.get('paths', {})
    components = spec.get('components', {})

    markdown = f"""# {info.get('title', 'API Documentation')}

{info.get('description', '')}

## Base URLs

"""

    for server in servers:
        markdown += f"- **{server.get('description', 'Server')}**: `{server['url']}`\n"

    markdown += """
## Authentication

The API supports multiple authentication methods:

### JWT Token Authentication
```bash
curl -H "Authorization: Bearer <jwt_token>" \\
     http://api.aurum-dev.svc.cluster.local:8080/v1/curves
```

### API Key Authentication
```bash
curl -H "X-API-Key: <api_key>" \\
     http://api.aurum-dev.svc.cluster.local:8080/v1/curves
```

## Endpoints

"""

    # Generate endpoint documentation
    for path, path_item in paths.items():
        markdown += f"### {path}\n\n"

        for method, operation in path_item.items():
            if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                summary = operation.get('summary', '')
                description = operation.get('description', '')
                parameters = operation.get('parameters', [])
                responses = operation.get('responses', {})

                markdown += f"#### {method.upper()} {summary}\n\n"

                if description:
                    markdown += f"{description}\n\n"

                if parameters:
                    markdown += "**Parameters:**\n\n"
                    for param in parameters:
                        name = param.get('name', '')
                        required = param.get('required', False)
                        param_type = param.get('schema', {}).get('type', 'string')
                        description = param.get('description', '')

                        required_str = " (required)" if required else ""
                        markdown += f"- `{name}` ({param_type}){required_str}: {description}\n"
                    markdown += "\n"

                markdown += "**Responses:**\n\n"
                for status_code, response in responses.items():
                    description = response.get('description', '')
                    markdown += f"- `{status_code}`: {description}\n"

                markdown += "\n**Example:**\n"
                markdown += f"```bash\ncurl \"{servers[0]['url'] if servers else 'http://localhost:8080'}{path}\"\n```\n\n"

    # Add schemas documentation
    schemas = components.get('schemas', {})
    if schemas:
        markdown += "## Data Models\n\n"

        for schema_name, schema in schemas.items():
            markdown += f"### {schema_name}\n\n"

            properties = schema.get('properties', {})
            required = schema.get('required', [])

            if properties:
                markdown += "**Properties:**\n\n"
                for prop_name, prop_info in properties.items():
                    prop_type = prop_info.get('type', 'any')
                    description = prop_info.get('description', '')
                    example = prop_info.get('example', '')

                    required_str = " (required)" if prop_name in required else ""
                    example_str = f" Example: `{example}`" if example else ""

                    markdown += f"- `{prop_name}` ({prop_type}){required_str}: {description}{example_str}\n"

                markdown += "\n"

    return markdown


def generate_html_docs(spec: Dict[str, Any]) -> str:
    """Generate HTML documentation from OpenAPI spec."""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{spec.get('info', {}).get('title', 'API Documentation')}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; border-bottom: 1px solid #bdc3c7; padding-bottom: 5px; }}
        h3 {{ color: #7f8c8d; }}
        .endpoint {{
            background: #f8f9fa;
            border-left: 4px solid #3498db;
            padding: 15px;
            margin: 10px 0;
            border-radius: 4px;
        }}
        .method {{ font-weight: bold; color: #27ae60; }}
        .url {{ font-family: monospace; background: #ecf0f1; padding: 2px 6px; border-radius: 3px; }}
        pre {{
            background: #2c3e50;
            color: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
        }}
        .tag {{ background: #3498db; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.8em; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{spec.get('info', {}).get('title', 'API Documentation')}</h1>
        <p>{spec.get('info', {}).get('description', '').split('.')[0]}...</p>

        <h2>Base URLs</h2>
        <ul>
"""

    for server in spec.get('servers', []):
        html += f"<li><strong>{server.get('description', 'Server')}:</strong> <code>{server['url']}</code></li>\n"

    html += """        </ul>

        <h2>Authentication</h2>
        <div class="endpoint">
            <p><strong>JWT Token:</strong> <code>Authorization: Bearer &lt;jwt_token&gt;</code></p>
            <p><strong>API Key:</strong> <code>X-API-Key: &lt;api_key&gt;</code></p>
        </div>

        <h2>Endpoints</h2>
"""

    for path, path_item in spec.get('paths', {}).items():
        for method, operation in path_item.items():
            if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                html += f"""
        <div class="endpoint">
            <h3><span class="method">{method.upper()}</span> <span class="url">{path}</span></h3>
            <p><strong>{operation.get('summary', '')}</strong></p>
            <p>{operation.get('description', '')}</p>
        </div>
"""

    html += """    </div>
</body>
</html>"""

    return html


def main():
    """Generate API documentation in multiple formats."""

    print("📚 Generating API documentation...")

    # Load OpenAPI spec
    spec = load_openapi_spec()

    # Generate Markdown docs
    markdown_content = generate_markdown_docs(spec)

    # Generate HTML docs
    html_content = generate_html_docs(spec)

    # Write files
    docs_dir = Path(__file__).parent.parent / "docs" / "api"

    with open(docs_dir / "api-docs.md", 'w') as f:
        f.write(markdown_content)

    with open(docs_dir / "api-docs.html", 'w') as f:
        f.write(html_content)

    print("✅ API documentation generated:")
    print(f"   📄 Markdown: {docs_dir}/api-docs.md")
    print(f"   🌐 HTML: {docs_dir}/api-docs.html")
    print(f"   📋 OpenAPI: {docs_dir}/openapi-spec.yaml")


if __name__ == "__main__":
    main()
