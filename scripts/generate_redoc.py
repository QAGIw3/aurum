#!/usr/bin/env python3
"""Generate Redoc documentation from OpenAPI specification."""

import json
import yaml
from pathlib import Path
from typing import Dict, Any


def generate_redoc_html(openapi_spec: Dict[str, Any], output_path: str) -> None:
    """Generate Redoc HTML documentation from OpenAPI spec."""

    # Redoc HTML template
    redoc_html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{openapi_spec.get('info', {}).get('title', 'API Documentation')}</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">

    <!--
    ReDoc doesn't change outer page styles
    -->
    <style>
      body {{
        margin: 0;
        padding: 0;
      }}
    </style>
</head>
<body>
    <redoc spec-url="{output_path.replace('.html', '.json')}"></redoc>
    <script src="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"> </script>
</body>
</html>"""

    # Write Redoc HTML
    with open(output_path, 'w') as f:
        f.write(redoc_html)

    print(f"✅ Generated Redoc HTML: {output_path}")


def generate_redoc_json(openapi_spec: Dict[str, Any], output_path: str) -> None:
    """Generate JSON version of OpenAPI spec for Redoc."""

    with open(output_path, 'w') as f:
        json.dump(openapi_spec, f, indent=2)

    print(f"✅ Generated Redoc JSON: {output_path}")


def main():
    """Generate Redoc documentation from OpenAPI spec."""

    # Find OpenAPI spec files
    openapi_files = []
    for pattern in ['openapi/**/*.yaml', 'openapi/**/*.yml', '**/openapi.yaml', '**/openapi.yml']:
        openapi_files.extend(Path('.').glob(pattern))

    if not openapi_files:
        print("❌ No OpenAPI specification files found")
        return 1

    for spec_file in openapi_files:
        print(f"📚 Processing: {spec_file}")

        # Load OpenAPI spec
        with open(spec_file) as f:
            if spec_file.suffix == '.json':
                spec = json.load(f)
            else:
                spec = yaml.safe_load(f)

        # Generate Redoc files
        base_name = spec_file.stem
        base_dir = spec_file.parent

        # Generate JSON version for Redoc
        json_file = base_dir / f"{base_name}.json"
        generate_redoc_json(spec, str(json_file))

        # Generate HTML version for Redoc
        html_file = base_dir / f"{base_name}.html"
        generate_redoc_html(spec, str(html_file))

        # Generate versioned documentation
        version = spec.get('info', {}).get('version', 'latest')
        versioned_dir = base_dir / 'docs' / version
        versioned_dir.mkdir(parents=True, exist_ok=True)

        # Copy files to versioned directory
        import shutil
        shutil.copy2(json_file, versioned_dir / f"{base_name}.json")
        shutil.copy2(html_file, versioned_dir / f"{base_name}.html")

        print(f"✅ Generated versioned docs: {versioned_dir}")

        # Generate index.html for versioned directory
        index_html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Aurum API Documentation - v{version}</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
    <style>
      body {{
        margin: 0;
        padding: 20px;
        font-family: Roboto, sans-serif;
      }}
      .header {{
        text-align: center;
        margin-bottom: 30px;
      }}
      .header h1 {{
        color: #333;
        margin-bottom: 10px;
      }}
      .header p {{
        color: #666;
        margin-bottom: 20px;
      }}
      .version-info {{
        background: #f5f5f5;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 20px;
        border-left: 4px solid #007acc;
      }}
      .version-info h3 {{
        margin-top: 0;
        color: #333;
      }}
      .links {{
        text-align: center;
      }}
      .links a {{
        display: inline-block;
        margin: 10px 20px;
        padding: 10px 20px;
        background: #007acc;
        color: white;
        text-decoration: none;
        border-radius: 5px;
        transition: background 0.3s;
      }}
      .links a:hover {{
        background: #0056b3;
      }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Aurum API Documentation</h1>
        <p>Market Intelligence Platform API</p>
        <div class="version-info">
            <h3>Version {version}</h3>
            <p>Generated: {spec.get('info', {}).get('title', 'API')}</p>
        </div>
    </div>

    <div class="links">
        <a href="{base_name}.json">Download OpenAPI JSON</a>
        <a href="{base_name}.html">View Interactive Documentation</a>
        <a href="https://github.com/your-org/aurum" target="_blank">View Source Code</a>
    </div>

    <!-- Auto-redirect to interactive docs after 3 seconds -->
    <script>
        setTimeout(function() {{
            window.location.href = '{base_name}.html';
        }}, 3000);
    </script>

    <div style="text-align: center; margin-top: 30px; color: #666;">
        <p>Redirecting to interactive documentation in 3 seconds...</p>
    </div>
</body>
</html>"""

        index_file = versioned_dir / "index.html"
        with open(index_file, 'w') as f:
            f.write(index_html)

        print(f"✅ Generated version index: {index_file}")

    print("
🎉 Redoc documentation generation completed!"    print(f"📁 Output directory: {base_dir}")
    print("🌐 Open the generated HTML files in your browser to view the documentation")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
