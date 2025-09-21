"""OpenAPI documentation generator with schema validation and client SDK generation."""

from __future__ import annotations

import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field


class DocumentationFormat(Enum):
    """Supported documentation formats."""
    JSON = "json"
    YAML = "yaml"
    MARKDOWN = "markdown"


class SDKLanguage(Enum):
    """Supported client SDK languages."""
    PYTHON = "python"
    TYPESCRIPT = "typescript"
    JAVASCRIPT = "javascript"
    GO = "go"
    RUST = "rust"
    JAVA = "java"
    C_SHARP = "csharp"
    PHP = "php"
    RUBY = "ruby"
    SWIFT = "swift"


@dataclass
class APIEndpoint:
    """Represents a single API endpoint."""
    path: str
    method: str
    summary: str = ""
    description: str = ""
    tags: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Any] = field(default_factory=dict)
    security: List[str] = field(default_factory=list)
    examples: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class APISchema:
    """Complete API schema representation."""
    title: str
    version: str
    description: str = ""
    endpoints: List[APIEndpoint] = field(default_factory=list)
    schemas: Dict[str, Any] = field(default_factory=dict)
    security_schemes: Dict[str, Any] = field(default_factory=dict)
    tags: List[Dict[str, str]] = field(default_factory=list)

    def add_endpoint(self, endpoint: APIEndpoint) -> None:
        """Add an endpoint to the schema."""
        self.endpoints.append(endpoint)

    def get_endpoints_by_tag(self, tag: str) -> List[APIEndpoint]:
        """Get all endpoints for a specific tag."""
        return [ep for ep in self.endpoints if tag in ep.tags]

    def get_endpoints_by_path(self, path: str) -> List[APIEndpoint]:
        """Get all endpoints for a specific path."""
        return [ep for ep in self.endpoints if ep.path == path]


class OpenAPIGenerator:
    """Generates OpenAPI documentation from FastAPI applications."""

    def __init__(self, app: FastAPI, title: str = "Aurum API", version: str = "1.0.0"):
        self.app = app
        self.title = title
        self.version = version
        self.schema = APISchema(title=title, version=version)

    def generate_schema(self) -> Dict[str, Any]:
        """Generate the complete OpenAPI schema."""
        # Get base schema from FastAPI
        openapi_schema = get_openapi(
            title=self.title,
            version=self.version,
            description="Aurum Market Intelligence Platform API",
            routes=self.app.routes,
        )

        # Enhance with additional metadata
        openapi_schema["info"]["contact"] = {
            "name": "Aurum API Support",
            "email": "api-support@aurum-platform.com"
        }

        openapi_schema["info"]["license"] = {
            "name": "MIT",
            "url": "https://opensource.org/licenses/MIT"
        }

        # Add server information
        openapi_schema["servers"] = [
            {
                "url": "https://api.aurum-platform.com",
                "description": "Production server"
            },
            {
                "url": "https://staging.api.aurum-platform.com",
                "description": "Staging server"
            },
            {
                "url": "http://localhost:8000",
                "description": "Development server"
            }
        ]

        # Add security schemes
        openapi_schema["components"]["securitySchemes"] = {
            "APIKeyAuth": {
                "type": "apiKey",
                "in": "header",
                "name": "X-API-Key"
            },
            "BearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT"
            },
            "BasicAuth": {
                "type": "http",
                "scheme": "basic"
            }
        }

        return openapi_schema

    def save_schema(self, output_path: str, format_type: DocumentationFormat = DocumentationFormat.JSON) -> None:
        """Save the OpenAPI schema to a file."""
        schema = self.generate_schema()

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if format_type == DocumentationFormat.JSON:
            with open(output_file, 'w') as f:
                json.dump(schema, f, indent=2)
        elif format_type == DocumentationFormat.YAML:
            with open(output_file, 'w') as f:
                yaml.dump(schema, f, default_flow_style=False, sort_keys=False)
        else:
            raise ValueError(f"Unsupported format: {format_type}")

    def generate_markdown_docs(self, output_path: str) -> None:
        """Generate Markdown documentation."""
        schema = self.generate_schema()

        markdown_content = f"""# {schema['info']['title']}

{schema['info']['description']}

## Version: {schema['info']['version']}

"""

        # Add tags section
        if 'tags' in schema:
            markdown_content += "## API Categories\n\n"
            for tag in schema['tags']:
                markdown_content += f"- **{tag['name']}**: {tag.get('description', '')}\n"

        markdown_content += "\n## Base URL\n\n"
        for server in schema['servers']:
            markdown_content += f"- {server['description']}: `{server['url']}`\n"

        # Add endpoints section
        markdown_content += "\n## Endpoints\n\n"

        # Group endpoints by path
        paths = schema.get('paths', {})
        for path, path_item in paths.items():
            markdown_content += f"### {path}\n\n"
            for method, operation in path_item.items():
                if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                    markdown_content += f"#### {method.upper()} {path}\n\n"
                    if 'summary' in operation:
                        markdown_content += f"**Summary:** {operation['summary']}\n\n"
                    if 'description' in operation:
                        markdown_content += f"**Description:** {operation['description']}\n\n"

                    # Parameters
                    if 'parameters' in operation:
                        markdown_content += "**Parameters:**\n\n"
                        for param in operation['parameters']:
                            required = param.get('required', False)
                            markdown_content += f"- `{param['name']}` ({param['in']}) {'*' if required else ''}: {param.get('description', '')}\n"
                        markdown_content += "\n"

                    # Request body
                    if 'requestBody' in operation:
                        markdown_content += "**Request Body:**\n\n"
                        content = operation['requestBody'].get('content', {})
                        for content_type, schema_info in content.items():
                            markdown_content += f"- Content-Type: `{content_type}`\n"
                        markdown_content += "\n"

                    # Responses
                    if 'responses' in operation:
                        markdown_content += "**Responses:**\n\n"
                        for status_code, response in operation['responses'].items():
                            description = response.get('description', '')
                            markdown_content += f"- `{status_code}`: {description}\n"
                        markdown_content += "\n"

        # Add schemas section
        if 'components' in schema and 'schemas' in schema['components']:
            markdown_content += "## Data Models\n\n"
            schemas = schema['components']['schemas']
            for schema_name, schema_def in schemas.items():
                markdown_content += f"### {schema_name}\n\n"
                if 'description' in schema_def:
                    markdown_content += f"{schema_def['description']}\n\n"

                if 'properties' in schema_def:
                    markdown_content += "**Properties:**\n\n"
                    for prop_name, prop_def in schema_def['properties'].items():
                        prop_type = prop_def.get('type', 'any')
                        required = prop_name in schema_def.get('required', [])
                        markdown_content += f"- `{prop_name}` ({prop_type}) {'*' if required else ''}: {prop_def.get('description', '')}\n"
                    markdown_content += "\n"

        # Write to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(markdown_content)


class SDKGenerator:
    """Generates client SDKs for different programming languages."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.info = schema.get('info', {})
        self.servers = schema.get('servers', [])
        self.paths = schema.get('paths', {})
        self.components = schema.get('components', {})

    def generate_python_sdk(self, output_path: str) -> None:
        """Generate Python client SDK with full type hints and response models."""
        sdk_content = f'''"""
{self.info.get('title', 'API')} Python Client SDK

Generated on: {datetime.utcnow().isoformat()}

This SDK provides convenient access to the {self.info.get('title', 'API')} API.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

try:
    import requests
    from pydantic import BaseModel, Field
except ImportError as e:
    raise ImportError("Required dependencies not installed. Install with: pip install requests pydantic") from e


# Type definitions
class RequestFormat(str, Enum):
    """Supported request/response formats."""
    JSON = "json"
    CSV = "csv"


class PaginationMeta(BaseModel):
    """Pagination metadata."""
    request_id: str
    query_time_ms: int = Field(ge=0)
    next_cursor: Optional[str] = None
    prev_cursor: Optional[str] = None
    count: Optional[int] = None
    total: Optional[int] = None
    offset: Optional[int] = None
    limit: Optional[int] = None


# Response models
@dataclass
class CurvePoint:
    """Curve data point."""
    curve_key: str
    tenor_label: str
    tenor_type: Optional[str] = None
    contract_month: Optional[date] = None
    asof_date: date
    mid: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    price_type: Optional[str] = None


@dataclass
class CurveResponse:
    """Curve data response."""
    meta: PaginationMeta
    data: List[CurvePoint]


class APIError(Exception):
    """Exception raised for API errors."""
    pass


class {self.info.get('title', 'API').replace(' ', '').replace('-', '')}Client:
    """Client for the {self.info.get('title', 'API')} API."""

    def __init__(
        self,
        base_url: str = "{self.servers[0]['url'] if self.servers else 'https://api.example.com'}",
        api_key: Optional[str] = None,
        timeout: int = 30
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

        if api_key:
            self.session.headers.update({{"X-API-Key": api_key}})

    def _make_request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        """Make an HTTP request to the API."""
        url = f"{{self.base_url}}{{path}}"

        # Set default timeout if not specified
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        response = self.session.request(method, url, **kwargs)

        if response.status_code >= 400:
            error_detail = response.text
            try:
                error_json = response.json()
                if 'detail' in error_json:
                    error_detail = error_json['detail']
            except:
                pass
            raise APIError(f"API request failed: {{response.status_code}} - {{error_detail}}")

        return response.json()

    def get_health(self) -> Dict[str, Any]:
        """Get API health status."""
        return self._make_request("GET", "/health")

    def get_ready(self) -> Dict[str, Any]:
        """Get API readiness status."""
        return self._make_request("GET", "/ready")

'''

        # Add methods for each endpoint
        for path, path_item in self.paths.items():
            for method, operation in path_item.items():
                if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                    method_name = self._python_method_name(path, method)
                    sdk_content += f'''
    def {method_name}(self, **kwargs) -> Dict[str, Any]:
        """{operation.get('summary', f'{method.upper()} {path}')}"""
        return self._make_request("{method.upper()}", "{path}", **kwargs)
'''

        sdk_content += '''
class APIError(Exception):
    """Exception raised for API errors."""
    pass


# Convenience function
def create_client(api_key: Optional[str] = None) -> {self.info.get('title', 'API').replace(' ', '').replace('-', '')}Client:
    """Create a new API client instance."""
    return {self.info.get('title', 'API').replace(' ', '').replace('-', '')}Client(api_key=api_key)
'''

        # Write to file
        output_file = Path(output_path) / "client.py"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(sdk_content)

    def generate_typescript_sdk(self, output_path: str) -> None:
        """Generate TypeScript client SDK with comprehensive type definitions."""
        sdk_content = f'''/**
 * {self.info.get('title', 'API')} TypeScript Client SDK
 *
 * Generated on: {datetime.utcnow().isoformat()}
 *
 * This SDK provides convenient access to the {self.info.get('title', 'API')} API.
 */

// Type definitions
export interface APIClientConfig {{
  baseURL?: string;
  apiKey?: string;
  timeout?: number;
  tenant?: string;
}}

export interface APIResponse<T = any> {{
  data: T;
  status: number;
  headers: Record<string, string>;
}}

export interface PaginationMeta {{
  requestId: string;
  queryTimeMs: number;
  nextCursor?: string;
  prevCursor?: string;
  count?: number;
  total?: number;
  offset?: number;
  limit?: number;
}}

export interface CurvePoint {{
  curveKey: string;
  tenorLabel: string;
  tenorType?: string;
  contractMonth?: string; // ISO date string
  asofDate: string; // ISO date string
  mid?: number;
  bid?: number;
  ask?: number;
  priceType?: string;
}}

export interface CurveResponse {{
  meta: PaginationMeta;
  data: CurvePoint[];
}}

export interface CurveDiffPoint {{
  curveKey: string;
  tenorLabel: string;
  tenorType?: string;
  contractMonth?: string;
  asofA: string; // ISO date string
  midA?: number;
  asofB: string; // ISO date string
  midB?: number;
  diffAbs?: number;
  diffPct?: number;
}}

export interface CurveDiffResponse {{
  meta: PaginationMeta;
  data: CurveDiffPoint[];
}}

export class APIError extends Error {{
  constructor(public status: number, message: string, public response?: any) {{
    super(message);
    this.name = 'APIError';
  }}
}}

export class {self.info.get('title', 'API').replace(' ', '').replace('-', '')}Client {{
  private baseURL: string;
  private apiKey?: string;
  private tenant?: string;
  private timeout: number;

  constructor(config: APIClientConfig = {{}}) {{
    this.baseURL = config.baseURL || '{self.servers[0]['url'] if self.servers else 'https://api.example.com'}';
    this.apiKey = config.apiKey;
    this.tenant = config.tenant;
    this.timeout = config.timeout || 30000;
  }}

  private async request<T = any>(method: string, path: string, options: RequestInit = {{}}): Promise<APIResponse<T>> {{
    const url = `${{this.baseURL}}${{path}}`;
    const headers: Record<string, string> = {{
      'Content-Type': 'application/json',
      ...options.headers as Record<string, string>
    }};

    if (this.apiKey) {{
      headers['X-API-Key'] = this.apiKey;
    }}

    if (this.tenant) {{
      headers['X-Aurum-Tenant'] = this.tenant;
    }}

    // Add common headers
    headers['User-Agent'] = '{self.info.get('title', 'API')} TypeScript Client';

    try {{
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);

      const response = await fetch(url, {{
        method,
        headers,
        signal: controller.signal,
        ...options
      }});

      clearTimeout(timeoutId);

      if (!response.ok) {{
        let errorMessage = `API request failed: ${{response.status}} ${{response.statusText}}`;
        let errorResponse: any = undefined;

        try {{
          errorResponse = await response.json();
          if (errorResponse.detail) {{
            errorMessage = errorResponse.detail;
          }}
        }} catch {{
          // Ignore JSON parsing errors for error responses
        }}

        throw new APIError(response.status, errorMessage, errorResponse);
      }}

      const data = await response.json();
      return {{
        data,
        status: response.status,
        headers: Object.fromEntries(response.headers.entries())
      }};
    }} catch (error) {{
      if (error instanceof APIError) {{
        throw error;
      }}
      throw new APIError(0, `Network error: ${{error instanceof Error ? error.message : String(error)}}`);
    }}
  }}

  async getHealth(): Promise<APIResponse<{{status: string}}>> {{
    return this.request('GET', '/health');
  }}

  async getReady(): Promise<APIResponse<{{status: string; checks: Record<string, any>}}>> {{
    return this.request('GET', '/ready');
  }}
'''

        # Add methods for each endpoint
        for path, path_item in self.paths.items():
            for method, operation in path_item.items():
                if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                    method_name = self._typescript_method_name(path, method)
                    sdk_content += f'''
  async {method_name}(options: RequestInit = {{}}): Promise<APIResponse> {{
    return this.request('{method.upper()}', '{path}', options);
  }}
'''

        sdk_content += '''
}

// Export convenience function
export function createClient(config: APIClientConfig = {}): {self.info.get('title', 'API').replace(' ', '').replace('-', '')}Client {
  return new {self.info.get('title', 'API').replace(' ', '').replace('-', '')}Client(config);
}

// Export types
export type {{ APIClientConfig, APIResponse, APIError }};
'''

        # Write to file
        output_file = Path(output_path) / "client.ts"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(sdk_content)

    def _python_method_name(self, path: str, method: str) -> str:
        """Generate Python method name from path and method."""
        # Convert path to snake_case method name
        path_parts = [part for part in path.strip('/').split('/') if part and not part.startswith('{')]

        if method.lower() == 'get' and len(path_parts) == 1:
            return path_parts[0]
        elif method.lower() == 'get':
            return '_'.join(path_parts)
        else:
            return f"{method.lower()}_{'_'.join(path_parts)}"

    def _typescript_method_name(self, path: str, method: str) -> str:
        """Generate TypeScript method name from path and method."""
        # Convert path to camelCase method name
        path_parts = [part for part in path.strip('/').split('/') if part and not part.startswith('{')]

        if method.lower() == 'get' and len(path_parts) == 1:
            return path_parts[0]
        elif method.lower() == 'get':
            return ''.join(word.capitalize() for word in path_parts)
        else:
            return f"{method.lower()}{''.join(word.capitalize() for word in path_parts)}"


class DocumentationValidator:
    """Validates API documentation for completeness and accuracy."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.issues: List[str] = []

    def validate(self) -> List[str]:
        """Validate the API documentation."""
        self.issues = []

        self._validate_info_section()
        self._validate_paths()
        self._validate_components()
        self._validate_security()

        return self.issues

    def _validate_info_section(self) -> None:
        """Validate the info section."""
        info = self.schema.get('info', {})

        if not info.get('title'):
            self.issues.append("Missing API title")

        if not info.get('version'):
            self.issues.append("Missing API version")

        if not info.get('description'):
            self.issues.append("Missing API description")

    def _validate_paths(self) -> None:
        """Validate all API paths."""
        paths = self.schema.get('paths', {})

        for path, path_item in paths.items():
            for method, operation in path_item.items():
                if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                    self._validate_operation(path, method, operation)

    def _validate_operation(self, path: str, method: str, operation: Dict[str, Any]) -> None:
        """Validate a single operation."""
        if not operation.get('summary'):
            self.issues.append(f"Missing summary for {method.upper()} {path}")

        if not operation.get('responses'):
            self.issues.append(f"Missing responses for {method.upper()} {path}")

        # Check for 200 response
        responses = operation.get('responses', {})
        if '200' not in responses and method.upper() != 'DELETE':
            self.issues.append(f"Missing 200 response for {method.upper()} {path}")

        # Check for error responses
        error_codes = ['400', '401', '403', '404', '500']
        for code in error_codes:
            if code not in responses:
                self.issues.append(f"Missing {code} response for {method.upper()} {path}")

    def _validate_components(self) -> None:
        """Validate components section."""
        components = self.schema.get('components', {})

        if not components.get('schemas'):
            self.issues.append("No data models defined")

        if not components.get('securitySchemes'):
            self.issues.append("No security schemes defined")

    def _validate_security(self) -> None:
        """Validate security configuration."""
        if not self.schema.get('components', {}).get('securitySchemes'):
            self.issues.append("No security schemes defined")


def generate_documentation(
    app: FastAPI,
    output_dir: str,
    formats: List[DocumentationFormat] = None,
    languages: List[SDKLanguage] = None
) -> None:
    """Generate complete API documentation and SDKs."""
    if formats is None:
        formats = [DocumentationFormat.JSON, DocumentationFormat.YAML, DocumentationFormat.MARKDOWN]

    if languages is None:
        languages = [SDKLanguage.PYTHON, SDKLanguage.TYPESCRIPT]

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate OpenAPI documentation
    generator = OpenAPIGenerator(app)
    schema = generator.generate_schema()

    # Save in different formats
    for format_type in formats:
        if format_type == DocumentationFormat.JSON:
            generator.save_schema(str(output_path / "openapi.json"), format_type)
        elif format_type == DocumentationFormat.YAML:
            generator.save_schema(str(output_path / "openapi.yaml"), format_type)
        elif format_type == DocumentationFormat.MARKDOWN:
            generator.generate_markdown_docs(str(output_path / "api_docs.md"))

    # Generate SDKs
    sdk_generator = SDKGenerator(schema)
    sdk_output_path = output_path / "sdks"
    sdk_output_path.mkdir(exist_ok=True)

    for language in languages:
        if language == SDKLanguage.PYTHON:
            sdk_generator.generate_python_sdk(sdk_output_path)
        elif language == SDKLanguage.TYPESCRIPT:
            sdk_generator.generate_typescript_sdk(sdk_output_path)

    # Validate documentation
    validator = DocumentationValidator(schema)
    issues = validator.validate()

    if issues:
        print("Documentation validation issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("Documentation validation passed!")

    # Generate summary
    summary = {
        "title": schema.get('info', {}).get('title', 'API'),
        "version": schema.get('info', {}).get('version', '1.0.0'),
        "endpoints": len(schema.get('paths', {})),
        "models": len(schema.get('components', {}).get('schemas', {})),
        "formats": [f.value for f in formats],
        "languages": [l.value for l in languages],
        "validation_issues": len(issues),
        "generated_at": datetime.utcnow().isoformat()
    }

    with open(output_path / "documentation_summary.json", 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"Documentation generated in {output_path}")
    print(f"Formats: {', '.join([f.value for f in formats])}")
    print(f"SDKs: {', '.join([l.value for l in languages])}")
    print(f"Validation issues: {len(issues)}")
