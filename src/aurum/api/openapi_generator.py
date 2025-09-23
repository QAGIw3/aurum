"""OpenAPI documentation generator with schema validation and client SDK generation."""

from __future__ import annotations

import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import json as json_module

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field

from .http.pagination import DEFAULT_PAGE_SIZE, MAX_CURSOR_LENGTH, MAX_PAGE_SIZE


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

        pagination_doc = (
            "\n\n**Pagination**\n"
            f"- Collection endpoints default to {DEFAULT_PAGE_SIZE} items per page and cap at {MAX_PAGE_SIZE}.\n"
            "- Cursor-based pagination returns `meta.next_cursor` and `meta.prev_cursor`; offsets are supported for legacy clients but deprecated.\n"
            f"- Cursor tokens are opaque strings with a maximum length of {MAX_CURSOR_LENGTH} characters.\n"
        )
        info_block = openapi_schema.setdefault("info", {})
        description = info_block.get("description") or ""
        if pagination_doc.strip() not in description:
            info_block["description"] = (description + pagination_doc).strip()

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

        # Add comprehensive security schemes
        openapi_schema["components"]["securitySchemes"] = {
            "APIKeyAuth": {
                "type": "apiKey",
                "in": "header",
                "name": "X-API-Key",
                "description": "API key for authentication"
            },
            "BearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT",
                "description": "JWT token authentication"
            },
            "BasicAuth": {
                "type": "http",
                "scheme": "basic",
                "description": "Basic authentication for admin endpoints"
            },
            "OIDCAuth": {
                "type": "openIdConnect",
                "openIdConnectUrl": "https://auth.aurum-platform.com/.well-known/openid_configuration",
                "description": "OpenID Connect authentication"
            }
        }

        # Add global security requirements
        openapi_schema["security"] = [
            {"APIKeyAuth": []},
            {"BearerAuth": []},
            {"OIDCAuth": []}
        ]

        # Add comprehensive error response schemas (RFC 7807)
        openapi_schema["components"]["schemas"] = openapi_schema.get("components", {}).get("schemas", {})

        openapi_schema["components"]["schemas"]["ErrorEnvelope"] = {
            "type": "object",
            "description": "Standard error response envelope following RFC 7807",
            "properties": {
                "error": {
                    "type": "string",
                    "description": "Error type identifier"
                },
                "message": {
                    "type": "string",
                    "description": "Human-readable error message"
                },
                "code": {
                    "type": "string",
                    "description": "Application-specific error code"
                },
                "field": {
                    "type": "string",
                    "description": "Field name that caused the error (for validation errors)"
                },
                "value": {
                    "description": "Invalid value that caused the error"
                },
                "context": {
                    "type": "object",
                    "description": "Additional error context"
                },
                "request_id": {
                    "type": "string",
                    "description": "Request identifier for debugging"
                },
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Error timestamp"
                }
            },
            "required": ["error", "timestamp"],
            "example": {
                "error": "ValidationError",
                "message": "Invalid request parameters",
                "code": "INVALID_CURSOR",
                "field": "cursor",
                "value": "invalid-cursor-token",
                "context": {"parameter": "cursor"},
                "request_id": "req-12345",
                "timestamp": "2025-01-15T10:30:00Z"
            }
        }


        openapi_schema["components"]["schemas"]["ValidationErrorResponse"] = {
            "type": "object",
            "description": "Validation error response",
            "properties": {
                "error": {
                    "type": "string",
                    "description": "Error type",
                    "default": "Validation Error"
                },
                "message": {
                    "type": "string",
                    "description": "Error message"
                },
                "field_errors": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/ValidationErrorDetail"},
                    "description": "List of field validation errors"
                },
                "request_id": {
                    "type": "string",
                    "description": "Request identifier"
                },
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Error timestamp"
                }
            },
            "required": ["error", "message", "timestamp"],
            "example": {
                "error": "ValidationError",
                "message": "Request validation failed",
                "field_errors": [
                    {
                        "field": "tenant_id",
                        "message": "tenant_id is required",
                        "code": "missing",
                        "constraint": "required"
                    },
                    {
                        "field": "cursor",
                        "message": "Invalid cursor format",
                        "code": "invalid_format",
                        "constraint": "format"
                    }
                ],
                "request_id": "req-12345",
                "timestamp": "2025-01-15T10:30:00Z"
            }
        }

        # Add pagination metadata schema
        openapi_schema["components"]["schemas"]["PaginationMeta"] = {
            "type": "object",
            "description": "Pagination metadata for cursor-based pagination",
            "properties": {
                "request_id": {
                    "type": "string",
                    "description": "Request identifier for debugging"
                },
                "tenant_id": {
                    "type": "string",
                    "description": "Tenant identifier"
                },
                "total_count": {
                    "type": "integer",
                    "description": "Total number of items"
                },
                "returned_count": {
                    "type": "integer",
                    "description": "Number of items returned in this response"
                },
                "has_more": {
                    "type": "boolean",
                    "description": "Whether there are more items available"
                },
                "cursor": {
                    "type": "string",
                    "description": "Current cursor value"
                },
                "next_cursor": {
                    "type": "string",
                    "description": "Cursor for next page"
                },
                "prev_cursor": {
                    "type": "string",
                    "description": "Cursor for previous page"
                },
                "processing_time_ms": {
                    "type": "number",
                    "description": "Processing time in milliseconds"
                }
            },
            "example": {
                "request_id": "req-12345",
                "tenant_id": "tenant-001",
                "total_count": 1000,
                "returned_count": 100,
                "has_more": true,
                "cursor": "eyJvZmZzZXQiOiAwfQ==",
                "next_cursor": "eyJvZmZzZXQiOiAxMDB9",
                "processing_time_ms": 45.2
            }
        }

        # Add Link headers schema
        openapi_schema["components"]["schemas"]["Links"] = {
            "type": "object",
            "description": "Navigation links for paginated resources",
            "properties": {
                "self": {
                    "type": "string",
                    "description": "Self-referencing link"
                },
                "next": {
                    "type": "string",
                    "description": "Link to next page"
                },
                "prev": {
                    "type": "string",
                    "description": "Link to previous page"
                },
                "canonical": {
                    "type": "string",
                    "description": "Canonical URL"
                }
            },
            "example": {
                "self": "https://api.aurum-platform.com/v2/scenarios?limit=100&cursor=eyJvZmZzZXQiOiAwfQ==",
                "next": "https://api.aurum-platform.com/v2/scenarios?limit=100&cursor=eyJvZmZzZXQiOiAxMDB9",
                "canonical": "https://api.aurum-platform.com/v2/scenarios"
            }
        }

        # Add tenant context schema
        openapi_schema["components"]["schemas"]["TenantContext"] = {
            "type": "object",
            "description": "Tenant context information",
            "properties": {
                "tenant_id": {
                    "type": "string",
                    "description": "Tenant identifier",
                    "pattern": "^[a-zA-Z0-9-_]+$"
                },
                "organization": {
                    "type": "string",
                    "description": "Organization name"
                },
                "permissions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "User permissions for this tenant"
                }
            },
            "required": ["tenant_id"],
            "example": {
                "tenant_id": "tenant-001",
                "organization": "Acme Corp",
                "permissions": ["curves:read", "scenarios:read", "scenarios:run"]
            }
        }

        openapi_schema["components"]["schemas"]["ValidationErrorDetail"] = {
            "type": "object",
            "description": "Detailed validation error information",
            "properties": {
                "field": {
                    "type": "string",
                    "description": "Field path that failed validation"
                },
                "message": {
                    "type": "string",
                    "description": "Validation error message"
                },
                "value": {
                    "description": "Invalid value"
                },
                "code": {
                    "type": "string",
                    "description": "Validation error code"
                },
                "constraint": {
                    "type": "string",
                    "enum": ["required", "type", "format", "min", "max", "length", "pattern", "enum", "unique", "custom"],
                    "description": "Constraint type that failed"
                }
            },
            "required": ["field", "message"]
        }

        openapi_schema["components"]["schemas"]["ValidationErrorResponse"] = {
            "type": "object",
            "description": "Validation error response",
            "properties": {
                "error": {
                    "type": "string",
                    "description": "Error type",
                    "default": "Validation Error"
                },
                "message": {
                    "type": "string",
                    "description": "Error message"
                },
                "field_errors": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/ValidationErrorDetail"},
                    "description": "List of field validation errors"
                },
                "request_id": {
                    "type": "string",
                    "description": "Request identifier"
                },
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Error timestamp"
                }
            },
            "required": ["error", "message", "timestamp"]
        }

        # Add common error responses to all operations
        for path_data in openapi_schema.get("paths", {}).values():
            for operation in path_data.values():
                if not isinstance(operation, dict):
                    continue

                # Add standard error responses if not present
                if "responses" not in operation:
                    operation["responses"] = {}

                responses = operation["responses"]

                # Add headers to successful responses (200, 201)
                for status_code in ["200", "201"]:
                    if status_code in responses:
                        response_def = responses[status_code]
                        if "headers" not in response_def:
                            response_def["headers"] = {}
                        
                        # Add rate limiting headers to successful responses
                        headers = response_def["headers"]
                        if "X-RateLimit-Limit" not in headers:
                            headers["X-RateLimit-Limit"] = {
                                "description": "Maximum number of requests allowed per time window",
                                "schema": {"type": "integer"}
                            }
                        if "X-RateLimit-Remaining" not in headers:
                            headers["X-RateLimit-Remaining"] = {
                                "description": "Number of requests remaining in the current time window",
                                "schema": {"type": "integer"}
                            }
                        if "X-RateLimit-Reset" not in headers:
                            headers["X-RateLimit-Reset"] = {
                                "description": "Time when the rate limit resets (Unix timestamp)",
                                "schema": {"type": "integer"}
                            }
                        if "X-Request-Id" not in headers:
                            headers["X-Request-Id"] = {
                                "description": "Request identifier for debugging and tracing",
                                "schema": {"type": "string"}
                            }
                        if "X-API-Version" not in headers:
                            headers["X-API-Version"] = {
                                "description": "API version",
                                "schema": {"type": "string"}
                            }

                # Add common error responses
                responses["400"] = {
                    "description": "Bad Request",
                    "content": {
                        "application/json": {
                            "schema": {
                                "oneOf": [
                                    {"$ref": "#/components/schemas/ErrorEnvelope"},
                                    {"$ref": "#/components/schemas/ValidationErrorResponse"}
                                ]
                            }
                        }
                    }
                }

                responses["401"] = {
                    "description": "Unauthorized",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
                        }
                    }
                }

                responses["403"] = {
                    "description": "Forbidden",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
                        }
                    }
                }

                responses["404"] = {
                    "description": "Not Found",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
                        }
                    }
                }

                responses["429"] = {
                    "description": "Too Many Requests",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
                        }
                    },
                    "headers": {
                        "X-RateLimit-Limit": {
                            "description": "Maximum number of requests allowed per time window",
                            "schema": {"type": "integer"}
                        },
                        "X-RateLimit-Remaining": {
                            "description": "Number of requests remaining in the current time window",
                            "schema": {"type": "integer"}
                        },
                        "X-RateLimit-Reset": {
                            "description": "Time when the rate limit resets (Unix timestamp)",
                            "schema": {"type": "integer"}
                        },
                        "Retry-After": {
                            "description": "Number of seconds to wait before retrying",
                            "schema": {"type": "integer"}
                        },
                        "X-Request-Id": {
                            "description": "Request identifier for debugging and tracing",
                            "schema": {"type": "string"}
                        },
                        "X-API-Version": {
                            "description": "API version",
                            "schema": {"type": "string"}
                        }
                    }
                }

                responses["500"] = {
                    "description": "Internal Server Error",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
                        }
                    }
                }

        self._ensure_examples(openapi_schema)
        return openapi_schema

    def _ensure_examples(self, schema: Dict[str, Any]) -> None:
        """Ensure request and response bodies contain example payloads."""

        components = schema.get("components", {})
        for path_item in schema.get("paths", {}).values():
            for method, operation in path_item.items():
                if method.upper() not in {"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"}:
                    continue

                request_body = operation.get("requestBody")
                if request_body:
                    for media_type, content in request_body.get("content", {}).items():
                        if media_type.startswith("application/") and "example" not in content and "examples" not in content:
                            example = self._build_example(content.get("schema", {}), components)
                            if example is not None:
                                content["example"] = example

                for response in operation.get("responses", {}).values():
                    for media_type, content in response.get("content", {}).items():
                        if media_type.startswith("application/") and "example" not in content and "examples" not in content:
                            example = self._build_example(content.get("schema", {}), components)
                            if example is not None:
                                content["example"] = example

    def _build_example(self, schema: Dict[str, Any], components: Dict[str, Any]) -> Any:
        """Recursively generate an example payload from a schema definition."""

        if not schema:
            return None

        if "$ref" in schema:
            ref = schema["$ref"].split("/")[-1]
            ref_schema = components.get("schemas", {}).get(ref, {})
            return self._build_example(ref_schema, components)

        if "allOf" in schema:
            combined: Dict[str, Any] = {}
            for item in schema.get("allOf", []):
                value = self._build_example(item, components)
                if isinstance(value, dict):
                    combined.update(value)
            return combined or None

        schema_type = schema.get("type")
        if schema_type == "object":
            properties = schema.get("properties", {})
            example: Dict[str, Any] = {}
            for key, prop_schema in properties.items():
                example[key] = self._build_example(prop_schema, components)
            if not example and schema.get("additionalProperties"):
                example = {
                    "key": self._build_example(schema["additionalProperties"], components)
                }
            return example
        if schema_type == "array":
            items = schema.get("items", {})
            item_example = self._build_example(items, components)
            return [item_example] if item_example is not None else []
        if schema_type == "string":
            if "enum" in schema:
                return schema["enum"][0]
            fmt = schema.get("format")
            if fmt == "date-time":
                return datetime.utcnow().isoformat() + "Z"
            if fmt == "date":
                return datetime.utcnow().date().isoformat()
            if fmt == "uuid":
                return "00000000-0000-0000-0000-000000000000"
            if fmt == "email":
                return "user@example.com"
            return schema.get("example", "string")
        if schema_type == "integer":
            return schema.get("example", 0)
        if schema_type == "number":
            return schema.get("example", 0.0)
        if schema_type == "boolean":
            return schema.get("example", True)
        if "enum" in schema:
            return schema["enum"][0]
        return schema.get("example")

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

    def generate_redoc_html(self, output_path: str) -> None:
        """Generate Redoc HTML documentation."""
        schema = self.generate_schema()

        redoc_html = f'''<!DOCTYPE html>
<html>
<head>
    <title>{schema.get('info', {}).get('title', 'API Documentation')}</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">

    <style>
        body {{
            margin: 0;
            padding: 0;
        }}
    </style>
</head>
<body>
    <redoc spec-url="{Path(output_path).parent / 'openapi.json'}"></redoc>
    <script src="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"> </script>
</body>
</html>'''

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(redoc_html)

    def generate_markdown_docs(self, output_path: str) -> None:
        """Generate comprehensive Markdown documentation with examples."""
        schema = self.generate_schema()

        markdown_content = f"""# {schema['info']['title']}

{schema['info']['description']}

## Version: {schema['info']['version']}

## Authentication

This API uses multiple authentication methods:

- **API Key**: `X-API-Key` header
- **Bearer Token**: `Authorization: Bearer <token>` header
- **Basic Auth**: For admin endpoints

## Tenant Context

All v2 endpoints require a `tenant_id` query parameter to specify the tenant context.
This ensures proper data isolation and security.

**Example:**
```
GET /v2/scenarios?tenant_id=your-tenant-id
```

## Pagination

The API uses cursor-based pagination for all collection endpoints:

- Use `limit` parameter to control page size (default: 10, max: 100)
- Use `cursor` parameter for pagination
- Response includes `meta.next_cursor` and `meta.prev_cursor`
- Link headers provide navigation URLs

**Example Response:**
```json
{{
  "data": [...],
  "meta": {{
    "request_id": "req-12345",
    "tenant_id": "tenant-001",
    "total_count": 1000,
    "returned_count": 100,
    "has_more": true,
    "cursor": "eyJvZmZzZXQiOiAwfQ==",
    "next_cursor": "eyJvZmZzZXQiOiAxMDB9",
    "processing_time_ms": 45.2
  }},
  "links": {{
    "self": "https://api.aurum-platform.com/v2/scenarios",
    "next": "https://api.aurum-platform.com/v2/scenarios?cursor=eyJvZmZzZXQiOiAxMDB9"
  }}
}}
```

## Error Handling

The API follows RFC 7807 for error responses:

```json
{{
  "error": "ValidationError",
  "message": "Invalid request parameters",
  "code": "INVALID_CURSOR",
  "field": "cursor",
  "value": "invalid-cursor-token",
  "context": {{}},
  "request_id": "req-12345",
  "timestamp": "2025-01-15T10:30:00Z"
}}
```

## Rate Limiting

Rate limits are enforced per tenant:

- Headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
- 429 responses include `Retry-After` header
- Rate limits can be configured per endpoint

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
                            param_type = param.get('schema', {}).get('type', 'string')
                            markdown_content += f"- `{param['name']}` ({param['in']}, {param_type}) {'*' if required else ''}: {param.get('description', '')}\n"
                        markdown_content += "\n"

                    # Request body
                    if 'requestBody' in operation:
                        markdown_content += "**Request Body:**\n\n"
                        content = operation['requestBody'].get('content', {})
                        for content_type, schema_info in content.items():
                            markdown_content += f"- Content-Type: `{content_type}`\n"
                            if 'example' in schema_info.get('schema', {}):
                                markdown_content += "**Example:**\n"
                                markdown_content += f"```json\n{json_module.dumps(schema_info['schema']['example'], indent=2)}\n```\n"
                        markdown_content += "\n"

                    # Responses
                    if 'responses' in operation:
                        markdown_content += "**Responses:**\n\n"
                        for status_code, response in operation['responses'].items():
                            description = response.get('description', '')
                            markdown_content += f"- `{status_code}`: {description}\n"
                            if 'content' in response:
                                for content_type, content_info in response.get('content', {}).items():
                                    if 'example' in content_info.get('schema', {}):
                                        markdown_content += f"  - Content-Type: `{content_type}`\n"
                                        markdown_content += "**Example:**\n"
                                        markdown_content += f"```json\n{json_module.dumps(content_info['schema']['example'], indent=2)}\n```\n"
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
                        description = prop_def.get('description', '')
                        if 'example' in prop_def:
                            description += f" (Example: `{prop_def['example']}`)"
                        markdown_content += f"- `{prop_name}` ({prop_type}) {'*' if required else ''}: {description}\n"
                    markdown_content += "\n"

                if 'example' in schema_def:
                    markdown_content += "**Example:**\n"
                    markdown_content += f"```json\n{json_module.dumps(schema_def['example'], indent=2)}\n```\n\n"

        # Add Spectral rules section
        markdown_content += """
## Spectral Rules

The API specification follows these custom Spectral rules:

### Tenant Context Rules
- All endpoints must require `tenant_id` parameter
- Tenant context must be enforced for data isolation
- Cross-tenant access must be explicitly prevented

### Pagination Rules
- Collection endpoints must support cursor-based pagination
- Response must include `meta` with pagination information
- Link headers must be provided for navigation
- Default and maximum page sizes must be documented

### Error Handling Rules
- All operations must define appropriate error responses
- Error responses must follow RFC 7807 format
- Validation errors must provide field-level details
- Request IDs must be included in error responses

### Security Rules
- Authentication methods must be clearly documented
- Rate limiting headers must be specified
- Sensitive data must not be exposed in examples
- CORS configuration must be documented

"""

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
        """Generate comprehensive Python client SDK with full type hints and response models."""
        # Generate models from OpenAPI schema
        models_content = self._generate_python_models()
        client_content = self._generate_python_client()

        # Write models file
        models_file = Path(output_path) / "models.py"
        models_file.parent.mkdir(parents=True, exist_ok=True)
        with open(models_file, 'w') as f:
            f.write(models_content)

        # Write client file
        client_file = Path(output_path) / "client.py"
        with open(client_file, 'w') as f:
            f.write(client_content)

        # Write __init__.py
        init_content = f'''"""
{self.info.get('title', 'API')} Python Client SDK

Generated on: {datetime.utcnow().isoformat()}

This SDK provides convenient access to the {self.info.get('title', 'API')} API.

Example usage:
    from aurum_client import AurumClient

    client = AurumClient(api_key="your-api-key")
    response = client.list_scenarios(tenant_id="your-tenant")
"""

from .client import AurumClient
from .models import *

__version__ = "0.2.0"
__all__ = ["AurumClient"]
'''
        init_file = Path(output_path) / "__init__.py"
        with open(init_file, 'w') as f:
            f.write(init_content)

        # Generate smoke tests
        self._generate_python_smoke_tests(output_path)

    def _generate_python_models(self) -> str:
        """Generate Python models from OpenAPI schema."""
        models_content = f'''"""
{self.info.get('title', 'API')} Python Models

Generated from OpenAPI specification.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

try:
    from pydantic import BaseModel, Field
except ImportError as e:
    raise ImportError("Required dependencies not installed. Install with: pip install pydantic") from e


# Base response models
class Meta(BaseModel):
    """Base metadata for all responses."""
    request_id: str
    tenant_id: str
    processing_time_ms: float


class ErrorEnvelope(BaseModel):
    """Standard error response envelope following RFC 7807."""
    error: str
    message: str
    code: Optional[str] = None
    field: Optional[str] = None
    value: Optional[Any] = None
    context: Optional[Dict[str, Any]] = None
    request_id: str
    timestamp: datetime


class PaginationMeta(Meta):
    """Pagination metadata for cursor-based pagination."""
    total_count: Optional[int] = None
    returned_count: Optional[int] = None
    has_more: bool
    cursor: Optional[str] = None
    next_cursor: Optional[str] = None
    prev_cursor: Optional[str] = None


class Links(BaseModel):
    """Navigation links for paginated resources."""
    self: str
    next: Optional[str] = None
    prev: Optional[str] = None
    canonical: Optional[str] = None


# Request/Response models
class ScenarioCreateRequest(BaseModel):
    """Request model for creating scenarios."""
    name: str = Field(..., description="Scenario name")
    description: Optional[str] = Field(None, description="Scenario description")
    parameters: Dict[str, Any] = Field(..., description="Scenario parameters")


class ScenarioResponse(BaseModel):
    """Response model for scenario data."""
    id: str
    name: str
    description: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime
    meta: Meta


class ScenarioListResponse(BaseModel):
    """Response model for listing scenarios."""
    data: List[ScenarioResponse]
    meta: PaginationMeta
    links: Links


class CurvePoint(BaseModel):
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


class CurveResponse(BaseModel):
    """Response model for curve data."""
    id: str
    name: str
    description: Optional[str]
    data_points: int
    created_at: datetime
    meta: Meta


class CurveListResponse(BaseModel):
    """Response model for listing curves."""
    data: List[CurveResponse]
    meta: PaginationMeta
    links: Links


# Error models
class ValidationErrorDetail(BaseModel):
    """Detailed validation error information."""
    field: str
    message: str
    value: Optional[Any] = None
    code: Optional[str] = None
    constraint: Optional[str] = None


class ValidationErrorResponse(BaseModel):
    """Validation error response."""
    error: str = "Validation Error"
    message: str
    field_errors: List[ValidationErrorDetail]
    request_id: str
    timestamp: datetime


# Tenant context model
class TenantContext(BaseModel):
    """Tenant context information."""
    tenant_id: str = Field(..., pattern=r"^[a-zA-Z0-9-_]+$")
    organization: Optional[str] = None
    permissions: List[str] = []


__all__ = [
    "Meta", "ErrorEnvelope", "PaginationMeta", "Links",
    "ScenarioCreateRequest", "ScenarioResponse", "ScenarioListResponse",
    "CurvePoint", "CurveResponse", "CurveListResponse",
    "ValidationErrorDetail", "ValidationErrorResponse",
    "TenantContext"
]
'''

        return models_content

    def _generate_python_client(self) -> str:
        """Generate Python client with comprehensive functionality."""
        client_content = f'''"""
{self.info.get('title', 'API')} Python Client SDK

Generated on: {datetime.utcnow().isoformat()}

This SDK provides convenient access to the {self.info.get('title', 'API')} API.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode, urljoin

try:
    import requests
    from pydantic import BaseModel, Field
except ImportError as e:
    raise ImportError("Required dependencies not installed. Install with: pip install requests pydantic") from e

from .models import (
    ScenarioCreateRequest, ScenarioResponse, ScenarioListResponse,
    CurveResponse, CurveListResponse,
    ErrorEnvelope, ValidationErrorResponse
)


class AurumAPIError(Exception):
    """Exception raised for API errors."""
    def __init__(self, status_code: int, error: ErrorEnvelope):
        self.status_code = status_code
        self.error = error
        super().__init__(f"API Error {{status_code}}: {{error.message}}")


class AurumClient:
    """Client for the {self.info.get('title', 'API')} API."""

    def __init__(
        self,
        base_url: str = "{self.servers[0]['url'] if self.servers else 'https://api.example.com'}",
        api_key: Optional[str] = None,
        timeout: int = 30,
        tenant_id: Optional[str] = None
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.tenant_id = tenant_id
        self.timeout = timeout
        self.session = requests.Session()

        if api_key:
            self.session.headers.update({{"X-API-Key": api_key}})

        if tenant_id:
            self.session.headers.update({{"X-Aurum-Tenant": tenant_id}})

    def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Make an HTTP request to the API."""
        url = urljoin(self.base_url, path)

        # Set default timeout if not specified
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        # Prepare request parameters
        request_params = params.copy() if params else {{}}

        # Add tenant_id to all requests if available
        if self.tenant_id and 'tenant_id' not in request_params:
            request_params['tenant_id'] = self.tenant_id

        response = self.session.request(
            method,
            url,
            params=request_params,
            json=data,
            headers=headers,
            **kwargs
        )

        # Handle API errors
        if response.status_code >= 400:
            try:
                error_data = response.json()
                if 'error' in error_data:
                    error = ErrorEnvelope(**error_data)
                    raise AurumAPIError(response.status_code, error)
                elif 'field_errors' in error_data:
                    error = ValidationErrorResponse(**error_data)
                    raise AurumAPIError(response.status_code, ErrorEnvelope(
                        error="ValidationError",
                        message=error.message,
                        context={{"field_errors": [e.model_dump() for e in error.field_errors]}},
                        request_id=error.request_id,
                        timestamp=error.timestamp
                    ))
            except json.JSONDecodeError:
                pass

            raise AurumAPIError(response.status_code, ErrorEnvelope(
                error=f"HTTP{{response.status_code}}",
                message=response.text[:200],
                request_id="unknown",
                timestamp=datetime.utcnow()
            ))

        return response.json()

    # Health endpoints
    def get_health(self) -> Dict[str, Any]:
        """Get API health status."""
        return self._make_request("GET", "/health")

    def get_ready(self) -> Dict[str, Any]:
        """Get API readiness status."""
        return self._make_request("GET", "/ready")

    # Scenario endpoints
    def list_scenarios(
        self,
        cursor: Optional[str] = None,
        limit: int = 10,
        name_filter: Optional[str] = None,
        **kwargs
    ) -> ScenarioListResponse:
        """List scenarios with pagination."""
        params = {{
            "cursor": cursor,
            "limit": limit,
            "name_filter": name_filter,
            **kwargs
        }}
        data = self._make_request("GET", "/v2/scenarios", params=params)
        return ScenarioListResponse(**data)

    def create_scenario(self, scenario: ScenarioCreateRequest) -> ScenarioResponse:
        """Create a new scenario."""
        data = self._make_request("POST", "/v2/scenarios", data=scenario.model_dump())
        return ScenarioResponse(**data)

    def get_scenario(self, scenario_id: str) -> ScenarioResponse:
        """Get a scenario by ID."""
        data = self._make_request("GET", f"/v2/scenarios/{{scenario_id}}")
        return ScenarioResponse(**data)

    # Curve endpoints
    def list_curves(
        self,
        cursor: Optional[str] = None,
        limit: int = 10,
        name_filter: Optional[str] = None,
        **kwargs
    ) -> CurveListResponse:
        """List curves with pagination."""
        params = {{
            "cursor": cursor,
            "limit": limit,
            "name_filter": name_filter,
            **kwargs
        }}
        data = self._make_request("GET", "/v2/curves", params=params)
        return CurveListResponse(**data)

    def get_curve_diff(
        self,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str
    ) -> CurveResponse:
        """Get curve diff between timestamps."""
        params = {{
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp
        }}
        data = self._make_request("GET", f"/v2/curves/{{curve_id}}/diff", params=params)
        return CurveResponse(**data)


__all__ = ["AurumClient", "AurumAPIError"]
'''

        return client_content

    def _generate_python_smoke_tests(self, output_path: str) -> None:
        """Generate smoke tests for the Python SDK."""
        test_content = f'''"""
Smoke tests for {self.info.get('title', 'API')} Python SDK

These tests verify basic functionality without requiring actual API access.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

try:
    from .client import AurumClient, AurumAPIError
    from .models import (
        ScenarioCreateRequest, ScenarioResponse, ScenarioListResponse,
        CurveResponse, CurveListResponse, ErrorEnvelope
    )
except ImportError:
    pytest.skip("SDK not generated", allow_module_level=True)


class TestAurumClient:
    """Test cases for AurumClient."""

    @pytest.fixture
    def client(self):
        """Create a test client with mocked requests."""
        with patch('requests.Session') as mock_session:
            mock_session_instance = Mock()
            mock_session.return_value = mock_session_instance

            # Mock successful response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {{
                "data": [],
                "meta": {{
                    "request_id": "test-123",
                    "tenant_id": "test-tenant",
                    "total_count": 0,
                    "returned_count": 0,
                    "has_more": False,
                    "processing_time_ms": 10.5
                }},
                "links": {{
                    "self": "https://api.example.com/v2/scenarios",
                    "canonical": "https://api.example.com/v2/scenarios"
                }}
            }}
            mock_session_instance.request.return_value = mock_response

            client = AurumClient(base_url="https://api.example.com")
            client.session = mock_session_instance
            yield client

    def test_client_initialization(self):
        """Test client initialization."""
        client = AurumClient(api_key="test-key", tenant_id="test-tenant")
        assert client.api_key == "test-key"
        assert client.tenant_id == "test-tenant"
        assert client.base_url == "https://api.example.com"

    def test_list_scenarios(self, client):
        """Test list_scenarios method."""
        response = client.list_scenarios(limit=10)
        assert isinstance(response, ScenarioListResponse)
        assert response.meta.request_id == "test-123"

    def test_create_scenario(self, client):
        """Test create_scenario method."""
        scenario_req = ScenarioCreateRequest(
            name="test-scenario",
            description="Test scenario",
            parameters={{}}
        )

        response = client.create_scenario(scenario_req)
        assert isinstance(response, ScenarioResponse)
        assert response.meta.request_id == "test-123"

    def test_list_curves(self, client):
        """Test list_curves method."""
        response = client.list_curves(limit=5)
        assert isinstance(response, CurveListResponse)
        assert response.meta.request_id == "test-123"

    def test_error_handling(self, client):
        """Test error handling."""
        # Mock error response
        error_response = Mock()
        error_response.status_code = 400
        error_response.json.return_value = {{
            "error": "ValidationError",
            "message": "Invalid request",
            "request_id": "test-123",
            "timestamp": datetime.utcnow().isoformat()
        }}
        client.session.request.return_value = error_response

        with pytest.raises(AurumAPIError) as exc_info:
            client.list_scenarios()

        assert exc_info.value.status_code == 400
        assert exc_info.value.error.error == "ValidationError"


class TestModels:
    """Test cases for data models."""

    def test_scenario_create_request(self):
        """Test ScenarioCreateRequest model."""
        request = ScenarioCreateRequest(
            name="test-scenario",
            description="Test description",
            parameters={{"key": "value"}}
        )
        assert request.name == "test-scenario"
        assert request.description == "Test description"

    def test_error_envelope(self):
        """Test ErrorEnvelope model."""
        error = ErrorEnvelope(
            error="TestError",
            message="Test message",
            request_id="test-123",
            timestamp=datetime.utcnow()
        )
        assert error.error == "TestError"
        assert error.message == "Test message"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
'''
        test_file = Path(output_path) / "test_smoke.py"
        with open(test_file, 'w') as f:
            f.write(test_content)

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

    # Generate Redoc HTML documentation
    generator.generate_redoc_html(str(output_path / "redoc.html"))

    # Generate SDKs
    sdk_generator = SDKGenerator(schema)
    sdk_output_path = output_path / "sdks"
    sdk_output_path.mkdir(exist_ok=True)

    for language in languages:
        if language == SDKLanguage.PYTHON:
            sdk_generator.generate_python_sdk(str(sdk_output_path))
        elif language == SDKLanguage.TYPESCRIPT:
            sdk_generator.generate_typescript_sdk(str(sdk_output_path))

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
    print(f"Redoc: {output_path / 'redoc.html'}")
    print(f"Validation issues: {len(issues)}")
