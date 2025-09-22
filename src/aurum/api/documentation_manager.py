"""API documentation management endpoints for generating and validating documentation."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, BackgroundTasks

from ..telemetry.context import get_request_id
from .openapi_generator import (
    generate_documentation,
    DocumentationFormat,
    SDKLanguage,
    DocumentationValidator
)
from .app import create_app


router = APIRouter()


@router.post("/v1/admin/docs/generate")
async def generate_api_documentation(
    request: Request,
    background_tasks: BackgroundTasks,
    output_dir: str = "docs",
    formats: List[str] = None,
    languages: List[str] = None,
    include_examples: bool = True,
) -> Dict[str, str]:
    """Generate complete API documentation and SDKs."""
    start_time = time.perf_counter()

    try:
        # Parse formats and languages
        if formats is None:
            formats = ["json", "yaml", "markdown"]
        if languages is None:
            languages = ["python", "typescript"]

        doc_formats = [DocumentationFormat(f) for f in formats if f in [e.value for e in DocumentationFormat]]
        sdk_languages = [SDKLanguage(l) for l in languages if l in [e.value for e in SDKLanguage]]

        # Get the FastAPI app instance
        # This would need to be passed or accessed differently in a real implementation
        from aurum.api import create_app
        app = create_app()

        # Add background task for documentation generation
        background_tasks.add_task(
            generate_documentation,
            app,
            output_dir,
            doc_formats,
            sdk_languages
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Documentation generation started",
            "output_dir": output_dir,
            "formats": formats,
            "languages": languages,
            "include_examples": include_examples,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate documentation: {str(exc)}"
        ) from exc


@router.get("/v1/admin/docs/status")
async def get_documentation_status(
    request: Request,
) -> Dict[str, str]:
    """Get current documentation generation status."""
    start_time = time.perf_counter()

    try:
        # In a real implementation, this would check the status of background tasks
        status_data = {
            "generation_in_progress": False,
            "last_generation": None,
            "available_formats": [f.value for f in DocumentationFormat],
            "available_languages": [l.value for l in SDKLanguage],
            "validation_status": "unknown",
            "validation_issues": [],
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": status_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get documentation status: {str(exc)}"
        ) from exc


@router.post("/v1/admin/docs/validate")
async def validate_api_documentation(
    request: Request,
) -> Dict[str, str]:
    """Validate the current API documentation."""
    start_time = time.perf_counter()

    try:
        # Get the FastAPI app instance
        from aurum.api import create_app
        app = create_app()

        # Generate schema for validation
        from .openapi_generator import OpenAPIGenerator
        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        # Validate documentation
        validator = DocumentationValidator(schema)
        issues = validator.validate()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        validation_status = "passed" if not issues else "failed"

        return {
            "validation_status": validation_status,
            "validation_issues": issues,
            "total_issues": len(issues),
            "schema_info": {
                "title": schema.get("info", {}).get("title"),
                "version": schema.get("info", {}).get("version"),
                "endpoints": len(schema.get("paths", {})),
                "models": len(schema.get("components", {}).get("schemas", {})),
            },
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to validate documentation: {str(exc)}"
        ) from exc


@router.get("/v1/admin/docs/schema")
async def get_api_schema(
    request: Request,
    format: str = Query("json", description="Response format"),
) -> Dict[str, str]:
    """Get the current API schema."""
    start_time = time.perf_counter()

    try:
        # Get the FastAPI app instance
        from aurum.api import create_app
        app = create_app()

        # Generate schema
        from .openapi_generator import OpenAPIGenerator
        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Return schema directly or as downloadable file
        if format == "json":
            return {
                "schema": schema,
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get API schema: {str(exc)}"
        ) from exc


@router.get("/v1/admin/docs/endpoints")
async def list_api_endpoints(
    request: Request,
    tag: Optional[str] = Query(None, description="Filter by tag"),
    method: Optional[str] = Query(None, description="Filter by HTTP method"),
) -> Dict[str, str]:
    """List all API endpoints with their documentation status."""
    start_time = time.perf_counter()

    try:
        # Get the FastAPI app instance
        from aurum.api import create_app
        app = create_app()

        # Generate schema to analyze endpoints
        from .openapi_generator import OpenAPIGenerator
        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        endpoints_data = []
        paths = schema.get("paths", {})

        for path, path_item in paths.items():
            for http_method, operation in path_item.items():
                if http_method.upper() in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
                    endpoint_data = {
                        "path": path,
                        "method": http_method.upper(),
                        "summary": operation.get("summary", ""),
                        "description": operation.get("description", ""),
                        "tags": operation.get("tags", []),
                        "has_examples": "examples" in operation,
                        "has_parameters": bool(operation.get("parameters", [])),
                        "has_request_body": "requestBody" in operation,
                        "responses": list(operation.get("responses", {}).keys()),
                    }
                    endpoints_data.append(endpoint_data)

        # Apply filters
        if tag:
            endpoints_data = [ep for ep in endpoints_data if tag in ep["tags"]]
        if method:
            endpoints_data = [ep for ep in endpoints_data if ep["method"] == method.upper()]

        # Calculate documentation completeness
        total_endpoints = len(endpoints_data)
        complete_endpoints = sum(
            1 for ep in endpoints_data
            if ep["summary"] and ep["description"] and ep["responses"]
        )
        completeness_score = (complete_endpoints / total_endpoints) if total_endpoints > 0 else 0

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_endpoints": total_endpoints,
                "complete_endpoints": complete_endpoints,
                "completeness_score": round(completeness_score, 2),
            },
            "data": endpoints_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list endpoints: {str(exc)}"
        ) from exc


@router.get("/v1/admin/docs/models")
async def list_data_models(
    request: Request,
) -> Dict[str, str]:
    """List all data models defined in the API."""
    start_time = time.perf_counter()

    try:
        # Get the FastAPI app instance
        from aurum.api import create_app
        app = create_app()

        # Generate schema to analyze models
        from .openapi_generator import OpenAPIGenerator
        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        models_data = []
        schemas = schema.get("components", {}).get("schemas", {})

        for model_name, model_def in schemas.items():
            model_data = {
                "name": model_name,
                "description": model_def.get("description", ""),
                "properties": len(model_def.get("properties", {})),
                "required_properties": len(model_def.get("required", [])),
                "has_examples": "example" in model_def,
                "type": model_def.get("type", "object"),
            }
            models_data.append(model_data)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_models": len(models_data),
            },
            "data": models_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list data models: {str(exc)}"
        ) from exc


@router.get("/v1/admin/docs/export")
async def export_documentation(
    request: Request,
    format: str = Query("json", description="Export format"),
    include_internal: bool = Query(False, description="Include internal endpoints"),
) -> Dict[str, str]:
    """Export API documentation in various formats."""
    start_time = time.perf_counter()

    try:
        # Get the FastAPI app instance
        from aurum.api import create_app
        app = create_app()

        # Generate schema
        from .openapi_generator import OpenAPIGenerator
        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        # Remove internal endpoints if requested
        if not include_internal:
            paths = schema.get("paths", {})
            internal_paths = [path for path in paths.keys() if path.startswith("/v1/admin/")]
            for path in internal_paths:
                del paths[path]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "schema": schema,
            "format": format,
            "include_internal": include_internal,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export documentation: {str(exc)}"
        ) from exc


@router.get("/v1/admin/docs/completeness")
async def get_documentation_completeness(
    request: Request,
) -> Dict[str, str]:
    """Get API documentation completeness analysis."""
    start_time = time.perf_counter()

    try:
        # Get the FastAPI app instance
        from aurum.api import create_app
        app = create_app()

        # Generate schema for analysis
        from .openapi_generator import OpenAPIGenerator
        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        # Analyze completeness
        analysis = {
            "total_endpoints": 0,
            "complete_endpoints": 0,
            "incomplete_endpoints": 0,
            "missing_summaries": 0,
            "missing_descriptions": 0,
            "missing_responses": 0,
            "missing_examples": 0,
            "total_models": 0,
            "models_with_examples": 0,
            "security_schemes": len(schema.get("components", {}).get("securitySchemes", {})),
            "servers_configured": len(schema.get("servers", [])),
            "tags_defined": len(schema.get("tags", [])),
        }

        paths = schema.get("paths", {})
        for path, path_item in paths.items():
            for method, operation in path_item.items():
                if method.upper() in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
                    analysis["total_endpoints"] += 1

                    is_complete = True

                    if not operation.get("summary"):
                        analysis["missing_summaries"] += 1
                        is_complete = False

                    if not operation.get("description"):
                        analysis["missing_descriptions"] += 1
                        is_complete = False

                    if not operation.get("responses"):
                        analysis["missing_responses"] += 1
                        is_complete = False

                    if not operation.get("examples"):
                        analysis["missing_examples"] += 1

                    if is_complete:
                        analysis["complete_endpoints"] += 1
                    else:
                        analysis["incomplete_endpoints"] += 1

        analysis["total_models"] = len(schema.get("components", {}).get("schemas", {}))
        schemas = schema.get("components", {}).get("schemas", {})
        for model_def in schemas.values():
            if model_def.get("example"):
                analysis["models_with_examples"] += 1

        # Calculate scores
        endpoint_completeness = (
            analysis["complete_endpoints"] / analysis["total_endpoints"]
            if analysis["total_endpoints"] > 0 else 0
        )

        model_completeness = (
            analysis["models_with_examples"] / analysis["total_models"]
            if analysis["total_models"] > 0 else 0
        )

        overall_score = (endpoint_completeness + model_completeness) / 2

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                **analysis,
                "scores": {
                    "endpoint_completeness": round(endpoint_completeness, 3),
                    "model_completeness": round(model_completeness, 3),
                    "overall_score": round(overall_score, 3),
                },
                "recommendations": _generate_completeness_recommendations(analysis)
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze documentation completeness: {str(exc)}"
        ) from exc


def _generate_completeness_recommendations(analysis: Dict[str, int]) -> List[str]:
    """Generate recommendations based on completeness analysis."""
    recommendations = []

    if analysis["missing_summaries"] > 0:
        recommendations.append(f"Add summaries to {analysis['missing_summaries']} endpoints")

    if analysis["missing_descriptions"] > 0:
        recommendations.append(f"Add descriptions to {analysis['missing_descriptions']} endpoints")

    if analysis["missing_responses"] > 0:
        recommendations.append(f"Define responses for {analysis['missing_responses']} endpoints")

    if analysis["missing_examples"] > 0:
        recommendations.append(f"Add request/response examples to {analysis['missing_examples']} endpoints")

    if analysis["total_models"] > 0 and analysis["models_with_examples"] / analysis["total_models"] < 0.8:
        recommendations.append("Add examples to more data models")

    if analysis["security_schemes"] == 0:
        recommendations.append("Define security schemes for API authentication")

    if not analysis["servers_configured"]:
        recommendations.append("Configure server information for different environments")

    return recommendations


@router.get("/v1/admin/docs/examples")
async def get_api_usage_examples(
    request: Request,
    endpoint: Optional[str] = Query(None, description="Specific endpoint"),
    language: str = Query("python", description="Example language"),
) -> Dict[str, str]:
    """Get API usage examples for different languages."""
    start_time = time.perf_counter()

    try:
        # Generate examples based on the API schema
        examples_data = {
            "language": language,
            "examples": {},
        }

        # Get the FastAPI app instance
        from aurum.api import create_app
        app = create_app()

        # Generate schema
        from .openapi_generator import OpenAPIGenerator
        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        # Generate examples for each endpoint
        paths = schema.get("paths", {})
        for path, path_item in paths.items():
            if endpoint and path != endpoint:
                continue

            for method, operation in path_item.items():
                if method.upper() in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
                    example = _generate_endpoint_example(path, method, operation, language)
                    examples_data["examples"][f"{method.upper()} {path}"] = example

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": examples_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get API examples: {str(exc)}"
        ) from exc


def _generate_endpoint_example(path: str, method: str, operation: dict, language: str) -> Dict[str, str]:
    """Generate usage example for a specific endpoint."""
    base_url = "https://api.aurum-platform.com"

    if language == "python":
        return {
            "code": f"""
import requests

# {operation.get('summary', f'{method.upper()} {path}')}
response = requests.{method.lower()}(
    f"{base_url}{path}",
    headers={{
        "X-API-Key": "your-api-key",
        "Content-Type": "application/json"
    }}
)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {{response.status_code}} - {{response.text}}")
""".strip(),
            "explanation": f"Python requests example for {method.upper()} {path}"
        }
    elif language == "javascript":
        return {
            "code": f"""
// {operation.get('summary', f'{method.upper()} {path}')}
fetch('{base_url}{path}', {{
    method: '{method.upper()}',
    headers: {{
        'X-API-Key': 'your-api-key',
        'Content-Type': 'application/json'
    }}
}})
.then(response => response.json())
.then(data => console.log(data))
.catch(error => console.error(error));
""".strip(),
            "explanation": f"JavaScript fetch example for {method.upper()} {path}"
        }
    elif language == "curl":
        return {
            "code": f"curl -X {method.upper()} {base_url}{path} -H 'X-API-Key: your-api-key'",
            "explanation": f"cURL example for {method.upper()} {path}"
        }
    else:
        return {
            "code": f"# {operation.get('summary', f'{method.upper()} {path}')}\n# {base_url}{path}",
            "explanation": f"Generic example for {method.upper()} {path}"
        }
