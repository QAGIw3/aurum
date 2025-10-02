"""API Documentation Service.

This service handles API documentation generation, code examples,
interactive testing, and OpenAPI spec management.

Extracted from the monolithic developer_workspace_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import json
import logging
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


class ApiEndpoint(BaseModel):
    """Represents an API endpoint with documentation."""
    
    endpoint_id: str = Field(default_factory=lambda: str(uuid4()))
    path: str
    method: str
    operation_id: str
    summary: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    parameters: List[Dict[str, Any]] = Field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    security: List[Dict[str, List[str]]] = Field(default_factory=list)
    examples: Dict[str, Any] = Field(default_factory=dict)
    deprecated: bool = False
    version: str = "v1"


class CodeExample(BaseModel):
    """Code example for API usage."""
    
    example_id: str = Field(default_factory=lambda: str(uuid4()))
    endpoint_id: str
    language: str
    title: str
    description: Optional[str] = None
    code: str
    setup_code: Optional[str] = None
    expected_output: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class InteractiveTest(BaseModel):
    """Interactive API test configuration."""
    
    test_id: str = Field(default_factory=lambda: str(uuid4()))
    endpoint_id: str
    name: str
    description: Optional[str] = None
    request_template: Dict[str, Any]
    parameter_defaults: Dict[str, Any] = Field(default_factory=dict)
    validation_rules: List[Dict[str, Any]] = Field(default_factory=list)
    example_responses: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ApiDocumentationRepository(BaseRepository):
    """Repository interface for API documentation operations."""
    
    async def save_endpoint(self, endpoint: ApiEndpoint) -> ApiEndpoint:
        """Save or update an API endpoint."""
        raise NotImplementedError
    
    async def get_endpoint(self, endpoint_id: str) -> Optional[ApiEndpoint]:
        """Get an endpoint by ID."""
        raise NotImplementedError
    
    async def list_endpoints(
        self,
        version: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[ApiEndpoint]:
        """List endpoints with optional filters."""
        raise NotImplementedError
    
    async def save_example(self, example: CodeExample) -> CodeExample:
        """Save or update a code example."""
        raise NotImplementedError
    
    async def list_examples(
        self,
        endpoint_id: Optional[str] = None,
        language: Optional[str] = None
    ) -> List[CodeExample]:
        """List code examples."""
        raise NotImplementedError
    
    async def save_test(self, test: InteractiveTest) -> InteractiveTest:
        """Save or update an interactive test."""
        raise NotImplementedError
    
    async def list_tests(
        self,
        endpoint_id: Optional[str] = None
    ) -> List[InteractiveTest]:
        """List interactive tests."""
        raise NotImplementedError


class ApiDocumentationService(BaseService):
    """
    API documentation and interactive testing service.
    
    This service provides comprehensive API documentation, code examples,
    and interactive testing capabilities for developers.
    """
    
    def __init__(
        self,
        repository: Optional[ApiDocumentationRepository] = None,
        openapi_spec_path: Optional[Path] = None,
        cache_enabled: bool = True,
        cache_ttl: int = 3600  # 1 hour for API docs
    ):
        """
        Initialize the API documentation service.
        
        Args:
            repository: Repository for data persistence
            openapi_spec_path: Path to OpenAPI specification
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.logger = logging.getLogger(__name__)
        
        # OpenAPI spec handling
        self.openapi_spec_path = openapi_spec_path
        self._openapi_spec: Optional[Dict[str, Any]] = None
        self._endpoints_by_operation: Dict[str, ApiEndpoint] = {}
        
        # Documentation cache
        self._documentation_cache: Dict[str, Any] = {}
        self._examples_by_endpoint: Dict[str, List[CodeExample]] = {}
        
        # Initialize OpenAPI spec
        if openapi_spec_path:
            self._load_openapi_spec()
    
    def _get_default_repository(self) -> ApiDocumentationRepository:
        """Get default repository from DI container."""
        class MockRepository(ApiDocumentationRepository):
            def __init__(self):
                self.endpoints = {}
                self.examples = {}
                self.tests = {}
            
            async def save_endpoint(self, endpoint: ApiEndpoint) -> ApiEndpoint:
                self.endpoints[endpoint.endpoint_id] = endpoint
                return endpoint
            
            async def get_endpoint(self, endpoint_id: str) -> Optional[ApiEndpoint]:
                return self.endpoints.get(endpoint_id)
            
            async def list_endpoints(self, **kwargs) -> List[ApiEndpoint]:
                endpoints = list(self.endpoints.values())
                if kwargs.get('version'):
                    endpoints = [e for e in endpoints if e.version == kwargs['version']]
                if kwargs.get('tags'):
                    tags = kwargs['tags']
                    endpoints = [e for e in endpoints if any(tag in e.tags for tag in tags)]
                return endpoints
            
            async def save_example(self, example: CodeExample) -> CodeExample:
                self.examples[example.example_id] = example
                return example
            
            async def list_examples(self, **kwargs) -> List[CodeExample]:
                examples = list(self.examples.values())
                if kwargs.get('endpoint_id'):
                    examples = [e for e in examples if e.endpoint_id == kwargs['endpoint_id']]
                if kwargs.get('language'):
                    examples = [e for e in examples if e.language == kwargs['language']]
                return examples
            
            async def save_test(self, test: InteractiveTest) -> InteractiveTest:
                self.tests[test.test_id] = test
                return test
            
            async def list_tests(self, **kwargs) -> List[InteractiveTest]:
                tests = list(self.tests.values())
                if kwargs.get('endpoint_id'):
                    tests = [t for t in tests if t.endpoint_id == kwargs['endpoint_id']]
                return tests
        
        return MockRepository()
    
    def _load_openapi_spec(self):
        """Load OpenAPI specification from file."""
        if not self.openapi_spec_path or not self.openapi_spec_path.exists():
            self.logger.warning("OpenAPI spec path not found")
            return
        
        try:
            with open(self.openapi_spec_path, 'r') as f:
                if self.openapi_spec_path.suffix == '.yaml':
                    self._openapi_spec = yaml.safe_load(f)
                else:
                    self._openapi_spec = json.load(f)
            
            # Parse endpoints from spec
            self._parse_openapi_endpoints()
            
            self.logger.info(f"Loaded OpenAPI spec from {self.openapi_spec_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load OpenAPI spec: {e}")
    
    def _parse_openapi_endpoints(self):
        """Parse endpoints from OpenAPI specification."""
        if not self._openapi_spec or 'paths' not in self._openapi_spec:
            return
        
        for path, path_item in self._openapi_spec['paths'].items():
            for method, operation in path_item.items():
                if method in ['get', 'post', 'put', 'patch', 'delete']:
                    endpoint = ApiEndpoint(
                        path=path,
                        method=method.upper(),
                        operation_id=operation.get('operationId', f"{method}_{path}"),
                        summary=operation.get('summary', ''),
                        description=operation.get('description'),
                        tags=operation.get('tags', []),
                        parameters=operation.get('parameters', []),
                        request_body=operation.get('requestBody'),
                        responses=operation.get('responses', {}),
                        security=operation.get('security', []),
                        deprecated=operation.get('deprecated', False)
                    )
                    
                    # Extract version from path
                    if path.startswith('/v1'):
                        endpoint.version = 'v1'
                    elif path.startswith('/v2'):
                        endpoint.version = 'v2'
                    
                    self._endpoints_by_operation[endpoint.operation_id] = endpoint
                    
                    # Save to repository asynchronously
                    import asyncio
                    asyncio.create_task(self.repository.save_endpoint(endpoint))
    
    async def get_openapi_spec(self) -> Optional[Dict[str, Any]]:
        """
        Get the loaded OpenAPI specification.
        
        Returns:
            OpenAPI spec dictionary
        """
        if not self._openapi_spec and self.openapi_spec_path:
            self._load_openapi_spec()
        
        return self._openapi_spec
    
    async def get_endpoint_documentation(
        self,
        operation_id: Optional[str] = None,
        path: Optional[str] = None,
        method: Optional[str] = None
    ) -> Optional[ApiEndpoint]:
        """
        Get documentation for a specific endpoint.
        
        Args:
            operation_id: Operation ID from OpenAPI spec
            path: API path
            method: HTTP method
            
        Returns:
            ApiEndpoint if found
        """
        # Try by operation ID first
        if operation_id and operation_id in self._endpoints_by_operation:
            return self._endpoints_by_operation[operation_id]
        
        # Try by path and method
        if path and method:
            endpoints = await self.repository.list_endpoints()
            for endpoint in endpoints:
                if endpoint.path == path and endpoint.method == method.upper():
                    return endpoint
        
        return None
    
    async def list_endpoints(
        self,
        version: Optional[str] = None,
        tags: Optional[List[str]] = None,
        include_deprecated: bool = False
    ) -> List[ApiEndpoint]:
        """
        List API endpoints with optional filters.
        
        Args:
            version: Filter by API version
            tags: Filter by tags
            include_deprecated: Include deprecated endpoints
            
        Returns:
            List of ApiEndpoint instances
        """
        endpoints = await self.repository.list_endpoints(
            version=version,
            tags=tags
        )
        
        # Add from parsed spec if not in repository
        for endpoint in self._endpoints_by_operation.values():
            if endpoint.endpoint_id not in [e.endpoint_id for e in endpoints]:
                if (not version or endpoint.version == version) and \
                   (not tags or any(tag in endpoint.tags for tag in tags)):
                    endpoints.append(endpoint)
        
        # Filter deprecated
        if not include_deprecated:
            endpoints = [e for e in endpoints if not e.deprecated]
        
        return endpoints
    
    async def create_code_example(
        self,
        endpoint_id: str,
        language: str,
        title: str,
        code: str,
        description: Optional[str] = None,
        setup_code: Optional[str] = None,
        expected_output: Optional[str] = None
    ) -> CodeExample:
        """
        Create a code example for an endpoint.
        
        Args:
            endpoint_id: Endpoint this example is for
            language: Programming language
            title: Example title
            code: Example code
            description: Example description
            setup_code: Setup/initialization code
            expected_output: Expected output
            
        Returns:
            Created CodeExample
        """
        example = CodeExample(
            endpoint_id=endpoint_id,
            language=language,
            title=title,
            description=description,
            code=code,
            setup_code=setup_code,
            expected_output=expected_output
        )
        
        # Save example
        example = await self.repository.save_example(example)
        
        # Update cache
        if endpoint_id not in self._examples_by_endpoint:
            self._examples_by_endpoint[endpoint_id] = []
        self._examples_by_endpoint[endpoint_id].append(example)
        
        self.logger.info(
            f"Created code example for endpoint",
            extra={
                "example_id": example.example_id,
                "endpoint_id": endpoint_id,
                "language": language
            }
        )
        
        return example
    
    async def get_code_examples(
        self,
        endpoint_id: Optional[str] = None,
        language: Optional[str] = None
    ) -> List[CodeExample]:
        """
        Get code examples with optional filters.
        
        Args:
            endpoint_id: Filter by endpoint
            language: Filter by language
            
        Returns:
            List of CodeExample instances
        """
        return await self.repository.list_examples(
            endpoint_id=endpoint_id,
            language=language
        )
    
    async def generate_client_code(
        self,
        endpoint_id: str,
        language: str,
        include_auth: bool = True,
        include_error_handling: bool = True
    ) -> str:
        """
        Generate client code for an endpoint.
        
        Args:
            endpoint_id: Endpoint to generate code for
            language: Target language
            include_auth: Include authentication code
            include_error_handling: Include error handling
            
        Returns:
            Generated client code
        """
        endpoint = await self.get_endpoint_documentation(operation_id=endpoint_id)
        if not endpoint:
            raise ValueError(f"Endpoint {endpoint_id} not found")
        
        if language == "python":
            return self._generate_python_client(endpoint, include_auth, include_error_handling)
        elif language == "javascript":
            return self._generate_javascript_client(endpoint, include_auth, include_error_handling)
        elif language == "curl":
            return self._generate_curl_command(endpoint, include_auth)
        else:
            raise ValueError(f"Unsupported language: {language}")
    
    def _generate_python_client(
        self,
        endpoint: ApiEndpoint,
        include_auth: bool,
        include_error_handling: bool
    ) -> str:
        """Generate Python client code."""
        code_lines = ["import requests"]
        
        if include_auth:
            code_lines.extend([
                "import os",
                "",
                "# Authentication",
                "api_key = os.getenv('AURUM_API_KEY')",
                "headers = {'Authorization': f'Bearer {api_key}'}"
            ])
        else:
            code_lines.append("headers = {}")
        
        code_lines.extend([
            "",
            "# API request",
            f"url = 'http://localhost:8095{endpoint.path}'",
        ])
        
        # Add parameters
        if endpoint.parameters:
            code_lines.append("params = {")
            for param in endpoint.parameters:
                if param.get('in') == 'query':
                    code_lines.append(f"    '{param['name']}': 'value',")
            code_lines.append("}")
        
        # Add request body
        if endpoint.request_body:
            code_lines.extend([
                "data = {",
                "    # Add request body here",
                "}"
            ])
        
        # Make request
        method = endpoint.method.lower()
        if endpoint.request_body:
            code_lines.append(f"response = requests.{method}(url, headers=headers, json=data)")
        elif endpoint.parameters:
            code_lines.append(f"response = requests.{method}(url, headers=headers, params=params)")
        else:
            code_lines.append(f"response = requests.{method}(url, headers=headers)")
        
        if include_error_handling:
            code_lines.extend([
                "",
                "# Error handling",
                "if response.status_code == 200:",
                "    result = response.json()",
                "    print(result)",
                "else:",
                "    print(f'Error: {response.status_code}')",
                "    print(response.text)"
            ])
        else:
            code_lines.extend([
                "",
                "result = response.json()",
                "print(result)"
            ])
        
        return "\n".join(code_lines)
    
    def _generate_javascript_client(
        self,
        endpoint: ApiEndpoint,
        include_auth: bool,
        include_error_handling: bool
    ) -> str:
        """Generate JavaScript client code."""
        code_lines = ["// Using fetch API"]
        
        if include_auth:
            code_lines.extend([
                "const apiKey = process.env.AURUM_API_KEY;",
                "const headers = {",
                "    'Authorization': `Bearer ${apiKey}`,",
                "    'Content-Type': 'application/json'",
                "};"
            ])
        else:
            code_lines.extend([
                "const headers = {",
                "    'Content-Type': 'application/json'",
                "};"
            ])
        
        code_lines.extend([
            "",
            f"const url = 'http://localhost:8095{endpoint.path}';"
        ])
        
        # Build fetch options
        code_lines.extend([
            "const options = {",
            f"    method: '{endpoint.method}',",
            "    headers: headers"
        ])
        
        if endpoint.request_body:
            code_lines.extend([
                "    body: JSON.stringify({",
                "        // Add request body here",
                "    })"
            ])
        
        code_lines.append("};")
        
        # Make request
        if include_error_handling:
            code_lines.extend([
                "",
                "try {",
                "    const response = await fetch(url, options);",
                "    if (!response.ok) {",
                "        throw new Error(`HTTP error! status: ${response.status}`);",
                "    }",
                "    const data = await response.json();",
                "    console.log(data);",
                "} catch (error) {",
                "    console.error('Error:', error);",
                "}"
            ])
        else:
            code_lines.extend([
                "",
                "const response = await fetch(url, options);",
                "const data = await response.json();",
                "console.log(data);"
            ])
        
        return "\n".join(code_lines)
    
    def _generate_curl_command(self, endpoint: ApiEndpoint, include_auth: bool) -> str:
        """Generate curl command."""
        parts = ["curl"]
        
        # Method
        if endpoint.method != "GET":
            parts.append(f"-X {endpoint.method}")
        
        # Headers
        if include_auth:
            parts.append('-H "Authorization: Bearer $AURUM_API_KEY"')
        
        if endpoint.request_body:
            parts.append('-H "Content-Type: application/json"')
            parts.append('-d \'{"key": "value"}\'')
        
        # URL
        parts.append(f'"http://localhost:8095{endpoint.path}"')
        
        return " \\\n  ".join(parts)
    
    async def create_interactive_test(
        self,
        endpoint_id: str,
        name: str,
        request_template: Dict[str, Any],
        description: Optional[str] = None,
        parameter_defaults: Optional[Dict[str, Any]] = None
    ) -> InteractiveTest:
        """
        Create an interactive test for an endpoint.
        
        Args:
            endpoint_id: Endpoint to test
            name: Test name
            request_template: Request template
            description: Test description
            parameter_defaults: Default parameter values
            
        Returns:
            Created InteractiveTest
        """
        test = InteractiveTest(
            endpoint_id=endpoint_id,
            name=name,
            description=description,
            request_template=request_template,
            parameter_defaults=parameter_defaults or {}
        )
        
        # Save test
        test = await self.repository.save_test(test)
        
        self.logger.info(
            f"Created interactive test",
            extra={
                "test_id": test.test_id,
                "endpoint_id": endpoint_id,
                "name": name
            }
        )
        
        return test
    
    async def get_interactive_tests(
        self,
        endpoint_id: Optional[str] = None
    ) -> List[InteractiveTest]:
        """
        Get interactive tests.
        
        Args:
            endpoint_id: Filter by endpoint
            
        Returns:
            List of InteractiveTest instances
        """
        return await self.repository.list_tests(endpoint_id=endpoint_id)
    
    async def search_documentation(
        self,
        query: str,
        include_examples: bool = True,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Search API documentation.
        
        Args:
            query: Search query
            include_examples: Include code examples in results
            limit: Maximum results
            
        Returns:
            List of search results
        """
        results = []
        query_lower = query.lower()
        
        # Search endpoints
        endpoints = await self.list_endpoints()
        for endpoint in endpoints:
            score = 0
            
            # Check various fields
            if query_lower in endpoint.path.lower():
                score += 10
            if query_lower in endpoint.summary.lower():
                score += 5
            if endpoint.description and query_lower in endpoint.description.lower():
                score += 3
            if any(query_lower in tag.lower() for tag in endpoint.tags):
                score += 2
            
            if score > 0:
                result = {
                    "type": "endpoint",
                    "score": score,
                    "endpoint": endpoint.dict()
                }
                
                if include_examples:
                    examples = await self.get_code_examples(endpoint_id=endpoint.endpoint_id)
                    result["examples"] = [e.dict() for e in examples[:3]]
                
                results.append(result)
        
        # Sort by score and limit
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:limit]
    
    async def _emit_metric(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Emit a metric (placeholder for actual implementation)."""
        # TODO: Integrate with telemetry service
        self.logger.debug(f"Metric: {metric_name}={value}, tags={tags}")
    
    async def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache (placeholder)."""
        # TODO: Integrate with cache service
        return None
    
    async def _set_cache(self, key: str, value: Dict[str, Any], ttl: int):
        """Set value in cache (placeholder)."""
        # TODO: Integrate with cache service
        pass
