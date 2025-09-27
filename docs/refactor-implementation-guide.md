# Aurum Refactoring Implementation Guide

This guide provides concrete implementation steps for the 10 prioritized refactoring tasks identified in the Aurum codebase analysis.

## Quick Start Tasks (Week 1)

### Task 1: Remove Duplicate HTTP Utilities

**Files to modify:**
- `src/aurum/api/http/__init__.py` (canonical implementations)
- `src/aurum/api/scenarios/http.py` (remove duplicates)
- Any imports of scenarios.http utilities

**Implementation steps:**
```bash
# 1. Compare implementations
diff src/aurum/api/http/pagination.py src/aurum/api/scenarios/http.py

# 2. Update imports in scenarios modules
grep -r "from.*scenarios.http" src/aurum/api/scenarios/
grep -r "from.*\.http" src/aurum/api/scenarios/

# 3. Replace with canonical imports
# Before: from aurum.api.scenarios.http import respond_with_etag
# After:  from aurum.api.http import respond_with_etag
```

**Validation:**
- Run existing scenario tests to ensure no regressions
- Verify response formats remain identical
- Check for unused imports after cleanup

### Task 5: Dependency Version Alignment

**Files to modify:**
- `pyproject.toml`
- `constraints/` (new directory)

**Implementation steps:**
```bash
# 1. Create constraints file
mkdir -p constraints/
cat > constraints/base.txt << EOF
# Core dependencies with pinned versions
fastapi==0.115.6
uvicorn[standard]==0.32.0
pydantic==2.10.3
# ... other shared deps
EOF

# 2. Refactor pyproject.toml groups
# Remove duplicates from api/worker/ingest groups
# Reference constraints file in each group
```

## Foundation Tasks (Weeks 2-4)

### Task 4: Standardize Health Check Infrastructure

**Files to modify:**
- `src/aurum/api/health.py`
- `src/aurum/api/health_checks.py` 
- Health check endpoints in routers

**Key changes:**
```python
# Before: Blocking health check
def check_schema_registry():
    with requests.get(url) as response:
        return response.status_code == 200

# After: Async health check  
async def check_schema_registry():
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.status_code == 200
```

**New health check provider pattern:**
```python
class HealthCheckProvider(ABC):
    @abstractmethod
    async def check_health(self, target: str) -> HealthCheckResult:
        pass

class TrinoHealthProvider(HealthCheckProvider):
    async def check_health(self, target: str) -> HealthCheckResult:
        # Implement Trino-specific health check
        pass
```

## Architecture Tasks (Weeks 5-12)

### Task 2: Complete Router Registry Migration

**Files to modify:**
- `src/aurum/api/router_registry.py`
- `src/aurum/api/v1/*.py` (domain routers)
- `src/aurum/api/routes.py` (reduce to utilities only)

**Implementation approach:**
```python
# Fix duplicate registration
class RouterRegistry:
    def __init__(self):
        self._registered_routers: Set[str] = set()
    
    def register_router(self, router: APIRouter, name: str):
        if name in self._registered_routers:
            raise ValueError(f"Router {name} already registered")
        self._registered_routers.add(name)
        # ... registration logic
```

**Validation tests:**
```python
def test_router_registry_prevents_duplicates():
    registry = RouterRegistry()
    router = APIRouter()
    
    registry.register_router(router, "test")
    
    with pytest.raises(ValueError, match="already registered"):
        registry.register_router(router, "test")
```

### Task 3: Decompose Monolithic Service Layer

**Implementation strategy:**
1. **Start with least coupled domain (e.g., metadata)**
2. **Extract service interface:**
```python
class MetadataService:
    def __init__(self, dao: MetadataDAO, cache: CacheManager):
        self.dao = dao
        self.cache = cache
    
    async def get_metadata(self, filters: Dict[str, Any]) -> List[MetadataRecord]:
        cache_key = self._build_cache_key(filters)
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
        
        result = await self.dao.query_metadata(filters)
        await self.cache.set(cache_key, result, ttl=300)
        return result
```

3. **Create service factory:**
```python
def create_metadata_service(settings: AurumSettings) -> MetadataService:
    dao = MetadataDAO(trino_client=get_trino_client(settings))
    cache = get_cache_manager(settings)
    return MetadataService(dao, cache)
```

4. **Update router to use service:**
```python
# Before: Direct service.py import
from . import service

@router.get("/metadata")
async def get_metadata():
    return service.get_metadata_records()

# After: Dependency injection
@router.get("/metadata")  
async def get_metadata(
    metadata_service: MetadataService = Depends(get_metadata_service)
):
    return await metadata_service.get_metadata()
```

### Task 6: Implement Async DAO Pattern

**Base DAO interface:**
```python
class AsyncDAO(ABC):
    def __init__(self, client: Any):
        self.client = client
    
    @abstractmethod
    async def query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass
    
    @abstractmethod  
    async def execute(self, query: str, params: Dict[str, Any]) -> int:
        pass
```

**Trino DAO implementation:**
```python
class TrinoDAO(AsyncDAO):
    async def query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with self.client.cursor() as cursor:
            await cursor.execute(query, params)
            return await cursor.fetchall()
```

## Enhancement Tasks (Weeks 13-16)

### Task 7: Enhance Feature Flag System

**Dynamic feature flag implementation:**
```python
class FeatureFlagManager:
    def __init__(self, config_source: str = "env"):
        self.flags: Dict[str, FeatureFlag] = {}
        self.config_source = config_source
    
    async def is_enabled(self, flag_name: str, context: Dict[str, Any] = None) -> bool:
        flag = self.flags.get(flag_name)
        if not flag:
            return False
        
        if flag.percentage_rollout:
            return self._check_percentage_rollout(flag, context)
        
        return flag.enabled
    
    def _check_percentage_rollout(self, flag: FeatureFlag, context: Dict[str, Any]) -> bool:
        # Implement consistent hash-based percentage rollout
        pass
```

### Task 8: Centralize Cache Management

**Extended cache manager:**
```python
class CacheManager:
    def __init__(self, redis_client: redis.Redis):
        self.client = redis_client
        self.policies: Dict[str, CachePolicy] = {}
    
    async def get_with_policy(self, key: str, policy_name: str) -> Any:
        policy = self.policies[policy_name]
        value = await self.client.get(key)
        
        if value is None and policy.warm_up_function:
            value = await policy.warm_up_function(key)
            await self.set_with_policy(key, value, policy_name)
        
        return value
```

### Task 9: Strengthen Error Handling

**RFC7807 error responses:**
```python
class ProblemDetail(BaseModel):
    type: str
    title: str
    status: int
    detail: Optional[str] = None
    instance: Optional[str] = None

async def create_error_response(
    error: Exception, 
    request: Request
) -> JSONResponse:
    if isinstance(error, ValidationError):
        problem = ProblemDetail(
            type="https://api.aurum.com/problems/validation-error",
            title="Validation Error", 
            status=422,
            detail=str(error),
            instance=str(request.url)
        )
    else:
        problem = ProblemDetail(
            type="https://api.aurum.com/problems/internal-error",
            title="Internal Server Error",
            status=500,
            instance=str(request.url)
        )
    
    return JSONResponse(
        status_code=problem.status,
        content=problem.dict(),
        headers={"Content-Type": "application/problem+json"}
    )
```

### Task 10: API Documentation Generation

**OpenAPI customization:**
```python
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="Aurum Energy Trading API",
        version="2.0.0",
        description="Energy trading platform API with real-time data",
        routes=app.routes,
    )
    
    # Add custom extensions
    openapi_schema["x-logo"] = {"url": "https://aurum.com/logo.png"}
    openapi_schema["servers"] = [
        {"url": "https://api.aurum.com/v1", "description": "V1 (Legacy)"},
        {"url": "https://api.aurum.com/v2", "description": "V2 (Current)"}
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema
```

## Testing Strategy

### Contract Testing with Schemathesis
```bash
# Install schemathesis
pip install schemathesis

# Run contract tests
schemathesis run http://localhost:8000/openapi.json \
  --checks all \
  --hypothesis-max-examples=50
```

### Performance Testing
```bash
# Baseline performance testing
k6 run --vus 10 --duration 30s performance-baseline.js

# After refactoring comparison  
k6 run --vus 10 --duration 30s performance-after.js
```

### Feature Flag Testing
```python
@pytest.mark.parametrize("flag_state", [True, False])
def test_v1_split_ppa_flag(flag_state):
    with patch.dict(os.environ, {"AURUM_API_V1_SPLIT_PPA": str(int(flag_state))}):
        app = create_app()
        routes = [route.path for route in app.routes]
        
        if flag_state:
            assert "/v1/ppa" in routes  # Split router active
        else:
            assert "/v1/ppa" not in routes  # Monolith handles PPA
```

## Rollback Procedures

### Immediate Rollback Steps
1. **Environment Variable Rollback:** Disable feature flags
2. **Code Rollback:** Git revert to last known good commit
3. **Cache Invalidation:** Clear relevant cache keys
4. **Health Check Verification:** Confirm all endpoints return healthy

### Gradual Rollback
1. **Percentage Decrease:** Reduce feature flag percentage
2. **Monitor Metrics:** Watch error rates and latency  
3. **Full Disable:** Turn off feature completely if issues persist

## Monitoring and Observability

### Key Metrics to Track
- API response times (p50, p95, p99)
- Error rates per endpoint
- Cache hit/miss ratios
- Database query performance
- Feature flag usage statistics

### Alerting Thresholds
- P95 latency increase >20% 
- Error rate increase >5%
- Cache miss rate increase >10%
- Any 5xx errors on health endpoints

---

*This implementation guide provides concrete steps for executing the 10 refactoring tasks. Each task should be implemented with comprehensive testing, monitoring, and rollback procedures to ensure system stability throughout the refactoring process.*