# Refactoring Implementation Checklist

This checklist provides concrete implementation steps for key refactoring tasks.

## 1. Router Registry Migration

**Fix duplicate registration:**
```python
class RouterRegistry:
    def __init__(self):
        self._registered_routers: Set[str] = set()
    
    def register_router(self, router: APIRouter, name: str):
        if name in self._registered_routers:
            raise ValueError(f"Router {name} already registered")
        self._registered_routers.add(name)
        # ... registration logic
```

**Validation test:**
```python
def test_router_registry_prevents_duplicates():
    registry = RouterRegistry()
    router = APIRouter()
    
    registry.register_router(router, "test")
    
    with pytest.raises(ValueError, match="already registered"):
        registry.register_router(router, "test")
```

## 2. Service Layer Decomposition

**Implementation pattern:**
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

**Service factory:**
```python
def create_metadata_service(settings: AurumSettings) -> MetadataService:
    dao = MetadataDAO(trino_client=get_trino_client(settings))
    cache = get_cache_manager(settings)
    return MetadataService(dao, cache)
```

**Router update:**
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

## 3. Async DAO Pattern

**Base interface:**
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

**Implementation:**
```python
class TrinoDAO(AsyncDAO):
    async def query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with self.client.cursor() as cursor:
            await cursor.execute(query, params)
            return await cursor.fetchall()
```

## 4. Health Check Standardization

**Convert to async:**
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

**Provider pattern:**
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

## 5. Feature Flag Enhancement

**Dynamic implementation:**
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
```

## 6. Cache Management

**Extended manager:**
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

## 7. RFC7807 Error Handling

**Problem detail responses:**
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

## 8. API Documentation

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

## Testing Commands

### Contract Testing
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

## Monitoring Checklist

### Key Metrics
- [ ] API response times (p50, p95, p99)
- [ ] Error rates per endpoint
- [ ] Cache hit/miss ratios
- [ ] Database query performance
- [ ] Feature flag usage statistics

### Alert Thresholds
- [ ] P95 latency increase >20%
- [ ] Error rate increase >5%
- [ ] Cache miss rate increase >10%
- [ ] Any 5xx errors on health endpoints

## Rollback Procedures

### Immediate Rollback
1. Disable feature flags via environment variables
2. Git revert to last known good commit
3. Clear relevant cache keys
4. Verify all health endpoints return healthy

### Gradual Rollback
1. Reduce feature flag percentage
2. Monitor error rates and latency
3. Disable feature completely if issues persist
