"""Example FastAPI application using the refactored architecture.

Demonstrates:
- DI container integration
- V2 routes registration
- Middleware stack configuration
- Health check endpoints
- Service layer integration
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
except ImportError:
    print("FastAPI not installed - this is a demo file")
    sys.exit(0)

from aurum.core.container import DependencyContainer, get_container
from aurum.core.settings import AurumSettings, get_settings
from aurum.api.middleware_stack import MiddlewareStackManager
from aurum.api.routes.v2 import get_v2_routers


def create_application(settings: AurumSettings = None) -> FastAPI:
    """Create FastAPI application with refactored architecture.
    
    Args:
        settings: Application settings (None = load from environment)
        
    Returns:
        Configured FastAPI application
    """
    settings = settings or get_settings()
    
    # Create FastAPI app
    app = FastAPI(
        title="Aurum API - Refactored",
        version="2.0.0",
        description="Modern async-first energy trading platform",
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # Initialize DI container and attach to app state
    container = get_container(settings)
    app.state.container = container
    app.state.di_container = container  # Alias
    app.state.settings = settings
    
    # Configure middleware stack
    middleware_manager = MiddlewareStackManager(settings)
    middleware_manager.configure_standard_stack()
    middleware_manager.apply_to_app(app)
    
    # Register v2 routes
    for router in get_v2_routers():
        app.include_router(router)
    
    # Add health check endpoints
    @app.get("/health")
    async def health_check():
        """Basic health check."""
        return {"status": "healthy", "service": "aurum-api"}
    
    @app.get("/health/detailed")
    async def detailed_health_check():
        """Detailed health check with service status."""
        all_health = container.get_all_service_health()
        container_metrics = container.get_container_metrics()
        
        return {
            "status": "healthy",
            "container": container_metrics,
            "services": {
                name: {
                    "healthy": health.is_healthy,
                    "successes": health.success_count,
                    "failures": health.failure_count,
                }
                for name, health in all_health.items()
            }
        }
    
    @app.get("/metrics")
    async def metrics_endpoint():
        """Prometheus-compatible metrics endpoint."""
        metrics = container.get_container_metrics()
        all_health = container.get_all_service_health()
        
        # Format for Prometheus
        lines = [
            f"# TYPE aurum_container_uptime_seconds gauge",
            f"aurum_container_uptime_seconds {metrics['uptime_seconds']}",
            f"# TYPE aurum_registered_services gauge",
            f"aurum_registered_services {metrics['registered_services']}",
            f"# TYPE aurum_service_health gauge",
        ]
        
        for service_name, health in all_health.items():
            healthy_value = 1 if health.is_healthy else 0
            lines.append(f'aurum_service_health{{service="{service_name}"}} {healthy_value}')
        
        return JSONResponse(content="\n".join(lines), media_type="text/plain")
    
    @app.on_event("startup")
    async def startup():
        """Startup event handler."""
        print("🚀 Starting Aurum API with refactored architecture")
        print(f"   - Container: {metrics['registered_services']} services registered")
        print(f"   - Routes: {len(app.routes)} endpoints")
        print(f"   - Middleware: Standard stack applied")
    
    @app.on_event("shutdown")
    async def shutdown():
        """Shutdown event handler."""
        print("👋 Shutting down Aurum API")
        await container.close_all()
        print("   - All services closed")
    
    return app


async def demo_api_usage():
    """Demonstrate API usage patterns."""
    print("=" * 60)
    print("Demo: API Usage Patterns")
    print("=" * 60)
    
    print("\nV2 API Endpoints Available:")
    print("  GET  /v2/curves?iso=PJM&market=DA&limit=100")
    print("  GET  /v2/curves/{curve_key}")
    print("  GET  /v2/curves/export?iso=PJM&format=json")
    print("  POST /v2/curves/cache/invalidate?iso=PJM")
    print()
    print("  GET  /v2/scenarios?limit=100")
    print("  POST /v2/scenarios")
    print("  GET  /v2/scenarios/{scenario_id}")
    print("  GET  /v2/scenarios/{scenario_id}/outputs")
    print()
    print("  GET  /v2/metadata/dimensions/{dataset}/{dimension}")
    print("  GET  /v2/metadata/search?q=power")
    print("  GET  /v2/metadata/locations/{iso}")
    print()
    print("  GET  /v2/ppa/contracts?counterparty=Acme")
    print("  GET  /v2/ppa/valuations?contract_id=C001")
    print("  GET  /v2/ppa/contracts/{contract_id}/risk")
    print()
    print("  GET  /v2/iso/lmp?iso=PJM&market_type=DA")
    print("  GET  /v2/iso/{iso}/nodes")
    print()
    
    print("All endpoints feature:")
    print("  - Dependency injection")
    print("  - Optional caching (use_cache parameter)")
    print("  - Standard error handling")
    print("  - Consistent response format")
    print("  - Service context support")
    print()


def main():
    """Main entry point."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 8 + "Refactored Architecture Integration" + " " * 14 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    # Create application
    settings = get_settings()
    app = create_application(settings)
    
    print("Application created successfully!")
    print(f"  - Routes: {len(app.routes)}")
    print(f"  - Middleware layers: Applied")
    print(f"  - DI Container: Initialized")
    print()
    
    # Run async demos
    asyncio.run(demo_di_container())
    asyncio.run(demo_service_with_caching())
    asyncio.run(demo_multiple_services())
    asyncio.run(demo_api_usage())
    
    print("=" * 60)
    print("To run the API server:")
    print("  uvicorn examples.fastapi_integration_example:app --reload")
    print()
    print("To access:")
    print("  - API Docs: http://localhost:8000/docs")
    print("  - Health: http://localhost:8000/health")
    print("  - Metrics: http://localhost:8000/metrics")
    print("=" * 60)
    print()


# Create app instance for uvicorn
app = create_application()

if __name__ == "__main__":
    main()

