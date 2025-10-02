"""Example usage of the unified dependency injection container.

Demonstrates how to register services, resolve dependencies, and use the
container in different contexts (API routes, background tasks, scripts).
"""

import asyncio
from aurum.core.container import DependencyContainer, ServiceLifetime, get_container
from aurum.core.settings import AurumSettings


async def example_basic_usage():
    """Basic container usage with service registration."""
    print("=== Basic DI Container Usage ===\n")
    
    # Get the global container
    container = get_container()
    
    # Register services
    from aurum.services.core import CurveService
    from aurum.data.repositories import CurveRepository
    
    # Register repository first
    container.register(
        CurveRepository,
        lambda: CurveRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # Register service that depends on repository
    async def create_curve_service():
        repo = await container.get(CurveRepository)
        await repo.initialize()
        return CurveService(repo)
    
    container.register(
        CurveService,
        create_curve_service,
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # Resolve service
    curve_service = await container.get(CurveService)
    print(f"Resolved CurveService: {curve_service}")
    print()


async def example_fastapi_integration():
    """Example of using container with FastAPI."""
    print("=== FastAPI Integration Example ===\n")
    
    print("""
from fastapi import APIRouter, Depends
from aurum.core.container import get_service
from aurum.services.core import CurveService

router = APIRouter()

async def get_curve_service() -> CurveService:
    return await get_service(CurveService)

@router.get("/curves")
async def list_curves(
    iso: str,
    service: CurveService = Depends(get_curve_service)
):
    result = await service.get_curves(iso=iso)
    return result.data
    """)
    print()


async def example_service_lifecycle():
    """Example of service lifecycle management."""
    print("=== Service Lifecycle Management ===\n")
    
    # Create container
    container = DependencyContainer()
    
    # Register services with different lifetimes
    
    # Singleton - shared across all requests
    container.register(
        AurumSettings,
        lambda: AurumSettings(),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # Transient - new instance each time
    class TransientService:
        def __init__(self):
            self.id = id(self)
    
    container.register(
        TransientService,
        lambda: TransientService(),
        lifetime=ServiceLifetime.TRANSIENT
    )
    
    # Get singleton multiple times - same instance
    settings1 = await container.get(AurumSettings)
    settings2 = await container.get(AurumSettings)
    print(f"Singleton instances are same: {settings1 is settings2}")
    
    # Get transient multiple times - different instances
    transient1 = await container.get(TransientService)
    transient2 = await container.get(TransientService)
    print(f"Transient instances are different: {transient1 is not transient2}")
    print(f"Transient1 ID: {transient1.id}, Transient2 ID: {transient2.id}")
    print()
    
    # Clean up
    await container.close_all()
    print("Container closed - all singletons cleaned up\n")


async def example_service_composition():
    """Example of composing services with dependencies."""
    print("=== Service Composition Example ===\n")
    
    container = DependencyContainer()
    
    # Register dependencies in order
    
    # 1. Settings (no dependencies)
    container.register_singleton(AurumSettings, AurumSettings())
    
    # 2. Repositories (depend on settings)
    from aurum.data.repositories import CurveRepository, MetadataRepository
    
    container.register(
        CurveRepository,
        lambda: CurveRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    container.register(
        MetadataRepository,
        lambda: MetadataRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # 3. Services (depend on repositories)
    from aurum.services.core import CurveService, MetadataService
    
    async def create_curve_service():
        repo = await container.get(CurveRepository)
        await repo.initialize()
        return CurveService(repo)
    
    async def create_metadata_service():
        repo = await container.get(MetadataRepository)
        await repo.initialize()
        return MetadataService(repo)
    
    container.register(
        CurveService,
        create_curve_service,
        lifetime=ServiceLifetime.SINGLETON
    )
    
    container.register(
        MetadataService,
        create_metadata_service,
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # Resolve services
    curve_service = await container.get(CurveService)
    metadata_service = await container.get(MetadataService)
    
    print(f"Resolved CurveService: {curve_service}")
    print(f"Resolved MetadataService: {metadata_service}")
    print("\nDependency graph:")
    print("  CurveService → CurveRepository → Settings")
    print("  MetadataService → MetadataRepository → Settings")
    print()
    
    # Clean up
    await container.close_all()


async def main():
    """Run all examples."""
    print("\n" + "="*60)
    print("DEPENDENCY INJECTION CONTAINER EXAMPLES")
    print("="*60 + "\n")
    
    await example_basic_usage()
    await example_fastapi_integration()
    await example_service_lifecycle()
    await example_service_composition()
    
    print("="*60)
    print("DI CONTAINER EXAMPLES COMPLETE")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())

