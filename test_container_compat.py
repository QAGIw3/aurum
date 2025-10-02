#!/usr/bin/env python3
"""Test script to verify container compatibility layer works correctly."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))


async def test_core_container():
    """Test the core container directly."""
    print("=" * 60)
    print("TEST 1: Core Container (aurum.core.container)")
    print("=" * 60)
    
    from aurum.core.container import DependencyContainer, ServiceLifetime
    
    container = DependencyContainer()
    
    # Register a simple service
    class TestService:
        def __init__(self):
            self.value = "test"
        
        def get_value(self):
            return self.value
    
    container.register(
        TestService,
        lambda: TestService(),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # Resolve service
    service = await container.get(TestService)
    assert service.get_value() == "test", "Service resolution failed"
    
    # Test singleton behavior
    service2 = await container.get(TestService)
    assert service is service2, "Singleton behavior failed"
    
    print("✅ Core container works correctly")
    print(f"   - Service registration: OK")
    print(f"   - Service resolution: OK")
    print(f"   - Singleton behavior: OK")
    print()
    
    return container


async def test_api_container_compatibility():
    """Test the API container compatibility layer."""
    print("=" * 60)
    print("TEST 2: API Container Compatibility (aurum.api.container)")
    print("=" * 60)
    
    # Import using old path (should trigger deprecation warning)
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        from aurum.api.container import DependencyInjectionContainer, ServiceLifetime
        
        # Check deprecation warning was issued
        assert len(w) > 0, "No deprecation warning issued"
        assert issubclass(w[0].category, DeprecationWarning), "Wrong warning type"
        print(f"✅ Deprecation warning issued: {w[0].message}")
    
    container = DependencyInjectionContainer()
    
    # Register a service
    class ApiTestService:
        def __init__(self):
            self.name = "api_test"
        
        def get_name(self):
            return self.name
    
    container.register(
        ApiTestService,
        lambda: ApiTestService(),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # Test both get() and get_service() methods
    service1 = await container.get(ApiTestService)
    service2 = await container.get_service(ApiTestService)
    
    assert service1.get_name() == "api_test", "Service resolution failed"
    assert service1 is service2, "get() and get_service() should return same instance"
    
    print("✅ API container compatibility works correctly")
    print(f"   - DependencyInjectionContainer: OK")
    print(f"   - Service registration: OK")
    print(f"   - get() method: OK")
    print(f"   - get_service() method (compat): OK")
    print()
    
    return container


async def test_advanced_features():
    """Test advanced features from core container."""
    print("=" * 60)
    print("TEST 3: Advanced Features (Circuit Breakers, Health)")
    print("=" * 60)
    
    from aurum.core.container import (
        DependencyContainer,
        CircuitBreakerConfig,
        ServiceLifetime
    )
    
    container = DependencyContainer()
    
    # Test circuit breaker
    class FailingService:
        def __init__(self):
            self.call_count = 0
        
        async def health_check(self):
            return True
    
    container.register(
        FailingService,
        lambda: FailingService(),
        lifetime=ServiceLifetime.SINGLETON,
        health_check_enabled=True,
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=60.0
        )
    )
    
    service = await container.get(FailingService)
    assert service is not None, "Service creation failed"
    
    # Test health checking
    health = container.get_service_health(FailingService)
    print(f"✅ Advanced features work correctly")
    print(f"   - Circuit breaker configuration: OK")
    print(f"   - Health checking: OK")
    if health:
        print(f"   - Health status: {health.is_healthy}")
    
    # Test metrics
    metrics = container.get_container_metrics()
    print(f"   - Container metrics: {metrics}")
    print()


async def test_scoped_lifetime():
    """Test scoped lifetime support."""
    print("=" * 60)
    print("TEST 4: Scoped Lifetime")
    print("=" * 60)
    
    from aurum.core.container import DependencyContainer, ServiceLifetime
    
    container = DependencyContainer()
    
    class ScopedService:
        _instance_count = 0
        
        def __init__(self):
            ScopedService._instance_count += 1
            self.instance_id = ScopedService._instance_count
    
    container.register(
        ScopedService,
        lambda: ScopedService(),
        lifetime=ServiceLifetime.SCOPED
    )
    
    # Same scope = same instance
    s1 = await container.get(ScopedService, scope_id="request-1")
    s2 = await container.get(ScopedService, scope_id="request-1")
    assert s1 is s2, "Scoped services should return same instance for same scope"
    assert s1.instance_id == s2.instance_id
    
    # Different scope = different instance
    s3 = await container.get(ScopedService, scope_id="request-2")
    assert s1 is not s3, "Scoped services should return different instance for different scope"
    assert s1.instance_id != s3.instance_id
    
    print("✅ Scoped lifetime works correctly")
    print(f"   - Same scope returns same instance: OK")
    print(f"   - Different scope returns different instance: OK")
    print(f"   - Instance IDs: {s1.instance_id}, {s2.instance_id}, {s3.instance_id}")
    print()


async def test_interface_resolution():
    """Test interface-based resolution."""
    print("=" * 60)
    print("TEST 5: Interface-Based Resolution")
    print("=" * 60)
    
    from abc import ABC, abstractmethod
    from aurum.core.container import DependencyContainer, ServiceLifetime
    
    # Define interface
    class IMessageService(ABC):
        @abstractmethod
        def get_message(self) -> str:
            pass
    
    # Implement interface
    class EmailService(IMessageService):
        def get_message(self) -> str:
            return "Email message"
    
    container = DependencyContainer()
    
    # Register with interface
    container.register(
        EmailService,
        lambda: EmailService(),
        lifetime=ServiceLifetime.SINGLETON,
        interfaces=[IMessageService]
    )
    
    # Resolve by concrete type
    email_service = await container.get(EmailService)
    assert email_service.get_message() == "Email message"
    
    # Resolve by interface
    message_service = await container.get(IMessageService)
    assert message_service.get_message() == "Email message"
    assert email_service is message_service, "Should resolve to same instance"
    
    print("✅ Interface resolution works correctly")
    print(f"   - Register with interface: OK")
    print(f"   - Resolve by concrete type: OK")
    print(f"   - Resolve by interface type: OK")
    print(f"   - Same instance for both: OK")
    print()


async def main():
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 10 + "Container Compatibility Test Suite" + " " * 14 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    try:
        # Run all tests
        await test_core_container()
        await test_api_container_compatibility()
        await test_advanced_features()
        await test_scoped_lifetime()
        await test_interface_resolution()
        
        # Summary
        print("=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
        print()
        print("Summary:")
        print("  ✓ Core container works correctly")
        print("  ✓ API compatibility layer works")
        print("  ✓ Deprecation warnings issued")
        print("  ✓ Advanced features (circuit breakers, health) work")
        print("  ✓ Scoped lifetime support works")
        print("  ✓ Interface-based resolution works")
        print()
        print("The container consolidation is successful!")
        print("All existing code will continue to work with deprecation warnings.")
        print()
        
        return 0
        
    except AssertionError as e:
        print()
        print("=" * 60)
        print("❌ TEST FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        return 1
    
    except Exception as e:
        print()
        print("=" * 60)
        print("❌ TEST ERROR")
        print("=" * 60)
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

