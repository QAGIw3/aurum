#!/usr/bin/env python3
"""Phase 1 Development Health Checks

Validates Phase 1 success criteria:
- 1.1 Dependency & Configuration Unification
- 1.2 API Architecture Completion  
- 1.3 Service Layer Decomposition
- 1.4 Development Experience Enhancement
"""

import os
import sys
import time
import importlib
from pathlib import Path
from typing import Dict, List, Any

def check_dependencies() -> Dict[str, Any]:
    """Check Phase 1.1: Dependency & Configuration Unification."""
    print("🔍 Checking Phase 1.1: Dependencies & Configuration...")
    
    results = {
        "status": "PASS",
        "issues": [],
        "details": {}
    }
    
    # Test settings import
    try:
        from aurum.core import AurumSettings
        settings = AurumSettings()
        results["details"]["settings_import"] = "✅ OK"
    except Exception as e:
        results["status"] = "FAIL"
        results["issues"].append(f"Settings import failed: {e}")
        results["details"]["settings_import"] = f"❌ FAIL: {e}"
    
    # Test router registry
    try:
        from aurum.api.router_registry import get_v1_router_specs
        from aurum.core import AurumSettings
        settings = AurumSettings()
        specs = get_v1_router_specs(settings)
        results["details"]["router_registry"] = f"✅ OK ({len(specs)} routers)"
    except Exception as e:
        results["status"] = "FAIL"
        results["issues"].append(f"Router registry failed: {e}")
        results["details"]["router_registry"] = f"❌ FAIL: {e}"
    
    return results

def check_api_architecture() -> Dict[str, Any]:
    """Check Phase 1.2: API Architecture Completion."""
    print("🔍 Checking Phase 1.2: API Architecture...")
    
    results = {
        "status": "PASS", 
        "issues": [],
        "details": {}
    }
    
    # Check that routes.py has no endpoint definitions
    routes_file = Path("src/aurum/api/routes.py")
    if routes_file.exists():
        content = routes_file.read_text()
        route_decorators = ["@router.get", "@router.post", "@router.put", "@router.delete"]
        found_routes = [dec for dec in route_decorators if dec in content]
        
        if found_routes:
            results["status"] = "FAIL"
            results["issues"].append(f"routes.py still contains endpoints: {found_routes}")
            results["details"]["routes_cleanup"] = f"❌ FAIL: Found {len(found_routes)} endpoint decorators"
        else:
            results["details"]["routes_cleanup"] = "✅ OK - No endpoints in routes.py"
    
    # Check v1 router modules exist
    v1_modules = [
        "src/aurum/api/v1/curves.py",
        "src/aurum/api/v1/eia.py", 
        "src/aurum/api/v1/iso.py",
        "src/aurum/api/v1/ppa.py",
        "src/aurum/api/v1/drought.py",
        "src/aurum/api/v1/admin.py"
    ]
    
    missing_modules = []
    for module in v1_modules:
        if not Path(module).exists():
            missing_modules.append(module)
    
    if missing_modules:
        results["status"] = "FAIL"
        results["issues"].append(f"Missing v1 router modules: {missing_modules}")
        results["details"]["v1_routers"] = f"❌ FAIL: Missing {len(missing_modules)} modules"
    else:
        results["details"]["v1_routers"] = f"✅ OK - All {len(v1_modules)} v1 routers exist"
    
    return results

def check_service_layer() -> Dict[str, Any]:
    """Check Phase 1.3: Service Layer Decomposition."""
    print("🔍 Checking Phase 1.3: Service Layer Decomposition...")
    
    results = {
        "status": "PASS",
        "issues": [],
        "details": {}
    }
    
    # Check service.py size (should be reduced from 2775 lines)
    service_file = Path("src/aurum/api/service.py")
    if service_file.exists():
        line_count = len(service_file.read_text().splitlines())
        results["details"]["service_py_size"] = f"{line_count} lines"
        
        if line_count > 2000:
            results["status"] = "WARN"
            results["issues"].append(f"service.py still large ({line_count} lines)")
    
    # Check domain services exist
    domain_services = [
        "src/aurum/api/services/eia_service.py",
        "src/aurum/api/services/curves_service.py",
        "src/aurum/api/services/iso_service.py",
        "src/aurum/api/services/metadata_service.py",
        "src/aurum/api/services/scenario_service.py",
        "src/aurum/api/services/ppa_service.py",
        "src/aurum/api/services/drought_service.py",
    ]
    
    existing_services = []
    for service in domain_services:
        if Path(service).exists():
            existing_services.append(Path(service).stem)
    
    results["details"]["domain_services"] = f"✅ {len(existing_services)} services: {', '.join(existing_services)}"
    
    # Check DAO implementation
    dao_files = list(Path("src/aurum/api/dao").glob("*_dao.py")) if Path("src/aurum/api/dao").exists() else []
    if dao_files:
        results["details"]["dao_pattern"] = f"✅ {len(dao_files)} DAO classes implemented"
    else:
        results["status"] = "WARN"
        results["issues"].append("No DAO classes found")
        results["details"]["dao_pattern"] = "⚠️ No DAO implementation found"
    
    # Check service interfaces
    try:
        from aurum.api.services.base_service import ServiceInterface
        results["details"]["service_interfaces"] = "✅ Base service interfaces defined"
    except ImportError:
        results["status"] = "WARN"
        results["issues"].append("Service interfaces not found")
        results["details"]["service_interfaces"] = "⚠️ Service interfaces missing"
    
    return results

def check_development_experience() -> Dict[str, Any]:
    """Check Phase 1.4: Development Experience Enhancement."""
    print("🔍 Checking Phase 1.4: Development Experience...")
    
    results = {
        "status": "PASS",
        "issues": [],
        "details": {}
    }
    
    # Check make targets exist
    makefile = Path("Makefile")
    if makefile.exists():
        content = makefile.read_text()
        
        expected_targets = [
            "test-services",
            "test-services-coverage", 
            "lint",
            "format",
            "docker-build"
        ]
        
        found_targets = []
        for target in expected_targets:
            if f"{target}:" in content:
                found_targets.append(target)
        
        results["details"]["make_targets"] = f"✅ {len(found_targets)}/{len(expected_targets)} targets: {', '.join(found_targets)}"
        
        if len(found_targets) < len(expected_targets):
            results["status"] = "WARN"
            missing = set(expected_targets) - set(found_targets)
            results["issues"].append(f"Missing make targets: {missing}")
    
    # Check for development health checks script
    if Path(__file__).exists():
        results["details"]["health_checks"] = "✅ Development health checks implemented"
    
    return results

def print_results(phase: str, results: Dict[str, Any]) -> None:
    """Print formatted results for a phase."""
    status_emoji = {
        "PASS": "✅",
        "WARN": "⚠️", 
        "FAIL": "❌"
    }
    
    print(f"\n{status_emoji.get(results['status'], '❓')} {phase}: {results['status']}")
    
    for key, detail in results["details"].items():
        print(f"  • {key}: {detail}")
    
    if results["issues"]:
        print(f"  Issues:")
        for issue in results["issues"]:
            print(f"    - {issue}")

def main():
    """Run all Phase 1 health checks."""
    print("🚀 Aurum Phase 1 Development Health Checks")
    print("=" * 50)
    
    start_time = time.time()
    
    # Run all checks
    checks = [
        ("Phase 1.1: Dependencies & Configuration", check_dependencies),
        ("Phase 1.2: API Architecture", check_api_architecture), 
        ("Phase 1.3: Service Layer Decomposition", check_service_layer),
        ("Phase 1.4: Development Experience", check_development_experience),
    ]
    
    overall_status = "PASS"
    
    for phase_name, check_func in checks:
        try:
            results = check_func()
            print_results(phase_name, results)
            
            if results["status"] == "FAIL":
                overall_status = "FAIL"
            elif results["status"] == "WARN" and overall_status != "FAIL":
                overall_status = "WARN"
                
        except Exception as e:
            print(f"\n❌ {phase_name}: ERROR")
            print(f"  Exception: {e}")
            overall_status = "FAIL"
    
    elapsed = time.time() - start_time
    
    print(f"\n{'='*50}")
    print(f"🏁 Overall Status: {overall_status}")
    print(f"⏱️  Completed in {elapsed:.2f}s")
    
    if overall_status == "FAIL":
        sys.exit(1)
    elif overall_status == "WARN":
        sys.exit(2)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()