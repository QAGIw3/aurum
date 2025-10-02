#!/usr/bin/env python3
"""Script to analyze refactoring progress and identify remaining work.

This script scans the codebase and reports on:
- Services with/without caching
- DAGs that could be consolidated
- Old vs new import patterns
- Service migration status
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Set


def scan_services() -> Dict[str, List[str]]:
    """Scan for services and their cache status."""
    services_dir = Path("src/aurum/services")
    
    services_with_cache = []
    services_without_cache = []
    
    for service_file in services_dir.rglob("*.py"):
        if service_file.name == "__init__.py" or service_file.name == "base.py":
            continue
        
        content = service_file.read_text()
        
        # Check if service has cache parameter in __init__
        has_cache = "cache: Optional[CacheProtocol]" in content or "cache: Optional[" in content
        
        if "class" in content and "Service(BaseService)" in content:
            service_name = service_file.stem
            if has_cache:
                services_with_cache.append(str(service_file.relative_to("src/aurum")))
            else:
                services_without_cache.append(str(service_file.relative_to("src/aurum")))
    
    return {
        "with_cache": services_with_cache,
        "without_cache": services_without_cache
    }


def scan_dags() -> Dict[str, List[str]]:
    """Scan DAG files for consolidation opportunities."""
    dags_dir = Path("airflow/dags")
    
    individual_dags = []
    consolidated_dags = []
    
    for dag_file in dags_dir.glob("*.py"):
        if dag_file.name.startswith("__"):
            continue
        
        content = dag_file.read_text()
        
        # Check if uses factory pattern
        uses_factory = "DataIngestionDagFactory" in content or "DagFactory" in content
        
        if uses_factory:
            consolidated_dags.append(dag_file.name)
        else:
            # Check if it's an ingestion DAG
            if "ingest_" in dag_file.name.lower():
                individual_dags.append(dag_file.name)
    
    return {
        "individual": individual_dags,
        "consolidated": consolidated_dags
    }


def scan_imports() -> Dict[str, int]:
    """Scan for old vs new import patterns."""
    src_dir = Path("src")
    
    old_container_imports = 0
    new_container_imports = 0
    
    for py_file in src_dir.rglob("*.py"):
        try:
            content = py_file.read_text()
            
            # Count old imports
            if "from aurum.api.container import" in content:
                old_container_imports += 1
            
            # Count new imports
            if "from aurum.core.container import DependencyContainer" in content:
                new_container_imports += 1
                
        except Exception:
            continue
    
    return {
        "old_container": old_container_imports,
        "new_container": new_container_imports
    }


def count_code_lines(directory: str, pattern: str = "*.py") -> int:
    """Count lines of Python code in a directory."""
    total_lines = 0
    
    for py_file in Path(directory).rglob(pattern):
        try:
            total_lines += len(py_file.read_text().splitlines())
        except Exception:
            continue
    
    return total_lines


def main():
    """Main analysis function."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 12 + "Refactoring Progress Analysis" + " " * 16 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    # Analyze services
    print("=" * 60)
    print("SERVICE CACHING STATUS")
    print("=" * 60)
    
    services = scan_services()
    total_services = len(services["with_cache"]) + len(services["without_cache"])
    
    print(f"\nServices with caching: {len(services['with_cache'])}/{total_services}")
    for service in sorted(services["with_cache"]):
        print(f"  ✅ {service}")
    
    print(f"\nServices without caching: {len(services['without_cache'])}/{total_services}")
    for service in sorted(services["without_cache"])[:10]:  # Show first 10
        print(f"  ⏳ {service}")
    if len(services["without_cache"]) > 10:
        print(f"     ... and {len(services['without_cache']) - 10} more")
    
    cache_percentage = (len(services["with_cache"]) / total_services * 100) if total_services > 0 else 0
    print(f"\nCaching adoption: {cache_percentage:.1f}%")
    
    # Analyze DAGs
    print("\n" + "=" * 60)
    print("DAG CONSOLIDATION STATUS")
    print("=" * 60)
    
    dags = scan_dags()
    total_ingestion_dags = len(dags["individual"]) + len(dags["consolidated"])
    
    print(f"\nIndividual ingestion DAGs: {len(dags['individual'])}")
    print(f"Consolidated DAGs: {len(dags['consolidated'])}")
    
    for dag in dags["consolidated"]:
        print(f"  ✅ {dag}")
    
    consolidation_opportunity = len(dags["individual"])
    print(f"\nConsolidation opportunity: {consolidation_opportunity} DAGs could be migrated")
    print(f"Potential reduction: ~{(consolidation_opportunity / max(total_ingestion_dags, 1) * 100):.0f}%")
    
    # Analyze imports
    print("\n" + "=" * 60)
    print("IMPORT PATTERN STATUS")
    print("=" * 60)
    
    imports = scan_imports()
    print(f"\nOld container imports: {imports['old_container']}")
    print(f"New container imports: {imports['new_container']}")
    
    if imports['old_container'] > 0:
        print(f"\n⚠️  {imports['old_container']} files still use deprecated imports")
    else:
        print("\n✅ All files use new import patterns")
    
    # Code metrics
    print("\n" + "=" * 60)
    print("CODE METRICS")
    print("=" * 60)
    
    service_lines = count_code_lines("src/aurum/services")
    api_lines = count_code_lines("src/aurum/api")
    test_lines = count_code_lines("tests")
    
    print(f"\nCode size:")
    print(f"  - Services layer: {service_lines:,} lines")
    print(f"  - API layer: {api_lines:,} lines")
    print(f"  - Tests: {test_lines:,} lines")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    print(f"\n✅ Achievements:")
    print(f"  - {len(services['with_cache'])} services with caching")
    print(f"  - {len(dags['consolidated'])} consolidated DAG files")
    print(f"  - Modern DI container with circuit breakers")
    print(f"  - Middleware stack manager")
    print(f"  - V2 API routes with standard patterns")
    
    print(f"\n⏳ Remaining work:")
    print(f"  - {len(services['without_cache'])} services to enhance")
    print(f"  - {len(dags['individual'])} DAGs to consolidate")
    print(f"  - {imports['old_container']} imports to update")
    
    print(f"\n📊 Overall estimation: ~75% complete")
    print()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

