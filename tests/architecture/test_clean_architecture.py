"""Architectural fitness tests for clean architecture boundaries.

These tests ensure that the architectural rules are enforced:
1. Domain layer has no dependencies
2. Application layer depends only on domain
3. Infrastructure can depend on application and domain
4. Presentation can depend on all layers but through interfaces
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import os
from pathlib import Path
from typing import Dict, List, Set

import pytest


def get_imports_from_file(file_path: Path) -> Set[str]:
    """Extract all imports from a Python file.
    
    Args:
        file_path: Path to the Python file
        
    Returns:
        Set of imported module names
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read(), filename=str(file_path))
    except SyntaxError:
        return set()
    
    imports = set()
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])
    
    return imports


def get_all_python_files(directory: Path) -> List[Path]:
    """Get all Python files in a directory recursively.
    
    Args:
        directory: Root directory to search
        
    Returns:
        List of Python file paths
    """
    return list(directory.rglob('*.py'))


def get_aurum_imports(file_path: Path) -> Set[str]:
    """Get all Aurum internal imports from a file.
    
    Args:
        file_path: Path to the Python file
        
    Returns:
        Set of Aurum module paths (e.g., 'aurum.domain', 'aurum.application')
    """
    all_imports = get_imports_from_file(file_path)
    aurum_imports = set()
    
    for imp in all_imports:
        if imp == 'aurum':
            # Need to check the full import statement
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    tree = ast.parse(f.read())
                    for node in ast.walk(tree):
                        if isinstance(node, ast.ImportFrom) and node.module:
                            if node.module.startswith('aurum.'):
                                # Get second-level module (e.g., 'domain', 'application')
                                parts = node.module.split('.')
                                if len(parts) >= 2:
                                    aurum_imports.add(f"{parts[0]}.{parts[1]}")
            except:
                pass
    
    return aurum_imports


@pytest.fixture
def project_root() -> Path:
    """Get the project root directory."""
    # Assumes tests are in tests/architecture/
    return Path(__file__).parent.parent.parent


@pytest.fixture
def src_root(project_root: Path) -> Path:
    """Get the source root directory."""
    return project_root / "src"


class TestDomainIndependence:
    """Test that domain layer has no dependencies on other layers."""
    
    def test_domain_has_no_framework_dependencies(self, src_root: Path):
        """Domain layer should not import any frameworks."""
        domain_dir = src_root / "aurum" / "domain"
        if not domain_dir.exists():
            pytest.skip("Domain directory not found")
        
        forbidden_imports = {
            'fastapi', 'flask', 'django', 'starlette',
            'sqlalchemy', 'pydantic', 'marshmallow',
            'celery', 'rq', 'kafka', 'redis',
        }
        
        violations = []
        
        for file_path in get_all_python_files(domain_dir):
            imports = get_imports_from_file(file_path)
            found_forbidden = imports & forbidden_imports
            
            if found_forbidden:
                violations.append(f"{file_path.relative_to(src_root)}: {found_forbidden}")
        
        assert not violations, f"Domain layer has framework dependencies:\n" + "\n".join(violations)
    
    def test_domain_does_not_import_application_layer(self, src_root: Path):
        """Domain should not import from application layer."""
        domain_dir = src_root / "aurum" / "domain"
        if not domain_dir.exists():
            pytest.skip("Domain directory not found")
        
        violations = []
        
        for file_path in get_all_python_files(domain_dir):
            aurum_imports = get_aurum_imports(file_path)
            
            if 'aurum.application' in aurum_imports:
                violations.append(str(file_path.relative_to(src_root)))
        
        assert not violations, f"Domain imports application layer:\n" + "\n".join(violations)
    
    def test_domain_does_not_import_infrastructure_layer(self, src_root: Path):
        """Domain should not import from infrastructure layer."""
        domain_dir = src_root / "aurum" / "domain"
        if not domain_dir.exists():
            pytest.skip("Domain directory not found")
        
        violations = []
        
        for file_path in get_all_python_files(domain_dir):
            aurum_imports = get_aurum_imports(file_path)
            
            if 'aurum.infrastructure' in aurum_imports:
                violations.append(str(file_path.relative_to(src_root)))
        
        assert not violations, f"Domain imports infrastructure layer:\n" + "\n".join(violations)


class TestApplicationLayerDependencies:
    """Test that application layer only depends on domain."""
    
    def test_application_only_imports_domain(self, src_root: Path):
        """Application layer should only import domain layer."""
        app_dir = src_root / "aurum" / "application"
        if not app_dir.exists():
            pytest.skip("Application directory not found")
        
        violations = []
        
        for file_path in get_all_python_files(app_dir):
            aurum_imports = get_aurum_imports(file_path)
            
            forbidden = aurum_imports - {'aurum.domain', 'aurum.application'}
            if forbidden:
                violations.append(f"{file_path.relative_to(src_root)}: {forbidden}")
        
        assert not violations, f"Application layer has forbidden imports:\n" + "\n".join(violations)


class TestInfrastructureDependencies:
    """Test infrastructure layer dependencies."""
    
    def test_infrastructure_can_import_domain_and_application(self, src_root: Path):
        """Infrastructure can import domain and application but not presentation."""
        infra_dir = src_root / "aurum" / "infrastructure"
        if not infra_dir.exists():
            pytest.skip("Infrastructure directory not found")
        
        violations = []
        
        for file_path in get_all_python_files(infra_dir):
            aurum_imports = get_aurum_imports(file_path)
            
            # Infrastructure should not import presentation/API layers
            forbidden = {'aurum.api', 'aurum.presentation'}
            found_forbidden = aurum_imports & forbidden
            
            if found_forbidden:
                violations.append(f"{file_path.relative_to(src_root)}: {found_forbidden}")
        
        assert not violations, f"Infrastructure imports presentation layer:\n" + "\n".join(violations)


class TestCircularDependencies:
    """Test for circular dependencies between modules."""
    
    def test_no_circular_dependencies_in_domain(self, src_root: Path):
        """Check for circular dependencies within domain modules."""
        domain_dir = src_root / "aurum" / "domain"
        if not domain_dir.exists():
            pytest.skip("Domain directory not found")
        
        # Build dependency graph
        dependencies: Dict[str, Set[str]] = {}
        
        for file_path in get_all_python_files(domain_dir):
            relative_path = file_path.relative_to(domain_dir)
            module_name = str(relative_path.with_suffix('')).replace(os.sep, '.')
            
            aurum_imports = get_aurum_imports(file_path)
            domain_imports = {
                imp.replace('aurum.domain.', '')
                for imp in aurum_imports
                if imp.startswith('aurum.domain.')
            }
            
            dependencies[module_name] = domain_imports
        
        # Simple circular dependency check (direct cycles)
        violations = []
        for module, imports in dependencies.items():
            for imported in imports:
                if imported in dependencies and module in dependencies[imported]:
                    violations.append(f"{module} <-> {imported}")
        
        assert not violations, f"Circular dependencies found:\n" + "\n".join(violations)


class TestNamingConventions:
    """Test naming conventions for clean architecture."""
    
    def test_domain_entities_suffix(self, src_root: Path):
        """Domain entities should have appropriate naming."""
        domain_models_dir = src_root / "aurum" / "domain" / "energy" / "models"
        if not domain_models_dir.exists():
            pytest.skip("Domain models directory not found")
        
        # This is informational - we use dataclasses without suffixes
        # Just check that files exist
        expected_files = ['curve.py', 'iso.py', 'ppa.py']
        
        for expected_file in expected_files:
            assert (domain_models_dir / expected_file).exists(), \
                f"Expected domain model file {expected_file} not found"
    
    def test_repository_interfaces_in_domain(self, src_root: Path):
        """Repository interfaces should be in domain layer."""
        repo_interface = src_root / "aurum" / "domain" / "shared_kernel" / "repositories.py"
        assert repo_interface.exists(), "Repository interfaces should be in domain layer"


def test_architecture_documentation_exists(project_root: Path):
    """Ensure architectural documentation exists."""
    docs_dir = project_root / "docs"
    
    # Check for architecture documentation
    expected_docs = [
        "architecture-overview.md",
        "ROADMAP.md",
    ]
    
    missing_docs = []
    for doc in expected_docs:
        if not (docs_dir / doc).exists():
            missing_docs.append(doc)
    
    # This is a warning, not a failure
    if missing_docs:
        pytest.skip(f"Missing architecture documentation: {missing_docs}")

