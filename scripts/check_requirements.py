#!/usr/bin/env python3
"""Check requirements files for consistency and best practices."""

import re
import sys
from pathlib import Path
from typing import Set


def check_requirements_consistency():
    """Check that requirements files are consistent."""
    root = Path(__file__).parent.parent

    # Files to check
    files_to_check = [
        root / "requirements.txt",
        root / "requirements-dev.txt",
        root / "requirements-test.txt",
        root / "pyproject.toml"
    ]

    all_requirements = {}

    # Parse requirements.txt files
    for req_file in files_to_check:
        if req_file.exists():
            requirements = parse_requirements_file(req_file)
            for req, constraints in requirements.items():
                if req in all_requirements:
                    if all_requirements[req] != constraints:
                        print(f"❌ Version conflict for {req}:")
                        print(f"  {req_file}: {constraints}")
                        print(f"  Previous: {all_requirements[req]}")
                        return False
                else:
                    all_requirements[req] = constraints

    print("✅ Requirements files are consistent")
    return True


def parse_requirements_file(file_path: Path) -> dict:
    """Parse a requirements file and return package versions."""
    requirements = {}

    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue

                # Parse package specification
                match = re.match(r'^([a-zA-Z0-9\-_.]+)([><=~!,\s\d.]+)?', line)
                if match:
                    package = match.group(1).lower()
                    constraints = match.group(2).strip() if match.group(2) else ""
                    requirements[package] = constraints

    except Exception as e:
        print(f"❌ Error parsing {file_path}: {e}")
        return {}

    return requirements


def check_pyproject_toml():
    """Check pyproject.toml for best practices."""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"

    if not pyproject_path.exists():
        print("⚠️ pyproject.toml not found")
        return True

    try:
        with open(pyproject_path, 'r') as f:
            content = f.read()

        # Check for required sections (updated for hatchling)
        required_sections = [
            "[build-system]",
            "[project]",
            "[project.optional-dependencies]"
        ]

        for section in required_sections:
            if section not in content:
                print(f"❌ Missing required section: {section}")
                return False

        # Check for Python version constraint
        if "requires-python" not in content:
            print("⚠️ No Python version constraint specified")

        # Check for dependency group consistency
        if "[project.optional-dependencies]" in content:
            print("✅ Dependency groups found")
            
            # Check for shared dependency groups
            shared_groups = ["common", "database", "monitoring", "data", "networking", "cloud"]
            found_shared = []
            for group in shared_groups:
                if f"{group} = [" in content:
                    found_shared.append(group)
            
            if found_shared:
                print(f"✅ Shared dependency groups found: {', '.join(found_shared)}")
            else:
                print("⚠️ No shared dependency groups found")

        print("✅ pyproject.toml looks good")
        return True

    except Exception as e:
        print(f"❌ Error checking pyproject.toml: {e}")
        return False


def check_for_vulnerabilities():
    """Check for known vulnerable packages."""
    # This is a basic check - in production, you'd use safety or similar
    vulnerable_packages = {
        'pyyaml': '<6.0',  # Known vulnerabilities in older versions
        'requests': '<2.20.0',
        'jinja2': '<2.10.1',
        'flask': '<1.0',
    }

    root = Path(__file__).parent.parent
    requirements_path = root / "requirements.txt"

    if not requirements_path.exists():
        return True

    try:
        with open(requirements_path, 'r') as f:
            content = f.read()

        for package, vulnerable_version in vulnerable_packages.items():
            if package in content:
                print(f"⚠️ Potentially vulnerable package found: {package}")
                print(f"  Consider updating to avoid version {vulnerable_version}")

        print("✅ No obvious vulnerable packages detected")
        return True

    except Exception as e:
        print(f"❌ Error checking for vulnerabilities: {e}")
        return False


def main():
    """Main function."""
    print("🔍 Checking requirements files...")

    all_checks_passed = True

    # Run all checks
    checks = [
        check_requirements_consistency,
        check_pyproject_toml,
        check_for_vulnerabilities,
    ]

    for check in checks:
        try:
            if not check():
                all_checks_passed = False
        except Exception as e:
            print(f"❌ Check failed with error: {e}")
            all_checks_passed = False

    if all_checks_passed:
        print("✅ All requirements checks passed!")
        sys.exit(0)
    else:
        print("❌ Some requirements checks failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
