#!/usr/bin/env python3
"""SeaTunnel job template validation script for CI/CD pipeline."""

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Set

from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError


class SeaTunnelTemplateValidator:
    """Validates SeaTunnel job templates."""

    def __init__(self, templates_dir: str = "seatunnel/jobs/templates"):
        """Initialize template validator."""
        self.templates_dir = Path(templates_dir)
        self.env = Environment(
            loader=FileSystemLoader(self.templates_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )

    def validate_template_syntax(self) -> List[str]:
        """Validate Jinja2 template syntax."""
        errors = []
        template_files = list(self.templates_dir.glob("*.tmpl"))

        for template_file in template_files:
            try:
                template = self.env.get_template(template_file.name)
                # Try to render with minimal context
                test_context = {
                    "job_name": "test_job",
                    "source_type": "http",
                    "sink_type": "kafka",
                    "parallelism": 1,
                    "batch_size": 1000,
                }

                rendered = template.render(**test_context)
                print(f"✓ Valid template syntax: {template_file}")

            except TemplateSyntaxError as e:
                errors.append(f"{template_file}: Template syntax error - {e}")
            except Exception as e:
                errors.append(f"{template_file}: Template validation error - {e}")

        return errors

    def validate_template_variables(self) -> List[str]:
        """Validate template variables are properly defined."""
        errors = []
        template_files = list(self.templates_dir.glob("*.tmpl"))

        for template_file in template_files:
            try:
                with open(template_file, 'r') as f:
                    content = f.read()

                # Find all variable references
                variable_pattern = r'\$\{([^}]+)\}'
                variables = set(re.findall(variable_pattern, content))

                # Check for common issues
                for var in variables:
                    # Check for undefined variables
                    if var.startswith('UNDEFINED_'):
                        errors.append(f"{template_file}: References undefined variable '{var}'")

                    # Check for empty variable references
                    if not var.strip():
                        errors.append(f"{template_file}: Empty variable reference found")

                    # Check for potentially dangerous variable usage
                    dangerous_patterns = [
                        'eval', 'exec', 'import', '__import__',
                        'globals', 'locals', 'vars', 'dir'
                    ]
                    for pattern in dangerous_patterns:
                        if pattern in var.lower():
                            errors.append(f"{template_file}: Potentially dangerous variable usage: '{var}'")

                print(f"✓ Validated variables in: {template_file}")

            except Exception as e:
                errors.append(f"{template_file}: Variable validation error - {e}")

        return errors

    def validate_template_structure(self) -> List[str]:
        """Validate template structure and required sections."""
        errors = []
        template_files = list(self.templates_dir.glob("*.tmpl"))

        required_sections = {
            'http_to_kafka': ['env', 'source', 'transform', 'sink'],
            'kafka_to_iceberg': ['env', 'source', 'sink'],
            'kafka_to_database': ['env', 'source', 'sink'],
            'database_to_kafka': ['env', 'source', 'transform', 'sink'],
            'generic': ['env', 'source', 'sink']
        }

        for template_file in template_files:
            try:
                with open(template_file, 'r') as f:
                    content = f.read()

                # Determine template type from filename
                template_type = 'generic'
                if 'http_to_kafka' in template_file.name:
                    template_type = 'http_to_kafka'
                elif 'kafka_to_iceberg' in template_file.name:
                    template_type = 'kafka_to_iceberg'
                elif 'kafka_to_database' in template_file.name:
                    template_type = 'kafka_to_database'
                elif 'database_to_kafka' in template_file.name:
                    template_type = 'database_to_kafka'

                # Check for required sections
                required = required_sections.get(template_type, ['env', 'source', 'sink'])
                for section in required:
                    if f"{section} {{" not in content:
                        errors.append(f"{template_file}: Missing required section '{section}'")

                # Validate section structure
                section_errors = self._validate_section_structure(content, template_file)
                errors.extend(section_errors)

                print(f"✓ Validated structure: {template_file}")

            except Exception as e:
                errors.append(f"{template_file}: Structure validation error - {e}")

        return errors

    def _validate_section_structure(self, content: str, template_file: Path) -> List[str]:
        """Validate the structure of configuration sections."""
        errors = []

        # Common section patterns
        section_patterns = {
            'env': r'env\s*{[^}]*}',
            'source': r'source\s*{[^}]*}',
            'sink': r'sink\s*{[^}]*}',
            'transform': r'transform\s*{[^}]*}'
        }

        for section, pattern in section_patterns.items():
            matches = re.findall(pattern, content, re.DOTALL | re.MULTILINE)
            if not matches:
                continue  # Section might not be required

            for match in matches:
                # Basic structure validation
                if '{' not in match or '}' not in match:
                    errors.append(f"{template_file}: Malformed {section} section")
                    continue

                # Check for balanced braces
                open_braces = match.count('{')
                close_braces = match.count('}')
                if open_braces != close_braces:
                    errors.append(f"{template_file}: Unbalanced braces in {section} section")

        return errors

    def validate_template_metadata(self) -> List[str]:
        """Validate template metadata and comments."""
        errors = []
        template_files = list(self.templates_dir.glob("*.tmpl"))

        for template_file in template_files:
            try:
                with open(template_file, 'r') as f:
                    lines = f.readlines()

                # Check for header comments
                has_header = False
                for line in lines[:10]:  # Check first 10 lines
                    if line.strip().startswith('#'):
                        has_header = True
                        break

                if not has_header:
                    errors.append(f"{template_file}: Missing header documentation")

                # Check for required environment variables documentation
                content = ''.join(lines)
                if 'Required environment variables' not in content:
                    errors.append(f"{template_file}: Missing environment variables documentation")

                print(f"✓ Validated metadata: {template_file}")

            except Exception as e:
                errors.append(f"{template_file}: Metadata validation error - {e}")

        return errors

    def validate_cross_template_consistency(self) -> List[str]:
        """Validate consistency across related templates."""
        errors = []
        template_files = list(self.templates_dir.glob("*.tmpl"))

        # Group templates by data source
        eia_templates = [f for f in template_files if 'eia' in f.name]
        fred_templates = [f for f in template_files if 'fred' in f.name]
        noaa_templates = [f for f in template_files if 'noaa' in f.name]

        # Check consistency within source groups
        for source_group, templates in [
            ('EIA', eia_templates),
            ('FRED', fred_templates),
            ('NOAA', noaa_templates)
        ]:
            if len(templates) > 1:
                consistency_errors = self._check_group_consistency(source_group, templates)
                errors.extend(consistency_errors)

        return errors

    def _check_group_consistency(self, source_group: str, templates: List[Path]) -> List[str]:
        """Check consistency within a group of related templates."""
        errors = []

        # Extract common configuration patterns
        common_patterns = {}
        for template in templates:
            with open(template, 'r') as f:
                content = f.read()

            # Check for consistent environment variables
            env_vars = set(re.findall(r'export\s+(\w+)=', content))
            if source_group not in common_patterns:
                common_patterns[source_group] = {'env_vars': env_vars}
            else:
                # Check for missing variables
                existing_vars = common_patterns[source_group]['env_vars']
                missing_vars = existing_vars - env_vars
                if missing_vars:
                    errors.append(f"{template}: Missing environment variables: {missing_vars}")

        return errors

    def generate_template_report(self, errors: List[str]) -> Dict:
        """Generate comprehensive template validation report."""
        template_files = list(self.templates_dir.glob("*.tmpl"))

        return {
            "total_templates": len(template_files),
            "errors": errors,
            "error_count": len(errors),
            "validation_status": "PASS" if len(errors) == 0 else "FAIL",
            "templates_validated": len(template_files),
            "validation_categories": {
                "syntax": "PASS" if not any("syntax" in e.lower() for e in errors) else "FAIL",
                "variables": "PASS" if not any("variable" in e.lower() for e in errors) else "FAIL",
                "structure": "PASS" if not any("structure" in e.lower() for e in errors) else "FAIL",
                "metadata": "PASS" if not any("metadata" in e.lower() or "documentation" in e.lower() for e in errors) else "FAIL",
                "consistency": "PASS" if not any("consistency" in e.lower() for e in errors) else "FAIL"
            }
        }


def main():
    """Main validation function."""
    validator = SeaTunnelTemplateValidator()

    all_errors = []

    print("🔧 Starting SeaTunnel template validation...")

    # 1. Validate template syntax
    print("\n📝 Validating template syntax...")
    syntax_errors = validator.validate_template_syntax()
    all_errors.extend(syntax_errors)
    if syntax_errors:
        print(f"❌ Found {len(syntax_errors)} syntax errors")
        for error in syntax_errors:
            print(f"  - {error}")
    else:
        print("✅ All templates have valid syntax")

    # 2. Validate template variables
    print("\n🔗 Validating template variables...")
    variable_errors = validator.validate_template_variables()
    all_errors.extend(variable_errors)
    if variable_errors:
        print(f"❌ Found {len(variable_errors)} variable errors")
        for error in variable_errors:
            print(f"  - {error}")
    else:
        print("✅ All template variables are properly defined")

    # 3. Validate template structure
    print("\n🏗️ Validating template structure...")
    structure_errors = validator.validate_template_structure()
    all_errors.extend(structure_errors)
    if structure_errors:
        print(f"❌ Found {len(structure_errors)} structure errors")
        for error in structure_errors:
            print(f"  - {error}")
    else:
        print("✅ All template structures are valid")

    # 4. Validate template metadata
    print("\n📋 Validating template metadata...")
    metadata_errors = validator.validate_template_metadata()
    all_errors.extend(metadata_errors)
    if metadata_errors:
        print(f"❌ Found {len(metadata_errors)} metadata errors")
        for error in metadata_errors:
            print(f"  - {error}")
    else:
        print("✅ All template metadata is valid")

    # 5. Validate cross-template consistency
    print("\n🔄 Validating cross-template consistency...")
    consistency_errors = validator.validate_cross_template_consistency()
    all_errors.extend(consistency_errors)
    if consistency_errors:
        print(f"❌ Found {len(consistency_errors)} consistency errors")
        for error in consistency_errors:
            print(f"  - {error}")
    else:
        print("✅ All templates are consistent")

    # 6. Generate final report
    report = validator.generate_template_report(all_errors)

    print("
📊 Template Validation Report:"    print(f"  Total templates: {report['total_templates']}")
    print(f"  Errors: {report['error_count']}")
    print(f"  Status: {report['validation_status']}")

    print("
📈 Category Breakdown:"    for category, status in report['validation_categories'].items():
        status_icon = "✅" if status == "PASS" else "❌"
        print(f"  {status_icon} {category.title()}")

    if report['error_count'] > 0:
        print(f"\n❌ Template validation failed with {report['error_count']} errors")
        return 1

    print("\n✅ All template validation checks passed!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
