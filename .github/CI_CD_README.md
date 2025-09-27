# CI/CD Quality Gates

This directory contains the comprehensive CI/CD pipeline configuration for the Aurum platform, ensuring code quality, security, and reliability through automated gates and checks.

## Overview

The CI/CD pipeline is structured around multiple quality gates that ensure:

- **Code Quality**: Formatting, linting, and type checking
- **Test Coverage**: Comprehensive testing with coverage requirements
- **Security**: Vulnerability scanning and dependency checks
- **API Consistency**: OpenAPI specification validation
- **Performance**: Regression testing and benchmarking

## Pipeline Structure

```
┌─────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ Code Changes    │───▶│ Quality Gates       │───▶│ Docker Build &   │
│ (Push/PR)       │    │                     │    │ Deploy           │
└─────────────────┘    │ - Pre-commit        │    └──────────────────┘
                       │ - Type Checking     │            │
                       │ - Linting           │    ┌──────────────────┐
                       │ - Testing           │───▶│ Security Scans   │
                       │ - Security Scans    │    │ - Vulnerability  │
                       │ - OpenAPI Validation│    │ - SBOM Generation│
                       └─────────────────────┘    └──────────────────┘
```

## Workflows

### 1. CI Quality Gates (`ci-quality-gates.yml`)

**Triggers:**
- Push to `main`/`develop` branches
- Pull requests to `main`/`develop` branches
- Changes to source code, tests, or configuration

**Jobs:**
- **Pre-commit Checks**: Basic syntax and formatting validation
- **Type Checking**: mypy static analysis
- **Code Quality**: ruff linting and formatting
- **Test Suite**: Unit, integration, and property-based tests
- **Security Scanning**: bandit and safety checks
- **OpenAPI Specification**: API schema validation and diffing
- **Dependency Scanning**: pip-audit vulnerability checks
- **Performance Testing**: Benchmark regression detection
- **Quality Gate Summary**: Overall status and reporting

### 2. Docker Build Pipeline (`docker-build.yml`)

**Triggers:**
- Push to `main`/`develop` branches
- Pull requests to `main` branch
- Manual workflow dispatch

**Features:**
- Multi-platform builds (AMD64/ARM64)
- Layer caching for faster builds
- Vulnerability scanning with Trivy
- SBOM generation with Syft
- Image signing with cosign
- Security artifact uploads

## Quality Gates

### Code Quality Gates

#### Pre-commit Checks
- Trailing whitespace removal
- End-of-file fixes
- YAML/JSON/TOML validation
- Merge conflict detection
- Large file detection
- Import validation

#### Type Checking (mypy)
```yaml
mypy src/ --ignore-missing-imports --no-strict-optional --warn-redundant-casts
```

#### Linting (ruff)
```yaml
ruff check src/ tests/ --select=E9,F63,F7,F82
ruff format --check --diff src/ tests/
```

### Test Coverage Gates

#### Minimum Coverage: 80%
- Unit tests across Python 3.9-3.11
- Integration tests for API endpoints
- Property-based tests for data validation
- Coverage reporting with Codecov

#### Test Execution
```bash
pytest tests/ \
  --cov=src/aurum \
  --cov-report=xml \
  --cov-fail-under=80
```

### Security Gates

#### Static Analysis (bandit)
```yaml
bandit -r src/ -f json -o bandit-report.json
```

#### Dependency Scanning (safety/pip-audit)
```bash
pip-audit --format=json > pip-audit-report.json
```

#### Container Scanning (Trivy)
```yaml
trivy image --format sarif --output trivy-results.sarif
```

### API Consistency Gates

#### OpenAPI Specification Validation
- Generate spec from running application
- Validate against OpenAPI schema
- Compare with baseline for breaking changes
- Upload spec as artifact

#### Breaking Change Detection
```python
# scripts/generate_openapi_diff.py
python scripts/generate_openapi_diff.py --base-spec openapi-baseline.json --new-spec openapi-new.json
```

### Performance Gates

#### Benchmark Regression Testing
```yaml
pytest --benchmark-only --benchmark-json=benchmark-results.json --benchmark-compare-fail=regression:5%
```

#### Key Performance Metrics
- API request latency (P95 < 500ms)
- Database query performance
- Cache hit rates (>90%)
- Memory usage patterns

## Configuration Files

### `pyproject.toml`
```toml
[tool.ruff]
line-length = 88
target-version = "py39"
select = ["E", "W", "F", "I", "B", "C4", "UP"]

[tool.mypy]
python_version = "3.9"
warn_return_any = true
warn_unused_configs = true
warn_unreachable = true

[tool.pytest.ini_options]
cov-fail-under = 80
testpaths = ["tests"]
```

### `.pre-commit-config.yaml`
```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-json
```

## Security Scanning

### Vulnerability Scanning
- **Trivy**: Container image vulnerability scanning
- **bandit**: Python code security analysis
- **safety**: Dependency vulnerability detection
- **pip-audit**: PyPI package vulnerability scanning

### SBOM Generation
- **Syft**: Software Bill of Materials generation
- **SPDX format**: Industry standard format
- **Artifact upload**: SBOMs stored for compliance

### Image Signing
- **cosign**: Container image signing
- **Keyless signing**: Using OIDC tokens
- **Attestation**: Sign with SLSA provenance

## Performance Monitoring

### Benchmarking
- **pytest-benchmark**: Performance regression testing
- **Comparison with baseline**: Detect performance degradations
- **Threshold alerts**: Fail on significant regressions (>5%)

### Coverage Reporting
- **Coverage.py**: Code coverage analysis
- **Codecov integration**: Coverage reporting and trends
- **HTML reports**: Detailed coverage visualization

## Artifact Management

### Generated Artifacts
- **OpenAPI Specifications**: API documentation
- **Coverage Reports**: HTML and XML coverage reports
- **Security Scan Results**: SARIF and JSON reports
- **SBOM Files**: Software Bill of Materials
- **Benchmark Results**: Performance regression data

### Artifact Retention
- **30 days**: Standard retention for most artifacts
- **Permanent**: Security scan results and SBOMs
- **Branch-specific**: PR artifacts cleaned up automatically

## Monitoring and Alerts

### Quality Gate Status
- **GitHub Status Checks**: Required checks for PRs
- **Summary Reports**: Detailed status in job summaries
- **Artifact Links**: Direct links to detailed reports

### Alert Integration
- **Security vulnerabilities**: GitHub Security tab
- **Test failures**: GitHub commit status
- **Coverage drops**: Codecov notifications

## Troubleshooting

### Common Issues

#### Pre-commit Hook Failures
```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files

# Skip hooks temporarily
git commit --no-verify
```

#### Type Checking Issues
```bash
# Run mypy manually
mypy src/ --ignore-missing-imports

# Check specific file
mypy src/aurum/api/app.py
```

#### Test Coverage Issues
```bash
# Run coverage locally
pytest --cov=src/aurum --cov-report=html

# Check coverage for specific module
pytest tests/api/test_scenarios.py --cov=src/aurum.api.scenarios
```

#### OpenAPI Spec Issues
```bash
# Generate spec locally
python scripts/generate_openapi_spec.py --output openapi.json

# Validate spec
python -c "from openapi_spec_validator import validate_spec; import json; validate_spec(json.load(open('openapi.json')))"
```

### Debug Mode

Enable detailed logging for CI issues:

```bash
export DEBUG_CI=true
export GITHUB_ACTIONS=true
```

### Performance Optimization

For faster CI runs:
1. Use dependency caching
2. Run tests in parallel
3. Use matrix builds for Python versions
4. Cache pre-commit installations

## Integration with Development

### Local Development
```bash
# Install development dependencies
pip install -e .[dev]

# Run pre-commit hooks
pre-commit run --all-files

# Run type checking
mypy src/

# Run tests
pytest

# Run security scan
bandit -r src/
```

### IDE Integration
- **VS Code**: Python, Pylint, mypy extensions
- **PyCharm**: Built-in type checking and linting
- **Vim/Emacs**: ALE or similar plugins

## Compliance and Standards

### Security Standards
- **OWASP Top 10**: Address common web vulnerabilities
- **SLSA**: Supply chain security
- **SBOM**: Software Bill of Materials requirements
- **Container Security**: Image scanning and signing

### Code Quality Standards
- **PEP 8**: Python style guide compliance
- **PEP 484**: Type hinting standards
- **Semantic Versioning**: API versioning strategy
- **Conventional Commits**: Commit message standards

## Extending Quality Gates

### Adding New Checks

1. **Add to CI workflow**:
```yaml
- name: Custom Check
  run: |
    python scripts/custom_check.py
```

2. **Add pre-commit hook**:
```yaml
- repo: local
  hooks:
    - id: custom-check
      name: Custom Check
      entry: python scripts/custom_check.py
      language: system
      files: \.py$
```

3. **Update quality gate summary**:
```yaml
needs: [..., custom-check]
if: always()
```

### Custom Metrics

Add custom performance metrics:
```python
# In your test files
def test_custom_performance(benchmark):
    result = benchmark(my_function)
    assert result < threshold
```

## Best Practices

### For Developers
1. **Run pre-commit hooks** before pushing
2. **Write tests** for new features
3. **Add type hints** to new functions
4. **Update OpenAPI spec** for API changes
5. **Review security scan** results

### For Maintainers
1. **Monitor CI performance** and optimize slow jobs
2. **Update dependencies** regularly
3. **Review security alerts** promptly
4. **Tune quality thresholds** based on team needs
5. **Document exceptions** to rules

### For CI/CD
1. **Use caching** to speed up builds
2. **Parallelize** independent jobs
3. **Fail fast** on critical errors
4. **Provide detailed** error messages
5. **Clean up** old artifacts

## Support

For issues with the CI/CD pipeline:
1. Check the GitHub Actions logs
2. Review the job summaries
3. Download and examine artifacts
4. Run local versions of checks
5. Contact the platform team

---

*This CI/CD pipeline ensures high code quality, security, and reliability while providing fast feedback to developers.*
