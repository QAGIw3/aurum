# Schema Registry Management

This document describes the Schema Registry management system for enforcing schema compatibility, managing schema evolution, and ensuring data quality across the Aurum ingestion pipeline.

## Overview

The Schema Registry system provides:

- **Schema Compatibility Enforcement**: Validates schema changes against compatibility rules
- **Automated Schema Registration**: Registers schemas as part of CI/CD pipeline
- **Evolution Management**: Tracks schema versions and evolution history
- **Data Quality Assurance**: Ensures schemas meet organizational standards
- **CI/CD Integration**: Automated schema validation and registration

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CI/CD         │───▶│ Schema Registry  │───▶│ Compatibility   │
│   Pipeline      │    │ Manager          │    │ Checker         │
│                 │    │                  │    │                 │
│ (GitHub Actions)│    └──────────────────┘    └─────────────────┘
│                 │            │                       │
│                 │            ▼                       ▼
│                 │    ┌──────────────────┐    ┌─────────────────┐
│                 │    │ Subject Manager  │    │ Evolution Rules │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Components

#### 1. Schema Registry Manager (`aurum.schema_registry.registry_manager`)
- Manages communication with Schema Registry
- Handles schema registration and compatibility checks
- Provides caching and error handling
- Enforces compatibility policies

#### 2. Compatibility Checker (`aurum.schema_registry.compatibility_checker`)
- Validates schema evolution rules
- Checks backward/forward compatibility
- Identifies breaking changes
- Provides migration recommendations

#### 3. CI/CD Integration (`scripts/ci/register_schemas.py`)
- Automated schema registration
- Validation and compatibility checking
- Report generation
- Integration with build pipeline

#### 4. Subject Manager
- Manages schema subject naming conventions
- Handles subject registration and updates
- Tracks subject versions and history
- Subject metadata, topics, and compatibility requirements are frozen in `kafka/schemas/contracts.yml`

## Configuration

### Schema Registry Configuration

```python
from aurum.schema_registry import SchemaRegistryConfig, SchemaCompatibilityMode

config = SchemaRegistryConfig(
    base_url="http://localhost:8081",
    default_compatibility_mode=SchemaCompatibilityMode.BACKWARD,
    timeout_seconds=30,
    enforce_compatibility=True,
    enforce_contracts=True,
    fail_on_incompatible=True,
    fail_on_missing_contract=True,
    contracts_path="kafka/schemas/contracts.yml",
    validate_schema=True,
    validate_references=True
)
```

### Compatibility Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `NONE` | No compatibility checks | Development/testing |
| `BACKWARD` | Consumers using new schema can read old data | Safe evolution |
| `FORWARD` | Consumers using old schema can read new data | Producer evolution |
| `FULL` | Both backward and forward compatibility | Strict compatibility |
| `BACKWARD_TRANSITIVE` | Backward compatibility across multiple versions | Complex evolution |
| `FORWARD_TRANSITIVE` | Forward compatibility across multiple versions | Complex evolution |
| `FULL_TRANSITIVE` | Full compatibility across multiple versions | Maximum compatibility |

## Usage

### Basic Schema Registration

```python
from aurum.schema_registry import SchemaCompatibilityMode, SchemaRegistryConfig, SchemaRegistryManager

config = SchemaRegistryConfig(
    base_url="http://localhost:8081",
    contracts_path="kafka/schemas/contracts.yml",
)
manager = SchemaRegistryManager(config)

schema = {
    "type": "record",
    "name": "MyRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "value", "type": "int"}
    ]
}

# Register schema (subject must be enumerated in contracts.yml beforehand)
schema_info = manager.register_subject(
    "aurum.example.my_record.v1-value",
    schema,
    SchemaCompatibilityMode.BACKWARD
)
```

### Compatibility Checking

```python
from aurum.schema_registry import CompatibilityChecker, SchemaCompatibilityMode

checker = CompatibilityChecker()

old_schema = {
    "type": "record",
    "name": "MyRecord",
    "fields": [{"name": "id", "type": "string"}]
}

new_schema = {
    "type": "record",
    "name": "MyRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": ["null", "string"], "default": None}
    ]
}

# Check compatibility
result = checker.check_compatibility(
    old_schema,
    new_schema,
    SchemaCompatibilityMode.BACKWARD
)

print(f"Compatible: {result.is_compatible}")
print(f"Breaking changes: {len(result.breaking_changes)}")
```

### CI/CD Integration

#### Manual Execution
```bash
# Register schemas from templates directory
python scripts/ci/register_schemas.py \
    --schema-dir seatunnel/jobs/templates \
    --registry-url http://localhost:8081 \
    --compatibility BACKWARD \
    --report schema_registration_report.md

# Dry run (validate without registering)
python scripts/ci/register_schemas.py \
    --schema-dir seatunnel/jobs/templates \
    --dry-run \
    --compatibility BACKWARD
```

#### Automated Execution
The system integrates with GitHub Actions for automated schema registration:

```yaml
# .github/workflows/schema-registry-ci.yml
- name: Run schema registration
  run: |
    python scripts/ci/register_schemas.py \
      --schema-dir seatunnel/jobs/templates \
      --registry-url $SCHEMA_REGISTRY_URL \
      --compatibility BACKWARD \
      --fail-on-error
```

## Schema Evolution Rules

### Backward Compatibility Rules

✅ **Allowed Changes:**
- Adding optional fields (with defaults)
- Adding enum values
- Adding union types
- Numeric type promotions (int → long → float → double)
- Adding record fields with defaults

❌ **Breaking Changes:**
- Removing required fields
- Changing field types incompatibly
- Removing enum values
- Removing union members
- Changing schema type (record ↔ non-record)

### Forward Compatibility Rules

✅ **Allowed Changes:**
- Adding optional fields
- Changing required fields to optional
- Adding enum values
- Expanding union types
- Adding record fields

❌ **Breaking Changes:**
- Making optional fields required
- Removing enum values
- Removing union members
- Changing field types

## Schema Naming Conventions

### Subject Naming Pattern
```
{namespace}.{record_name}
```

Examples:
- `aurum.eia.EiaSeriesRecord`
- `aurum.fred.FredSeriesRecord`
- `aurum.noaa.NoaaWeatherRecord`

### Version Management
- Subjects are versioned automatically by Schema Registry
- Each schema change creates a new version
- Compatibility is checked between versions

## Error Handling

### Common Issues

#### Schema Validation Errors
```python
# Invalid schema example
invalid_schema = {
    "type": "record",
    "name": "InvalidRecord"
    # Missing required "fields"
}
```

#### Compatibility Errors
```python
# Incompatible change example
old_schema = {"type": "record", "name": "Test", "fields": [{"name": "count", "type": "int"}]}
new_schema = {"type": "record", "name": "Test", "fields": [{"name": "count", "type": "string"}]}

# Results in compatibility error
```

#### Connection Errors
```python
# Schema Registry unavailable
config = SchemaRegistryConfig(base_url="http://unavailable:8081")
manager = SchemaRegistryManager(config)
# Raises SchemaRegistryConnectionError
```

### Debugging

#### Enable Debug Logging
```bash
export SCHEMA_REGISTRY_LOG_LEVEL=DEBUG
python scripts/ci/register_schemas.py --verbose
```

#### Test Individual Schemas
```python
from aurum.schema_registry import SchemaRegistryManager

manager = SchemaRegistryManager(config)
schema_info = manager.get_latest_schema("aurum.test.MyRecord")
if schema_info:
    print(f"Latest version: {schema_info.version}")
else:
    print("Schema not found")
```

## Testing

### Unit Tests
```bash
# Run schema registry tests
python -m pytest tests/schema_registry/ -v

# Run specific test file
python -m pytest tests/schema_registry/test_schema_registry.py::TestCompatibilityChecker -v
```

### Integration Tests
```bash
# Test with real Schema Registry
python scripts/schema_registry/test_schema_registration.py

# Test CI script simulation
python scripts/schema_registry/test_schema_registration.py
```

### Sample Schema Testing
```bash
# Test sample schemas in templates directory
python -c "
from aurum.schema_registry import SchemaRegistryManager, SchemaRegistryConfig
import json

config = SchemaRegistryConfig(base_url='http://localhost:8081')
manager = SchemaRegistryManager(config)

# Load and test a schema
schema_path = 'seatunnel/jobs/templates/sample_eia_record.avsc'
with open(schema_path) as f:
    schema = json.load(f)

print('Testing schema:', schema['name'])
result = manager.check_compatibility('aurum.test.EiaSeriesRecord', schema)
print('Compatible:', result.is_compatible)
"
```

## Monitoring and Alerting

### Registry Health Checks
```python
# Check Schema Registry health
status = manager.get_registry_status()
print(f"Total subjects: {status['total_subjects']}")
print(f"Total schemas: {status['total_schemas']}")
```

### CI/CD Monitoring
- Schema registration success/failure rates
- Compatibility check pass/fail rates
- Schema evolution trends
- Registry performance metrics

### Alerting Rules
- Schema registration failures
- Compatibility violations
- Registry downtime
- Performance degradation

## Performance Optimization

### Caching
- Schema information is cached to reduce API calls
- Cache TTL configurable per environment
- Cache invalidation on schema updates

### Batch Operations
- Register multiple schemas in batch
- Compatibility checks for multiple subjects
- Bulk schema validation

### Connection Pooling
- HTTP connection pooling for efficiency
- Configurable timeout and retry settings
- SSL/TLS optimization

## Security

### Authentication
```python
config = SchemaRegistryConfig(
    base_url="https://registry.company.com",
    username="service-account",
    password="secret-token",
    ssl_verify=True
)
```

### Authorization
- Role-based access control (RBAC)
- Schema Registry permissions
- API key management via Vault

### Network Security
- HTTPS/TLS encryption
- Certificate validation
- Firewall rules

## Best Practices

### Schema Design
1. **Start Simple**: Begin with basic schemas and evolve incrementally
2. **Use Defaults**: Always provide default values for new fields
3. **Optional Fields**: Make new fields optional when possible
4. **Consistent Naming**: Use consistent field naming conventions
5. **Documentation**: Document schema purpose and field meanings

### Evolution Strategy
1. **Backward Compatibility**: Always maintain backward compatibility
2. **Gradual Migration**: Roll out schema changes gradually
3. **Testing**: Test schema changes in staging first
4. **Monitoring**: Monitor schema evolution impact
5. **Rollback Plan**: Have rollback procedures for breaking changes

### CI/CD Integration
1. **Automated Testing**: Test schemas in CI pipeline
2. **Pre-merge Validation**: Validate compatibility before merge
3. **Staged Rollout**: Register schemas in staging first
4. **Monitoring**: Monitor registration success rates
5. **Reporting**: Generate reports for schema changes

## Troubleshooting

### Common Issues

#### Schema Not Found
```python
# Check if subject exists
schema_info = manager.get_latest_schema("aurum.test.MyRecord")
if not schema_info:
    print("Schema not found - may need registration")
```

#### Compatibility Check Fails
```python
# Debug compatibility issues
result = checker.check_compatibility(old_schema, new_schema)
print(f"Issues: {result.messages}")
print(f"Breaking changes: {result.breaking_changes}")
print(f"Recommendations: {result.recommendations}")
```

#### Registration Fails
```python
# Check registry connectivity
status = manager.get_registry_status()
if "error" in status:
    print(f"Registry error: {status['error']}")
```

### Debug Mode
```bash
# Enable debug logging
export DEBUG=1
python scripts/ci/register_schemas.py --verbose

# Check environment
python -c "
from aurum.schema_registry import SchemaRegistryConfig
config = SchemaRegistryConfig(base_url='http://localhost:8081')
print('Config:', config)
"
```

## Contributing

### Adding New Compatibility Rules
1. Extend `CompatibilityChecker` class
2. Add new compatibility issue types
3. Implement validation logic
4. Add unit tests

### Custom Schema Validation
1. Override `_validate_schema` method
2. Add custom validation rules
3. Integrate with organizational standards
4. Update documentation

### CI/CD Enhancements
1. Add new workflow steps
2. Enhance error reporting
3. Add schema diffing
4. Integrate with monitoring systems

## API Reference

### SchemaRegistryManager

```python
# Register a schema
manager.register_subject(subject, schema, compatibility_mode)

# Get latest schema
schema_info = manager.get_latest_schema(subject)

# Check compatibility
result = manager.check_compatibility(subject, new_schema)

# Get registry status
status = manager.get_registry_status()
```

### CompatibilityChecker

```python
# Check schema compatibility
result = checker.check_compatibility(old_schema, new_schema, mode)

# Get compatibility summary
summary = result.summary()

# Check if has breaking changes
has_breaking = result.has_breaking_changes()
```

### SchemaRegistrationScript

```python
# Register schemas from directory
script.register_schemas_from_directory(schema_dir, mode, dry_run)

# Generate registration report
report = script.generate_schema_report(results, output_file)
```

## Migration Guide

### From Manual Schema Management
1. **Inventory**: Catalog existing schemas
2. **Standardize**: Apply consistent naming conventions
3. **Validate**: Run compatibility checks
4. **Migrate**: Register schemas with Schema Registry
5. **Update**: Modify applications to use registered schemas

### Schema Evolution
1. **Plan**: Define evolution strategy
2. **Test**: Validate changes in staging
3. **Deploy**: Register new schemas
4. **Monitor**: Track compatibility and performance
5. **Rollback**: Prepare rollback procedures

## Performance Benchmarks

### Registration Performance
- Single schema: ~100ms
- Batch (10 schemas): ~500ms
- Large batch (100 schemas): ~2s

### Code-Generated Models

```python
from datetime import datetime

from aurum.schema_registry.codegen import get_model

ScenarioRequest = get_model("aurum.scenario.request.v1-value")
payload = ScenarioRequest(
    scenario_id="scenario-123",
    tenant_id="tenant-abc",
    requested_by="api",
    asof_date=None,
    curve_def_ids=[],
    assumptions=[],
    submitted_ts=datetime.utcnow(),
)

avro_ready = payload.model_dump(mode="python")
```

Generated models enforce the frozen contract locally (Pydantic validators) and return dictionaries ready for Avro serialization. Use `payload_from_contract(subject, payload_dict)` to coerce existing payloads.

### Compatibility Checking
- Simple schema: ~10ms
- Complex schema: ~50ms
- Multi-version check: ~100ms

### Memory Usage
- Base manager: ~50MB
- With caching: ~100MB
- Full registry: ~200MB

## Support

### Resources
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/)
- [Avro Schema Guide](https://avro.apache.org/docs/current/spec.html)
- [Compatibility Rules](https://docs.confluent.io/platform/current/schema-registry/develop/api.html#compatibility)

### Community
- GitHub Issues: Report bugs and request features
- Discussions: Ask questions and share solutions
- Contributing: See CONTRIBUTING.md for guidelines

### Monitoring
- Health checks: `/health` endpoint
- Metrics: Prometheus-compatible metrics
- Logs: Structured logging with correlation IDs
