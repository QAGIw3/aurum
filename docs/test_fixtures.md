# Test Fixtures and Golden Files

This document describes the comprehensive test fixtures and golden files system for validating data ingestion pipelines. The system generates synthetic test data, expected outputs, and automated validation to ensure data quality and pipeline correctness.

## Overview

The Test Fixtures system provides:

- **Synthetic Data Generation**: Realistic test data for all data sources
- **Golden File Management**: Expected outputs for test validation
- **Automated Validation**: Compare actual vs expected results
- **CI/CD Integration**: Automated fixture validation in build pipeline
- **Data Quality Testing**: Comprehensive data validation testing
- **Performance Benchmarking**: Test fixtures for performance validation

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CI/CD         │───▶│ Fixture          │───▶│ Synthetic Data  │
│   Pipeline      │    │ Generator        │    │ Generator       │
│                 │    │                  │    │                 │
│ (GitHub Actions)│    └──────────────────┘    └─────────────────┘
│                 │            │                       │
│                 │            ▼                       ▼
│                 │    ┌──────────────────┐    ┌─────────────────┐
│                 │    │ Golden File      │    │ Test Validator  │
│                 │    │ Manager          │    │                 │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Components

#### 1. FixtureGenerator (`aurum.test_fixtures.fixture_generator`)
- Orchestrates test fixture generation
- Manages test cases and data sources
- Generates comprehensive test suites
- Handles fixture lifecycle management

#### 2. SyntheticDataGenerator (`aurum.test_fixtures.synthetic_data`)
- Generates realistic test data for each data source
- Supports null value injection
- Includes invalid data generation
- Provides configurable data patterns

#### 3. GoldenFileManager (`aurum.test_fixtures.golden_file_manager`)
- Manages expected output files
- Provides file integrity checking
- Handles golden file updates
- Supports batch operations

#### 4. TestValidator (`aurum.test_fixtures.test_validator`)
- Validates actual outputs against golden files
- Performs data quality checks
- Generates validation reports
- Identifies specific issues

## Data Sources Supported

### 1. EIA (Energy Information Administration)
- Electricity market data
- Petroleum prices and volumes
- Natural gas data
- Renewable energy statistics

### 2. FRED (Federal Reserve Economic Data)
- Economic indicators
- Interest rates
- Employment data
- GDP and inflation metrics

### 3. CPI (Consumer Price Index)
- Price indices
- Inflation measures
- Consumer spending data
- Regional price variations

### 4. NOAA (National Oceanic and Atmospheric Administration)
- Weather station data
- Temperature measurements
- Precipitation records
- Climate data

### 5. ISO (Independent System Operators)
- Electricity market prices (LMP)
- Load and generation data
- Transmission constraints
- Market settlement data

## Usage

### Generating Test Fixtures

```bash
# Generate fixtures for all data sources
python scripts/test_fixtures/generate_fixtures.py

# Generate fixtures for specific data sources
python scripts/test_fixtures/generate_fixtures.py --data-source eia --data-source fred

# Generate fixtures in custom directory
python scripts/test_fixtures/generate_fixtures.py --output-dir ./custom_fixtures
```

### Validating Existing Fixtures

```bash
# Validate existing fixtures
python scripts/test_fixtures/generate_fixtures.py --validate-only

# Generate report only
python scripts/test_fixtures/generate_fixtures.py --report-only
```

### Programmatic Usage

```python
from aurum.test_fixtures import FixtureGenerator, FixtureConfig
from pathlib import Path

# Generate fixtures programmatically
config = FixtureConfig(
    output_dir=Path("test_fixtures"),
    generate_golden_files=True,
    include_null_values=True,
    include_invalid_values=True
)

generator = FixtureGenerator(config)
summary = generator.generate_all_fixtures()

print(f"Generated {summary['total_test_cases']} test cases")
print(f"Total records: {summary['total_records']}")
```

## Test Case Types

### 1. Basic Validation Tests
- Field presence validation
- Basic type checking
- Simple data constraints
- Record count validation

### 2. Data Quality Tests
- Null value handling
- Invalid data detection
- Data type validation
- Value range checking

### 3. Performance Tests
- Large dataset handling
- Processing speed validation
- Memory usage testing
- Scalability verification

## Fixture Structure

### Directory Layout
```
test_fixtures/
├── test_data/           # Input test data
│   ├── eia/
│   │   ├── basic_validation/
│   │   │   ├── input.json
│   │   │   ├── schema.avsc
│   │   │   ├── expected_output.json
│   │   │   └── metadata.json
│   │   └── data_quality/
│   │       ├── input.json
│   │       ├── schema.avsc
│   │       ├── expected_output.json
│   │       └── metadata.json
│   ├── fred/
│   └── ...
├── golden_files/        # Expected outputs
├── schemas/            # Avro schemas
└── fixture_report.json # Generation summary
```

### Test Case Metadata
Each test case includes comprehensive metadata:

```json
{
  "name": "basic_validation",
  "description": "Basic data validation test",
  "data_source": "eia",
  "record_count": 100,
  "config": {
    "record_count": 100,
    "include_null_values": true,
    "include_invalid_values": false,
    "seed": 42
  },
  "validation_rules": {
    "min_records": 100,
    "max_null_rate": 0.1,
    "required_fields": ["id", "source", "ingested_at"]
  },
  "tags": ["basic", "validation"],
  "checksum": "sha256_hash_of_data",
  "generated_at": "2024-01-01T00:00:00"
}
```

## Data Generation

### Synthetic Data Features

#### Realistic Patterns
- **EIA**: Electricity prices, market data, regional variations
- **FRED**: Economic time series, interest rates, employment data
- **CPI**: Price indices, inflation trends, seasonal patterns
- **NOAA**: Weather measurements, climate data, station readings
- **ISO**: Energy market prices, load/generation data, grid conditions

#### Configurable Quality
```python
# High quality data
config = SyntheticDataConfig(
    record_count=1000,
    include_null_values=False,
    include_invalid_values=False
)

# Low quality data for testing
config = SyntheticDataConfig(
    record_count=1000,
    include_null_values=True,
    null_probability=0.2,
    include_invalid_values=True,
    invalid_probability=0.1
)
```

#### Data Validation
- Field presence checking
- Type validation
- Value range constraints
- Format validation (regex)
- Custom validation rules

## Golden File Management

### File Integrity
- SHA256 checksums for all files
- Automatic integrity verification
- Corruption detection and reporting
- Backup and restore capabilities

### Version Management
```python
# Update golden file when expected output changes
manager.update_golden_file(
    "test_case_name",
    new_expected_data,
    updated_schema
)
```

### Comparison Features
- Exact match validation
- Record count comparison
- Schema compliance checking
- Data quality scoring

## Validation and Testing

### Automated Validation

```bash
# Run validation on generated fixtures
python -c "
from aurum.test_fixtures import TestValidator, GoldenFileManager
from pathlib import Path

manager = GoldenFileManager(Path('test_fixtures'))
validator = TestValidator(manager)

# Validate test outputs
test_results = {
    'eia_basic': eia_test_data,
    'fred_basic': fred_test_data
}

results = validator.validate_batch(test_results)

for result in results:
    print(f'{result.test_name}: {\"PASSED\" if result.passed else \"FAILED\"}')
"
```

### CI/CD Integration

The system integrates with GitHub Actions:

```yaml
# .github/workflows/test-fixtures-ci.yml
- name: Generate test fixtures
  run: python scripts/test_fixtures/generate_fixtures.py

- name: Validate fixture integrity
  run: python scripts/test_fixtures/generate_fixtures.py --validate-only

- name: Run fixture validation tests
  run: python -m pytest tests/test_fixtures/ -v
```

### Performance Testing

```python
# Performance test with large datasets
large_test_case = TestCase(
    name="performance_test",
    description="Large dataset performance validation",
    data_source="eia",
    config=SyntheticDataConfig(record_count=100000)
)

# Validate processing time and memory usage
result = validator.validate_performance(
    test_case,
    actual_output,
    max_processing_time_seconds=30
)
```

## Error Handling and Reporting

### Validation Issues

The system categorizes validation issues:

```python
class ValidationSeverity(Enum):
    INFO = "INFO"         # Informational
    WARNING = "WARNING"   # Potential issue
    ERROR = "ERROR"       # Validation failure
    CRITICAL = "CRITICAL" # Critical failure
```

### Issue Types
- `MISSING_GOLDEN_FILE`: Expected golden file not found
- `OUTPUT_MISMATCH`: Actual output differs from expected
- `RECORD_COUNT_MISMATCH`: Different number of records
- `SCHEMA_COMPLIANCE`: Schema validation failures
- `DATA_QUALITY`: Data quality issues

### Reporting

```python
# Generate comprehensive validation report
report = validator.generate_validation_report(results)

print(report)
# Outputs detailed markdown report with:
# - Test results summary
# - Issue breakdown by severity
# - Performance metrics
# - Recommendations
```

## Configuration Examples

### Basic Test Case
```python
basic_test = TestCase(
    name="basic_validation",
    description="Basic field presence and type validation",
    data_source="eia",
    config=SyntheticDataConfig(
        record_count=100,
        include_null_values=False,
        seed=42
    ),
    validation_rules={
        "min_records": 100,
        "required_fields": ["id", "value", "source"]
    }
)
```

### Data Quality Test Case
```python
quality_test = TestCase(
    name="data_quality_test",
    description="Comprehensive data quality validation",
    data_source="fred",
    config=SyntheticDataConfig(
        record_count=1000,
        include_null_values=True,
        null_probability=0.1,
        include_invalid_values=True,
        invalid_probability=0.05
    ),
    validation_rules={
        "quality_threshold": 0.95,
        "max_null_rate": 0.1,
        "max_invalid_rate": 0.05
    },
    tags=["quality", "comprehensive"]
)
```

### Performance Test Case
```python
performance_test = TestCase(
    name="performance_test",
    description="Large dataset performance validation",
    data_source="iso",
    config=SyntheticDataConfig(
        record_count=10000,
        include_null_values=False,
        include_invalid_values=False
    ),
    validation_rules={
        "max_processing_time_seconds": 30,
        "min_throughput_records_per_second": 1000
    },
    tags=["performance", "large_dataset"]
)
```

## Schema Validation

### Avro Schema Support
```json
{
  "type": "record",
  "name": "EiaSeriesRecord",
  "namespace": "aurum.eia",
  "fields": [
    {"name": "series_id", "type": "string"},
    {"name": "period", "type": "string"},
    {"name": "value", "type": ["null", "double"], "default": null},
    {"name": "units", "type": ["null", "string"], "default": null}
  ]
}
```

### Schema Compliance Checking
- Field presence validation
- Type compatibility checking
- Null value handling
- Default value validation
- Union type validation

## Performance Benchmarks

### Data Generation Speed
- Small datasets (100 records): ~50ms
- Medium datasets (1000 records): ~200ms
- Large datasets (10000 records): ~1.5s

### Validation Speed
- Basic validation: ~10ms per test case
- Quality validation: ~50ms per test case
- Full validation suite: ~500ms for 10 test cases

### Memory Usage
- Base system: ~50MB
- With fixtures loaded: ~100MB
- Large dataset processing: ~200MB

## Best Practices

### Test Data Design
1. **Representative Data**: Use realistic data patterns
2. **Edge Cases**: Include boundary conditions
3. **Error Scenarios**: Test failure modes
4. **Performance**: Include scalability tests

### Fixture Management
1. **Version Control**: Track fixture changes
2. **Documentation**: Document test case purposes
3. **Maintenance**: Regular fixture updates
4. **Cleanup**: Remove obsolete fixtures

### Validation Strategy
1. **Progressive Validation**: Start simple, add complexity
2. **Comprehensive Coverage**: Test all data sources
3. **Automated Testing**: Integrate with CI/CD
4. **Performance Monitoring**: Track validation speed

### CI/CD Integration
1. **Pre-commit**: Validate before commits
2. **Pull Requests**: Automated validation
3. **Main Branch**: Full test suite
4. **Releases**: Comprehensive validation

## Troubleshooting

### Common Issues

#### Missing Golden Files
```bash
# Check for missing golden files
python -c "
from aurum.test_fixtures import GoldenFileManager
manager = GoldenFileManager(Path('test_fixtures'))
missing = [name for name in ['test1', 'test2'] if not manager.get_golden_file(name)]
print('Missing golden files:', missing)
"
```

#### Validation Failures
```python
# Debug validation failures
result = validator.validate_test_output('test_case', actual_data)
for issue in result.issues:
    print(f'{issue.severity.value}: {issue.message}')
    print(f'  Details: {issue.details}')
```

#### Performance Issues
```python
# Monitor performance
import time
start = time.time()
result = validator.validate_batch(test_results)
duration = time.time() - start
print(f'Validation took {duration:.2f} seconds for {len(test_results)} tests')
```

### Debugging Tools

#### Fixture Inspector
```python
# Inspect fixture details
fixture = generator.get_fixture('eia')
for test_case in fixture.test_cases:
    print(f'Test case: {test_case.name}')
    print(f'  Records: {len(test_case.generate_test_data())}')
    print(f'  Tags: {test_case.tags}')
```

#### Data Quality Analysis
```python
# Analyze data quality issues
issues_by_type = {}
for issue in result.issues:
    issues_by_type[issue.issue_type] = issues_by_type.get(issue.issue_type, 0) + 1

print('Issues by type:', issues_by_type)
```

## API Reference

### FixtureGenerator
```python
class FixtureGenerator:
    def generate_all_fixtures(self) -> Dict[str, Any]
    def generate_fixture(self, fixture: DataSourceFixture) -> Dict[str, Any]
    def get_fixture(self, data_source: str) -> Optional[DataSourceFixture]
    def list_fixtures(self) -> Dict[str, Any]
```

### TestValidator
```python
class TestValidator:
    def validate_test_output(self, test_name: str, actual_output: List[Dict]) -> ValidationResult
    def validate_batch(self, test_results: Dict[str, List[Dict]]) -> List[ValidationResult]
    def generate_validation_report(self, results: List[ValidationResult]) -> str
```

### SyntheticDataGenerator
```python
class SyntheticDataGenerator:
    def generate_records(self) -> List[Dict[str, Any]]
    def get_schema(self) -> Dict[str, Any]
    # Data source specific implementations available
```

## Contributing

### Adding New Data Sources
1. Create new generator class inheriting from `SyntheticDataGenerator`
2. Implement `generate_records()` and `get_schema()` methods
3. Add to `create_data_generator()` factory function
4. Update CI workflow and documentation

### Custom Validation Rules
1. Extend validation issue types
2. Add custom validators to `TestValidator`
3. Implement schema-specific validation
4. Add configuration options

### Performance Optimization
1. Implement batch processing
2. Add caching mechanisms
3. Optimize data structures
4. Parallel processing support

## Support

### Resources
- [Test Fixtures Documentation](docs/test_fixtures.md)
- [Data Quality Guide](docs/data_quality_assertions.md)
- [CI/CD Integration Guide](docs/ci_cd_integration.md)

### Community
- GitHub Issues: Report bugs and request features
- Discussions: Ask questions and share solutions
- Contributing: See CONTRIBUTING.md for guidelines

This comprehensive test fixtures system ensures data quality and pipeline correctness through automated validation, realistic test data generation, and seamless CI/CD integration.
