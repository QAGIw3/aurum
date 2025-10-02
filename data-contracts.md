# Data Contract Validation System

This document describes the comprehensive data contract validation system for the Aurum platform, ensuring data quality, consistency, and compliance through automated validation using Great Expectations.

## Overview

The data contract validation system provides:

- **Automated Validation**: Continuous validation of data contracts against Great Expectations suites
- **Sample Data Testing**: Validation against realistic sample datasets
- **Comprehensive Reporting**: JSON and HTML reports with detailed results
- **CI/CD Integration**: Automated gating of deployments based on validation results
- **Performance Monitoring**: Tracking of validation performance over time

## Architecture

```
┌─────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ Data Sources    │───▶│ Great Expectations  │───▶│ Validation       │
│ (CSV, DB, API)  │    │ Validation Engine   │    │ Engine           │
└─────────────────┘    └─────────────────────┘    └──────────────────┘
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ Sample Dataset  │    │ Expectation Suites  │    │ Validation       │
│ Generation      │    │ (JSON/YAML)         │    │ Reports          │
└─────────────────┘    └─────────────────────┘    └──────────────────┘
```

## Components

### 1. Enhanced Validation Script

**Features:**
- Great Expectations integration with actual data execution
- Sample dataset generation for testing
- Comprehensive JSON and HTML reporting
- Critical failure gating
- Performance metrics collection

### 2. Great Expectations Suites

#### Scenarios Suite
Validates scenario data contracts:
- Required fields (id, tenant_id, name)
- Data types (strings, dictionaries, timestamps)
- Naming conventions (regex patterns)
- Uniqueness constraints
- Reasonable value ranges

#### Scenario Runs Suite
Validates scenario run data contracts:
- Required fields (id, scenario_id, status)
- Valid status values
- Reference integrity
- Timestamp formats
- Uniqueness constraints

#### Curves Suite
Validates curve data contracts:
- Required fields (id, name, data_points)
- Data types (strings, integers)
- Reasonable value ranges
- Naming conventions

### 3. Sample Dataset Generation

Creates realistic test data:
- **Scenarios**: Sample scenario definitions
- **Scenario Runs**: Sample execution records
- **Curves**: Sample curve metadata

### 4. Validation Reports

#### JSON Report Structure
```json
{
  "validation_timestamp": "2024-01-01T12:00:00.000Z",
  "total_datasets": 3,
  "total_suites": 3,
  "passed_validations": 8,
  "failed_validations": 1,
  "results": [...]
}
```

#### HTML Report Features
- Executive summary with overall status
- Detailed results per dataset
- Failed expectation details
- Performance metrics
- Visual indicators for pass/fail status

## CI/CD Integration

### Workflow Integration

**Triggers:**
- Push to main/develop branches
- Pull requests to main/develop branches
- Changes to data contract files

**Quality Gates:**
- Fail on critical validation failures
- Comment on PRs with detailed results
- Block merges if critical contracts fail
- Upload reports as artifacts

### Usage Examples

```bash
# Run validation
python scripts/data_contracts/validate_contracts_enhanced.py \
  --ge-expectations scripts/data_contracts/great_expectations_suites \
  --report-dir validation_reports \
  --create-sample-data \
  --fail-on-critical
```

## Configuration

### Environment Variables

```bash
# Great Expectations Configuration
GE_HOME=/path/to/great_expectations
GE_ENVIRONMENT=local

# Validation Configuration
VALIDATION_REPORT_DIR=validation_reports
FAIL_ON_CRITICAL=true
CREATE_SAMPLE_DATA=true
```

### Expectation Suite Design

1. **Start Simple**: Begin with basic expectations (not null, types)
2. **Add Business Logic**: Include domain-specific validations
3. **Test Thoroughly**: Validate expectations against real data
4. **Document Clearly**: Add meta information to expectations

## Troubleshooting

### Common Issues

#### Validation Failures
```bash
# Run with debug output
python scripts/data_contracts/validate_contracts_enhanced.py \
  --verbose \
  --report-dir ./debug_reports
```

#### Performance Issues
```bash
# Profile validation performance
python -m cProfile scripts/data_contracts/validate_contracts_enhanced.py
```

## Best Practices

### Expectation Suite Design

1. **Start Simple**: Begin with basic expectations
2. **Add Business Logic**: Include domain-specific validations
3. **Test Thoroughly**: Validate expectations against real data
4. **Document Clearly**: Add meta information

### CI/CD Integration

1. **Fast Feedback**: Run validation early in pipeline
2. **Clear Reporting**: Provide actionable error messages
3. **Gradual Rollout**: Start with warnings, then gate deployments
4. **Regular Updates**: Keep expectations current with data model

## Security Considerations

### Data Protection

- **Sensitive Data**: Avoid including PII in sample datasets
- **Encryption**: Encrypt sensitive validation results
- **Access Control**: Restrict access to validation reports
- **Data Retention**: Clean up old validation artifacts

---

*This data contract validation system ensures high data quality and consistency across the Aurum platform while providing actionable feedback and comprehensive reporting.*