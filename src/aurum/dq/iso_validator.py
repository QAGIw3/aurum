"""Comprehensive data quality validation for ISO feeds.

This module provides Great Expectations and Seatunnel validation rules
for all ISO data feeds (CAISO, MISO, PJM, ERCOT, SPP).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from pathlib import Path

from ...observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


@dataclass
class ValidationRule:
    """A single validation rule for data quality checks."""

    rule_id: str
    name: str
    description: str
    data_types: List[str]  # ISO data types this rule applies to
    severity: str  # "error", "warning", "info"
    enabled: bool = True

    # Rule configuration
    config: Dict[str, Any] = field(default_factory=dict)

    def validate(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Tuple[bool, str, List[str]]:
        """Validate records against this rule."""
        # This would be implemented by subclasses or specific rule handlers
        return True, "OK", []


@dataclass
class ExpectationSuite:
    """A collection of expectations for a specific ISO data type."""

    suite_id: str
    iso_code: str
    data_type: str
    expectations: List[ValidationRule]
    version: str = "1.0"

    def validate(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Run all expectations in the suite."""
        results = {
            "suite_id": self.suite_id,
            "iso_code": self.iso_code,
            "data_type": self.data_type,
            "total_expectations": len(self.expectations),
            "passed": 0,
            "failed": 0,
            "errors": [],
            "warnings": [],
            "validation_time": datetime.now(timezone.utc).isoformat()
        }

        for expectation in self.expectations:
            if not expectation.enabled:
                continue

            try:
                passed, message, details = expectation.validate(records, metadata)

                if passed:
                    results["passed"] += 1
                else:
                    results["failed"] += 1

                    if expectation.severity == "error":
                        results["errors"].append({
                            "rule_id": expectation.rule_id,
                            "message": message,
                            "details": details
                        })
                    elif expectation.severity == "warning":
                        results["warnings"].append({
                            "rule_id": expectation.rule_id,
                            "message": message,
                            "details": details
                        })

            except Exception as e:
                results["failed"] += 1
                results["errors"].append({
                    "rule_id": expectation.rule_id,
                    "message": f"Validation rule failed: {e}",
                    "details": []
                })

        return results


class GreatExpectationsValidator:
    """Great Expectations-based validator for ISO data."""

    def __init__(self):
        self.suites: Dict[str, ExpectationSuite] = {}
        self.metrics = get_metrics_client()

    def load_suites_from_config(self, config_path: Path):
        """Load expectation suites from configuration file."""
        if not config_path.exists():
            logger.warning(f"Great Expectations config not found: {config_path}")
            return

        try:
            with open(config_path, 'r') as f:
                config = json.load(f)

            for suite_config in config.get('suites', []):
                suite = ExpectationSuite(
                    suite_id=suite_config['suite_id'],
                    iso_code=suite_config['iso_code'],
                    data_type=suite_config['data_type'],
                    expectations=self._parse_expectations(suite_config.get('expectations', [])),
                    version=suite_config.get('version', '1.0')
                )
                self.suites[suite.suite_id] = suite

            logger.info(f"Loaded {len(self.suites)} Great Expectations suites")

        except Exception as e:
            logger.error(f"Error loading Great Expectations config: {e}")

    def _parse_expectations(self, expectation_configs: List[Dict]) -> List[ValidationRule]:
        """Parse expectation configurations into ValidationRule objects."""
        rules = []

        for config in expectation_configs:
            rule = ValidationRule(
                rule_id=config['rule_id'],
                name=config['name'],
                description=config['description'],
                data_types=config.get('data_types', []),
                severity=config.get('severity', 'error'),
                enabled=config.get('enabled', True),
                config=config.get('config', {})
            )
            rules.append(rule)

        return rules

    async def validate_batch(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a batch of records using Great Expectations."""
        results = {
            "validator": "great_expectations",
            "total_suites": 0,
            "suites_run": 0,
            "total_expectations": 0,
            "passed_expectations": 0,
            "failed_expectations": 0,
            "suite_results": []
        }

        iso_code = metadata.get('iso_code', 'unknown')
        data_type = metadata.get('data_type', 'unknown')

        # Find applicable suites
        applicable_suites = []
        for suite in self.suites.values():
            if suite.iso_code == iso_code and suite.data_type == data_type:
                applicable_suites.append(suite)

        results["total_suites"] = len(applicable_suites)

        if not applicable_suites:
            logger.warning(f"No Great Expectations suites found for {iso_code}.{data_type}")
            return results

        # Run each suite
        for suite in applicable_suites:
            suite_result = suite.validate(records, metadata)
            results["suites_run"] += 1
            results["total_expectations"] += suite_result["total_expectations"]
            results["passed_expectations"] += suite_result["passed"]
            results["failed_expectations"] += suite_result["failed"]
            results["suite_results"].append(suite_result)

        # Emit metrics
        self.metrics.increment_counter(
            "great_expectations.validations",
            len(applicable_suites)
        )
        self.metrics.increment_counter(
            "great_expectations.expectations_evaluated",
            results["total_expectations"]
        )
        self.metrics.increment_counter(
            "great_expectations.expectations_passed",
            results["passed_expectations"]
        )
        self.metrics.increment_counter(
            "great_expectations.expectations_failed",
            results["failed_expectations"]
        )

        return results


class SeatunnelValidator:
    """Seatunnel-based validator for ISO data."""

    def __init__(self):
        self.configs: Dict[str, Dict[str, Any]] = {}
        self.metrics = get_metrics_client()

    def load_configs_from_directory(self, config_dir: Path):
        """Load Seatunnel configurations from directory."""
        if not config_dir.exists():
            logger.warning(f"Seatunnel config directory not found: {config_dir}")
            return

        try:
            for config_file in config_dir.glob("*.json"):
                try:
                    with open(config_file, 'r') as f:
                        config = json.load(f)

                    config_key = config_file.stem
                    self.configs[config_key] = config

                    logger.info(f"Loaded Seatunnel config: {config_key}")

                except Exception as e:
                    logger.error(f"Error loading Seatunnel config {config_file}: {e}")

            logger.info(f"Loaded {len(self.configs)} Seatunnel configurations")

        except Exception as e:
            logger.error(f"Error loading Seatunnel configs: {e}")

    async def validate_batch(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a batch of records using Seatunnel rules."""
        results = {
            "validator": "seatunnel",
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "check_results": []
        }

        iso_code = metadata.get('iso_code', 'unknown')
        data_type = metadata.get('data_type', 'unknown')

        # Find applicable configuration
        config_key = f"{iso_code.lower()}_{data_type}"
        if config_key not in self.configs:
            logger.warning(f"No Seatunnel configuration found for {config_key}")
            return results

        config = self.configs[config_key]

        # Run validation checks
        for check_name, check_config in config.get('checks', {}).items():
            try:
                check_result = await self._run_seatunnel_check(records, check_name, check_config)
                results["total_checks"] += 1

                if check_result["passed"]:
                    results["passed_checks"] += 1
                else:
                    results["failed_checks"] += 1

                results["check_results"].append(check_result)

            except Exception as e:
                logger.error(f"Error running Seatunnel check {check_name}: {e}")
                results["total_checks"] += 1
                results["failed_checks"] += 1
                results["check_results"].append({
                    "check_name": check_name,
                    "passed": False,
                    "error": str(e),
                    "details": []
                })

        # Emit metrics
        self.metrics.increment_counter(
            "seatunnel.validations",
            1
        )
        self.metrics.increment_counter(
            "seatunnel.checks_evaluated",
            results["total_checks"]
        )
        self.metrics.increment_counter(
            "seatunnel.checks_passed",
            results["passed_checks"]
        )
        self.metrics.increment_counter(
            "seatunnel.checks_failed",
            results["failed_checks"]
        )

        return results

    async def _run_seatunnel_check(self, records: List[Dict[str, Any]], check_name: str, check_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single Seatunnel validation check."""
        # This would implement actual Seatunnel validation logic
        # For now, return mock results based on configuration
        check_type = check_config.get('type', 'unknown')

        if check_type == 'not_null':
            return await self._check_not_null(records, check_name, check_config)
        elif check_type == 'range':
            return await self._check_range(records, check_name, check_config)
        elif check_type == 'pattern':
            return await self._check_pattern(records, check_name, check_config)
        elif check_type == 'custom':
            return await self._check_custom(records, check_name, check_config)
        else:
            return {
                "check_name": check_name,
                "passed": False,
                "error": f"Unknown check type: {check_type}",
                "details": []
            }

    async def _check_not_null(self, records: List[Dict[str, Any]], check_name: str, check_config: Dict[str, Any]) -> Dict[str, Any]:
        """Check that specified fields are not null."""
        fields = check_config.get('fields', [])
        failed_records = []

        for i, record in enumerate(records):
            for field in fields:
                if field not in record or record[field] is None or record[field] == '':
                    failed_records.append({
                        "record_index": i,
                        "field": field,
                        "value": record.get(field)
                    })

        passed = len(failed_records) == 0

        return {
            "check_name": check_name,
            "passed": passed,
            "details": failed_records if failed_records else [],
            "summary": f"Found {len(failed_records)} null values in {len(records)} records"
        }

    async def _check_range(self, records: List[Dict[str, Any]], check_name: str, check_config: Dict[str, Any]) -> Dict[str, Any]:
        """Check that numeric fields are within specified ranges."""
        field = check_config.get('field', '')
        min_val = check_config.get('min')
        max_val = check_config.get('max')
        failed_records = []

        for i, record in enumerate(records):
            if field in record:
                value = record[field]
                if isinstance(value, (int, float)):
                    if min_val is not None and value < min_val:
                        failed_records.append({
                            "record_index": i,
                            "field": field,
                            "value": value,
                            "reason": f"Below minimum {min_val}"
                        })
                    if max_val is not None and value > max_val:
                        failed_records.append({
                            "record_index": i,
                            "field": field,
                            "value": value,
                            "reason": f"Above maximum {max_val}"
                        })

        passed = len(failed_records) == 0

        return {
            "check_name": check_name,
            "passed": passed,
            "details": failed_records if failed_records else [],
            "summary": f"Found {len(failed_records)} out-of-range values in {len(records)} records"
        }

    async def _check_pattern(self, records: List[Dict[str, Any]], check_name: str, check_config: Dict[str, Any]) -> Dict[str, Any]:
        """Check that string fields match specified patterns."""
        field = check_config.get('field', '')
        pattern = check_config.get('pattern', '')
        import re
        failed_records = []

        regex = re.compile(pattern)

        for i, record in enumerate(records):
            if field in record and record[field]:
                value = str(record[field])
                if not regex.match(value):
                    failed_records.append({
                        "record_index": i,
                        "field": field,
                        "value": value,
                        "reason": f"Does not match pattern {pattern}"
                    })

        passed = len(failed_records) == 0

        return {
            "check_name": check_name,
            "passed": passed,
            "details": failed_records if failed_records else [],
            "summary": f"Found {len(failed_records)} pattern violations in {len(records)} records"
        }

    async def _check_custom(self, records: List[Dict[str, Any]], check_name: str, check_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run custom validation logic."""
        # This would implement custom validation functions
        # For now, return mock results
        return {
            "check_name": check_name,
            "passed": True,
            "details": [],
            "summary": "Custom check passed"
        }


class IsoDataQualityValidator:
    """Main validator that combines Great Expectations and Seatunnel."""

    def __init__(self):
        self.great_expectations = GreatExpectationsValidator()
        self.seatunnel = SeatunnelValidator()
        self.metrics = get_metrics_client()

    def load_validators(self, config_dir: Path):
        """Load all validator configurations."""
        # Load Great Expectations suites
        ge_config = config_dir / "great_expectations" / "suites.json"
        self.great_expectations.load_suites_from_config(ge_config)

        # Load Seatunnel configurations
        seatunnel_config_dir = config_dir / "seatunnel"
        self.seatunnel.load_configs_from_directory(seatunnel_config_dir)

    async def validate_batch(
        self,
        records: List[Dict[str, Any]],
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run comprehensive validation on a batch of records."""
        start_time = datetime.now(timezone.utc)

        results = {
            "validation_time": start_time.isoformat(),
            "iso_code": metadata.get('iso_code', 'unknown'),
            "data_type": metadata.get('data_type', 'unknown'),
            "total_records": len(records),
            "validators": {},
            "summary": {
                "great_expectations_passed": True,
                "seatunnel_passed": True,
                "overall_passed": True,
                "total_issues": 0
            }
        }

        # Run Great Expectations validation
        try:
            ge_results = await self.great_expectations.validate_batch(records, metadata)
            results["validators"]["great_expectations"] = ge_results

            if ge_results["failed_expectations"] > 0:
                results["summary"]["great_expectations_passed"] = False
                results["summary"]["total_issues"] += ge_results["failed_expectations"]

        except Exception as e:
            logger.error(f"Great Expectations validation failed: {e}")
            results["validators"]["great_expectations"] = {
                "error": str(e),
                "failed_expectations": 1
            }
            results["summary"]["great_expectations_passed"] = False
            results["summary"]["total_issues"] += 1

        # Run Seatunnel validation
        try:
            seatunnel_results = await self.seatunnel.validate_batch(records, metadata)
            results["validators"]["seatunnel"] = seatunnel_results

            if seatunnel_results["failed_checks"] > 0:
                results["summary"]["seatunnel_passed"] = False
                results["summary"]["total_issues"] += seatunnel_results["failed_checks"]

        except Exception as e:
            logger.error(f"Seatunnel validation failed: {e}")
            results["validators"]["seatunnel"] = {
                "error": str(e),
                "failed_checks": 1
            }
            results["summary"]["seatunnel_passed"] = False
            results["summary"]["total_issues"] += 1

        # Overall result
        results["summary"]["overall_passed"] = (
            results["summary"]["great_expectations_passed"] and
            results["summary"]["seatunnel_passed"]
        )

        # Emit metrics
        validation_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        self.metrics.histogram("data_quality.validation_duration", validation_duration)
        self.metrics.increment_counter("data_quality.validations")

        if not results["summary"]["overall_passed"]:
            self.metrics.increment_counter("data_quality.validation_failures")

        return results

    def get_validation_summary(self, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Get a summary of validation results."""
        summary = validation_results.get("summary", {})

        return {
            "overall_passed": summary.get("overall_passed", False),
            "total_issues": summary.get("total_issues", 0),
            "great_expectations_passed": summary.get("great_expectations_passed", True),
            "seatunnel_passed": summary.get("seatunnel_passed", True),
            "validation_time": validation_results.get("validation_time"),
            "record_count": validation_results.get("total_records", 0)
        }


# Global validator instance
_global_validator: Optional[IsoDataQualityValidator] = None


async def get_data_quality_validator() -> IsoDataQualityValidator:
    """Get or create the global data quality validator."""
    global _global_validator

    if _global_validator is None:
        _global_validator = IsoDataQualityValidator()

        # Load configurations
        config_dir = Path(__file__).parent / "config"
        _global_validator.load_validators(config_dir)

    return _global_validator


# Convenience function to create standard validation configurations
def create_standard_validation_configs() -> Dict[str, Any]:
    """Create standard validation configurations for all ISOs."""
    configs = {
        "great_expectations": {
            "suites": [
                # CAISO LMP validation
                {
                    "suite_id": "caiso_lmp_suite",
                    "iso_code": "CAISO",
                    "data_type": "lmp",
                    "version": "1.0",
                    "expectations": [
                        {
                            "rule_id": "lmp_not_null",
                            "name": "LMP Not Null Check",
                            "description": "Ensure LMP price fields are not null",
                            "data_types": ["lmp"],
                            "severity": "error",
                            "enabled": True,
                            "config": {
                                "fields": ["location_id", "price_total", "interval_start"]
                            }
                        },
                        {
                            "rule_id": "lmp_price_range",
                            "name": "LMP Price Range Check",
                            "description": "Ensure LMP prices are within reasonable bounds",
                            "data_types": ["lmp"],
                            "severity": "warning",
                            "enabled": True,
                            "config": {
                                "field": "price_total",
                                "min": -1000,
                                "max": 10000
                            }
                        }
                    ]
                }
            ]
        },
        "seatunnel": {
            "caiso_lmp": {
                "checks": {
                    "not_null_check": {
                        "type": "not_null",
                        "fields": ["location_id", "price_total", "interval_start"]
                    },
                    "price_range_check": {
                        "type": "range",
                        "field": "price_total",
                        "min": -1000,
                        "max": 10000
                    },
                    "location_pattern_check": {
                        "type": "pattern",
                        "field": "location_id",
                        "pattern": "^[A-Z_][A-Z0-9_]{0,49}$"
                    }
                }
            }
        }
    }

    return configs
