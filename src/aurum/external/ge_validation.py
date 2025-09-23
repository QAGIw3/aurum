"""Great Expectations validation for external data sources."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from pathlib import Path

try:
    import great_expectations as ge
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.validator.validator import Validator
    from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
    GREAT_EXPECTATIONS_AVAILABLE = True
except ImportError:
    GREAT_EXPECTATIONS_AVAILABLE = False

from aurum.external.collect.base import CollectorError
from aurum.observability.metrics import (
    GE_VALIDATION_COUNTER,
    GE_VALIDATION_LATENCY,
    GE_VALIDATION_SUCCESS,
    GE_VALIDATION_FAILURES
)

try:
    from aurum.kafka.optimized_producer import OptimizedKafkaProducer
except ImportError:
    OptimizedKafkaProducer = None

logger = logging.getLogger(__name__)


class GreatExpectationsValidator:
    """Great Expectations validator for external data sources with Kafka emission."""

    def __init__(
        self,
        context_root_dir: Optional[str] = None,
        kafka_bootstrap_servers: Optional[str] = None,
        kafka_topic: str = "qa.result.v1"
    ):
        if not GREAT_EXPECTATIONS_AVAILABLE:
            raise RuntimeError("Great Expectations is required for data validation")

        if context_root_dir:
            self.context = ge.get_context(context_root_dir=context_root_dir)
        else:
            self.context = ge.get_context()

        self._expectation_suites = {}
        self.kafka_topic = kafka_topic
        self.kafka_producer = None

        if kafka_bootstrap_servers and OptimizedKafkaProducer:
            self.kafka_producer = OptimizedKafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                topic=kafka_topic
            )

    async def validate_data(
        self,
        table_name: str,
        expectation_suite_name: str,
        data: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Validate data against Great Expectations suite.

        Args:
            table_name: Name of the table/dataset being validated
            expectation_suite_name: Name of the expectation suite to use
            data: Data to validate
            context: Additional context for validation

        Returns:
            Validation results with success/failure status and details

        Raises:
            CollectorError: If validation fails
        """
        start_time = datetime.now()

        try:
            # Get or create expectation suite
            suite = await self._get_or_create_expectation_suite(
                expectation_suite_name,
                table_name
            )

            # Create runtime batch
            batch_request = RuntimeBatchRequest(
                datasource_name="runtime_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name=table_name,
                runtime_parameters={"batch_data": data},
                batch_identifiers={"default_identifier_name": table_name}
            )

            # Create validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name
            )

            # Run validation
            results: ExpectationSuiteValidationResult = validator.validate()

            duration = (datetime.now() - start_time).total_seconds()

            # Record metrics
            if GE_VALIDATION_COUNTER:
                GE_VALIDATION_COUNTER.labels(
                    table=table_name,
                    suite=expectation_suite_name
                ).inc()

            if GE_VALIDATION_LATENCY:
                GE_VALIDATION_LATENCY.labels(
                    table=table_name,
                    suite=expectation_suite_name
                ).observe(duration)

            if results.success:
                if GE_VALIDATION_SUCCESS:
                    GE_VALIDATION_SUCCESS.labels(
                        table=table_name,
                        suite=expectation_suite_name
                    ).inc()
            else:
                if GE_VALIDATION_FAILURES:
                    GE_VALIDATION_FAILURES.labels(
                        table=table_name,
                        suite=expectation_suite_name
                    ).inc()

            # Log validation results
            await self._log_validation_results(
                table_name,
                expectation_suite_name,
                results,
                duration,
                context
            )

            # Check if validation passed
            if not results.success:
                raise CollectorError(
                    f"Great Expectations validation failed for {table_name}",
                    details={
                        "table_name": table_name,
                        "expectation_suite": expectation_suite_name,
                        "validation_results": results.to_json_dict()
                    }
                )

            # Emit QA results to Kafka
            await self._emit_qa_results(
                table_name=table_name,
                expectation_suite=expectation_suite_name,
                results=results,
                duration=duration,
                context=context
            )

            return {
                "status": "success",
                "table_name": table_name,
                "expectation_suite": expectation_suite_name,
                "validation_time_seconds": duration,
                "results": results.to_json_dict()
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()

            if GE_VALIDATION_FAILURES:
                GE_VALIDATION_FAILURES.labels(
                    table=table_name,
                    suite=expectation_suite_name
                ).inc()

            await self._log_validation_error(
                table_name,
                expectation_suite_name,
                e,
                duration,
                context
            )
            raise

    async def _get_or_create_expectation_suite(
        self,
        suite_name: str,
        table_name: str
    ) -> ExpectationSuite:
        """Get or create an expectation suite for the given table."""
        if suite_name in self._expectation_suites:
            return self._expectation_suites[suite_name]

        try:
            # Try to get existing suite
            suite = self.context.get_expectation_suite(suite_name)
        except ge.exceptions.DataContextError:
            # Create new suite if it doesn't exist
            suite = self.context.create_expectation_suite(
                expectation_suite_name=suite_name,
                overwrite_existing=False
            )

            # Add common expectations based on table type
            if "timeseries" in table_name.lower():
                await self._add_timeseries_expectations(suite)
            elif "obs" in table_name.lower():
                await self._add_observation_expectations(suite)
            else:
                await self._add_generic_expectations(suite)

            # Save the suite
            suite = self.context.save_expectation_suite(suite)

        self._expectation_suites[suite_name] = suite
        return suite

    async def _add_timeseries_expectations(self, suite: ExpectationSuite) -> None:
        """Add common expectations for timeseries data."""
        validator = self.context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="runtime_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="timeseries_template",
                runtime_parameters={"batch_data": []}
            ),
            expectation_suite=suite
        )

        validator.expect_table_row_count_to_be_between(min_value=0, max_value=None)
        validator.expect_table_column_count_to_equal(10)  # Adjust based on schema
        validator.expect_column_to_exist("timestamp")
        validator.expect_column_to_exist("value")
        validator.expect_column_to_exist("series_id")
        validator.expect_column_values_to_not_be_null("series_id")
        validator.expect_column_values_to_not_be_null("timestamp")
        validator.expect_column_values_to_not_be_null("value")
        validator.expect_column_values_to_be_of_type("value", "float64")
        validator.expect_column_values_to_be_of_type("timestamp", "datetime64")

    async def _add_observation_expectations(self, suite: ExpectationSuite) -> None:
        """Add common expectations for observation data."""
        validator = self.context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="runtime_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="observation_template",
                runtime_parameters={"batch_data": []}
            ),
            expectation_suite=suite
        )

        validator.expect_table_row_count_to_be_between(min_value=0, max_value=None)
        validator.expect_table_column_count_to_equal(15)  # Adjust based on schema
        validator.expect_column_to_exist("external_provider")
        validator.expect_column_to_exist("external_series_id")
        validator.expect_column_to_exist("curve_key")
        validator.expect_column_to_exist("value")
        validator.expect_column_to_exist("timestamp")
        validator.expect_column_values_to_not_be_null("external_provider")
        validator.expect_column_values_to_not_be_null("external_series_id")
        validator.expect_column_values_to_not_be_null("curve_key")
        validator.expect_column_values_to_not_be_null("value")
        validator.expect_column_values_to_not_be_null("timestamp")

    async def _add_generic_expectations(self, suite: ExpectationSuite) -> None:
        """Add generic expectations for unknown data types."""
        validator = self.context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="runtime_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="generic_template",
                runtime_parameters={"batch_data": []}
            ),
            expectation_suite=suite
        )

        validator.expect_table_row_count_to_be_between(min_value=0, max_value=None)
        validator.expect_table_column_count_to_be_between(min_value=1, max_value=100)

    async def _log_validation_results(
        self,
        table_name: str,
        suite_name: str,
        results: ExpectationSuiteValidationResult,
        duration: float,
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Log validation results."""
        successful_expectations = sum(1 for r in results.results if r.success)
        total_expectations = len(results.results)

        logger.info(
            "Great Expectations validation completed",
            extra={
                "table_name": table_name,
                "suite_name": suite_name,
                "duration_seconds": duration,
                "successful_expectations": successful_expectations,
                "total_expectations": total_expectations,
                "success_rate": successful_expectations / total_expectations if total_expectations > 0 else 0,
                "validation_success": results.success,
                "context": context or {}
            }
        )

    async def _log_validation_error(
        self,
        table_name: str,
        suite_name: str,
        error: Exception,
        duration: float,
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Log validation errors."""
        logger.error(
            "Great Expectations validation failed",
            extra={
                "table_name": table_name,
                "suite_name": suite_name,
                "duration_seconds": duration,
                "error": str(error),
                "context": context or {}
            }
        )

    async def _emit_qa_results(
        self,
        table_name: str,
        expectation_suite: str,
        results: ExpectationSuiteValidationResult,
        duration: float,
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Emit QA validation results to Kafka."""
        if not self.kafka_producer:
            return

        # Build QA result message
        qa_result = {
            "event_type": "qa_validation_result",
            "timestamp": datetime.now().isoformat(),
            "table_name": table_name,
            "expectation_suite": expectation_suite,
            "validation_success": results.success,
            "validation_duration_seconds": duration,
            "total_expectations": len(results.results),
            "successful_expectations": sum(1 for r in results.results if r.success),
            "failed_expectations": sum(1 for r in results.results if not r.success),
            "context": context or {},
            "results": results.to_json_dict()
        }

        try:
            await self.kafka_producer.produce_message(qa_result)
            logger.info(
                "QA results emitted to Kafka",
                extra={
                    "table_name": table_name,
                    "expectation_suite": expectation_suite,
                    "validation_success": results.success
                }
            )
        except Exception as e:
            logger.error(
                "Failed to emit QA results to Kafka",
                extra={
                    "table_name": table_name,
                    "expectation_suite": expectation_suite,
                    "error": str(e)
                }
            )


# Convenience functions for DAGs
async def run_ge_validation(
    table_name: str,
    expectation_suite: str,
    vault_addr: str,
    vault_token: str,
    data_context_root: Optional[str] = None,
    kafka_bootstrap_servers: Optional[str] = None
) -> Dict[str, Any]:
    """Run Great Expectations validation for external data with Kafka emission.

    Args:
        table_name: Name of the table/dataset to validate
        expectation_suite: Name of the expectation suite to use
        vault_addr: Vault address for secrets
        vault_token: Vault token for authentication
        data_context_root: Optional Great Expectations context root
        kafka_bootstrap_servers: Optional Kafka bootstrap servers for QA results

    Returns:
        Validation results dictionary
    """
    # This would typically load data from the table and validate it
    # For now, return placeholder implementation
    validator = GreatExpectationsValidator(
        data_context_root,
        kafka_bootstrap_servers
    )
    return await validator.validate_data(
        table_name=table_name,
        expectation_suite_name=expectation_suite,
        data=[]  # Would load actual data here
    )
