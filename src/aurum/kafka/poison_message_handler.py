"""Poison message handling with schema validation, retry budget, and DLQ alerting."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, Callable

from pydantic import BaseModel, ValidationError
from confluent_kafka import Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger


@dataclass
class SchemaValidationRule:
    """Schema validation rule for messages."""
    field_name: str
    field_type: str
    required: bool = True
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allowed_values: Optional[List[Any]] = None
    numeric_min: Optional[Union[int, float]] = None
    numeric_max: Optional[Union[int, float]] = None
    regex_pattern: Optional[str] = None


@dataclass
class PoisonMessageConfig:
    """Configuration for poison message handling."""
    max_retries_per_key: int = 3
    retry_backoff_seconds: float = 60.0
    dlq_topic: str = "aurum.poison_messages"
    alert_threshold_rate: float = 0.1  # Alert if 10% of messages go to DLQ
    alert_window_seconds: int = 300  # 5 minutes window for rate calculation
    schema_registry_url: Optional[str] = None
    schema_registry_auth: str = ""

    # Retry budget settings
    retry_budget_ttl_seconds: int = 3600  # 1 hour TTL for retry budgets
    retry_budget_reset_threshold: int = 10  # Reset budget after 10 successful messages

    # Schema validation settings
    strict_schema_validation: bool = True
    fail_on_unknown_fields: bool = False


@dataclass
class PoisonMessageStats:
    """Statistics for poison message handling."""
    total_processed: int = 0
    schema_validation_failures: int = 0
    business_logic_failures: int = 0
    retry_exhaustion_failures: int = 0
    dlq_messages: int = 0
    successful_messages: int = 0
    last_alert_time: float = 0.0
    alert_count: int = 0


class MessageValidator:
    """Validates messages against schemas and business rules."""

    def __init__(self, config: PoisonMessageConfig):
        self.config = config
        self.schema_registry_client = None
        self.avro_deserializer = None
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

        if config.schema_registry_url:
            self._init_schema_registry()

    def _init_schema_registry(self):
        """Initialize schema registry for validation."""
        try:
            self.schema_registry_client = SchemaRegistryClient({
                'url': self.config.schema_registry_url,
                'basic.auth.user.info': self.config.schema_registry_auth,
            })
        except Exception as e:
            self.logger.warning(f"Failed to initialize schema registry: {e}")

    async def validate_message(
        self,
        message: Dict[str, Any],
        schema_name: Optional[str] = None
    ) -> tuple[bool, Optional[str]]:
        """Validate a message against schema and business rules."""
        try:
            # Schema validation
            schema_valid, schema_error = await self._validate_schema(message, schema_name)
            if not schema_valid:
                return False, f"Schema validation failed: {schema_error}"

            # Business rules validation
            business_valid, business_error = await self._validate_business_rules(message)
            if not business_valid:
                return False, f"Business rules failed: {business_error}"

            return True, None

        except Exception as e:
            self.metrics.increment_counter("poison_message_validation_errors")
            self.logger.error(f"Validation error: {e}")
            return False, f"Validation error: {str(e)}"

    async def _validate_schema(
        self,
        message: Dict[str, Any],
        schema_name: Optional[str] = None
    ) -> tuple[bool, Optional[str]]:
        """Validate message against schema."""
        if not self.config.strict_schema_validation:
            return True, None

        try:
            # Basic type validation
            validation_errors = []

            # Required field validation
            required_fields = self._get_required_fields(schema_name)
            for field in required_fields:
                if field not in message or message[field] is None:
                    validation_errors.append(f"Missing required field: {field}")

            # Type validation
            type_rules = self._get_type_rules(schema_name)
            for field, expected_type in type_rules.items():
                if field in message and message[field] is not None:
                    actual_type = type(message[field]).__name__
                    if actual_type != expected_type:
                        validation_errors.append(
                            f"Type mismatch for {field}: expected {expected_type}, got {actual_type}"
                        )

            # Length validation
            length_rules = self._get_length_rules(schema_name)
            for field, (min_len, max_len) in length_rules.items():
                if field in message and isinstance(message[field], str):
                    length = len(message[field])
                    if min_len is not None and length < min_len:
                        validation_errors.append(
                            f"Field {field} too short: {length} < {min_len}"
                        )
                    if max_len is not None and length > max_len:
                        validation_errors.append(
                            f"Field {field} too long: {length} > {max_len}"
                        )

            # Numeric range validation
            range_rules = self._get_range_rules(schema_name)
            for field, (min_val, max_val) in range_rules.items():
                if field in message and message[field] is not None:
                    value = message[field]
                    if min_val is not None and value < min_val:
                        validation_errors.append(
                            f"Field {field} below minimum: {value} < {min_val}"
                        )
                    if max_val is not None and value > max_val:
                        validation_errors.append(
                            f"Field {field} above maximum: {value} > {max_val}"
                        )

            if validation_errors:
                return False, "; ".join(validation_errors)

            return True, None

        except Exception as e:
            return False, f"Schema validation error: {str(e)}"

    async def _validate_business_rules(
        self,
        message: Dict[str, Any]
    ) -> tuple[bool, Optional[str]]:
        """Validate business rules."""
        # This would be implemented based on specific business requirements
        # For now, just return success
        return True, None

    def _get_required_fields(self, schema_name: Optional[str]) -> List[str]:
        """Get required fields for schema."""
        # Default required fields for common message types
        defaults = ["timestamp", "data_source", "message_type"]
        return defaults

    def _get_type_rules(self, schema_name: Optional[str]) -> Dict[str, str]:
        """Get type validation rules."""
        return {
            "timestamp": "int",
            "data_source": "str",
            "message_type": "str",
            "value": "float"
        }

    def _get_length_rules(self, schema_name: Optional[str]) -> Dict[str, tuple[Optional[int], Optional[int]]]:
        """Get length validation rules."""
        return {
            "data_source": (1, 50),
            "message_type": (1, 100),
            "description": (None, 500)
        }

    def _get_range_rules(self, schema_name: Optional[str]) -> Dict[str, tuple[Optional[Union[int, float]], Optional[Union[int, float]]]]:
        """Get numeric range validation rules."""
        return {
            "timestamp": (0, int(time.time() * 1000) + 86400000),  # Max 1 day in future
            "value": (None, None)  # No specific range
        }


class PoisonMessageHandler:
    """Handles poison messages with validation, retry budgets, and DLQ."""

    def __init__(
        self,
        config: PoisonMessageConfig,
        producer_client,
        alert_callback: Optional[Callable] = None
    ):
        self.config = config
        self.producer_client = producer_client
        self.validator = MessageValidator(config)
        self.alert_callback = alert_callback
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

        # State management
        self.stats = PoisonMessageStats()
        self.retry_budgets: Dict[str, Dict[str, Any]] = {}  # key -> {budget, last_failure, successful_count}
        self.dlq_rate_history: List[float] = []  # Recent DLQ rates for alerting
        self._cleanup_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Initialize the poison message handler."""
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_budgets())

    async def close(self):
        """Close the poison message handler."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def handle_message(
        self,
        message: Message,
        schema_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Handle a potentially poisonous message."""
        self.stats.total_processed += 1
        start_time = time.time()

        try:
            # Deserialize message
            deserialized = self._deserialize_message(message)
            if not deserialized:
                return await self._handle_invalid_message(message, "Deserialization failed")

            # Get message key for tracking
            message_key = message.key().decode('utf-8') if message.key() else str(message.offset())

            # Check retry budget
            budget_info = self.retry_budgets.get(message_key, self._create_budget_info())
            remaining_budget = budget_info['budget']

            if remaining_budget <= 0:
                return await self._handle_retry_exhausted(message, deserialized)

            # Validate message
            is_valid, validation_error = await self.validator.validate_message(
                deserialized, schema_name
            )

            if not is_valid:
                return await self._handle_validation_failure(message, deserialized, validation_error, budget_info)

            # Try to process the message
            try:
                # This would call the actual message handler
                # For now, simulate success/failure
                processing_success = await self._simulate_processing(deserialized)

                if processing_success:
                    return await self._handle_success(message, deserialized, budget_info)
                else:
                    return await self._handle_processing_failure(message, deserialized, budget_info)

            except Exception as e:
                return await self._handle_processing_failure(message, deserialized, budget_info, str(e))

        except Exception as e:
            self.metrics.increment_counter("poison_message_handling_errors")
            self.logger.error(f"Error handling message: {e}")
            return {
                "success": False,
                "error": str(e),
                "action": "error"
            }
        finally:
            processing_time = time.time() - start_time
            self.metrics.histogram("poison_message_handling_time", processing_time)

    def _deserialize_message(self, message: Message) -> Optional[Dict[str, Any]]:
        """Deserialize Kafka message."""
        try:
            if message.value():
                return json.loads(message.value().decode('utf-8'))
            return None
        except Exception as e:
            self.metrics.increment_counter("poison_message_deserialization_errors")
            self.logger.error(f"Failed to deserialize message: {e}")
            return None

    def _create_budget_info(self) -> Dict[str, Any]:
        """Create retry budget information for a message key."""
        return {
            "budget": self.config.max_retries_per_key,
            "last_failure": None,
            "successful_count": 0,
            "created_at": time.time()
        }

    async def _handle_invalid_message(
        self,
        message: Message,
        error: str
    ) -> Dict[str, Any]:
        """Handle a message that couldn't be deserialized."""
        self.stats.schema_validation_failures += 1
        await self._send_to_dlq(message, error, "schema_validation_failed")

        return {
            "success": False,
            "error": error,
            "action": "dlq"
        }

    async def _handle_validation_failure(
        self,
        message: Message,
        deserialized: Dict[str, Any],
        validation_error: str,
        budget_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle a message that failed validation."""
        self.stats.schema_validation_failures += 1

        # Update budget
        budget_info['budget'] -= 1
        budget_info['last_failure'] = time.time()

        if budget_info['budget'] <= 0:
            await self._send_to_dlq(message, validation_error, "validation_failed")
            return {
                "success": False,
                "error": validation_error,
                "action": "dlq"
            }

        return {
            "success": False,
            "error": validation_error,
            "action": "retry",
            "remaining_budget": budget_info['budget']
        }

    async def _handle_processing_failure(
        self,
        message: Message,
        deserialized: Dict[str, Any],
        budget_info: Dict[str, Any],
        error: str = "Processing failed"
    ) -> Dict[str, Any]:
        """Handle a message that failed processing."""
        self.stats.business_logic_failures += 1

        # Update budget
        budget_info['budget'] -= 1
        budget_info['last_failure'] = time.time()

        if budget_info['budget'] <= 0:
            await self._send_to_dlq(message, error, "processing_failed")
            return {
                "success": False,
                "error": error,
                "action": "dlq"
            }

        return {
            "success": False,
            "error": error,
            "action": "retry",
            "remaining_budget": budget_info['budget']
        }

    async def _handle_retry_exhausted(
        self,
        message: Message,
        deserialized: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle a message that has exhausted its retry budget."""
        self.stats.retry_exhaustion_failures += 1
        await self._send_to_dlq(message, "Retry budget exhausted", "retry_exhausted")

        return {
            "success": False,
            "error": "Retry budget exhausted",
            "action": "dlq"
        }

    async def _handle_success(
        self,
        message: Message,
        deserialized: Dict[str, Any],
        budget_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle a successfully processed message."""
        self.stats.successful_messages += 1

        # Update budget for success
        budget_info['successful_count'] += 1

        # Reset budget if enough successful messages
        if budget_info['successful_count'] >= self.config.retry_budget_reset_threshold:
            budget_info['budget'] = self.config.max_retries_per_key
            budget_info['successful_count'] = 0
            budget_info['last_failure'] = None

        # Check for alerting
        await self._check_alert_thresholds()

        return {
            "success": True,
            "action": "processed"
        }

    async def _send_to_dlq(
        self,
        message: Message,
        error: str,
        reason: str
    ):
        """Send a message to the dead letter queue."""
        try:
            if not self.producer_client:
                self.logger.error("No producer client available for DLQ")
                return

            # Create DLQ message with metadata
            dlq_message = {
                'original_message': {
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset(),
                    'key': message.key().decode('utf-8') if message.key() else None,
                    'value': message.value().decode('utf-8') if message.value() else None,
                    'timestamp': message.timestamp()[1],
                    'headers': dict(message.headers()) if message.headers() else {}
                },
                'poison_info': {
                    'error': error,
                    'reason': reason,
                    'dlq_timestamp': int(time.time() * 1000),
                    'retry_budget_exhausted': reason in ['retry_exhausted', 'processing_failed'],
                    'validation_failed': reason == 'validation_failed'
                }
            }

            # Send to DLQ
            await self.producer_client.produce(
                topic=self.config.dlq_topic,
                value=dlq_message,
                key=message.key(),
                headers={
                    'dlq_reason': reason,
                    'original_topic': message.topic(),
                    'error_message': error,
                    'poison_timestamp': str(int(time.time() * 1000))
                }
            )

            self.stats.dlq_messages += 1
            self.metrics.increment_counter("poison_message_dlq_sent")

            # Update rate history for alerting
            self.dlq_rate_history.append(1.0)  # 1 = DLQ, 0 = processed

            # Keep only recent history
            if len(self.dlq_rate_history) > 100:
                self.dlq_rate_history.pop(0)

        except Exception as e:
            self.metrics.increment_counter("poison_message_dlq_errors")
            self.logger.error(f"Failed to send message to DLQ: {e}")

    async def _check_alert_thresholds(self):
        """Check if alerting thresholds are exceeded."""
        if not self.dlq_rate_history:
            return

        # Calculate DLQ rate over recent messages
        recent_messages = min(50, len(self.dlq_rate_history))
        recent_dlg_rate = sum(self.dlq_rate_history[-recent_messages:]) / recent_messages

        if recent_dlg_rate >= self.config.alert_threshold_rate:
            current_time = time.time()

            # Don't spam alerts - only alert once per window
            if current_time - self.stats.last_alert_time >= self.config.alert_window_seconds:
                await self._trigger_alert(
                    f"High DLQ rate: {recent_dlg_rate".2%"} over last {recent_messages} messages"
                )
                self.stats.last_alert_time = current_time
                self.stats.alert_count += 1

    async def _trigger_alert(self, message: str):
        """Trigger an alert for poison message issues."""
        self.logger.warning(f"Poison message alert: {message}")

        if self.alert_callback:
            try:
                await self.alert_callback({
                    "type": "poison_message_alert",
                    "message": message,
                    "stats": {
                        "total_processed": self.stats.total_processed,
                        "dlq_rate": sum(self.dlq_rate_history) / len(self.dlq_rate_history) if self.dlq_rate_history else 0,
                        "alert_count": self.stats.alert_count
                    },
                    "timestamp": int(time.time() * 1000)
                })
            except Exception as e:
                self.logger.error(f"Failed to send alert: {e}")

    async def _cleanup_expired_budgets(self):
        """Clean up expired retry budgets."""
        while True:
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes

                current_time = time.time()
                expired_keys = []

                for key, budget_info in self.retry_budgets.items():
                    if current_time - budget_info['created_at'] > self.config.retry_budget_ttl_seconds:
                        expired_keys.append(key)

                for key in expired_keys:
                    del self.retry_budgets[key]

                if expired_keys:
                    self.logger.info(f"Cleaned up {len(expired_keys)} expired retry budgets")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error cleaning up retry budgets: {e}")

    async def _simulate_processing(self, message: Dict[str, Any]) -> bool:
        """Simulate message processing (replace with actual logic)."""
        # This would be replaced with actual message processing logic
        # For demonstration, randomly fail some messages
        import random
        return random.random() > 0.1  # 90% success rate

    def get_stats(self) -> PoisonMessageStats:
        """Get poison message handling statistics."""
        return self.stats.copy()

    def get_retry_budget(self, key: str) -> int:
        """Get remaining retry budget for a key."""
        budget_info = self.retry_budgets.get(key, self._create_budget_info())
        return budget_info['budget']


__all__ = [
    "PoisonMessageConfig",
    "PoisonMessageStats",
    "MessageValidator",
    "PoisonMessageHandler",
    "SchemaValidationRule",
]
