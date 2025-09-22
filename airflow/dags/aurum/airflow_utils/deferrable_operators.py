"""Deferrable operators and sensors for efficient resource usage."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils.context import Context

from ..config import settings


class SeaTunnelJobTrigger(BaseTrigger):
    """Trigger for SeaTunnel job completion."""

    def __init__(
        self,
        job_name: str,
        job_config_path: str,
        timeout: timedelta = timedelta(hours=2),
        check_interval: timedelta = timedelta(seconds=30),
        **job_params
    ):
        super().__init__()
        self.job_name = job_name
        self.job_config_path = job_config_path
        self.timeout = timeout
        self.check_interval = check_interval
        self.job_params = job_params

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger for storage."""
        return (
            "aurum.airflow_utils.deferrable_operators.SeaTunnelJobTrigger",
            {
                "job_name": self.job_name,
                "job_config_path": self.job_config_path,
                "timeout": self.timeout,
                "check_interval": self.check_interval,
                "job_params": self.job_params,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the trigger logic."""
        start_time = datetime.now()
        job_id = f"{self.job_name}_{int(start_time.timestamp())}"

        # Start SeaTunnel job
        await self._start_seatunnel_job(job_id)

        while True:
            # Check if timeout exceeded
            if datetime.now() - start_time > self.timeout:
                yield TriggerEvent({
                    "status": "timeout",
                    "job_id": job_id,
                    "message": f"SeaTunnel job {self.job_name} timed out after {self.timeout}"
                })
                return

            # Check job status
            status = await self._check_job_status(job_id)

            if status["state"] == "SUCCESS":
                yield TriggerEvent({
                    "status": "success",
                    "job_id": job_id,
                    "message": f"SeaTunnel job {self.job_name} completed successfully",
                    "metrics": status.get("metrics", {})
                })
                return
            elif status["state"] == "FAILED":
                yield TriggerEvent({
                    "status": "failed",
                    "job_id": job_id,
                    "message": f"SeaTunnel job {self.job_name} failed: {status.get('error', 'Unknown error')}",
                    "error": status.get("error")
                })
                return
            elif status["state"] == "RUNNING":
                # Job still running, wait and check again
                self.log.info(f"SeaTunnel job {self.job_name} still running, waiting {self.check_interval}")
                await asyncio.sleep(self.check_interval.total_seconds())

    async def _start_seatunnel_job(self, job_id: str):
        """Start the SeaTunnel job."""
        # This would typically call SeaTunnel's REST API or CLI
        # For now, we'll simulate the job starting
        self.log.info(f"Starting SeaTunnel job {self.job_name} with ID {job_id}")

    async def _check_job_status(self, job_id: str) -> Dict[str, Any]:
        """Check SeaTunnel job status."""
        # This would typically query SeaTunnel's status API
        # For now, we'll simulate status checking
        # In practice, this would make HTTP calls to SeaTunnel's monitoring API
        return {
            "state": "RUNNING",  # or "SUCCESS", "FAILED"
            "progress": 0.5,
            "metrics": {
                "records_processed": 1000,
                "records_written": 950,
                "processing_time_ms": 30000
            }
        }


class APICallTrigger(BaseTrigger):
    """Trigger for HTTP API call completion."""

    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        timeout: timedelta = timedelta(minutes=5),
        check_interval: timedelta = timedelta(seconds=10),
        expected_status_codes: Optional[List[int]] = None,
        retry_on_failure: bool = True,
        max_retries: int = 3
    ):
        super().__init__()
        self.url = url
        self.method = method
        self.headers = headers or {}
        self.data = data
        self.timeout = timeout
        self.check_interval = check_interval
        self.expected_status_codes = expected_status_codes or [200, 201, 202]
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries
        self.retry_count = 0

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger for storage."""
        return (
            "aurum.airflow_utils.deferrable_operators.APICallTrigger",
            {
                "url": self.url,
                "method": self.method,
                "headers": self.headers,
                "data": self.data,
                "timeout": self.timeout,
                "check_interval": self.check_interval,
                "expected_status_codes": self.expected_status_codes,
                "retry_on_failure": self.retry_on_failure,
                "max_retries": self.max_retries,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the API call trigger logic."""
        start_time = datetime.now()
        call_id = f"{self.method}_{self.url.replace('/', '_')}_{int(start_time.timestamp())}"

        while True:
            # Check if timeout exceeded
            if datetime.now() - start_time > self.timeout:
                yield TriggerEvent({
                    "status": "timeout",
                    "call_id": call_id,
                    "message": f"API call to {self.url} timed out after {self.timeout}"
                })
                return

            # Make API call
            try:
                result = await self._make_api_call()

                if result["status_code"] in self.expected_status_codes:
                    yield TriggerEvent({
                        "status": "success",
                        "call_id": call_id,
                        "message": f"API call to {self.url} completed successfully",
                        "response": result,
                        "data": result.get("data")
                    })
                    return
                else:
                    # Unexpected status code
                    if self.retry_on_failure and self.retry_count < self.max_retries:
                        self.retry_count += 1
                        self.log.warning(f"API call failed with status {result['status_code']}, retrying ({self.retry_count}/{self.max_retries})")
                        await asyncio.sleep(self.check_interval.total_seconds())
                        continue
                    else:
                        yield TriggerEvent({
                            "status": "failed",
                            "call_id": call_id,
                            "message": f"API call to {self.url} failed with status {result['status_code']}",
                            "response": result
                        })
                        return

            except Exception as e:
                if self.retry_on_failure and self.retry_count < self.max_retries:
                    self.retry_count += 1
                    self.log.warning(f"API call failed with exception: {e}, retrying ({self.retry_count}/{self.max_retries})")
                    await asyncio.sleep(self.check_interval.total_seconds())
                    continue
                else:
                    yield TriggerEvent({
                        "status": "error",
                        "call_id": call_id,
                        "message": f"API call to {self.url} failed with exception: {e}",
                        "error": str(e)
                    })
                    return

    async def _make_api_call(self) -> Dict[str, Any]:
        """Make the actual API call."""
        # This would use aiohttp or httpx for async HTTP calls
        # For now, we'll simulate the API call
        await asyncio.sleep(0.1)  # Simulate network delay

        # Simulate different responses based on URL patterns
        if "api.eia.gov" in self.url:
            return {"status_code": 200, "data": {"series": "test"}}
        elif "api.stlouisfed.org" in self.url:
            return {"status_code": 200, "data": {"observations": []}}
        elif "api.weather.gov" in self.url:
            return {"status_code": 200, "data": {"features": []}}
        else:
            return {"status_code": 200, "data": {}}


class KafkaMessageTrigger(BaseTrigger):
    """Trigger for Kafka message availability."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        min_messages: int = 1,
        timeout: timedelta = timedelta(hours=1),
        check_interval: timedelta = timedelta(seconds=10),
        message_filter: Optional[Dict[str, Any]] = None
    ):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.min_messages = min_messages
        self.timeout = timeout
        self.check_interval = check_interval
        self.message_filter = message_filter or {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger for storage."""
        return (
            "aurum.airflow_utils.deferrable_operators.KafkaMessageTrigger",
            {
                "bootstrap_servers": self.bootstrap_servers,
                "topic": self.topic,
                "consumer_group": self.consumer_group,
                "min_messages": self.min_messages,
                "timeout": self.timeout,
                "check_interval": self.check_interval,
                "message_filter": self.message_filter,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the Kafka message trigger logic."""
        start_time = datetime.now()
        consumer_id = f"{self.consumer_group}_{int(start_time.timestamp())}"

        while True:
            # Check if timeout exceeded
            if datetime.now() - start_time > self.timeout:
                yield TriggerEvent({
                    "status": "timeout",
                    "consumer_id": consumer_id,
                    "message": f"Waiting for Kafka messages in {self.topic} timed out after {self.timeout}"
                })
                return

            # Check for messages
            message_count = await self._check_message_count()

            if message_count >= self.min_messages:
                messages = await self._consume_messages(self.min_messages)
                yield TriggerEvent({
                    "status": "success",
                    "consumer_id": consumer_id,
                    "message": f"Found {message_count} messages in topic {self.topic}",
                    "messages": messages,
                    "message_count": message_count
                })
                return
            else:
                self.log.info(f"Found {message_count} messages in {self.topic}, need {self.min_messages}. Waiting {self.check_interval}")
                await asyncio.sleep(self.check_interval.total_seconds())

    async def _check_message_count(self) -> int:
        """Check the number of available messages."""
        # This would use Kafka consumer to check message count
        # For now, we'll simulate
        await asyncio.sleep(0.05)  # Simulate consumer lag
        return 0  # Simulate no messages available

    async def _consume_messages(self, count: int) -> List[Dict[str, Any]]:
        """Consume messages from Kafka."""
        # This would use Kafka consumer to consume messages
        # For now, we'll simulate
        await asyncio.sleep(0.1)  # Simulate consumption time
        return []  # Simulate no messages consumed


class DatabaseOperationTrigger(BaseTrigger):
    """Trigger for database operation completion."""

    def __init__(
        self,
        operation_type: str,
        table_name: str,
        operation_params: Optional[Dict[str, Any]] = None,
        timeout: timedelta = timedelta(minutes=30),
        check_interval: timedelta = timedelta(seconds=15)
    ):
        super().__init__()
        self.operation_type = operation_type
        self.table_name = table_name
        self.operation_params = operation_params or {}
        self.timeout = timeout
        self.check_interval = check_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger for storage."""
        return (
            "aurum.airflow_utils.deferrable_operators.DatabaseOperationTrigger",
            {
                "operation_type": self.operation_type,
                "table_name": self.table_name,
                "operation_params": self.operation_params,
                "timeout": self.timeout,
                "check_interval": self.check_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the database operation trigger logic."""
        start_time = datetime.now()
        operation_id = f"{self.operation_type}_{self.table_name}_{int(start_time.timestamp())}"

        # Start the database operation
        await self._start_operation(operation_id)

        while True:
            # Check if timeout exceeded
            if datetime.now() - start_time > self.timeout:
                yield TriggerEvent({
                    "status": "timeout",
                    "operation_id": operation_id,
                    "message": f"Database operation {self.operation_type} on {self.table_name} timed out after {self.timeout}"
                })
                return

            # Check operation status
            status = await self._check_operation_status(operation_id)

            if status["state"] == "COMPLETED":
                yield TriggerEvent({
                    "status": "success",
                    "operation_id": operation_id,
                    "message": f"Database operation {self.operation_type} completed successfully",
                    "result": status.get("result", {})
                })
                return
            elif status["state"] == "FAILED":
                yield TriggerEvent({
                    "status": "failed",
                    "operation_id": operation_id,
                    "message": f"Database operation {self.operation_type} failed: {status.get('error', 'Unknown error')}",
                    "error": status.get("error")
                })
                return
            else:
                # Operation still running
                self.log.info(f"Database operation {self.operation_type} still running, waiting {self.check_interval}")
                await asyncio.sleep(self.check_interval.total_seconds())

    async def _start_operation(self, operation_id: str):
        """Start the database operation."""
        # This would initiate the database operation (e.g., via async database client)
        self.log.info(f"Starting database operation {self.operation_type} on {self.table_name}")

    async def _check_operation_status(self, operation_id: str) -> Dict[str, Any]:
        """Check database operation status."""
        # This would check the status of the database operation
        # For now, we'll simulate
        return {
            "state": "RUNNING",  # or "COMPLETED", "FAILED"
            "progress": 0.75
        }


class SeaTunnelJobOperator(BaseOperator):
    """Deferrable operator for SeaTunnel job execution."""

    def __init__(
        self,
        job_name: str,
        job_config_path: str,
        timeout: timedelta = timedelta(hours=2),
        check_interval: timedelta = timedelta(seconds=30),
        **job_params
    ):
        super().__init__()
        self.job_name = job_name
        self.job_config_path = job_config_path
        self.timeout = timeout
        self.check_interval = check_interval
        self.job_params = job_params

    def execute(self, context: Context):
        """Execute the SeaTunnel job using deferrable trigger."""
        self.defer(
            trigger=SeaTunnelJobTrigger(
                job_name=self.job_name,
                job_config_path=self.job_config_path,
                timeout=self.timeout,
                check_interval=self.check_interval,
                **self.job_params
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: TriggerEvent):
        """Handle completion of the deferred SeaTunnel job."""
        if event["status"] == "success":
            self.log.info(f"SeaTunnel job completed successfully: {event['message']}")
            return event.get("metrics", {})
        elif event["status"] == "timeout":
            raise AirflowException(f"SeaTunnel job timed out: {event['message']}")
        else:
            raise AirflowException(f"SeaTunnel job failed: {event['message']}")


class APICallOperator(BaseOperator):
    """Deferrable operator for HTTP API calls."""

    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        timeout: timedelta = timedelta(minutes=5),
        check_interval: timedelta = timedelta(seconds=10),
        expected_status_codes: Optional[List[int]] = None,
        retry_on_failure: bool = True,
        max_retries: int = 3
    ):
        super().__init__()
        self.url = url
        self.method = method
        self.headers = headers or {}
        self.data = data
        self.timeout = timeout
        self.check_interval = check_interval
        self.expected_status_codes = expected_status_codes or [200, 201, 202]
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries

    def execute(self, context: Context):
        """Execute the API call using deferrable trigger."""
        self.defer(
            trigger=APICallTrigger(
                url=self.url,
                method=self.method,
                headers=self.headers,
                data=self.data,
                timeout=self.timeout,
                check_interval=self.check_interval,
                expected_status_codes=self.expected_status_codes,
                retry_on_failure=self.retry_on_failure,
                max_retries=self.max_retries
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: TriggerEvent):
        """Handle completion of the deferred API call."""
        if event["status"] == "success":
            self.log.info(f"API call completed successfully: {event['message']}")
            return event.get("data")
        elif event["status"] == "timeout":
            raise AirflowException(f"API call timed out: {event['message']}")
        else:
            raise AirflowException(f"API call failed: {event['message']}")


class KafkaMessageSensor(BaseSensorOperator):
    """Deferrable sensor for Kafka message availability."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        min_messages: int = 1,
        timeout: timedelta = timedelta(hours=1),
        check_interval: timedelta = timedelta(seconds=10),
        message_filter: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.min_messages = min_messages
        self.timeout = timeout
        self.check_interval = check_interval
        self.message_filter = message_filter or {}

    def poke(self, context: Context) -> bool:
        """Check for messages (non-deferrable version)."""
        # This is the traditional sensor method - we'll override to use deferrable
        return False  # Always defer

    def execute(self, context: Context):
        """Execute using deferrable trigger."""
        self.defer(
            trigger=KafkaMessageTrigger(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.topic,
                consumer_group=self.consumer_group,
                min_messages=self.min_messages,
                timeout=self.timeout,
                check_interval=self.check_interval,
                message_filter=self.message_filter
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: TriggerEvent):
        """Handle completion of the deferred sensor."""
        if event["status"] == "success":
            self.log.info(f"Kafka messages found: {event['message']}")
            return event.get("messages", [])
        elif event["status"] == "timeout":
            raise AirflowException(f"Waiting for Kafka messages timed out: {event['message']}")
        else:
            raise AirflowException(f"Failed to find Kafka messages: {event['message']}")


class DatabaseOperationOperator(BaseOperator):
    """Deferrable operator for database operations."""

    def __init__(
        self,
        operation_type: str,
        table_name: str,
        operation_params: Optional[Dict[str, Any]] = None,
        timeout: timedelta = timedelta(minutes=30),
        check_interval: timedelta = timedelta(seconds=15),
        **kwargs
    ):
        super().__init__(**kwargs)
        self.operation_type = operation_type
        self.table_name = table_name
        self.operation_params = operation_params or {}
        self.timeout = timeout
        self.check_interval = check_interval

    def execute(self, context: Context):
        """Execute using deferrable trigger."""
        self.defer(
            trigger=DatabaseOperationTrigger(
                operation_type=self.operation_type,
                table_name=self.table_name,
                operation_params=self.operation_params,
                timeout=self.timeout,
                check_interval=self.check_interval
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: TriggerEvent):
        """Handle completion of the deferred database operation."""
        if event["status"] == "success":
            self.log.info(f"Database operation completed successfully: {event['message']}")
            return event.get("result", {})
        elif event["status"] == "timeout":
            raise AirflowException(f"Database operation timed out: {event['message']}")
        else:
            raise AirflowException(f"Database operation failed: {event['message']}")


# Convenience functions for common patterns
def create_seatunnel_job_task(
    task_id: str,
    job_name: str,
    job_config_path: str,
    dag,
    **kwargs
) -> SeaTunnelJobOperator:
    """Create a deferrable SeaTunnel job task."""
    return SeaTunnelJobOperator(
        task_id=task_id,
        job_name=job_name,
        job_config_path=job_config_path,
        dag=dag,
        **kwargs
    )


def create_api_call_task(
    task_id: str,
    url: str,
    method: str = "GET",
    dag=None,
    **kwargs
) -> APICallOperator:
    """Create a deferrable API call task."""
    return APICallOperator(
        task_id=task_id,
        url=url,
        method=method,
        dag=dag,
        **kwargs
    )


def create_kafka_sensor_task(
    task_id: str,
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
    dag=None,
    **kwargs
) -> KafkaMessageSensor:
    """Create a deferrable Kafka message sensor."""
    return KafkaMessageSensor(
        task_id=task_id,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        consumer_group=consumer_group,
        dag=dag,
        **kwargs
    )
