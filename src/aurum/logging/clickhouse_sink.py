"""ClickHouse sink for structured logging analytics."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
import time

import requests

from .structured_logger import LogEvent


class ClickHouseLogSink:
    """ClickHouse sink for structured logging analytics."""

    def __init__(
        self,
        clickhouse_url: str,
        table_name: str = "aurum_logs",
        batch_size: int = 100,
        flush_interval_seconds: int = 30
    ):
        """Initialize ClickHouse log sink.

        Args:
            clickhouse_url: ClickHouse HTTP URL
            table_name: Table name for logs
            batch_size: Batch size for bulk inserts
            flush_interval_seconds: Auto-flush interval in seconds
        """
        self.clickhouse_url = clickhouse_url.rstrip('/')
        self.table_name = table_name
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds

        # Batch storage
        self.batch: List[Dict[str, Any]] = []
        self._pending_flush: List[Dict[str, Any]] = []
        self.last_flush = time.time()

        # Ensure table exists
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Ensure the ClickHouse table exists."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            timestamp DateTime64(3),
            level String,
            message String,
            event_type String,
            source_name Nullable(String),
            dataset Nullable(String),
            job_id Nullable(String),
            task_id Nullable(String),
            duration_ms Nullable(Float64),
            records_processed Nullable(Int64),
            bytes_processed Nullable(Int64),
            error_code Nullable(String),
            error_message Nullable(String),
            hostname Nullable(String),
            container_id Nullable(String),
            thread_id Nullable(String),
            metadata String,
            session_id String DEFAULT 'unknown'
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, source_name, event_type)
        SETTINGS index_granularity = 8192
        """

        try:
            response = requests.post(
                f"{self.clickhouse_url}/",
                data=create_table_sql,
                headers={'Content-Type': 'text/plain'},
                timeout=10
            )
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to ensure ClickHouse table exists: {e}")

    def emit(self, event: LogEvent) -> None:
        """Emit log event to ClickHouse.

        Args:
            event: Log event to emit
        """
        # Add to batch
        record = event.to_dict()
        self.batch.append(record)
        history_limit = max(1, self.batch_size) * 10
        if len(self.batch) > history_limit:
            self.batch = self.batch[-history_limit:]
        self._pending_flush.append(record)

        # Check if we should flush
        now = time.time()
        should_flush = (
            len(self._pending_flush) >= self.batch_size or
            now - self.last_flush >= self.flush_interval_seconds
        )

        if should_flush:
            self.flush()

    def flush(self) -> None:
        """Flush batched logs to ClickHouse."""
        if not self._pending_flush:
            return

        # Prepare batch insert
        batch_data = "\n".join(
            json.dumps(record, default=str)
            for record in self._pending_flush
        )

        insert_sql = f"""
        INSERT INTO {self.table_name} FORMAT JSONEachRow
        {batch_data}
        """

        try:
            response = requests.post(
                f"{self.clickhouse_url}/",
                data=insert_sql,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )
            response.raise_for_status()

            # Clear batch on success
            self._pending_flush.clear()
            self.last_flush = time.time()

        except Exception as e:
            print(f"Failed to flush logs to ClickHouse: {e}")
            # Don't clear batch on failure - will retry later

    def query_logs(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        source_name: Optional[str] = None,
        event_type: Optional[str] = None,
        level: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query logs from ClickHouse.

        Args:
            start_time: Start time filter
            end_time: End time filter
            source_name: Source name filter
            event_type: Event type filter
            level: Log level filter
            limit: Maximum number of results

        Returns:
            List of log records
        """
        conditions = []

        if start_time:
            conditions.append(f"timestamp >= '{start_time}'")
        if end_time:
            conditions.append(f"timestamp <= '{end_time}'")
        if source_name:
            conditions.append(f"source_name = '{source_name}'")
        if event_type:
            conditions.append(f"event_type = '{event_type}'")
        if level:
            conditions.append(f"level = '{level}'")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """

        try:
            response = requests.post(
                f"{self.clickhouse_url}/",
                data=query,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )
            response.raise_for_status()

            # Parse tab-separated output
            lines = response.text.strip().split('\n')
            if not lines or lines[0].startswith('No data'):
                return []

            # First line is column names
            columns = lines[0].split('\t')

            results = []
            for line in lines[1:]:
                if not line.strip():
                    continue

                values = line.split('\t')
                record = dict(zip(columns, values))
                results.append(record)

            return results

        except Exception as e:
            print(f"Failed to query logs from ClickHouse: {e}")
            return []

    def get_error_summary(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        source_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get error summary statistics.

        Args:
            start_time: Start time filter
            end_time: End time filter
            source_name: Source name filter

        Returns:
            Dictionary with error statistics
        """
        conditions = ["level IN ('ERROR', 'FATAL')"]

        if start_time:
            conditions.append(f"timestamp >= '{start_time}'")
        if end_time:
            conditions.append(f"timestamp <= '{end_time}'")
        if source_name:
            conditions.append(f"source_name = '{source_name}'")

        where_clause = " AND ".join(conditions)

        query = f"""
        SELECT
            source_name,
            event_type,
            error_code,
            COUNT(*) as error_count,
            MIN(timestamp) as first_error,
            MAX(timestamp) as last_error,
            groupArray(error_message)[1:5] as sample_messages
        FROM {self.table_name}
        WHERE {where_clause}
        GROUP BY source_name, event_type, error_code
        ORDER BY error_count DESC
        LIMIT 100
        """

        try:
            response = requests.post(
                f"{self.clickhouse_url}/",
                data=query,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )
            response.raise_for_status()

            # Parse tab-separated output
            lines = response.text.strip().split('\n')
            if not lines or lines[0].startswith('No data'):
                return {"errors": []}

            # First line is column names
            columns = lines[0].split('\t')

            errors = []
            for line in lines[1:]:
                if not line.strip():
                    continue

                values = line.split('\t')
                record = dict(zip(columns, values))
                errors.append(record)

            return {"errors": errors}

        except Exception as e:
            print(f"Failed to get error summary from ClickHouse: {e}")
            return {"errors": []}

    def close(self) -> None:
        """Close the ClickHouse sink and flush any remaining data."""
        self.flush()


# Import time module at the end to avoid circular imports
import time
