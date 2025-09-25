"""Tests for structured logging system."""

from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from pathlib import Path

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.logging import (
    StructuredLogger,
    LogLevel,
    LogEvent,
    KafkaLogSink,
    ClickHouseLogSink,
    LoggingMetrics
)


class TestLogEvent:
    def test_log_event_creation(self) -> None:
        """Test LogEvent creation and basic functionality."""
        event = LogEvent(
            level=LogLevel.ERROR,
            message="Test error message",
            event_type="test_error",
            source_name="test_source",
            dataset="test_dataset"
        )

        assert event.level == LogLevel.ERROR
        assert event.message == "Test error message"
        assert event.event_type == "test_error"
        assert event.source_name == "test_source"
        assert event.dataset == "test_dataset"
        assert isinstance(event.timestamp, str)

    def test_log_event_to_dict(self) -> None:
        """Test LogEvent to dictionary conversion."""
        event = LogEvent(
            level=LogLevel.INFO,
            message="Test message",
            event_type="test_event",
            source_name="test_source",
            duration_ms=123.45,
            records_processed=100,
            metadata={"key": "value"}
        )

        result = event.to_dict()

        assert result["level"] == "INFO"
        assert result["message"] == "Test message"
        assert result["event_type"] == "test_event"
        assert result["source_name"] == "test_source"
        assert result["duration_ms"] == 123.45
        assert result["records_processed"] == 100
        assert result["metadata"] == {"key": "value"}

    def test_log_event_to_json(self) -> None:
        """Test LogEvent to JSON conversion."""
        event = LogEvent(
            level=LogLevel.WARN,
            message="Test warning",
            event_type="test_warning"
        )

        json_str = event.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed["level"] == "WARN"
        assert parsed["message"] == "Test warning"
        assert parsed["event_type"] == "test_warning"

    def test_log_event_from_dict(self) -> None:
        """Test LogEvent creation from dictionary."""
        data = {
            "timestamp": "2024-01-01T12:00:00.000000+00:00",
            "level": "ERROR",
            "message": "Test error",
            "event_type": "test_error",
            "source_name": "test_source",
            "dataset": "test_dataset",
            "duration_ms": 123.45
        }

        event = LogEvent.from_dict(data)

        assert event.level == LogLevel.ERROR
        assert event.message == "Test error"
        assert event.event_type == "test_error"
        assert event.source_name == "test_source"
        assert event.dataset == "test_dataset"
        assert event.duration_ms == 123.45


class TestStructuredLogger:
    def test_logger_creation(self) -> None:
        """Test StructuredLogger creation."""
        logger = StructuredLogger("test_source")

        assert logger.source_name == "test_source"
        assert logger.dataset is None
        assert logger.kafka_sink is None
        assert logger.clickhouse_sink is None
        assert isinstance(logger.session_id, str)
        assert len(logger.session_id) == 36  # UUID length

    def test_log_method(self) -> None:
        """Test basic logging method."""
        logger = StructuredLogger("test_source")

        with patch.object(logger, '_emit_event') as mock_emit:
            logger.log(LogLevel.INFO, "Test message", "test_event")

            mock_emit.assert_called_once()
            call_args = mock_emit.call_args[0][0]

            assert call_args.level == LogLevel.INFO
            assert call_args.message == "Test message"
            assert call_args.event_type == "test_event"
            assert call_args.source_name == "test_source"

    def test_start_end_operation(self) -> None:
        """Test operation tracking."""
        logger = StructuredLogger("test_source")

        with patch.object(logger, 'log') as mock_log:
            operation_id = logger.start_operation("test_operation")

            assert operation_id in logger._operation_starts

            # Mock time to control duration
            with patch('aurum.logging.structured_logger.time.time', return_value=123.0):
                logger.end_operation(operation_id, success=True)

            assert operation_id not in logger._operation_starts

            # Should log start and end events
            assert mock_log.call_count == 2

            # Check start event
            start_call = mock_log.call_args_list[0]
            assert start_call[0][0] == LogLevel.INFO  # level
            assert "Starting operation: test_operation" in start_call[0][1]  # message

            # Check end event
            end_call = mock_log.call_args_list[1]
            assert end_call[0][0] == LogLevel.INFO  # level
            assert "completed in" in end_call[0][1]  # message

    def test_data_quality_logging(self) -> None:
        """Test data quality logging."""
        logger = StructuredLogger("test_source")

        with patch.object(logger, 'log') as mock_log:
            logger.log_data_quality(
                records_processed=1000,
                records_valid=950,
                records_invalid=50
            )

            mock_log.assert_called_once()
            call_args = mock_log.call_args

            assert call_args[0][0] == LogLevel.INFO  # level
            assert "95.0%" in call_args[0][1]  # message contains quality score
            assert call_args[1]["event_type"] == "data_quality"
            assert call_args[1]["records_processed"] == 1000
            assert call_args[1]["records_valid"] == 950
            assert call_args[1]["records_invalid"] == 50

    def test_api_call_logging(self) -> None:
        """Test API call logging."""
        logger = StructuredLogger("test_source")

        with patch.object(logger, 'log') as mock_log:
            logger.log_api_call(
                endpoint="/api/test",
                method="POST",
                status_code=200,
                response_time_ms=123.45
            )

            mock_log.assert_called_once()
            call_args = mock_log.call_args

            assert call_args[0][0] == LogLevel.DEBUG  # level
            assert "/api/test" in call_args[0][1]  # message
            assert call_args[1]["event_type"] == "api_call"
            assert call_args[1]["endpoint"] == "/api/test"
            assert call_args[1]["method"] == "POST"
            assert call_args[1]["status_code"] == 200
            assert call_args[1]["response_time_ms"] == 123.45

    def test_error_logging(self) -> None:
        """Test error logging."""
        logger = StructuredLogger("test_source")

        test_error = ValueError("Test error message")

        with patch.object(logger, 'log') as mock_log:
            logger.log_error(test_error, context="test context")

            mock_log.assert_called_once()
            call_args = mock_log.call_args

            assert call_args[0][0] == LogLevel.ERROR  # level
            assert "test context: ValueError: Test error message" in call_args[0][1]  # message
            assert call_args[1]["event_type"] == "error"
            assert call_args[1]["error_code"] == "ValueError"
            assert call_args[1]["error_message"] == "Test error message"


class TestKafkaLogSink:
    def test_kafka_sink_creation(self) -> None:
        """Test KafkaLogSink creation."""
        sink = KafkaLogSink(
            bootstrap_servers="localhost:9092",
            topic="test_topic"
        )

        assert sink.topic == "test_topic"
        assert sink.use_avro is True  # Default

    def test_emit_with_mock_producer(self) -> None:
        """Test emitting events with mocked producer."""
        sink = KafkaLogSink(
            bootstrap_servers="localhost:9092",
            topic="test_topic",
            use_avro=False  # Disable Avro for simpler testing
        )

        event = LogEvent(
            level=LogLevel.INFO,
            message="Test message",
            event_type="test_event",
            source_name="test_source"
        )

        with patch.object(sink, 'producer') as mock_producer:
            sink.emit(event)

            mock_producer.produce.assert_called_once()
            call_args = mock_producer.produce.call_args

            assert call_args[1]['topic'] == 'test_topic'
            assert 'key' in call_args[1]
            assert 'value' in call_args[1]


class TestClickHouseLogSink:
    def test_clickhouse_sink_creation(self) -> None:
        """Test ClickHouseLogSink creation."""
        sink = ClickHouseLogSink(
            clickhouse_url="http://localhost:8123",
            table_name="test_logs"
        )

        assert sink.clickhouse_url == "http://localhost:8123"
        assert sink.table_name == "test_logs"
        assert sink.batch_size == 100  # Default

    def test_emit_and_flush(self) -> None:
        """Test emitting and flushing events."""
        sink = ClickHouseLogSink(
            clickhouse_url="http://localhost:8123",
            table_name="test_logs",
            batch_size=2  # Small batch size for testing
        )

        event1 = LogEvent(
            level=LogLevel.INFO,
            message="Test message 1",
            event_type="test_event",
            source_name="test_source"
        )

        event2 = LogEvent(
            level=LogLevel.ERROR,
            message="Test message 2",
            event_type="test_error",
            source_name="test_source"
        )

        with patch('requests.post') as mock_post:
            mock_post.return_value.raise_for_status.return_value = None

            sink.emit(event1)
            assert len(sink.batch) == 1

            sink.emit(event2)
            assert len(sink.batch) == 2

            # Should auto-flush due to batch size
            mock_post.assert_called_once()

    def test_query_logs(self) -> None:
        """Test querying logs."""
        sink = ClickHouseLogSink(
            clickhouse_url="http://localhost:8123",
            table_name="test_logs"
        )

        mock_response = MagicMock()
        mock_response.text = "timestamp\tlevel\tmessage\tsource_name\n2024-01-01 12:00:00\tINFO\tTest message\ttest_source\n"
        mock_response.raise_for_status.return_value = None

        with patch('requests.post') as mock_post:
            mock_post.return_value = mock_response

            results = sink.query_logs(limit=10)

            assert len(results) == 1
            assert results[0]['message'] == 'Test message'
            assert results[0]['source_name'] == 'test_source'

    def test_get_error_summary(self) -> None:
        """Test getting error summary."""
        sink = ClickHouseLogSink(
            clickhouse_url="http://localhost:8123",
            table_name="test_logs"
        )

        mock_response = MagicMock()
        mock_response.text = "source_name\terror_code\terror_count\n2024-01-01 12:00:00\ttest_source\tValueError\t5\n"
        mock_response.raise_for_status.return_value = None

        with patch('requests.post') as mock_post:
            mock_post.return_value = mock_response

            summary = sink.get_error_summary()

            assert "errors" in summary
            assert len(summary["errors"]) == 1
            assert summary["errors"][0]["source_name"] == "2024-01-01 12:00:00"


class TestLoggingMetrics:
    def test_record_log_event(self) -> None:
        """Test recording log event metrics."""
        metrics = LoggingMetrics()

        metrics.record_log_event(
            level="ERROR",
            source_name="test_source",
            event_type="api_error",
            processing_time_ms=123.45
        )

        assert metrics.log_events_total == 1
        assert metrics.log_events_by_level["ERROR"] == 1
        assert metrics.log_events_by_source["test_source"] == 1
        assert metrics.log_events_by_type["api_error"] == 1
        assert len(metrics.log_processing_time["test_source.api_error"]) == 1
        assert metrics.log_processing_time["test_source.api_error"][0] == 123.45

    def test_get_summary(self) -> None:
        """Test getting metrics summary."""
        metrics = LoggingMetrics()

        metrics.record_log_event("INFO", "source1", "event1", 100.0)
        metrics.record_log_event("ERROR", "source1", "event1", 200.0)
        metrics.record_log_event("INFO", "source2", "event2", 150.0)

        summary = metrics.get_summary()

        assert summary["log_events_total"] == 3
        assert summary["log_events_by_level"]["INFO"] == 2
        assert summary["log_events_by_level"]["ERROR"] == 1
        assert summary["log_events_by_source"]["source1"] == 2
        assert summary["log_events_by_source"]["source2"] == 1
        assert summary["log_events_by_type"]["event1"] == 2
        assert summary["log_events_by_type"]["event2"] == 1

        # Check processing time percentiles
        assert "source1.event1" in summary["processing_time_percentiles"]
        assert "source2.event2" in summary["processing_time_percentiles"]

    def test_reset_metrics(self) -> None:
        """Test resetting metrics."""
        metrics = LoggingMetrics()

        metrics.record_log_event("INFO", "test", "event", 100.0)
        metrics.active_sessions = 5
        metrics.kafka_backlog = 100

        assert metrics.log_events_total == 1
        assert metrics.active_sessions == 5
        assert metrics.kafka_backlog == 100

        metrics.reset()

        assert metrics.log_events_total == 0
        assert metrics.active_sessions == 0
        assert metrics.kafka_backlog == 0
        assert len(metrics.log_events_by_level) == 0
        assert len(metrics.log_events_by_source) == 0
        assert len(metrics.log_events_by_type) == 0
        assert len(metrics.log_processing_time) == 0


class TestIntegrationScenarios:
    def test_complete_logging_flow(self) -> None:
        """Test complete logging flow with mocked sinks."""
        # Mock sinks
        mock_kafka = MagicMock()
        mock_clickhouse = MagicMock()

        logger = StructuredLogger(
            source_name="test_source",
            kafka_sink=mock_kafka,
            clickhouse_sink=mock_clickhouse,
            dataset="test_dataset"
        )

        # Log various events
        logger.log(LogLevel.INFO, "Starting job", "job_start")
        logger.log_data_quality(1000, 950, 50)
        logger.log_api_call("/api/test", "GET", 200, 123.45)

        # Verify all events were emitted
        assert mock_kafka.emit.call_count == 3
        assert mock_clickhouse.emit.call_count == 3

        # Test operation tracking
        operation_id = logger.start_operation("test_operation")
        logger.end_operation(operation_id, success=True)

        assert mock_kafka.emit.call_count == 5  # 3 + 2 operation events
        assert mock_clickhouse.emit.call_count == 5

    def test_error_handling(self) -> None:
        """Test error handling in logging."""
        # Mock sinks that raise exceptions
        mock_kafka = MagicMock()
        mock_kafka.emit.side_effect = Exception("Kafka error")

        mock_clickhouse = MagicMock()
        mock_clickhouse.emit.side_effect = Exception("ClickHouse error")

        logger = StructuredLogger(
            source_name="test_source",
            kafka_sink=mock_kafka,
            clickhouse_sink=mock_clickhouse
        )

        # Should not raise exceptions, but handle them gracefully
        logger.log(LogLevel.ERROR, "Test error", "test_error")

        # Should still attempt to log to console
        assert mock_kafka.emit.call_count == 1
        assert mock_clickhouse.emit.call_count == 1

    def test_session_tracking(self) -> None:
        """Test session tracking across multiple loggers."""
        logger1 = StructuredLogger("source1")
        logger2 = StructuredLogger("source2")

        # Each logger should have a different session ID
        assert logger1.session_id != logger2.session_id
        assert len(logger1.session_id) == 36  # UUID length
        assert len(logger2.session_id) == 36

        # Log events should include session IDs
        with patch.object(logger1, '_emit_event') as mock_emit1:
            with patch.object(logger2, '_emit_event') as mock_emit2:
                logger1.log(LogLevel.INFO, "Message 1", "event1")
                logger2.log(LogLevel.INFO, "Message 2", "event2")

                # Check that session IDs are included in metadata
                call1_args = mock_emit1.call_args[0][0]
                call2_args = mock_emit2.call_args[0][0]

                assert call1_args.metadata["session_id"] == logger1.session_id
                assert call2_args.metadata["session_id"] == logger2.session_id
