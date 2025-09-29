"""Container fixtures for integration testing."""

import pytest
from typing import Generator, Dict, Any
from testcontainers.postgres import PostgresContainer
from testcontainers.generic import GenericContainer
from testcontainers.core.waiting_utils import wait_for_logs


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """PostgreSQL container for integration testing."""
    container = PostgresContainer(
        image="postgres:15-alpine",
        username="aurum_test",
        password="aurum_test",
        database="aurum_test"
    )

    # Start the container
    container.start()

    yield container

    # Cleanup
    container.stop()


@pytest.fixture(scope="session")
def timescale_container() -> Generator[PostgresContainer, None, None]:
    """TimescaleDB container for time-series testing."""
    container = PostgresContainer(
        image="timescale/timescaledb:latest-pg15",
        username="aurum_test",
        password="aurum_test",
        database="aurum_test"
    )

    # Start the container
    container.start()

    yield container

    # Cleanup
    container.stop()


@pytest.fixture(scope="session")
def kafka_container() -> Generator[GenericContainer, None, None]:
    """Kafka container for message queue testing."""
    container = GenericContainer(
        image="confluentinc/cp-kafka:7.4.0",
        ports={"9092": "9092"},
        environment={
            "KAFKA_BROKER_ID": "1",
            "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092",
            "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        }
    )

    # Wait for Kafka to be ready
    container = wait_for_logs(container, "started", timeout=60)

    # Start the container
    container.start()

    yield container

    # Cleanup
    container.stop()


@pytest.fixture(scope="session")
def zookeeper_container() -> Generator[GenericContainer, None, None]:
    """Zookeeper container for Kafka."""
    container = GenericContainer(
        image="confluentinc/cp-zookeeper:7.4.0",
        ports={"2181": "2181"},
        environment={
            "ZOOKEEPER_CLIENT_PORT": "2181",
            "ZOOKEEPER_TICK_TIME": "2000",
        }
    )

    # Start the container
    container.start()

    yield container

    # Cleanup
    container.stop()


@pytest.fixture(scope="session")
def clickhouse_container() -> Generator[GenericContainer, None, None]:
    """ClickHouse container for analytics testing."""
    container = GenericContainer(
        image="clickhouse/clickhouse-server:23.8-alpine",
        ports={"8123": "8123", "9000": "9000"},
        environment={
            "CLICKHOUSE_DB": "aurum_test",
            "CLICKHOUSE_USER": "aurum_test",
            "CLICKHOUSE_PASSWORD": "aurum_test",
        }
    )

    # Start the container
    container.start()

    yield container

    # Cleanup
    container.stop()


@pytest.fixture(scope="session")
def trino_container() -> Generator[GenericContainer, None, None]:
    """Trino container for SQL analytics testing."""
    container = GenericContainer(
        image="trinodb/trino:428",
        ports={"8080": "8080"},
        environment={
            "TRINO_HTTP_PORT": "8080",
        }
    )

    # Start the container
    container.start()

    yield container

    # Cleanup
    container.stop()


# Connection fixtures that provide DSN strings for the containers

@pytest.fixture(scope="session")
def postgres_dsn(postgres_container: PostgresContainer) -> str:
    """PostgreSQL connection string."""
    return postgres_container.get_connection_url()


@pytest.fixture(scope="session")
def timescale_dsn(timescale_container: PostgresContainer) -> str:
    """TimescaleDB connection string."""
    return timescale_container.get_connection_url()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container: GenericContainer) -> str:
    """Kafka bootstrap servers string."""
    host = kafka_container.get_container_host_ip()
    port = kafka_container.get_exposed_port("9092")
    return f"{host}:{port}"


@pytest.fixture(scope="session")
def clickhouse_dsn(clickhouse_container: GenericContainer) -> str:
    """ClickHouse connection string."""
    host = clickhouse_container.get_container_host_ip()
    port = clickhouse_container.get_exposed_port("8123")
    return f"clickhouse://{host}:{port}/aurum_test"


@pytest.fixture(scope="session")
def trino_dsn(trino_container: GenericContainer) -> str:
    """Trino connection string."""
    host = trino_container.get_container_host_ip()
    port = trino_container.get_exposed_port("8080")
    return f"http://{host}:{port}"


# Combined container fixture for tests that need multiple services

@pytest.fixture(scope="session")
def integration_containers(
    postgres_container: PostgresContainer,
    timescale_container: PostgresContainer,
    kafka_container: GenericContainer,
    clickhouse_container: GenericContainer,
    trino_container: GenericContainer,
) -> Dict[str, Any]:
    """All integration test containers."""
    return {
        "postgres": postgres_container,
        "timescale": timescale_container,
        "kafka": kafka_container,
        "clickhouse": clickhouse_container,
        "trino": trino_container,
    }


@pytest.fixture(scope="session")
def database_urls(
    postgres_dsn: str,
    timescale_dsn: str,
    kafka_bootstrap_servers: str,
    clickhouse_dsn: str,
    trino_dsn: str,
) -> Dict[str, str]:
    """Connection URLs for all services."""
    return {
        "postgres": postgres_dsn,
        "timescale": timescale_dsn,
        "kafka": kafka_bootstrap_servers,
        "clickhouse": clickhouse_dsn,
        "trino": trino_dsn,
    }
