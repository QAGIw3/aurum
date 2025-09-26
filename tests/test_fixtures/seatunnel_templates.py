"""SeaTunnel template fixtures for stable testing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class SeaTunnelTemplateFixture:
    """A SeaTunnel template fixture for testing."""

    name: str
    description: str
    template_content: str
    environment_variables: Dict[str, str]
    expected_placeholders: List[str]
    expected_rendered_content: str
    validation_rules: Dict[str, Any]


class SeaTunnelTemplateFixtures:
    """Collection of SeaTunnel template fixtures for testing."""

    @staticmethod
    def get_basic_eia_template() -> SeaTunnelTemplateFixture:
        """Get a basic EIA data ingestion template fixture."""
        return SeaTunnelTemplateFixture(
            name="eia_series_to_kafka",
            description="EIA series data ingestion to Kafka",
            template_content="""env {
  job.mode = "BATCH"
  parallelism = 1
  job.name = "EIA_Series_Ingestion_${EIA_SERIES_ID}"
}

source {
  Http {
    url = "${EIA_BASE_URL}/series/${EIA_SERIES_ID}/data"
    method = "GET"
    headers = {
      "X-API-Key": "${EIA_API_KEY}"
    }
    retry = 3
    retry_delay = 1000
  }
}

transform {
  JsonPath {
    path = "$.data[*]"
  }
}

sink {
  Kafka {
    bootstrap.servers = "${AURUM_KAFKA_BOOTSTRAP_SERVERS}"
    topic = "${EIA_KAFKA_TOPIC}"
    format = "json"
    kafka_config = {
      "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
      "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
    }
  }
}""",
            environment_variables={
                "EIA_BASE_URL": "https://api.eia.gov/v1",
                "EIA_SERIES_ID": "ELEC.PRICE.NY-RES.M",
                "EIA_API_KEY": "test_api_key",
                "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "EIA_KAFKA_TOPIC": "eia_series_data"
            },
            expected_placeholders=[
                "EIA_BASE_URL",
                "EIA_SERIES_ID",
                "EIA_API_KEY",
                "AURUM_KAFKA_BOOTSTRAP_SERVERS",
                "EIA_KAFKA_TOPIC"
            ],
            expected_rendered_content="""env {
  job.mode = "BATCH"
  parallelism = 1
  job.name = "EIA_Series_Ingestion_ELEC.PRICE.NY-RES.M"
}

source {
  Http {
    url = "https://api.eia.gov/v1/series/ELEC.PRICE.NY-RES.M/data"
    method = "GET"
    headers = {
      "X-API-Key": "test_api_key"
    }
    retry = 3
    retry_delay = 1000
  }
}

transform {
  JsonPath {
    path = "$.data[*]"
  }
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "eia_series_data"
    format = "json"
    kafka_config = {
      "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
      "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
    }
  }
}""",
            validation_rules={
                "required_sections": ["env", "source", "transform", "sink"],
                "required_env_vars": ["EIA_API_KEY", "AURUM_KAFKA_BOOTSTRAP_SERVERS"],
                "max_complexity": 10
            }
        )

    @staticmethod
    def get_basic_fred_template() -> SeaTunnelTemplateFixture:
        """Get a basic FRED data ingestion template fixture."""
        return SeaTunnelTemplateFixture(
            name="fred_series_to_kafka",
            description="FRED series data ingestion to Kafka",
            template_content="""env {
  job.mode = "BATCH"
  parallelism = 1
  job.name = "FRED_Series_Ingestion_${FRED_SERIES_ID}"
}

source {
  Http {
    url = "${FRED_BASE_URL}/series/observations"
    method = "GET"
    params = {
      "series_id": "${FRED_SERIES_ID}"
      "api_key": "${FRED_API_KEY}"
      "file_type": "json"
    }
    retry = 3
    retry_delay = 1000
  }
}

transform {
  JsonPath {
    path = "$.observations[*]"
  }
}

sink {
  Kafka {
    bootstrap.servers = "${AURUM_KAFKA_BOOTSTRAP_SERVERS}"
    topic = "${FRED_KAFKA_TOPIC}"
    format = "json"
  }
}""",
            environment_variables={
                "FRED_BASE_URL": "https://api.stlouisfed.org/fred",
                "FRED_SERIES_ID": "GDP",
                "FRED_API_KEY": "test_fred_key",
                "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "FRED_KAFKA_TOPIC": "fred_series_data"
            },
            expected_placeholders=[
                "FRED_BASE_URL",
                "FRED_SERIES_ID",
                "FRED_API_KEY",
                "AURUM_KAFKA_BOOTSTRAP_SERVERS",
                "FRED_KAFKA_TOPIC"
            ],
            expected_rendered_content="""env {
  job.mode = "BATCH"
  parallelism = 1
  job.name = "FRED_Series_Ingestion_GDP"
}

source {
  Http {
    url = "https://api.stlouisfed.org/fred/series/observations"
    method = "GET"
    params = {
      "series_id": "GDP"
      "api_key": "test_fred_key"
      "file_type": "json"
    }
    retry = 3
    retry_delay = 1000
  }
}

transform {
  JsonPath {
    path = "$.observations[*]"
  }
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "fred_series_data"
    format = "json"
  }
}""",
            validation_rules={
                "required_sections": ["env", "source", "transform", "sink"],
                "required_env_vars": ["FRED_API_KEY", "AURUM_KAFKA_BOOTSTRAP_SERVERS"],
                "max_complexity": 8
            }
        )

    @staticmethod
    def get_invalid_template() -> SeaTunnelTemplateFixture:
        """Get an invalid template for testing error handling."""
        return SeaTunnelTemplateFixture(
            name="invalid_template",
            description="Template with missing required variables",
            template_content="""env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Http {
    url = "${MISSING_URL_VARIABLE}"
    method = "GET"
  }
}

sink {
  Kafka {
    bootstrap.servers = "${AURUM_KAFKA_BOOTSTRAP_SERVERS}"
    topic = "test_topic"
  }
}""",
            environment_variables={
                "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"
            },
            expected_placeholders=["MISSING_URL_VARIABLE", "AURUM_KAFKA_BOOTSTRAP_SERVERS"],
            expected_rendered_content="""env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Http {
    url = "${MISSING_URL_VARIABLE}"
    method = "GET"
  }
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "test_topic"
  }
}""",
            validation_rules={
                "required_sections": ["env", "source", "sink"],
                "required_env_vars": ["MISSING_URL_VARIABLE"],
                "max_complexity": 5
            }
        )

    @staticmethod
    def get_all_templates() -> List[SeaTunnelTemplateFixture]:
        """Get all available template fixtures."""
        return [
            SeaTunnelTemplateFixtures.get_basic_eia_template(),
            SeaTunnelTemplateFixtures.get_basic_fred_template(),
            SeaTunnelTemplateFixtures.get_invalid_template()
        ]


# Export fixtures for easy access
EIA_TEMPLATE = SeaTunnelTemplateFixtures.get_basic_eia_template()
FRED_TEMPLATE = SeaTunnelTemplateFixtures.get_basic_fred_template()
INVALID_TEMPLATE = SeaTunnelTemplateFixtures.get_invalid_template()
ALL_TEMPLATES = SeaTunnelTemplateFixtures.get_all_templates()


def create_template_file(fixture: SeaTunnelTemplateFixture, temp_dir: Path) -> Path:
    """Create a temporary template file from fixture."""
    template_file = temp_dir / f"{fixture.name}.conf.tmpl"
    template_file.write_text(fixture.template_content)
    return template_file


def create_environment_file(fixture: SeaTunnelTemplateFixture, temp_dir: Path) -> Path:
    """Create a temporary environment file from fixture."""
    env_content = "\n".join(f"{k}={v}" for k, v in fixture.environment_variables.items())
    env_file = temp_dir / f"{fixture.name}.env"
    env_file.write_text(env_content)
    return env_file


def validate_template_structure(content: str) -> Dict[str, Any]:
    """Validate the structure of a rendered SeaTunnel template."""
    lines = content.split('\n')
    sections = {}

    current_section = None
    for line in lines:
        line = line.strip()
        if line.endswith('{') and not line.startswith('//'):
            current_section = line[:-1].strip()
            sections[current_section] = []
        elif current_section and line:
            sections[current_section].append(line)
        elif line == '}' and current_section:
            current_section = None

    return {
        "sections": list(sections.keys()),
        "has_env_section": "env" in sections,
        "has_source_section": "source" in sections,
        "has_sink_section": "sink" in sections,
        "has_transform_section": "transform" in sections,
        "total_lines": len(lines)
    }


__all__ = [
    "SeaTunnelTemplateFixture",
    "SeaTunnelTemplateFixtures",
    "EIA_TEMPLATE",
    "FRED_TEMPLATE",
    "INVALID_TEMPLATE",
    "ALL_TEMPLATES",
    "create_template_file",
    "create_environment_file",
    "validate_template_structure"
]
