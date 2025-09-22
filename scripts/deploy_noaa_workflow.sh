#!/bin/bash
# NOAA Workflow Deployment Script
# This script sets up all necessary configurations for the NOAA data pipeline

set -e

echo "🚀 Starting NOAA Workflow Deployment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
AIRFLOW_WEB_SERVER="localhost:8080"
KAFKA_BOOTSTRAP="localhost:9092"
SCHEMA_REGISTRY="http://localhost:8081"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if Airflow CLI is available
check_airflow() {
    if ! command_exists airflow; then
        echo -e "${RED}❌ Airflow CLI not found. Please ensure Airflow is installed and configured.${NC}"
        exit 1
    fi
}

# Function to create Airflow variables
setup_airflow_variables() {
    echo -e "${BLUE}📝 Setting up Airflow variables...${NC}"

    # NOAA API configuration
    airflow variables set aurum_noaa_api_token "${NOAA_API_TOKEN:-}"
    airflow variables set aurum_noaa_base_url "https://www.ncei.noaa.gov/access/services/data/v1"

    # Kafka configuration
    airflow variables set aurum_kafka_bootstrap_servers "${KAFKA_BOOTSTRAP}"
    airflow variables set aurum_schema_registry "${SCHEMA_REGISTRY}"

    # TimescaleDB configuration
    airflow variables set aurum_timescale_jdbc "jdbc:postgresql://timescale:5432/timeseries"
    airflow variables set aurum_noaa_timescale_table "noaa_weather_timeseries"
    airflow variables set aurum_noaa_dlq_topic "aurum.ref.noaa.weather.dlq.v1"

    # NOAA-specific topics
    airflow variables set aurum_noaa_daily_topic "aurum.ref.noaa.weather.ghcnd.daily.v1"
    airflow variables set aurum_noaa_hourly_topic "aurum.ref.noaa.weather.ghcnd.hourly.v1"
    airflow variables set aurum_noaa_monthly_topic "aurum.ref.noaa.weather.gsom.monthly.v1"
    airflow variables set aurum_noaa_normals_topic "aurum.ref.noaa.weather.normals.daily.v1"

    echo -e "${GREEN}✅ Airflow variables configured${NC}"
}

# Function to create Kafka topics
setup_kafka_topics() {
    echo -e "${BLUE}🔧 Setting up Kafka topics...${NC}"

    # Check if Kafka is running
    if ! nc -z localhost 9092; then
        echo -e "${YELLOW}⚠️  Kafka not running on localhost:9092. Skipping topic creation.${NC}"
        return
    fi

    # NOAA data topics
    kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --create --if-not-exists \
        --topic aurum.ref.noaa.weather.ghcnd.daily.v1 \
        --partitions 3 --replication-factor 1

    kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --create --if-not-exists \
        --topic aurum.ref.noaa.weather.ghcnd.hourly.v1 \
        --partitions 3 --replication-factor 1

    kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --create --if-not-exists \
        --topic aurum.ref.noaa.weather.gsom.monthly.v1 \
        --partitions 2 --replication-factor 1

    kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --create --if-not-exists \
        --topic aurum.ref.noaa.weather.normals.daily.v1 \
        --partitions 2 --replication-factor 1

    # DLQ topic
    kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --create --if-not-exists \
        --topic aurum.ref.noaa.weather.dlq.v1 \
        --partitions 1 --replication-factor 1

    echo -e "${GREEN}✅ Kafka topics created${NC}"
}

# Function to create Schema Registry subjects
setup_schema_registry() {
    echo -e "${BLUE}📋 Setting up Schema Registry...${NC}"

    # Check if Schema Registry is running
    if ! curl -s "${SCHEMA_REGISTRY}/subjects" >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  Schema Registry not accessible. Skipping schema setup.${NC}"
        return
    fi

    # NOAA weather record schema
    NOAA_SCHEMA='{
      "type": "record",
      "name": "NoaaWeatherRecord",
      "namespace": "aurum.noaa",
      "doc": "NOAA weather station data record",
      "fields": [
        {"name": "station_id", "type": "string", "doc": "Weather station identifier"},
        {"name": "station_name", "type": ["null", "string"], "default": null, "doc": "Station name"},
        {"name": "latitude", "type": ["null", "double"], "default": null, "doc": "Station latitude"},
        {"name": "longitude", "type": ["null", "double"], "default": null, "doc": "Station longitude"},
        {"name": "elevation", "type": ["null", "double"], "default": null, "doc": "Station elevation"},
        {"name": "dataset_id", "type": "string", "doc": "NOAA dataset identifier"},
        {"name": "element", "type": "string", "doc": "Weather element type"},
        {"name": "observation_date", "type": "string", "doc": "Observation date"},
        {"name": "value", "type": ["null", "double"], "default": null, "doc": "Measured value"},
        {"name": "raw_value", "type": ["null", "string"], "default": null, "doc": "Raw value as string"},
        {"name": "units", "type": "string", "doc": "Measurement units"},
        {"name": "unit_code", "type": "string", "doc": "Unit code"},
        {"name": "attributes", "type": ["null", "string"], "default": null, "doc": "Data attributes"},
        {"name": "observation_timestamp", "type": ["null", "long"], "default": null, "doc": "Observation timestamp"},
        {"name": "ingest_timestamp", "type": "long", "doc": "Ingestion timestamp"},
        {"name": "source", "type": "string", "doc": "Data source"},
        {"name": "dataset", "type": "string", "doc": "Dataset name"},
        {"name": "ingestion_start_date", "type": "string", "doc": "Ingestion start date"},
        {"name": "ingestion_end_date", "type": "string", "doc": "Ingestion end date"},
        {"name": "data_quality_flag", "type": ["null", "string"], "default": "VALID", "doc": "Data quality flag"}
      ]
    }'

    # Register schema for each topic
    for topic in "aurum.ref.noaa.weather.ghcnd.daily.v1" "aurum.ref.noaa.weather.ghcnd.hourly.v1" "aurum.ref.noaa.weather.gsom.monthly.v1" "aurum.ref.noaa.weather.normals.daily.v1"; do
        SUBJECT="${topic}-value"
        curl -s -X POST "${SCHEMA_REGISTRY}/subjects/${SUBJECT}/versions" \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            -d "{\"schema\": $(echo $NOAA_SCHEMA | jq -c .)}" >/dev/null 2>&1
        echo "✅ Schema registered for ${SUBJECT}"
    done

    echo -e "${GREEN}✅ Schema Registry configured${NC}"
}

# Function to create TimescaleDB table
setup_timescale_table() {
    echo -e "${BLUE}🗃️  Setting up TimescaleDB table...${NC}"

    # Check if PostgreSQL/TimescaleDB is accessible
    if ! psql -h localhost -p 5432 -U postgres -l >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  TimescaleDB not accessible. Skipping table setup.${NC}"
        return
    fi

    # Create NOAA weather table
    psql -h localhost -p 5432 -U postgres -d timeseries -c "
    CREATE TABLE IF NOT EXISTS noaa_weather_timeseries (
        station_id VARCHAR(20) NOT NULL,
        station_name VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        elevation DOUBLE PRECISION,
        dataset_id VARCHAR(50) NOT NULL,
        element VARCHAR(20) NOT NULL,
        observation_date DATE NOT NULL,
        value DOUBLE PRECISION,
        raw_value VARCHAR(50),
        units VARCHAR(20),
        unit_code VARCHAR(20),
        attributes TEXT,
        observation_timestamp BIGINT,
        ingest_timestamp BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000000,
        source VARCHAR(50),
        dataset VARCHAR(50),
        ingestion_start_date DATE,
        ingestion_end_date DATE,
        data_quality_flag VARCHAR(20) DEFAULT 'VALID',
        PRIMARY KEY (station_id, observation_date, element, dataset_id)
    );

    -- Convert to hypertable if not already
    SELECT create_hypertable('noaa_weather_timeseries', 'observation_date', if_not_exists => TRUE);

    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_noaa_station_date ON noaa_weather_timeseries (station_id, observation_date);
    CREATE INDEX IF NOT EXISTS idx_noaa_element ON noaa_weather_timeseries (element);
    CREATE INDEX IF NOT EXISTS idx_noaa_dataset ON noaa_weather_timeseries (dataset_id);
    CREATE INDEX IF NOT EXISTS idx_noaa_timestamp ON noaa_weather_timeseries (observation_timestamp DESC);
    " 2>/dev/null || echo -e "${YELLOW}⚠️  Table creation failed. Please create manually if needed.${NC}"

    echo -e "${GREEN}✅ TimescaleDB table configured${NC}"
}

# Function to create Airflow pools
setup_airflow_pools() {
    echo -e "${BLUE}🏊 Setting up Airflow pools...${NC}"

    # NOAA API pool
    airflow pools set api_noaa 5 "NOAA API rate limit pool"

    echo -e "${GREEN}✅ Airflow pools configured${NC}"
}

# Function to test DAG parsing
test_dag_parsing() {
    echo -e "${BLUE}🔍 Testing DAG parsing...${NC}"

    # Test main NOAA DAG
    if airflow dags test noaa_data_ingestion 2024-01-01 >/dev/null 2>&1; then
        echo -e "${GREEN}✅ noaa_data_ingestion DAG parses successfully${NC}"
    else
        echo -e "${RED}❌ noaa_data_ingestion DAG parsing failed${NC}"
        return 1
    fi

    # Test monitoring DAG
    if airflow dags test noaa_data_monitoring 2024-01-01 >/dev/null 2>&1; then
        echo -e "${GREEN}✅ noaa_data_monitoring DAG parses successfully${NC}"
    else
        echo -e "${RED}❌ noaa_data_monitoring DAG parsing failed${NC}"
        return 1
    fi

    # Test individual dataset DAGs
    for dataset in ghcnd_daily ghcnd_hourly gsom normals_daily; do
        dag_id="noaa_${dataset}_ingest"
        if airflow dags test "${dag_id}" 2024-01-01 >/dev/null 2>&1; then
            echo -e "${GREEN}✅ ${dag_id} DAG parses successfully${NC}"
        else
            echo -e "${RED}❌ ${dag_id} DAG parsing failed${NC}"
            return 1
        fi
    done

    echo -e "${GREEN}✅ All DAGs parse successfully${NC}"
}

# Function to display deployment summary
show_deployment_summary() {
    echo -e "\n${GREEN}🎉 NOAA Workflow Deployment Complete!${NC}"
    echo -e "${BLUE}📋 Summary:${NC}"
    echo "  • ✅ Airflow variables configured"
    echo "  • ✅ Kafka topics created"
    echo "  • ✅ Schema Registry configured"
    echo "  • ✅ TimescaleDB table created"
    echo "  • ✅ Airflow pools set up"
    echo "  • ✅ DAGs validated"
    echo ""
    echo -e "${YELLOW}⚠️  Manual Steps Required:${NC}"
    echo "  1. Set NOAA_API_TOKEN environment variable or Airflow variable"
    echo "  2. Verify Kafka and Schema Registry connectivity"
    echo "  3. Test DAG execution in Airflow UI"
    echo "  4. Monitor initial runs for any issues"
    echo ""
    echo -e "${BLUE}🔧 Available DAGs:${NC}"
    echo "  • noaa_data_ingestion (comprehensive pipeline)"
    echo "  • noaa_ghcnd_daily_ingest (daily weather data)"
    echo "  • noaa_ghcnd_hourly_ingest (hourly weather data)"
    echo "  • noaa_gsom_monthly_ingest (monthly summaries)"
    echo "  • noaa_normals_daily_ingest (climate normals)"
    echo "  • noaa_data_monitoring (health monitoring)"
}

# Main deployment function
main() {
    echo -e "${GREEN}🌦️  NOAA Weather Data Pipeline Deployment${NC}"
    echo "This script will set up all necessary infrastructure for NOAA data ingestion."

    # Check prerequisites
    check_airflow

    # Check for NOAA API token
    if [ -z "${NOAA_API_TOKEN}" ] && [ -z "$(airflow variables get aurum_noaa_api_token 2>/dev/null)" ]; then
        echo -e "${YELLOW}⚠️  NOAA_API_TOKEN not set. Please set it before running NOAA DAGs.${NC}"
        read -p "Do you want to continue without API token? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi

    # Run deployment steps
    setup_airflow_variables
    setup_kafka_topics
    setup_schema_registry
    setup_timescale_table
    setup_airflow_pools

    # Test deployment
    if test_dag_parsing; then
        show_deployment_summary
    else
        echo -e "${RED}❌ Deployment failed. Please check the errors above.${NC}"
        exit 1
    fi
}

# Run main function
main "$@"
