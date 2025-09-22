# NOAA Weather Data Pipeline Deployment Guide

This guide provides comprehensive instructions for deploying the NOAA weather data ingestion pipeline.

## 🎯 Overview

The NOAA workflow consists of:
- **4 dedicated DAGs** for different datasets (GHCND daily/hourly, GSOM monthly, Normals daily)
- **Enhanced SeaTunnel jobs** for data ingestion and processing
- **Comprehensive monitoring** with health checks and alerting
- **Quality validation** and outlier detection
- **Scalable architecture** with proper error handling

## 🚀 Quick Start

### Prerequisites

1. **Airflow Environment**: Running Airflow instance
2. **Kafka Cluster**: With Schema Registry
3. **TimescaleDB**: PostgreSQL with TimescaleDB extension
4. **NOAA API Token**: Valid NOAA CDO Web API token

### 1. Environment Setup

```bash
# Set NOAA API token
export NOAA_API_TOKEN="your_noaa_api_token_here"

# Navigate to project directory
cd /Users/mstudio/dev/aurum
```

### 2. Run Deployment Script

```bash
# Make deployment script executable
chmod +x scripts/deploy_noaa_workflow.sh

# Run deployment
./scripts/deploy_noaa_workflow.sh
```

This script will:
- ✅ Configure Airflow variables
- ✅ Create Kafka topics
- ✅ Set up Schema Registry
- ✅ Create TimescaleDB table
- ✅ Configure Airflow pools
- ✅ Validate DAG parsing

### 3. Manual Configuration (if needed)

#### Airflow Variables

```bash
# NOAA API configuration
airflow variables set aurum_noaa_api_token "your_token_here"
airflow variables set aurum_noaa_base_url "https://www.ncei.noaa.gov/cdo-web/api/v2"

# Kafka configuration
airflow variables set aurum_kafka_bootstrap_servers "localhost:9092"
airflow variables set aurum_schema_registry "http://localhost:8081"

# TimescaleDB configuration
airflow variables set aurum_timescale_jdbc "jdbc:postgresql://timescale:5432/timeseries"
airflow variables set aurum_noaa_timescale_table "noaa_weather_timeseries"
airflow variables set aurum_noaa_dlq_topic "aurum.ref.noaa.weather.dlq.v1"

# NOAA-specific topics
airflow variables set aurum_noaa_daily_topic "aurum.ref.noaa.weather.ghcnd.daily.v1"
airflow variables set aurum_noaa_hourly_topic "aurum.ref.noaa.weather.ghcnd.hourly.v1"
airflow variables set aurum_noaa_monthly_topic "aurum.ref.noaa.weather.gsom.monthly.v1"
airflow variables set aurum_noaa_normals_topic "aurum.ref.noaa.weather.normals.daily.v1"
```

#### Kafka Topics

```bash
# Create required topics
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic aurum.ref.noaa.weather.ghcnd.daily.v1 \
    --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic aurum.ref.noaa.weather.ghcnd.hourly.v1 \
    --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic aurum.ref.noaa.weather.gsom.monthly.v1 \
    --partitions 2 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic aurum.ref.noaa.weather.normals.daily.v1 \
    --partitions 2 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic aurum.ref.noaa.weather.dlq.v1 \
    --partitions 1 --replication-factor 1
```

#### Schema Registry

```bash
# NOAA weather record schema
curl -X POST http://localhost:8081/subjects/aurum.ref.noaa.weather.ghcnd.daily.v1-value/versions \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"schema": "{\"type\": \"record\", \"name\": \"NoaaWeatherRecord\", \"namespace\": \"aurum.noaa\", \"doc\": \"NOAA weather station data record\", \"fields\": [{\"name\": \"station_id\", \"type\": \"string\", \"doc\": \"Weather station identifier\"}, {\"name\": \"station_name\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"Station name\"}, {\"name\": \"latitude\", \"type\": [\"null\", \"double\"], \"default\": null, \"doc\": \"Station latitude\"}, {\"name\": \"longitude\", \"type\": [\"null\", \"double\"], \"default\": null, \"doc\": \"Station longitude\"}, {\"name\": \"elevation\", \"type\": [\"null\", \"double\"], \"default\": null, \"doc\": \"Station elevation\"}, {\"name\": \"dataset_id\", \"type\": \"string\", \"doc\": \"NOAA dataset identifier\"}, {\"name\": \"element\", \"type\": \"string\", \"doc\": \"Weather element type\"}, {\"name\": \"observation_date\", \"type\": \"string\", \"doc\": \"Observation date\"}, {\"name\": \"value\", \"type\": [\"null\", \"double\"], \"default\": null, \"doc\": \"Measured value\"}, {\"name\": \"raw_value\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"Raw value as string\"}, {\"name\": \"units\", \"type\": \"string\", \"doc\": \"Measurement units\"}, {\"name\": \"unit_code\", \"type\": \"string\", \"doc\": \"Unit code\"}, {\"name\": \"attributes\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"Data attributes\"}, {\"name\": \"observation_timestamp\", \"type\": [\"null\", \"long\"], \"default\": null, \"doc\": \"Observation timestamp\"}, {\"name\": \"ingest_timestamp\", \"type\": \"long\", \"doc\": \"Ingestion timestamp\"}, {\"name\": \"source\", \"type\": \"string\", \"doc\": \"Data source\"}, {\"name\": \"dataset\", \"type\": \"string\", \"doc\": \"Dataset name\"}, {\"name\": \"ingestion_start_date\", \"type\": \"string\", \"doc\": \"Ingestion start date\"}, {\"name\": \"ingestion_end_date\", \"type\": \"string\", \"doc\": \"Ingestion end date\"}, {\"name\": \"data_quality_flag\", \"type\": [\"null\", \"string\"], \"default\": \"VALID\", \"doc\": \"Data quality flag\"}]}}'
```

#### TimescaleDB Table

```sql
-- Create NOAA weather table
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

-- Convert to hypertable
SELECT create_hypertable('noaa_weather_timeseries', 'observation_date', if_not_exists => TRUE);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_noaa_station_date ON noaa_weather_timeseries (station_id, observation_date);
CREATE INDEX IF NOT EXISTS idx_noaa_element ON noaa_weather_timeseries (element);
CREATE INDEX IF NOT EXISTS idx_noaa_dataset ON noaa_weather_timeseries (dataset_id);
CREATE INDEX IF NOT EXISTS idx_noaa_timestamp ON noaa_weather_timeseries (observation_timestamp DESC);
```

#### Airflow Pools

```bash
# Create NOAA API pool
airflow pools set api_noaa 5 "NOAA API rate limit pool"
```

## 📊 Available DAGs

### Core Ingestion DAGs

1. **`noaa_data_ingestion`**
   - Comprehensive NOAA data pipeline
   - Manual trigger with dataset configuration
   - Supports backfill operations

2. **`noaa_ghcnd_daily_ingest`**
   - Daily weather observations (TMAX, TMIN, PRCP, etc.)
   - Schedule: `0 6 * * *` (Daily at 6 AM)
   - 10 major US cities

3. **`noaa_ghcnd_hourly_ingest`**
   - Hourly weather observations
   - Schedule: `0 */6 * * *` (Every 6 hours)
   - NYC, Chicago, Seattle

4. **`noaa_gsom_monthly_ingest`**
   - Monthly weather summaries
   - Schedule: `0 8 1 * *` (Monthly on 1st at 8 AM)
   - 10 major US cities

5. **`noaa_normals_daily_ingest`**
   - 30-year climate normals
   - Schedule: `0 2 * * *` (Daily at 2 AM)
   - 5 major US cities

### Monitoring DAGs

6. **`noaa_data_monitoring`**
   - Real-time health monitoring
   - Schedule: `*/15 * * * *` (Every 15 minutes)
   - API health, data freshness, quality checks

7. **`noaa_data_monitoring_daily`**
   - Daily summary monitoring
   - Schedule: `0 9 * * *` (Daily at 9 AM)
   - Comprehensive daily reports

## 🧪 Testing

### Run API Connectivity Test

```bash
export NOAA_API_TOKEN="your_token_here"
python3 scripts/test_noaa_api.py
```

### Run Simple DAG Validation

```bash
python3 scripts/test_noaa_dags_simple.py
```

### Test Individual Components

```bash
# Test NOAA API connectivity
curl "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets"

# Test Kafka topics
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic aurum.ref.noaa.weather.ghcnd.daily.v1 --from-beginning --max-messages 1

# Test Schema Registry
curl http://localhost:8081/subjects

# Test TimescaleDB
psql -h localhost -p 5432 -U postgres -d timeseries -c "SELECT COUNT(*) FROM noaa_weather_timeseries;"
```

## 🔧 Configuration

### NOAA Datasets Configuration

Edit `/Users/mstudio/dev/aurum/config/noaa_ingest_datasets.json` to modify:
- Station lists
- Data types
- Quality thresholds
- API parameters
- Scheduling

### SeaTunnel Job Templates

Templates are located in `/Users/mstudio/dev/aurum/seatunnel/jobs/templates/`:
- `noaa_weather_enhanced.conf.tmpl` - Enhanced ingestion
- `noaa_kafka_to_timescale_enhanced.conf.tmpl` - TimescaleDB loading

## 📈 Monitoring

### Health Checks
- API connectivity every 15 minutes
- Data freshness monitoring
- Quality score tracking
- Volume validation

### Alerts
- Critical staleness (>24 hours)
- Quality issues (<95% threshold)
- Volume drops (>20% reduction)
- API failures

### Dashboards
- Data ingestion metrics
- Quality trends
- Staleness reports
- Error summaries

## 🛠️ Troubleshooting

### Common Issues

1. **NOAA API Token Issues**
   ```bash
   # Verify token
   curl -H "token: your_token" "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets"
   ```

2. **Kafka Connection Issues**
   ```bash
   # Test connectivity
   kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

3. **Schema Registry Issues**
   ```bash
   # Check health
   curl http://localhost:8081/config
   ```

4. **TimescaleDB Issues**
   ```bash
   # Check extension
   psql -c "SELECT * FROM pg_extension WHERE extname = 'timescaledb';"
   ```

### Debug Mode

Enable debug logging in Airflow:
```bash
export AIRFLOW__CORE__LOGGING_LEVEL=DEBUG
```

## 📝 Production Checklist

- [ ] NOAA API token configured and tested
- [ ] Kafka topics created and accessible
- [ ] Schema Registry running and configured
- [ ] TimescaleDB table created and optimized
- [ ] Airflow pools configured
- [ ] DAGs parsing successfully
- [ ] Monitoring alerts configured
- [ ] Backfill strategy planned
- [ ] Documentation updated

## 🎯 Next Steps

1. **Initial Testing**: Run with small date ranges
2. **Backfill Planning**: Schedule historical data ingestion
3. **Performance Tuning**: Monitor resource usage
4. **Alert Configuration**: Set up notification channels
5. **Documentation**: Update runbooks and procedures

---

**Contact**: Data Engineering Team
**Documentation**: See `/Users/mstudio/dev/aurum/docs/` for detailed documentation
