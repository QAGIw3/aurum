# ISO New England Data Integration

This document describes the implementation of ISO New England (ISO-NE) data collection and processing in the Aurum platform.

## Overview

The ISO-NE implementation provides comprehensive coverage of ISO-NE's market data including:

- **Pricing Data**: Day-ahead and real-time locational marginal pricing (LMP)
- **Load Data**: Real-time system load by location
- **Generation Mix**: Generation by fuel type and resource
- **Ancillary Services**: AS prices and quantities
- **Reference Data**: Node/zone crosswalks and generator rosters

## Implementation Components

### 1. API Client (`src/aurum/external/providers/isone.py`)

- **IsoNeApiClient**: Handles ISO-NE Web Services API with retry logic, rate limiting, and JSON/XML support
- **IsoNeCollector**: Orchestrates data collection with checkpointing, normalization, and alerting
- **Fallback Support**: Multiple endpoint URLs and format fallbacks for reliability

### 2. Configuration (`config/isone_ingest_datasets.json`)

Defines datasets with schedules, topics, and metadata:
- Real-time LMP: 5-minute intervals
- Day-ahead LMP: Hourly intervals
- Load data: 5-minute real-time intervals
- Generation mix: Daily intervals
- Ancillary services: Daily intervals

### 3. Airflow DAGs

- `ingest_iso_prices_isone.py`: LMP data collection (DA/RT)
- `ingest_iso_load_isone.py`: Load and generation data collection

### 4. Data Processing

- **Normalization**: All timestamps converted to UTC with source timezone metadata
- **Schema Validation**: Drift detection with configurable expected fields
- **Alerting**: Empty responses and schema changes trigger warnings

### 5. Reference Data Management

- **Node Crosswalk**: `config/iso_nodes.csv` updated with discovered nodes/zones
- **Generator Roster**: `config/generators.csv` maintains generator metadata
- **Historical Tracking**: Incremental updates preserve existing data

## Data Types Supported

| Data Type | Market | Schedule | Description |
|-----------|--------|----------|-------------|
| LMP | DA/RT | Hourly/5min | Locational Marginal Pricing |
| Load | RT | 5min | System load by location |
| Generation Mix | RT | Daily | Fuel-type generation by resource |
| Ancillary Services | DA | Daily | AS prices and quantities |

## Key Features

### Scalability & Resilience
- Retry logic with exponential backoff
- Rate limiting (60 req/min, 1000 req/hour)
- Fallback endpoints and formats
- Circuit breaker for external calls

### Data Quality
- UTC normalization with timezone metadata
- Schema drift detection
- Empty response alerting
- Idempotent processing with record hashing

### Monitoring & Observability
- Structured logging with context
- Alert integration points
- Checkpoint-based recovery
- Performance metrics collection

## API Endpoints Used

### Pricing Data
- **Day-Ahead LMP**: `/api/v1.1/dayaheadlmp`
- **Real-Time LMP**: `/api/v1.1/fiveminutelmp/current`

### Load Data
- **System Load**: `/api/v1.1/fiveminutesystemload/current`

### Generation Data
- **Generation Mix**: `/api/v1.1/generation/resource`

### Ancillary Services
- **AS Data**: `/api/v1.1/ancillaryservices`

## Usage

### Data Collection
```python
from aurum.external.providers.isone import IsoNeCollector, IsoNeApiConfig, load_isone_dataset_configs

# Configure API
config = IsoNeApiConfig(
    base_url="https://webservices.iso-ne.com/api/v1.1",
    api_key="your-api-key"
)

# Load dataset configurations
datasets = load_isone_dataset_configs()

# Collect data
collector = IsoNeCollector(datasets[0], config)
data = await collector.collect(start_date, end_date)
```

### Node Crosswalk Updates
```python
# Build and update node crosswalk
await collector.build_node_crosswalk()
```

### Generator Roster Harvesting
```python
# Harvest generator information
await collector.harvest_generator_roster()
```

## Integration Points

- **Kafka Topics**: `aurum.iso.isone.*.v1` for normalized data
- **dbt Models**: Iceberg tables in market schema
- **Monitoring**: Alert topics for operational issues
- **Reference Data**: CSV files updated incrementally

## Dependencies

- `requests` for HTTP client
- `aiohttp` for async operations
- `csv` for reference data processing
- Airflow for orchestration
- SeaTunnel for data transformation
- Kafka for message publishing

## Key Differences from MISO Implementation

1. **API Structure**: ISO-NE uses different endpoint structures and authentication
2. **Data Formats**: ISO-NE provides more structured JSON responses
3. **Time Zones**: ISO-NE operates in America/New_York timezone
4. **Rate Limits**: More conservative limits (60/min vs 50/min for MISO)
5. **Authentication**: X-API-Key header vs Bearer tokens

## Next Steps

1. **Testing**: Validate with actual ISO-NE API endpoints
2. **Performance Tuning**: Optimize batch sizes and concurrency
3. **Monitoring Setup**: Configure production alerting
4. **Documentation**: Generate OpenAPI specs for API definitions
5. **Backfill Implementation**: Complete backfill collector methods

## Notes

- ISO-NE Web Services API access may require registration
- Real-time data may have additional latency/availability considerations
- Data accuracy depends on ISO-NE's market systems
- Node and generator mappings evolve over time and require maintenance
