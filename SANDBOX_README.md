# Aurum External Data Ingestion Sandbox

This sandbox provides a complete end-to-end testing environment for the Aurum external data ingestion pipeline, including mocked providers, Kafka, Iceberg, and monitoring.

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Make

### 1. Start the Sandbox

```bash
# Start all services
docker-compose -f docker-compose.sandbox.yml up -d

# Wait for services to be ready (about 2-3 minutes)
docker-compose -f docker-compose.sandbox.yml ps

# Check logs
docker-compose -f docker-compose.sandbox.yml logs -f
```

#### Readiness Checks

After the services start, verify the core APIs are healthy:

```bash
# Aurum API readiness and liveness
curl -s http://localhost:8080/ready | jq
curl -s http://localhost:8080/live | jq

# Scenario worker probe server
curl -s http://localhost:9464/ready
curl -s http://localhost:9464/live

# Prometheus scrape endpoint
curl -s http://localhost:9090/-/ready
```

All commands should return a `200` response and JSON payload indicating `ready`/`ok` status.

### 2. Run the Sandbox Runner

```bash
# Run the sandbox ingestion test
python scripts/sandbox_runner.py

# Or run with specific providers
python scripts/sandbox_runner.py --providers caiso,eia
```

### 3. Access Services

- **Kafka**: http://localhost:9092
- **Schema Registry**: http://localhost:8081
- **PostgreSQL**: localhost:5432 (user: aurum, password: aurum)
- **Redis**: localhost:6379
- **Trino**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Mock Providers│ -> │   Aurum Ingestion│ -> │   Data Lake     │
│   (CAISO, EIA,  │    │   Pipeline      │    │   (Iceberg)     │
│   FRED)         │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         v                       v                       v
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Validation    │    │   Lineage       │    │   Monitoring    │
│   (Great Exp.)  │    │   (OpenLineage) │    │   (Prometheus)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🔧 Configuration

### Environment Variables

Key environment variables for the sandbox:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Provider API Keys (mock)
CAISO_API_KEY=sandbox_key
EIA_API_KEY=sandbox_key
FRED_API_KEY=sandbox_key

# Database
AURUM_APP_DB_DSN=postgresql://aurum:aurum@postgres:5432/aurum
```

### Provider Endpoints

- **CAISO**: http://localhost:8001 (LMP, Load data)
- **EIA**: http://localhost:8002 (Electricity, Petroleum data)
- **FRED**: http://localhost:8003 (Economic indicators)

### Test Data

Mock data is automatically generated with realistic patterns:

- **Time Series**: 1000 data points per series
- **Catalog**: 50 series per provider
- **Update Frequency**: 15-60 minutes
- **Data Quality**: 99%+ success rate

## 🧪 Testing Features

### 1. Provider Testing

```bash
# Test individual provider connectivity
curl http://localhost:8001/health
curl http://localhost:8002/catalog
curl http://localhost:8003/data/mock_series_1
```

### 2. Performance Testing

```bash
# Run K6 performance tests
docker-compose -f docker-compose.sandbox.yml exec k6 k6 run /scripts/sandbox_test.js

# Run Locust load tests
python -m locust -f perf/locustfile.py --host http://localhost:8001
```

### 3. Data Validation

```bash
# Check Kafka topics
docker-compose -f docker-compose.sandbox.yml exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic aurum.ext.caiso.sandbox.v1 \
  --from-beginning \
  --max-messages 10

# Query Iceberg tables via Trino
docker-compose -f docker-compose.sandbox.yml exec trino trino \
  --server localhost:8080 \
  --execute "SELECT COUNT(*) FROM iceberg.sandbox.external_caiso_sandbox"
```

### 4. Monitoring

- **Grafana Dashboards**: Real-time metrics and alerts
- **Prometheus**: Metrics collection and querying
- **Schema Registry**: Avro schema management
- **OpenLineage**: Data lineage tracking

## 📊 Performance Baselines

### Expected Performance

| Component | Metric | Baseline |
|-----------|--------|----------|
| Provider Response | P95 Latency | < 200ms |
| Data Ingestion | Throughput | 1000 records/sec |
| Kafka Emission | Success Rate | 99.9% |
| Schema Validation | Processing Time | < 50ms |
| End-to-End | Total Latency | < 2 seconds |

### Load Testing

```bash
# K6 Test Configuration
export K6_DURATION=30s
export K6_VUS=10
export K6_ITERATIONS=100

# Run performance test
docker-compose -f docker-compose.sandbox.yml exec k6 k6 run \
  -e DURATION=${K6_DURATION} \
  -e VUS=${K6_VUS} \
  -e ITERATIONS=${K6_ITERATIONS} \
  /scripts/sandbox_test.js
```

## 🛠️ Development

### Adding New Providers

1. **Create Mock Provider**:
   ```python
   # scripts/sandbox_mock_provider.py
   # Add new provider endpoint
   @app.get("/provider/{provider_name}")
   async def get_provider_data(provider_name: str):
       # Implement provider-specific logic
       pass
   ```

2. **Update Configuration**:
   ```json
   // config/sandbox_config.json
   {
     "name": "new_provider",
     "enabled": true,
     "base_url": "http://mock-new-provider:8004",
     "api_key": "sandbox_key",
     "datasets": ["new_dataset"]
   }
   ```

3. **Update Docker Compose**:
   ```yaml
   # docker-compose.sandbox.yml
   mock-new-provider:
     build: .
     ports: ["8004:8004"]
     environment:
       PROVIDER_NAME: new_provider
   ```

### Custom Test Scenarios

```python
# scripts/custom_sandbox_test.py
async def test_custom_scenario():
    """Implement custom test scenarios."""
    # Test specific provider behavior
    # Validate data quality
    # Measure performance characteristics
    pass
```

## 🐛 Troubleshooting

### Common Issues

1. **Services not starting**:
   ```bash
   docker-compose -f docker-compose.sandbox.yml logs [service_name]
   ```

2. **Port conflicts**:
   ```bash
   # Check port usage
   lsof -i :5432,6379,9092,8081
   ```

3. **Kafka connectivity**:
   ```bash
   # Test Kafka connection
   docker-compose -f docker-compose.sandbox.yml exec kafka kafka-broker-api-versions \
     --bootstrap-server localhost:9092
   ```

4. **Schema registry issues**:
   ```bash
   curl -s http://localhost:8081/subjects | jq
   ```

### Logs and Debugging

```bash
# View all logs
docker-compose -f docker-compose.sandbox.yml logs -f

# View specific service logs
docker-compose -f docker-compose.sandbox.yml logs -f postgres
docker-compose -f docker-compose.sandbox.yml logs -f kafka
docker-compose -f docker-compose.sandbox.yml logs -f mock-caiso

# Enter service container
docker-compose -f docker-compose.sandbox.yml exec postgres bash
```

## 📈 Metrics and Monitoring

### Key Metrics to Monitor

- **Ingestion Success Rate**: > 99%
- **Data Freshness**: < 30 minutes
- **System Availability**: > 99.9%
- **Processing Latency**: < 2 seconds
- **Resource Utilization**: < 70% CPU/Memory

### Performance Benchmarks

```bash
# Run benchmark suite
python scripts/benchmark_sandbox.py

# Generate performance report
python scripts/generate_perf_report.py
```

## 🔄 CI/CD Integration

The sandbox can be integrated into CI/CD pipelines:

```yaml
# .github/workflows/sandbox-test.yml
name: Sandbox Integration Tests
on: [push, pull_request]

jobs:
  sandbox-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Start sandbox
        run: docker-compose -f docker-compose.sandbox.yml up -d
      - name: Wait for services
        run: sleep 180
      - name: Run tests
        run: python scripts/sandbox_runner.py
      - name: Collect metrics
        run: python scripts/generate_perf_report.py
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: sandbox-test-results
          path: perf/reports/
```

## 📚 Documentation

- [External Data Ingestion Guide](./docs/external-data.md)
- [Schema Registry Guide](./docs/schema_registry.md)
- [Performance Testing Guide](./perf/README.md)
- [OpenLineage Integration](./docs/openlineage-integration.md)

## 🆘 Support

For issues with the sandbox:

1. Check the troubleshooting section above
2. Review service logs: `docker-compose logs -f`
3. Verify configuration: `cat config/sandbox_config.json`
4. Test connectivity: `curl http://localhost:8001/health`

## 🎯 Roadmap

- [ ] Add more mock providers (NOAA, World Bank)
- [ ] Implement real Iceberg table creation
- [ ] Add data quality dashboards
- [ ] Support for streaming data simulation
- [ ] Advanced failure scenario testing
- [ ] Multi-region deployment testing
- [ ] Automated performance regression detection
