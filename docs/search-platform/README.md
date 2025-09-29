# Aurum Advanced Search and Discovery Platform

## Overview

The Aurum Advanced Search and Discovery Platform provides comprehensive search capabilities across all data and metadata with full-text search, semantic search, natural language processing, faceted search, and advanced analytics.

## Key Features

- **Full-text Search**: BM25-based search with field boosts and fuzziness
- **Semantic Search**: Vector-based similarity search with hybrid BM25 + ANN
- **Natural Language Processing**: Entity extraction, intent detection, and query enhancement
- **Faceted Search**: Hierarchical filtering with drill-down navigation
- **Advanced Ranking**: Multi-signal scoring with learning-to-rank capabilities
- **Analytics & Insights**: User behavior tracking and performance monitoring
- **Production Ready**: Circuit breakers, lifecycle management, and resilience
- **Multi-tenant**: Tenant isolation and authorization

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FastAPI       │    │   Search        │    │   Elasticsearch │
│   Endpoints     │───▶│   Services      │───▶│   Engine        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Query         │    │   Semantic      │    │   Analytics     │
│   Processor     │    │   Search        │    │   Service       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Advanced      │    │   Ranking       │    │   Index         │
│   Filtering     │    │   Engine        │    │   Lifecycle     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Quick Start

### 1. Enable Search Feature

```bash
export AURUM_SEARCH_ENABLED=true
export AURUM_SEARCH_HOSTS=http://localhost:9200
```

### 2. Initialize Search Index

```bash
# Run batch loader to populate initial data
python scripts/search_batch_loader.py --sources datasets documentation
```

### 3. Start API Server

```bash
python start_api.py
```

### 4. Perform Search

```bash
# Basic search
curl "http://localhost:8095/v2/search/?q=power%20demand%20texas"

# Advanced search with semantic enhancement
curl -X POST "http://localhost:8095/v2/search/query" \
  -H "Content-Type: application/json" \
  -d '{
    "q": "power demand in texas",
    "semantic": {"enabled": true, "weight": 0.3},
    "facets": ["asset_class", "iso"],
    "filters": {"doc_type": ["dataset"]}
  }'
```

## API Endpoints

### Core Search Endpoints

#### GET `/v2/search/`
Basic search with optional semantic enhancement.

**Parameters:**
- `q` (required): Search query string
- `filters` (optional): JSON-encoded filters
- `facets` (optional): Comma-separated facet fields
- `limit` (optional): Results per page (1-100, default: 20)
- `cursor` (optional): Cursor for pagination
- `semantic` (optional): Enable semantic search (default: false)
- `semantic_weight` (optional): Semantic search weight (0-1, default: 0.3)
- `tenant_id` (optional): Tenant ID for filtering

**Example:**
```bash
curl "http://localhost:8095/v2/search/?q=wind%20forecast&semantic=true&limit=10"
```

#### POST `/v2/search/query`
Advanced search with full DSL support.

**Request Body:**
```json
{
  "q": "power demand texas",
  "filters": {"iso": ["ERCOT"], "asset_class": ["power"]},
  "facets": ["asset_class", "iso"],
  "semantic": {"enabled": true, "weight": 0.4},
  "page": {"limit": 20, "cursor": null}
}
```

#### GET `/v2/search/suggest`
Autocomplete suggestions for query completion.

**Parameters:**
- `q` (required): Search prefix
- `limit` (optional): Maximum suggestions (1-50, default: 10)

**Example:**
```bash
curl "http://localhost:8095/v2/search/suggest?q=pow&limit=5"
```

#### GET `/v2/search/explain`
Explain how a query would be processed.

**Parameters:**
- `q` (required): Query to explain

**Example:**
```bash
curl "http://localhost:8095/v2/search/explain?q=power%20demand%20in%20texas"
```

### Advanced Features

#### GET `/v2/search/facets`
Get facet options for a specific field.

**Parameters:**
- `field` (required): Field to get facet options for
- `q` (optional): Current search query
- `filters` (optional): JSON-encoded current filters
- `size` (optional): Maximum facet options (1-200, default: 50)

**Example:**
```bash
curl "http://localhost:8095/v2/search/facets?field=asset_class&q=power&size=20"
```

#### GET `/v2/search/filters/suggest`
Get intelligent filter suggestions.

**Parameters:**
- `q` (required): Current search query
- `filters` (optional): JSON-encoded current filters
- `limit` (optional): Maximum suggestions (1-20, default: 5)

**Example:**
```bash
curl "http://localhost:8095/v2/search/filters/suggest?q=power%20demand&limit=5"
```

#### GET `/v2/search/facets/hierarchical`
Get hierarchical facet structure.

**Parameters:**
- `q` (optional): Base search query
- `filters` (optional): JSON-encoded current filters
- `hierarchy` (optional): JSON-encoded hierarchy configuration

**Example:**
```bash
curl "http://localhost:8095/v2/search/facets/hierarchical?q=energy&hierarchy={\"doc_type\":[\"asset_class\",\"iso\"]}"
```

### ANN & Semantic Search

#### POST `/v2/search/ann/tune`
Tune ANN index parameters for optimal performance.

**Request Body:**
```json
{
  "test_queries": ["power demand", "wind forecast", "gas prices"],
  "ground_truth": {
    "power demand": ["doc1", "doc2"],
    "wind forecast": ["doc3", "doc4"]
  }
}
```

#### POST `/v2/search/ann/search`
Perform optimized ANN search with hybrid text + semantic.

**Request Body:**
```json
{
  "query": "power demand in texas",
  "query_embedding": [0.1, 0.2, 0.3, ...],
  "text_weight": 0.7,
  "semantic_weight": 0.3,
  "k": 100
}
```

### Analytics & Monitoring

#### GET `/v2/search/analytics`
Get comprehensive search analytics summary.

#### POST `/v2/search/analytics/click`
Record that a search result was clicked.

**Request Body:**
```json
{
  "query": "power demand texas",
  "result_id": "doc123",
  "result_rank": 1
}
```

#### POST `/v2/search/analytics/facet`
Record that a facet filter was applied.

**Request Body:**
```json
{
  "query": "power demand texas",
  "facet_field": "asset_class",
  "facet_value": "power"
}
```

#### GET `/v2/search/analytics/export`
Export search analytics data.

**Parameters:**
- `format` (optional): Export format (json, csv, default: json)

### Operations & Maintenance

#### POST `/v2/search/maintenance`
Perform comprehensive index maintenance.

#### POST `/v2/search/backup`
Create a backup of all search indices.

**Request Body:**
```json
{
  "backup_name": "pre-upgrade-backup"
}
```

#### GET `/v2/search/health`
Get comprehensive search health information.

## Configuration

### Environment Variables

```bash
# Core search settings
AURUM_SEARCH_ENABLED=true
AURUM_SEARCH_HOSTS=http://localhost:9200
AURUM_SEARCH_INDEX_PREFIX=aurum

# Semantic search settings
AURUM_SEARCH_SEMANTIC_ENABLED=true
AURUM_SEARCH_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
AURUM_SEARCH_KNN_K=100
AURUM_SEARCH_SEMANTIC_WEIGHT=0.3

# Analytics settings
AURUM_SEARCH_ANALYTICS_ENABLED=true
AURUM_SEARCH_SUGGESTIONS_ENABLED=true

# Performance settings
AURUM_SEARCH_QUERY_TIMEOUT_MS=30000
AURUM_SEARCH_MAX_RESULT_WINDOW=10000
AURUM_SEARCH_CURSOR_PAGE_SIZE_DEFAULT=20
```

### Feature Flags

```bash
# Enable/disable search features
AURUM_SEARCH_ENABLED=true
AURUM_SEARCH_SEMANTIC_ENABLED=true
AURUM_SEARCH_SUGGESTIONS_ENABLED=true
AURUM_SEARCH_ANALYTICS_ENABLED=true
```

## Data Sources

The platform indexes data from multiple sources:

### Data Products (from Data Mesh Catalog)
- Product metadata, descriptions, domains, quality scores
- Owner teams, status, and lineage information

### Datasets (EIA, ISO, FRED)
- Energy market data series
- Regional and temporal coverage
- Data quality and completeness metrics

### Energy Trading Curves
- Forward price curves
- Historical pricing data
- Market indicators and benchmarks

### Trading Scenarios
- Scenario definitions and configurations
- Execution history and results
- Risk metrics and performance data

### Documentation
- Technical documentation and guides
- API specifications and examples
- Operational runbooks

### Plugin Marketplace
- Plugin metadata and descriptions
- Author information and ratings
- Usage statistics and downloads

## Index Structure

### Index Name
```
aurum-search-v1
```

### Document Schema
```json
{
  "id": "unique_document_id",
  "doc_type": "dataset|curve|scenario|documentation|plugin",
  "tenant_id": "tenant_identifier",
  "title": "Document title",
  "name": "Alternative name",
  "description": "Document description",
  "content_text": "Full searchable content",
  "tags": ["tag1", "tag2"],
  "domains": ["domain1", "domain2"],
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:00:00Z",
  "quality_score": 0.85,
  "popularity_score": 0.6,
  "embedding": [0.1, 0.2, 0.3, ...],
  "metadata": {
    "source_system": "eia|iso|fred|curves|scenarios|documentation",
    "additional_fields": "..."
  }
}
```

### Field Mappings

- **id**: Keyword field for exact matching
- **doc_type**: Keyword for filtering by document type
- **tenant_id**: Keyword for multi-tenant isolation
- **title/name/description/content_text**: Text fields with BM25 scoring
- **tags/domains**: Keyword arrays for faceting
- **created_at/updated_at**: Date fields for temporal queries
- **quality_score/popularity_score**: Float fields for ranking
- **embedding**: Dense vector field for semantic search
- **metadata**: Object field for domain-specific data

## Performance Optimization

### Index Settings
- **Number of Shards**: 1 (single-node setup)
- **Refresh Interval**: 30s (balance between freshness and performance)
- **Max Result Window**: 10,000 (for deep pagination)

### HNSW Index Configuration
- **M**: 16 (bi-directional links)
- **ef_construction**: 200 (construction candidate list)
- **ef_search**: 128 (search candidate list)

### Circuit Breaker Settings
- **Elasticsearch**: 3 failures, 30s recovery, 10s timeout
- **Semantic Search**: 2 failures, 60s recovery, 15s timeout
- **Analytics**: 5 failures, 30s recovery, 5s timeout

### Caching Strategy
- **Query Results**: 5-minute TTL for popular queries
- **Embedding Models**: Persistent model loading
- **Facet Options**: 1-hour TTL for facet aggregations

## Monitoring & Observability

### Key Metrics
- **Search Response Time**: P50, P95, P99 latencies
- **Query Throughput**: Requests per second
- **Index Health**: Size, document count, segment count
- **User Engagement**: CTR, facet usage, suggestion adoption
- **Error Rates**: By component and error type

### Alerts
- **Slow Queries**: >1 second response time
- **High Error Rate**: >5% error rate
- **Index Issues**: Segment count >20, memory usage >1GB
- **Circuit Breaker Trips**: Any circuit breaker opens

### Dashboards
- **Search Performance**: Response times, throughput, error rates
- **User Behavior**: Popular queries, CTR, facet usage
- **Index Health**: Size, growth, optimization status
- **Semantic Search**: Embedding usage, similarity scores

## Security Considerations

### Multi-tenant Isolation
- All queries filtered by `tenant_id`
- Index-level access controls
- Document-level security with field-based filtering

### Input Validation
- Query length limits (max 1000 characters)
- Filter value sanitization
- SQL injection prevention
- XSS protection in highlights

### Rate Limiting
- Per-tenant rate limits
- Burst protection
- Graceful degradation under load

### Audit Logging
- All search operations logged
- User context preserved
- Performance metrics captured
- Security events recorded

## Troubleshooting

### Common Issues

#### Elasticsearch Connection Issues
```bash
# Check Elasticsearch health
curl http://localhost:9200/_cluster/health

# Restart Elasticsearch service
docker restart aurum_elasticsearch_1
```

#### Index Performance Issues
```bash
# Check index status
curl "http://localhost:9200/_cat/indices/aurum*?v"

# Force merge segments
curl -X POST "http://localhost:9200/aurum-search-v1/_forcemerge?max_num_segments=1"
```

#### Semantic Search Issues
```bash
# Check model loading
python -c "from sentence_transformers import SentenceTransformer; print('Model loaded successfully')"

# Restart semantic search service
# (Handled automatically by circuit breaker)
```

### Debug Endpoints

#### Query Explanation
```bash
curl "http://localhost:8095/v2/search/explain?q=power%20demand%20in%20texas"
```

#### Health Check
```bash
curl "http://localhost:8095/v2/search/health"
```

#### Analytics Summary
```bash
curl "http://localhost:8095/v2/search/analytics"
```

## Deployment

### Docker Compose
```yaml
services:
  elasticsearch:
    image: elasticsearch:8.15.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports:
      - "9200:9200"
    volumes:
      - elasticsearch_data:/usr/share/elasticsearch/data

  api:
    environment:
      - AURUM_SEARCH_ENABLED=true
      - AURUM_SEARCH_HOSTS=http://elasticsearch:9200
      - AURUM_SEARCH_SEMANTIC_ENABLED=true
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: elasticsearch
spec:
  serviceName: elasticsearch
  replicas: 1
  template:
    spec:
      containers:
      - name: elasticsearch
        image: elasticsearch:8.15.0
        ports:
        - containerPort: 9200
        env:
        - name: discovery.type
          value: "single-node"
        - name: xpack.security.enabled
          value: "false"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
```

## Performance Benchmarks

### Expected Performance
- **Search Latency**: <200ms P95 for basic queries
- **Semantic Search**: <500ms P95 for hybrid queries
- **Indexing Throughput**: 10,000+ documents/minute
- **Facet Aggregation**: <100ms for typical facets
- **Memory Usage**: <2GB for 1M documents

### Scalability Targets
- **Concurrent Users**: 100+ simultaneous searches
- **Query Volume**: 1000+ QPS
- **Index Size**: 100M+ documents
- **Storage Growth**: 10GB/month for typical usage

## Migration Guide

### From Basic Search
1. Enable search feature flags
2. Run batch loader to populate index
3. Update client applications to use new endpoints
4. Migrate existing search logic to use advanced features

### Index Schema Changes
- Added embedding field for semantic search
- Enhanced field mappings with proper analyzers
- Added quality and popularity scores for ranking

## Contributing

### Adding New Data Sources
1. Create mapper class in `src/aurum/search/mappers.py`
2. Add loader class in `scripts/search_batch_loader.py`
3. Update batch loader to include new source
4. Add tests for new mapper

### Extending Search Features
1. Add new search service class
2. Implement feature flag if needed
3. Add API endpoints in `src/aurum/api/v2/search.py`
4. Update documentation

### Performance Optimization
1. Profile slow queries using `/v2/search/explain`
2. Optimize Elasticsearch queries and mappings
3. Add caching for frequently accessed data
4. Tune circuit breaker parameters

## Support

For issues and questions:
- Check troubleshooting section above
- Review API documentation
- Enable debug logging: `AURUM_LOG_LEVEL=DEBUG`
- Contact the Aurum platform team
