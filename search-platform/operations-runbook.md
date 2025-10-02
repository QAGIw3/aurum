# Search Platform Operations Runbook

## Overview

This runbook provides operational procedures for managing the Aurum Advanced Search and Discovery Platform, including deployment, monitoring, troubleshooting, and maintenance tasks.

## Daily Operations

### 1. Health Check

**Frequency**: Daily
**Command**: `curl http://localhost:8095/v2/search/health`

**Expected Response**:
```json
{
  "status": "healthy",
  "elasticsearch": {"healthy": true, "index_name": "aurum-search-v1"},
  "indices": {...},
  "circuit_breakers": {...},
  "timestamp": 1699123456789
}
```

**Actions**:
- ✅ Status = "healthy": Continue normal operations
- ⚠️ Status = "degraded": Check specific component health
- ❌ Status = "unhealthy": Immediate investigation required

### 2. Performance Monitoring

**Frequency**: Continuous (automated)
**Metrics to Monitor**:
- Response time P95 < 200ms (basic search)
- Response time P95 < 500ms (semantic search)
- Error rate < 1%
- Index size growth < 10GB/month
- Circuit breaker trip rate < 0.1%

**Dashboards**:
- Grafana: `http://localhost:3000/d/search-performance`
- Kibana: `http://localhost:5601/app/discover`

### 3. Index Maintenance

**Frequency**: Weekly
**Command**:
```bash
curl -X POST http://localhost:8095/v2/search/maintenance
```

**Expected Actions**:
- Index rollover if size > 50GB or age > 30 days
- Old index cleanup (retention > 90 days)
- Index optimization (force merge segments)

### 4. Backup Verification

**Frequency**: Weekly
**Command**:
```bash
curl -X POST http://localhost:8095/v2/search/backup \
  -H "Content-Type: application/json" \
  -d '{"backup_name": "weekly-backup"}'
```

**Verification**:
- Check Elasticsearch snapshot status
- Verify backup integrity
- Test restore procedure quarterly

## Incident Response

### Priority 1: Search Service Down

**Symptoms**:
- `/v2/search/health` returns "unhealthy"
- 5xx errors on search endpoints
- Circuit breakers open

**Immediate Actions**:
1. Check Elasticsearch cluster health:
   ```bash
   curl http://localhost:9200/_cluster/health
   ```

2. Restart Elasticsearch if unresponsive:
   ```bash
   docker restart aurum_elasticsearch_1
   ```

3. Check application logs:
   ```bash
   docker logs aurum_api_1 | grep -i search
   ```

4. Reset circuit breakers:
   ```bash
   # Access circuit breaker management (requires admin)
   ```

**Escalation**:
- If Elasticsearch restart fails: Infrastructure team
- If persistent application errors: Development team

### Priority 2: Performance Degradation

**Symptoms**:
- Response times > 500ms P95
- High memory usage
- Frequent garbage collection

**Actions**:
1. Check index statistics:
   ```bash
   curl "http://localhost:9200/_cat/indices/aurum*?v"
   ```

2. Force merge segments:
   ```bash
   curl -X POST "http://localhost:9200/aurum-search-v1/_forcemerge?max_num_segments=1"
   ```

3. Check memory usage:
   ```bash
   docker stats aurum_elasticsearch_1
   ```

**Optimization**:
- Increase JVM heap size if needed
- Optimize search queries
- Add result caching

### Priority 3: Data Quality Issues

**Symptoms**:
- Missing search results
- Incorrect facet counts
- Stale data in results

**Actions**:
1. Check index refresh status:
   ```bash
   curl "http://localhost:9200/aurum-search-v1/_refresh"
   ```

2. Verify recent indexing:
   ```bash
   curl "http://localhost:9200/aurum-search-v1/_count"
   ```

3. Check data pipeline health

## Maintenance Procedures

### Index Optimization

**Scheduled**: Weekly (Sundays 2-4 AM)

1. **Pre-optimization**:
   ```bash
   # Check current index state
   curl "http://localhost:9200/_cat/indices/aurum*?v&h=index,docs.count,store.size,segments.count"
   ```

2. **Force merge**:
   ```bash
   curl -X POST "http://localhost:9200/aurum-search-v1/_forcemerge?max_num_segments=1&only_expunge_deletes=true"
   ```

3. **Post-optimization verification**:
   ```bash
   # Verify reduced segments
   curl "http://localhost:9200/_cat/indices/aurum*?v&h=index,segments.count"
   ```

### Backup and Recovery

**Daily Backups**: Automated via index lifecycle management

**Manual Backup**:
```bash
curl -X POST http://localhost:8095/v2/search/backup \
  -H "Content-Type: application/json" \
  -d '{"backup_name": "manual-backup-$(date +%Y%m%d)"}'
```

**Restore from Backup**:
```bash
# 1. Stop indexing operations
# 2. Restore snapshot (requires Elasticsearch admin)
curl -X POST "http://localhost:9200/_snapshot/aurum-search-snapshots/backup-name/_restore"
# 3. Verify restoration
# 4. Resume operations
```

### Configuration Updates

**Search Settings**:
```bash
# Update environment variables
export AURUM_SEARCH_SEMANTIC_WEIGHT=0.4
export AURUM_SEARCH_KNN_K=200

# Restart API service
docker restart aurum_api_1
```

**Feature Flags**:
```bash
# Enable/disable features
export AURUM_SEARCH_SEMANTIC_ENABLED=false  # Disable semantic search
export AURUM_SEARCH_ANALYTICS_ENABLED=true  # Enable analytics
```

## Capacity Planning

### Storage Requirements

**Index Growth**:
- Expected: 10-20GB per month
- Current: Monitor via `/v2/search/health`
- Threshold: Alert when >80% of allocated storage

**Memory Requirements**:
- Elasticsearch: 2-4GB heap
- Application: 1-2GB for search services
- Semantic models: 500MB-1GB per model

**CPU Requirements**:
- Search queries: 1-2 cores
- Indexing: 2-4 cores during batch loads
- Semantic processing: 2-4 cores for embedding

### Scaling Considerations

**Horizontal Scaling**:
- Add Elasticsearch replicas for read scaling
- Load balance API requests
- Distribute indexing load

**Vertical Scaling**:
- Increase JVM heap size for larger indices
- Add CPU cores for semantic processing
- Increase memory for caching

## Security Operations

### Access Control

**Multi-tenant Isolation**:
- Verify tenant_id filtering in all queries
- Check audit logs for cross-tenant access attempts
- Monitor for unauthorized index access

**API Security**:
- Rate limiting enforcement
- Input validation and sanitization
- Authentication bypass attempts

### Data Protection

**Backup Security**:
- Encrypted backup storage
- Access controls on backup repositories
- Regular backup integrity verification

**Data Retention**:
- Automatic cleanup of old indices
- Secure deletion procedures
- Compliance with data retention policies

## Troubleshooting Guides

### Common Issues

#### 1. Search Returns No Results

**Diagnosis**:
```bash
# Check index exists and has documents
curl "http://localhost:9200/aurum-search-v1/_count"

# Check query execution
curl "http://localhost:8095/v2/search/explain?q=test%20query"
```

**Solutions**:
- Verify index is populated
- Check query syntax and field mappings
- Review tenant_id filtering
- Validate document mappings

#### 2. Slow Search Performance

**Diagnosis**:
```bash
# Check index segments
curl "http://localhost:9200/_cat/indices/aurum*?v&h=index,segments.count"

# Check query performance
curl -X POST "http://localhost:8095/v2/search/query" \
  -H "Content-Type: application/json" \
  -d '{"q": "slow query test", "explain": true}'
```

**Solutions**:
- Force merge segments
- Optimize query structure
- Add query result caching
- Scale Elasticsearch resources

#### 3. Semantic Search Issues

**Diagnosis**:
```bash
# Check model loading
python -c "from sentence_transformers import SentenceTransformer; print('Model OK')"

# Check circuit breaker status
curl "http://localhost:8095/v2/search/health"
```

**Solutions**:
- Restart semantic search service
- Verify model cache integrity
- Check memory allocation
- Review embedding generation logs

#### 4. Facet Count Inaccuracies

**Diagnosis**:
```bash
# Compare facet counts with document counts
curl "http://localhost:8095/v2/search/facets?field=doc_type"
curl "http://localhost:9200/aurum-search-v1/_search?size=0"
```

**Solutions**:
- Refresh index mappings
- Rebuild facet aggregations
- Check for mapping conflicts
- Verify document field consistency

## Emergency Procedures

### Service Restoration

**Critical Failure**:
1. **Immediate**: Switch to read-only mode
2. **Short-term**: Restore from latest backup
3. **Long-term**: Investigate root cause and implement fixes

**Data Recovery**:
1. **Identify affected indices**: Check modification times
2. **Restore from backup**: Use snapshot restore
3. **Verify integrity**: Cross-check document counts
4. **Resume operations**: Enable write operations

### Rollback Procedures

**Application Rollback**:
```bash
# Revert to previous version
docker tag aurum-api:previous aurum-api:latest
docker restart aurum_api_1
```

**Configuration Rollback**:
```bash
# Restore previous environment variables
source /path/to/previous/.env
docker restart aurum_api_1
```

## Contact Information

### On-Call Rotation
- **Primary**: Search Platform Team
- **Secondary**: Infrastructure Team
- **Escalation**: Platform Engineering Director

### Communication Channels
- **Slack**: #search-platform
- **Email**: search-team@aurum.com
- **Phone**: Emergency hotline (24/7)

### Documentation Updates
- Update this runbook after major incidents
- Document new procedures and lessons learned
- Review quarterly for accuracy and completeness

---

*Last Updated: January 2024*
*Next Review: April 2024*
