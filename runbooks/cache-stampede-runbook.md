# Cache Stampede Runbook

## Overview

This runbook provides procedures for responding to cache stampede incidents in the Aurum platform, where multiple concurrent requests cause excessive load on backend systems.

## Contact Information

- **Primary On-call**: platform-team@aurum.com
- **Data Team**: data-team@aurum.com
- **Emergency**: +1 (555) 123-4567

## Incident Classification

### Severity Levels

#### Critical (P0)
- System-wide cache stampede affecting all services
- Database connection pool exhaustion
- Complete API unavailability

#### High (P1)
- Cache stampede affecting critical endpoints
- Backend services overwhelmed
- Partial API degradation

#### Medium (P2)
- Localized cache stampede
- Single service affected
- Minor performance impact

## Detection & Assessment

### 1. Initial Detection

#### Check Cache Metrics
```bash
# Monitor cache hit/miss rates
kubectl port-forward svc/prometheus 9090:9090 -n aurum-dev
open http://localhost:9090/alerts

# Check cache performance metrics
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=cache_hit_rate
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=cache_miss_rate
```

#### Verify Backend Load
```bash
# Check database connection pool
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_db_connections_active

# Check API request patterns
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_api_requests_total

# Monitor worker queue depth
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_scenario_queue_size
```

#### Assess Impact
```bash
# Check system performance
kubectl top nodes
kubectl top pods -A

# Review recent cache invalidations
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query_range?query=increase(cache_invalidations_total[5m])&start=$(date -d '5 minutes ago' +%s)&end=$(date +%s)&step=30s
```

### 2. Identify Stampede Patterns

#### Cache Key Analysis
```bash
# Find most accessed cache keys
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=topk(10, cache_key_access_total)

# Identify high-miss rate keys
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=cache_key_miss_rate > 0.8
```

#### Request Pattern Analysis
```bash
# Check for sudden traffic spikes
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(aurum_api_requests_total[1m]) > rate(aurum_api_requests_total[5m]) * 3

# Identify problematic endpoints
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=topk(5, rate(aurum_api_requests_total[5m]))
```

## Response Procedures

### Phase 1: Immediate Mitigation (0-5 minutes)

#### Enable Circuit Breaker
```bash
# Enable circuit breaker for affected services
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_CIRCUIT_BREAKER_ENABLED": "true",
    "AURUM_API_CIRCUIT_BREAKER_THRESHOLD": "1000",
    "AURUM_API_CIRCUIT_BREAKER_TIMEOUT": "30s"
  }
}'

# Enable circuit breaker for workers
kubectl patch configmap/aurum-worker-config -n aurum-dev --patch '{
  "data": {
    "AURUM_WORKER_CIRCUIT_BREAKER_ENABLED": "true",
    "AURUM_WORKER_CIRCUIT_BREAKER_THRESHOLD": "500"
  }
}'
```

#### Implement Request Coalescing
```bash
# Enable request coalescing to prevent duplicate requests
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_REQUEST_COALESCING_ENABLED": "true",
    "AURUM_API_REQUEST_COALESCING_WINDOW": "5s"
  }
}'
```

#### Scale Backend Services
```bash
# Scale database connections
kubectl scale deployment postgres --replicas=3

# Scale API services
kubectl scale deployment aurum-api --replicas=10

# Scale worker services
kubectl scale deployment aurum-worker --replicas=8
```

### Phase 2: Cache Stabilization (5-15 minutes)

#### Implement Staggered Cache Refresh
```bash
# Enable staggered cache refresh to prevent simultaneous expiration
kubectl patch configmap/cache-config -n aurum-dev --patch '{
  "data": {
    "CACHE_REFRESH_STRATEGY": "staggered",
    "CACHE_STAGGER_WINDOW": "300",
    "CACHE_REFRESH_JITTER": "60"
  }
}'
```

#### Enable Cache Warming
```bash
# Trigger cache warming for critical keys
kubectl create job cache-warming-$(date +%s) --from=cronjob/cache-warming

# Verify cache warming progress
kubectl logs job/cache-warming-$(date +%s)
```

#### Optimize Cache TTL
```bash
# Increase TTL for frequently accessed keys
kubectl patch configmap/cache-config -n aurum-dev --patch '{
  "data": {
    "CACHE_TTL_CRITICAL": "3600",
    "CACHE_TTL_IMPORTANT": "1800",
    "CACHE_TTL_REGULAR": "900"
  }
}'
```

### Phase 3: Load Distribution (15-30 minutes)

#### Implement Load Balancing
```bash
# Enable advanced load balancing strategies
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_LOAD_BALANCING_STRATEGY": "least_connections",
    "AURUM_API_CONNECTION_POOL_SIZE": "50"
  }
}'
```

#### Enable Request Queuing
```bash
# Implement request queuing to smooth traffic spikes
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_REQUEST_QUEUE_ENABLED": "true",
    "AURUM_API_REQUEST_QUEUE_SIZE": "1000",
    "AURUM_API_REQUEST_QUEUE_TIMEOUT": "30s"
  }
}'
```

#### Distribute Cache Load
```bash
# Enable cache sharding to distribute load
kubectl patch configmap/cache-config -n aurum-dev --patch '{
  "data": {
    "CACHE_SHARDING_ENABLED": "true",
    "CACHE_SHARD_COUNT": "16",
    "CACHE_SHARD_STRATEGY": "consistent_hash"
  }
}'
```

## Communication Templates

### Initial Stampede Detection
```
🚨 [SEVERITY] Cache Stampede Detected

**Time**: $(date)
**Affected Component**: [API Cache / Data Cache / Session Cache]
**Impact**: [High latency, backend overload, partial unavailability]
**Status**: Mitigating

**Immediate Actions**:
- Circuit breakers activated
- Request coalescing enabled
- Backend services scaling up

**Expected Duration**: 15-30 minutes
**Contact**: platform-team@aurum.com
```

### Mitigation Update
```
📊 Cache Stampede Mitigation Progress

**Time**: $(date)
**Status**: [Stabilizing / Recovering / Monitoring]
**Component**: [Component name]

**Progress**:
- ✅ Circuit breakers: ACTIVE
- ✅ Request coalescing: ENABLED
- ✅ Cache warming: IN PROGRESS
- ✅ Load balancing: OPTIMIZED

**Current Impact**: [Reduced / Minimal / None]
**Next Update**: [Time]
**Contact**: platform-team@aurum.com
```

### Resolution Notification
```
✅ Cache Stampede Resolved

**Time**: $(date)
**Resolution**: [Brief resolution summary]
**Root Cause**: [Cache key expiration / Traffic spike / Configuration issue]

**Actions Taken**:
- Implemented circuit breakers
- Enabled request coalescing
- Optimized cache TTL settings
- Enhanced load balancing

**Monitoring**: Increased cache monitoring for 24 hours
**Contact**: platform-team@aurum.com
```

## Recovery Procedures

### Cache System Recovery
```bash
# Flush problematic cache keys
kubectl exec -it deployment/cache-manager -- redis-cli FLUSHDB

# Rebuild cache from persistent storage
kubectl create job cache-rebuild-$(date +%s) --from=cronjob/cache-rebuild

# Verify cache health
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=cache_hit_rate > 0.9
```

### Database Recovery
```bash
# Reset database connection pool
kubectl rollout restart deployment/postgres

# Optimize database configuration
kubectl patch configmap/postgres-config -n aurum-dev --patch '{
  "data": {
    "max_connections": "200",
    "shared_buffers": "2GB",
    "effective_cache_size": "6GB"
  }
}'
```

### Application Recovery
```bash
# Restart affected services
kubectl rollout restart deployment/aurum-api
kubectl rollout restart deployment/aurum-worker

# Verify service health
kubectl get pods -l app.kubernetes.io/name=aurum-api
kubectl get pods -l app.kubernetes.io/name=aurum-worker
```

## Prevention Measures

### Cache Optimization
```bash
# Implement cache hierarchies
kubectl patch configmap/cache-config -n aurum-dev --patch '{
  "data": {
    "CACHE_HIERARCHY_ENABLED": "true",
    "CACHE_L1_TTL": "300",
    "CACHE_L2_TTL": "3600",
    "CACHE_L3_TTL": "86400"
  }
}'

# Enable predictive cache warming
kubectl patch configmap/cache-config -n aurum-dev --patch '{
  "data": {
    "PREDICTIVE_WARMING_ENABLED": "true",
    "WARMING_THRESHOLD": "0.8",
    "WARMING_WINDOW": "300"
  }
}'
```

### Rate Limiting
```bash
# Implement intelligent rate limiting
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_RATE_LIMIT_ADAPTIVE": "true",
    "AURUM_API_RATE_LIMIT_MIN": "10",
    "AURUM_API_RATE_LIMIT_MAX": "1000",
    "AURUM_API_RATE_LIMIT_WINDOW": "60"
  }
}'
```

### Load Shedding
```bash
# Enable graceful degradation
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_GRACEFUL_DEGRADATION": "true",
    "AURUM_API_DEGRADATION_THRESHOLD": "80",
    "AURUM_API_DEGRADATION_LEVELS": "3"
  }
}'
```

## Escalation Procedures

### When to Escalate
- Stampede duration > 30 minutes
- Multiple services affected
- Database connection pool exhaustion
- Complete API unavailability

### Escalation Path
1. **Primary**: Platform team lead
2. **Secondary**: Engineering director
3. **Emergency**: CTO/VP Engineering

## Post-Incident Tasks

### Immediate Actions
1. Document incident timeline and impact
2. Review cache access patterns
3. Update monitoring thresholds
4. Communicate resolution to stakeholders

### Follow-up Tasks
1. **Post-mortem**: Schedule within 1 business day
2. **Cache Analysis**: Review cache hit/miss patterns
3. **Performance Optimization**: Implement identified improvements
4. **Documentation**: Update cache best practices

## Tools & Resources

### Monitoring Dashboards
- **Cache Performance Dashboard**: https://grafana.aurum-dev.com/d/cache-performance
- **API Performance Dashboard**: https://grafana.aurum-dev.com/d/aurum-api-performance
- **Database Performance Dashboard**: https://grafana.aurum-dev.com/d/database-performance

### Cache Management Tools
- **Redis CLI**: kubectl exec -it deployment/redis -- redis-cli
- **Cache Manager Service**: http://cache-manager.aurum-dev.svc.cluster.local:8080
- **Cache Analytics**: http://cache-analytics.aurum-dev.svc.cluster.local:9090

### Documentation
- **Cache Architecture Guide**: docs/cache/
- **Performance Tuning Guide**: docs/performance/
- **Load Testing Guide**: docs/load-testing/

## Metrics & KPIs

### Response Time Targets
- **Detection**: < 2 minutes
- **Initial Mitigation**: < 5 minutes
- **Stabilization**: < 15 minutes
- **Full Recovery**: < 30 minutes

### Success Metrics
- **MTTR**: < 15 minutes
- **Cache Hit Rate**: > 95% during normal operations
- **Request Coalescing Success Rate**: > 90%
- **False Positive Rate**: < 5% of alerts
