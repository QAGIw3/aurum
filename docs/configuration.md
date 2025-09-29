# Aurum Advanced Configuration Management System

This project provides a comprehensive, dynamic configuration management system with advanced features for enterprise deployments.

## Overview

The system provides:

- **Dynamic Configuration**: Hot-reloadable configuration with layered sources
- **Environment Inheritance**: Environment-specific configuration overlays
- **Schema Validation**: Strong typing and validation with JSON Schema export
- **Change Tracking**: Full audit trail with versioning and diffing
- **Backup & Recovery**: Point-in-time recovery with disaster recovery support
- **Feature Flag Integration**: Dynamic feature flags tied to configuration
- **Security & Governance**: RBAC permissions, audit trails, and compliance features

## Architecture

The configuration system consists of several key components:

### Configuration Sources (Layered Precedence)

Configuration is loaded from multiple sources with the following precedence (highest to lowest):

1. **Environment Variables** (`AURUM_*`): Runtime overrides, highest priority
2. **Ephemeral Overrides**: API/CLI overrides with TTL
3. **Environment Overlays**: `config/<ENV>.(yaml|json)` files
4. **Base Overlays**: `config/base.(yaml|json)` files
5. **Code Defaults**: Built-in configuration defaults

### Core Components

- **DynamicConfigService**: Main service managing configuration lifecycle
- **SchemaRegistry**: Schema definitions and validation
- **ChangeTracker**: Audit trail, versioning, and backup
- **SettingsManager**: Integration layer with existing AurumSettings

## Quick Start

### Basic Usage

```python
from aurum.config.dynamic_config import DynamicConfigService
from aurum.core.settings import get_settings_manager

# Get dynamic configuration service
config_service = DynamicConfigService(environment="production")
config = config_service.get()

# Or use the integrated settings manager
settings_manager = get_settings_manager(use_dynamic_config=True)
settings = settings_manager.get()
```

### Environment Variables

Primary configuration method using `AURUM_` prefixed variables:

```bash
export AURUM_API_TITLE="Aurum API"
export AURUM_DATABASE_URL="postgresql://user:pass@localhost/aurum"
export AURUM_REDIS_URL="redis://localhost:6379"
```

### Configuration Files

Environment-specific configuration in YAML/JSON format:

```yaml
# config/base.yaml - Common defaults
api:
  title: "Aurum API"
  version: "1.0.0"
  debug: false

database:
  host: "localhost"
  port: 5432
  pool:
    min_size: 1
    max_size: 10

redis:
  host: "localhost"
  port: 6379
  db: 0
```

```yaml
# config/production.yaml - Production overrides
api:
  debug: false
  host: "0.0.0.0"
  port: 8000

database:
  host: "prod-db.example.com"
  ssl: true

redis:
  host: "prod-redis.example.com"
  password: "secret"
```

### Feature Flags

Enable the dynamic configuration system:

```bash
export AURUM_USE_DYNAMIC_CONFIG=true
```

## CLI Tools

### Configuration Diffing

```bash
# Show effective configuration
python scripts/config/diff.py show-effective --env=production

# Compare versions
python scripts/config/diff.py diff --from=1 --to=5

# List configuration versions
python scripts/config/diff.py versions --limit=10

# Show change history
python scripts/config/diff.py changes --actor=admin --limit=20
```

### Backup and Restore

```bash
# Backup current configuration
python scripts/config/backup_restore.py backup --reason="Pre-deployment backup"

# List available backups
python scripts/config/backup_restore.py list-backups

# Restore to specific version
python scripts/config/backup_restore.py restore --version=3 --actor=admin --reason="Rollback due to issues"
```

### Schema Export

```bash
# Export all schemas
python scripts/config/diff.py export-schema --output=docs/schemas
```

## API Endpoints

All admin configuration endpoints require `CONFIG_MANAGE` permission.

### Get Effective Configuration

```http
GET /v2/admin/config/effective?environment=production
```

### Configuration Versions

```http
GET /v2/admin/config/versions
GET /v2/admin/config/versions/{version}
```

### Configuration Changes

```http
GET /v2/admin/config/changes?namespace=api&limit=50
```

### Configuration Diffing

```http
GET /v2/admin/config/diff?from_version=1&to_version=5
```

### Backup and Restore

```http
POST /v2/admin/config/backup?reason=Pre-deployment
POST /v2/admin/config/restore?version=3&reason=Rollback
```

### Schema Export

```http
GET /v2/admin/config/schemas
```

### Ephemeral Overrides

```http
POST /v2/admin/config/overrides?key=feature_flags&value={"new_feature":true}&ttl_seconds=3600
DELETE /v2/admin/config/overrides/{key}
```

## Advanced Features

### Schema Validation

The system validates configuration against predefined schemas for each namespace:

- **api**: API server configuration
- **redis**: Redis connection settings
- **database**: Database connection settings
- **security**: Security and authentication settings
- **feature_flags**: Feature flag configuration

### Change Tracking

Every configuration change is tracked with:

- Unique change ID
- Timestamp and actor
- Before/after configuration snapshots
- Structured diffs
- Correlation IDs for tracing

### Hot Reloading

Configuration files are monitored for changes:

- Automatic reload in development environments
- Manual reload in production
- Graceful handling of invalid configurations
- Rollback to previous valid configuration

### Backup and Recovery

- Point-in-time snapshots
- Compressed storage for efficiency
- Metadata tracking (reason, actor, timestamp)
- Disaster recovery capabilities

### Security and Governance

- RBAC permissions for configuration management
- Audit trails for compliance
- Secret management (external secret stores recommended)
- Change approval workflows

## Migration Guide

### From Legacy Settings

1. Enable feature flag: `AURUM_USE_DYNAMIC_CONFIG=true`
2. Add configuration files to `config/` directory
3. Gradually migrate environment variables to files
4. Use CLI tools for validation and testing

### Best Practices

1. **Environment Separation**: Use separate config files per environment
2. **Validation**: Always validate configuration before deployment
3. **Backups**: Regular automated backups before changes
4. **Testing**: Test configuration changes in staging first
5. **Secrets**: Never store secrets in configuration files
6. **Documentation**: Document configuration changes with reasons

## Troubleshooting

### Common Issues

1. **Configuration not loading**: Check file permissions and YAML/JSON syntax
2. **Validation errors**: Review schema requirements and data types
3. **Hot reload not working**: Ensure file watcher permissions and check logs
4. **Permission denied**: Verify RBAC permissions for admin operations

### Debug Mode

Enable debug logging:

```bash
export AURUM_DEBUG=true
export AURUM_LOG_LEVEL=DEBUG
```

Check configuration status:

```bash
python -c "
from aurum.config.dynamic_config import DynamicConfigService
from aurum.core.settings import get_settings_manager
import json

service = DynamicConfigService()
config = service.get()
print('Configuration loaded successfully')
print(f'Version: {service.get_snapshot().version}')
print(f'Sources: {[s.name for s in service._sources]}')
"
```

# Configuration Reference

Centralized environment configuration for Aurum services is defined in `src/aurum/core/settings.py` and loaded by `AurumSettings.from_env()` with prefix `AURUM_` and nested fields using `__` (double underscore) delimiter.

This guide lists the most relevant settings for local/dev and production. Defaults are sensible for local development.

## Global

- `ENV` (alias of `AURUM_ENV`): environment label, default `local`
- `DEBUG` (alias of `AURUM_DEBUG`): enable debug mode, default `false`

## API Server

- `AURUM_API_TITLE`: API title (defaults from code)
- `AURUM_API_VERSION`: API version (defaults from code)
- `AURUM_API_REQUEST_TIMEOUT`: request timeout seconds (default 30)
- `AURUM_API_GZIP_MIN_BYTES`: minimum response bytes for gzip (default 500)
- `AURUM_API_MAX_REQUEST_BODY_SIZE`: optional cap in bytes
- `AURUM_API_MAX_RESPONSE_BODY_SIZE`: optional cap in bytes
- `AURUM_API_MAX_CONCURRENT_REQUESTS`: optional concurrency guard
- `AURUM_API_CORS_ORIGINS`: comma-separated list (e.g. `https://app.example.com,*`)
- `AURUM_API_CORS_ALLOW_CREDENTIALS`: `1/0` (default `0`)
- `AURUM_API_METRICS_ENABLED`: expose `/metrics` (default `1`)
- `AURUM_API_METRICS_PATH`: metrics path (default `/metrics`)

## Authentication and Admin

- `AURUM_API_AUTH_DISABLED`: disable auth entirely (dev only)
- `AURUM_API_ADMIN_GROUP`: comma-separated admin groups allowed to use admin endpoints
- `AURUM_API_ADMIN_GUARD_ENABLED`: set `1` to enforce an admin guard on `/v1/admin/*` and `/v2/admin/*` routes (returns 403 for non-admins)
- OIDC/JWT:
  - `AURUM_API_OIDC_ISSUER`
  - `AURUM_API_OIDC_AUDIENCE`
  - `AURUM_API_OIDC_JWKS_URL`
  - `AURUM_API_OIDC_CLIENT_ID`
  - `AURUM_API_OIDC_CLIENT_SECRET`
  - `AURUM_API_JWT_SECRET` (HS256 fallback)
  - `AURUM_API_JWT_LEEWAY` (seconds)

## Data Backends

Primary engine for the API is Trino. See also ClickHouse/Timescale options in `DataBackendSettings` if used elsewhere.

- `AURUM_API_TRINO_HOST` (default `localhost`)
- `AURUM_API_TRINO_PORT` (default `8080`)
- `AURUM_API_TRINO_USER` (default `aurum`)
- `AURUM_API_TRINO_SCHEME` (`http|https`, default `http`)
- `AURUM_API_TRINO_CATALOG` (default `iceberg`)
- `AURUM_API_TRINO_SCHEMA` (default `market`)
- `AURUM_API_TRINO_PASSWORD` (optional)

### Backend Selector (experimental)

The API can read from multiple backends. Set `AURUM_API_BACKEND` to one of `trino`, `clickhouse`, or `timescale` and configure base tables per backend as needed. The selector is tried first and falls back to legacy helpers on errors.

Dimensions (curve metadata):
- `AURUM_API_DIMENSIONS_TABLE_TRINO` (default `iceberg.market.curve_observation`)
- `AURUM_API_DIMENSIONS_TABLE_CLICKHOUSE` (default `aurum.curve_observation`)
- `AURUM_API_DIMENSIONS_TABLE_TIMESCALE` (default `market.curve_observation`)

EIA series:
- `AURUM_API_EIA_SERIES_TABLE_TRINO` (default `iceberg.market.eia_series`; falls back to `AURUM_EIA_SERIES_BASE_TABLE` if set)
- `AURUM_API_EIA_SERIES_TABLE_CLICKHOUSE` (default `aurum.eia_series`)
- `AURUM_API_EIA_SERIES_TABLE_TIMESCALE` (default `market.eia_series`)

ISO LMP:
- `AURUM_API_ISO_LMP_LAST24H_TABLE_{TRINO,CLICKHOUSE,TIMESCALE}` (defaults `environment.iso_lmp_last_24h`, `aurum.iso_lmp_last_24h`, `public.iso_lmp_last_24h`)
- `AURUM_API_ISO_LMP_HOURLY_TABLE_{TRINO,CLICKHOUSE,TIMESCALE}` (defaults `environment.iso_lmp_hourly`, `aurum.iso_lmp_hourly`, `public.iso_lmp_hourly`)
- `AURUM_API_ISO_LMP_DAILY_TABLE_{TRINO,CLICKHOUSE,TIMESCALE}` (defaults `environment.iso_lmp_daily`, `aurum.iso_lmp_daily`, `public.iso_lmp_daily`)

Drought indices + geographies join:
- `AURUM_API_DROUGHT_INDEX_TABLE_{TRINO,CLICKHOUSE,TIMESCALE}` (defaults `environment.drought_index`, `aurum.drought_index`, `public.drought_index`)
- `AURUM_API_USDM_AREA_TABLE_{TRINO,CLICKHOUSE,TIMESCALE}` (defaults `environment.usdm_area`, `aurum.usdm_area`, `public.usdm_area`)
- `AURUM_API_GEOGRAPHIES_TABLE_{TRINO,CLICKHOUSE,TIMESCALE}` (defaults `ref.geographies`, `aurum.geographies`, `ref.geographies`)

Timescale and EIA wiring used by metadata/external data features:

- `AURUM_TIMESCALE_DSN` (e.g. `postgresql://timescale:timescale@localhost:5433/timeseries`)
- `AURUM_API_EIA_SERIES_TABLE` (override base table for EIA series lookups)

## Redis Cache

- `AURUM_API_REDIS_URL` (e.g. `redis://localhost:6379/0`)
- `AURUM_API_REDIS_MODE` (`standalone|sentinel|cluster|disabled`)
- `AURUM_API_REDIS_DB` (int)
- `AURUM_API_REDIS_USERNAME`, `AURUM_API_REDIS_PASSWORD`
- `AURUM_API_CACHE_TTL` (seconds, default 60)
- Sentinel: `AURUM_API_REDIS_SENTINEL_ENDPOINTS` (comma-separated `host:port`), `AURUM_API_REDIS_SENTINEL_MASTER`
- Cluster: `AURUM_API_REDIS_CLUSTER_NODES` (comma-separated)
- Misc: `AURUM_API_REDIS_NAMESPACE`, `AURUM_API_REDIS_SOCKET_TIMEOUT`, `AURUM_API_REDIS_CONNECT_TIMEOUT`

Per-slice TTLs for hot endpoints (Golden Query Cache, see `docs/golden_query_cache.md`):

- `AURUM_API_CACHE_TTL_HIGH_FREQUENCY`
- `AURUM_API_CACHE_TTL_MEDIUM_FREQUENCY`
- `AURUM_API_CACHE_TTL_LOW_FREQUENCY`
- `AURUM_API_CACHE_TTL_STATIC`
- `AURUM_API_CACHE_TTL_CURVE_DATA`
- `AURUM_API_CACHE_TTL_METADATA`
- `AURUM_API_CACHE_TTL_EXTERNAL_DATA`
- `AURUM_API_CACHE_TTL_SCENARIO_DATA`
- `AURUM_API_CURVE_TTL`, `AURUM_API_CURVE_DIFF_TTL`, `AURUM_API_CURVE_STRIP_TTL`

## Pagination Limits

- `AURUM_API_CURVE_MAX_LIMIT`
- `AURUM_API_SCENARIO_OUTPUT_MAX_LIMIT`
- `AURUM_API_SCENARIO_METRIC_MAX_LIMIT`
- `AURUM_API_EIA_SERIES_MAX_LIMIT`

## Rate Limiting

- `AURUM_API_RATE_LIMIT_ENABLED` (`1/0`)
- `AURUM_API_RATE_LIMIT_RPS` (default 10)
- `AURUM_API_RATE_LIMIT_BURST` (default 20)
- `AURUM_API_RATE_LIMIT_HEADER` (optional request header for identifier)
- `AURUM_API_RATE_LIMIT_WHITELIST` (comma-separated subjects)
- Global overrides: `AURUM_API_RATE_LIMIT_OVERRIDES` (format: `/path=rps:burst,...`)
- Tenant overrides: `AURUM_API_RATE_LIMIT_TENANT_OVERRIDES` (format: `tenant=/path=rps:burst,...`)

## Observability

- `AURUM_OTEL_SERVICE_NAME`, `AURUM_OTEL_SERVICE_NAMESPACE`, `AURUM_OTEL_SERVICE_INSTANCE_ID`
- `AURUM_OTEL_EXPORTER_ENDPOINT` (OTLP/HTTP|gRPC)
- `AURUM_OTEL_EXPORTER_INSECURE` (`1/0`)
- `AURUM_OTEL_SAMPLER_RATIO` (`0.0..1.0`)

## External Audit Logging

- `AURUM_API_EXTERNAL_AUDIT_ENABLED`: enable the external audit middleware (`0` by default)
- `AURUM_API_AUDIT_SINKS`: comma-separated sinks (`file`, `kafka`, `clickhouse`)
- `AURUM_AUDIT_LOG_DIR` / `AURUM_API_AUDIT_LOG_DIR`: on-disk fallback directory for audit files
- Kafka sink configuration:
  - `AURUM_API_AUDIT_KAFKA_BOOTSTRAP`
  - `AURUM_API_AUDIT_KAFKA_TOPIC`
  - `AURUM_API_AUDIT_KAFKA_CLIENT_ID`
  - `AURUM_API_AUDIT_KAFKA_SECURITY_PROTOCOL`
  - Optional SASL: `AURUM_API_AUDIT_KAFKA_USERNAME`, `AURUM_API_AUDIT_KAFKA_PASSWORD`, `AURUM_API_AUDIT_KAFKA_SASL_MECHANISM`
  - Optional compression/acks: `AURUM_API_AUDIT_KAFKA_COMPRESSION`, `AURUM_API_AUDIT_KAFKA_ACKS`
- ClickHouse sink configuration:
  - `AURUM_API_AUDIT_CLICKHOUSE_ENDPOINT`
  - `AURUM_API_AUDIT_CLICKHOUSE_DATABASE`
  - `AURUM_API_AUDIT_CLICKHOUSE_TABLE`
  - Optional auth + timeout: `AURUM_API_AUDIT_CLICKHOUSE_USERNAME`, `AURUM_API_AUDIT_CLICKHOUSE_PASSWORD`, `AURUM_API_AUDIT_CLICKHOUSE_TIMEOUT`

## Example .env Snippet (API)

```env
AURUM_API_TRINO_HOST=localhost
AURUM_API_TRINO_PORT=8080
AURUM_API_TRINO_USER=aurum
AURUM_API_TRINO_CATALOG=iceberg
AURUM_API_TRINO_SCHEMA=market

AURUM_API_REDIS_URL=redis://localhost:6379/0
AURUM_API_CACHE_TTL_CURVE_DATA=900

AURUM_API_RATE_LIMIT_ENABLED=1
AURUM_API_RATE_LIMIT_RPS=20
AURUM_API_RATE_LIMIT_BURST=40

AURUM_API_CORS_ORIGINS=http://localhost:3000
AURUM_API_METRICS_ENABLED=1

# Optional OIDC
AURUM_API_AUTH_DISABLED=0
AURUM_API_OIDC_ISSUER=https://id.example.com
AURUM_API_OIDC_AUDIENCE=aurum-api
AURUM_API_OIDC_JWKS_URL=https://id.example.com/oidc/jwks
AURUM_API_ADMIN_GROUP=aurum-admin
```

For a deeper dive, see `src/aurum/core/settings.py` and inline docstrings.
