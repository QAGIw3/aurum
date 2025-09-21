# Aurum Market Intelligence Platform API

Comprehensive API for accessing market data, curves, and analytics from the Aurum platform.

## Overview
The Aurum API provides access to:
- Market curve data with historical and real-time pricing
- Scenario analysis and modeling capabilities
- Metadata dimensions for data exploration
- ISO location mappings and market intelligence
- Performance monitoring and health checks

## Authentication
The API supports multiple authentication methods:
- JWT tokens for user authentication
- Service-to-service authentication via Kubernetes RBAC
- Optional OIDC integration for enterprise SSO

## Rate Limiting
Rate limiting is applied per endpoint with configurable limits.
Default limits: 10 requests/second, 20 burst requests.

## Pagination
Large datasets support cursor-based pagination for efficient data retrieval.

## Error Handling
All endpoints return structured error responses with appropriate HTTP status codes.


## Base URLs

- **Development server (Kubernetes internal)**: `http://api.aurum-dev.svc.cluster.local:8080`
- **Production server**: `https://api.aurum.local`
- **Local development server**: `http://localhost:8080`

## Authentication

The API supports multiple authentication methods:

### JWT Token Authentication
```bash
curl -H "Authorization: Bearer <jwt_token>" \
     http://api.aurum-dev.svc.cluster.local:8080/v1/curves
```

### API Key Authentication
```bash
curl -H "X-API-Key: <api_key>" \
     http://api.aurum-dev.svc.cluster.local:8080/v1/curves
```

## Endpoints

### /health

#### GET Health Check

Basic health check endpoint for load balancer and monitoring.
Returns 200 OK if service is healthy.


**Responses:**

- `200`: Service is healthy

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/health"
```

### /ready

#### GET Readiness Check

Readiness probe for load balancer checks.
Validates connectivity to critical dependencies (Trino, TimescaleDB, Redis).


**Responses:**

- `200`: Service is ready to handle requests
- `503`: Service is not ready

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/ready"
```

### /v1/curves

#### GET Get Curve Data

Retrieve market curve data with optional filtering and pagination.

## Filtering
Multiple filter parameters can be combined for complex queries.

## Pagination
Use cursor-based pagination for large datasets.
The response includes `next_cursor` and `prev_cursor` fields.


**Parameters:**

- `` (string): 
- `` (string): 
- `asset_class` (string): Filter by asset class
- `iso` (string): Filter by ISO country code
- `market` (string): Filter by market
- `location` (string): Filter by location
- `product` (string): Filter by product
- `block` (string): Filter by trading block
- `tenor_type` (string): Filter by tenor type
- `price_type` (string): Filter by price type
- `` (string): 
- `` (string): 

**Responses:**

- `200`: Curve data retrieved successfully
- `400`: Invalid request parameters
- `503`: Service unavailable

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/curves"
```

### /v1/curves/diff

#### GET Get Curve Differences

Compare curve data between two dates to identify price changes.

## Price Differences
- `diff_abs`: Absolute price difference (mid_b - mid_a)
- `diff_pct`: Percentage price difference ((mid_b - mid_a) / mid_a * 100)


**Parameters:**

- `` (string): 
- `asof_a` (string) (required): First comparison date
- `asof_b` (string) (required): Second comparison date
- `iso` (string): Filter by ISO country code
- `market` (string): Filter by market
- `location` (string): Filter by location
- `` (string): 
- `` (string): 

**Responses:**

- `200`: Curve differences retrieved successfully
- `400`: Invalid request parameters
- `503`: Service unavailable

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/curves/diff"
```

### /v1/metadata/dimensions

#### GET Get Metadata Dimensions

Retrieve available values for metadata dimensions used in filtering.

## Usage
Use this endpoint to populate filter dropdowns and understand available data dimensions.


**Parameters:**

- `include_counts` (boolean): Include count information for each dimension value

**Responses:**

- `200`: Dimensions retrieved successfully
- `503`: Service unavailable

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/metadata/dimensions"
```

### /v1/locations/iso

#### GET Get ISO Locations

Retrieve mapping of ISO country codes to trading locations and hubs.

## Location Types
- **HUB**: Major trading hub (e.g., Henry Hub)
- **CITY**: Major city trading point
- **REGION**: Regional trading area
- **ZONE**: Specific delivery zone


**Parameters:**

- `` (string): 
- `location_type` (string): Filter by location type
- `` (string): 
- `` (string): 

**Responses:**

- `200`: ISO locations retrieved successfully
- `400`: Invalid request parameters
- `503`: Service unavailable

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/locations/iso"
```

### /v1/locations/iso/{location_id}

#### GET Get ISO Location Details

Retrieve detailed information about a specific trading location.


**Parameters:**

- `` (string): 

**Responses:**

- `200`: Location details retrieved successfully
- `404`: Location not found
- `503`: Service unavailable

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/locations/iso/{location_id}"
```

## Data Models

### Meta

**Properties:**

- `request_id` (string) (required): Unique request identifier for tracing Example: `req-12345678-90ab-cdef-1234-567890abcdef`
- `query_time_ms` (integer) (required): Query execution time in milliseconds Example: `145`
- `next_cursor` (string): Cursor for next page of results Example: `eyJwYWdlIjoxLCJsaW1pdCI6MTAwfQ==`
- `prev_cursor` (string): Cursor for previous page of results Example: `eyJwYWdlIjowLCJsaW1pdCI6MTAwfQ==`

### CurvePoint

**Properties:**

- `curve_key` (string) (required): Unique identifier for the curve Example: `NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY`
- `tenor_label` (string) (required): Tenor description (e.g., "Jan24", "Q1-24") Example: `Jan24`
- `tenor_type` (string): Type of tenor period Example: `MONTHLY`
- `contract_month` (string): Contract delivery month Example: `2024-01-01`
- `asof_date` (string) (required): Date this curve data is as of Example: `2024-01-15`
- `mid` (number): Mid price Example: `2.85`
- `bid` (number): Bid price Example: `2.84`
- `ask` (number): Ask price Example: `2.86`
- `price_type` (string): Type of price quoted Example: `MID`

### CurveResponse

**Properties:**

- `meta` (any) (required): 
- `data` (array) (required): List of curve data points

### CurveDiffPoint

**Properties:**

- `curve_key` (string) (required): Curve identifier Example: `NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY`
- `tenor_label` (string) (required): Tenor label Example: `Jan24`
- `tenor_type` (string):  Example: `MONTHLY`
- `contract_month` (string):  Example: `2024-01-01`
- `asof_a` (string) (required): First comparison date Example: `2024-01-10`
- `mid_a` (number): Mid price on first date Example: `2.8`
- `asof_b` (string) (required): Second comparison date Example: `2024-01-15`
- `mid_b` (number): Mid price on second date Example: `2.85`
- `diff_abs` (number): Absolute price difference Example: `0.05`
- `diff_pct` (number): Percentage price difference Example: `1.79`

### CurveDiffResponse

**Properties:**

- `meta` (any) (required): 
- `data` (array) (required): List of curve difference data points

### DimensionsData

**Properties:**

- `asset_class` (array): Available asset classes Example: `['NATURAL_GAS', 'COAL', 'OIL']`
- `iso` (array): Available ISO country codes Example: `['US', 'CA', 'MX']`
- `location` (array): Available locations Example: `['HENRY_HUB', 'ALGONQUIN', 'CHICAGO']`
- `market` (array): Available markets Example: `['NYMEX', 'ICE', 'OTC']`
- `product` (array): Available products Example: `['NATURAL_GAS', 'POWER', 'EMISSIONS']`
- `block` (array): Available trading blocks Example: `['PEAK', 'OFF_PEAK', 'FLAT']`
- `tenor_type` (array): Available tenor types Example: `['MONTHLY', 'QUARTERLY', 'CALENDAR']`

### DimensionCount

**Properties:**

- `value` (string) (required): Dimension value Example: `NATURAL_GAS`
- `count` (integer) (required): Number of records with this value Example: `15420`

### DimensionsCountData

**Properties:**

- `asset_class` (array): Asset class counts
- `iso` (array): ISO country counts
- `location` (array): Location counts
- `market` (array): Market counts
- `product` (array): Product counts
- `block` (array): Block counts
- `tenor_type` (array): Tenor type counts

### DimensionsResponse

**Properties:**

- `meta` (any) (required): 
- `data` (any) (required): Available dimension values
- `counts` (any): Count data for dimensions

### IsoLocationOut

**Properties:**

- `iso` (string) (required): ISO country code Example: `US`
- `location_id` (string) (required): Location identifier Example: `HENRY_HUB`
- `location_name` (string): Human-readable location name Example: `Henry Hub`
- `location_type` (string): Type of location (hub, city, region, etc.) Example: `HUB`
- `zone` (string): Time zone identifier Example: `America/Chicago`
- `hub` (string): Trading hub identifier Example: `HENRY_HUB`
- `timezone` (string): Time zone name Example: `Central Time`

### IsoLocationsResponse

**Properties:**

- `meta` (any) (required): 
- `data` (array) (required): List of ISO locations

### IsoLocationResponse

**Properties:**

- `meta` (any) (required): 
- `data` (any) (required): Single ISO location

### ErrorResponse

**Properties:**

- `error` (object) (required): 

