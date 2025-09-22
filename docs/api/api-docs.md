# Aurum API

Aurum Market Intelligence Platform API

## Version: 0.1.0


## Base URL

- Production server: `https://api.aurum-platform.com`
- Staging server: `https://staging.api.aurum-platform.com`
- Development server: `http://localhost:8000`

## Endpoints

### /v1/admin/ratelimit/config

#### GET /v1/admin/ratelimit/config

**Summary:** Get Ratelimit Config

**Description:** Get current rate limiting configuration.

**Responses:**

- `200`: Successful Response
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/ratelimit/status

#### GET /v1/admin/ratelimit/status

**Summary:** Get Ratelimit Status

**Description:** Get current rate limiting status and active windows.

**Parameters:**

- `tenant_id` (query) : Filter by tenant ID
- `path` (query) : Filter by path prefix

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/ratelimit/metrics

#### GET /v1/admin/ratelimit/metrics

**Summary:** Get Ratelimit Metrics

**Description:** Get rate limiting metrics.

**Parameters:**

- `tenant_id` (query) : Filter by tenant ID

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/ratelimit/test

#### POST /v1/admin/ratelimit/test

**Summary:** Test Ratelimit

**Description:** Test rate limiting for a specific path and tenant.

**Parameters:**

- `path` (query) *: Path to test rate limiting for
- `tenant_id` (query) *: Tenant ID to test
- `requests_per_second` (query) : RPS to test with

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/config/ratelimit/{tenant_id}/{path}

#### PUT /v1/admin/config/ratelimit/{tenant_id}/{path}

**Summary:** Update Tenant Rate Limit

**Description:** Update rate limit configuration for a specific tenant and path.

**Parameters:**

- `tenant_id` (path) *: 
- `path` (path) *: 

**Request Body:**

- Content-Type: `application/json`

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/config/feature-flags/{tenant_id}/{feature_name}

#### PUT /v1/admin/config/feature-flags/{tenant_id}/{feature_name}

**Summary:** Update Feature Flag

**Description:** Update feature flag configuration for a specific tenant.

**Parameters:**

- `tenant_id` (path) *: 
- `feature_name` (path) *: 

**Request Body:**

- Content-Type: `application/json`

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/config/audit-log

#### GET /v1/admin/config/audit-log

**Summary:** Get Audit Log

**Description:** Get audit log for configuration changes.

**Parameters:**

- `tenant_id` (query) : Filter by tenant ID
- `user_id` (query) : Filter by user ID
- `limit` (query) : Maximum number of entries to return

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/config/feature-flags/{tenant_id}

#### GET /v1/admin/config/feature-flags/{tenant_id}

**Summary:** Get Tenant Feature Flags

**Description:** Get all feature flags for a tenant.

**Parameters:**

- `tenant_id` (path) *: 

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

### /v1/admin/config/ratelimit/{tenant_id}

#### GET /v1/admin/config/ratelimit/{tenant_id}

**Summary:** Get Tenant Rate Limits

**Description:** Get all rate limit configurations for a tenant.

**Parameters:**

- `tenant_id` (path) *: 

**Responses:**

- `200`: Successful Response
- `422`: Validation Error
- `400`: Bad Request
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not Found
- `429`: Too Many Requests
- `500`: Internal Server Error

## Data Models

### FeatureFlagUpdate

Feature flag configuration update.

**Properties:**

- `enabled` (boolean) *: Whether the feature is enabled
- `configuration` (object) : Feature configuration

### HTTPValidationError

**Properties:**

- `detail` (array) : 

### RateLimitConfigUpdate

Rate limit configuration update.

**Properties:**

- `requests_per_second` (integer) *: Requests per second
- `burst` (integer) *: Burst capacity
- `enabled` (boolean) : Whether rate limiting is enabled

### ValidationError

**Properties:**

- `loc` (array) *: 
- `msg` (string) *: 
- `type` (string) *: 

### ErrorEnvelope

Standard error response envelope following RFC 7807

**Properties:**

- `error` (string) *: Error type identifier
- `message` (string) : Human-readable error message
- `code` (string) : Application-specific error code
- `field` (string) : Field name that caused the error (for validation errors)
- `value` (any) : Invalid value that caused the error
- `context` (object) : Additional error context
- `request_id` (string) : Request identifier for debugging
- `timestamp` (string) *: Error timestamp

### ValidationErrorDetail

Detailed validation error information

**Properties:**

- `field` (string) *: Field path that failed validation
- `message` (string) *: Validation error message
- `value` (any) : Invalid value
- `code` (string) : Validation error code
- `constraint` (string) : Constraint type that failed

### ValidationErrorResponse

Validation error response

**Properties:**

- `error` (string) *: Error type
- `message` (string) *: Error message
- `field_errors` (array) : List of field validation errors
- `request_id` (string) : Request identifier
- `timestamp` (string) *: Error timestamp

