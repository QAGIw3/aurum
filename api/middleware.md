## Middleware architecture

This project uses a composable middleware system managed by `aurum.api.middleware.manager.MiddlewareManager`.

Goals:
- Clear ordering via priorities or explicit order list
- Configurable activation using settings
- Testable composition with a deterministic `describe_order()`

### Where middleware is applied

`aurum.api.app.ApplicationFactory` constructs the FastAPI app and delegates to `MiddlewareManager.add_defaults()` and `MiddlewareManager.apply()` to assemble the middleware chain. The manager handles:
- Logging context, RFC7807 errors, CORS, GZip
- Admin guard, authentication, tenant context
- Concurrency and rate limiting wrappers
- Access logging, vary headers, standard response headers

### Configuration

Set these fields under `settings.api` (e.g., env or config file) to control activation and ordering:
- `middleware_disabled`: list of names to disable (e.g., `["cors", "gzip"]`)
- `middleware_enabled`: list of names to force-enable
- `middleware_order`: list of names from outermost to innermost. Unspecified items keep their relative order after these.

Built-in names include: `logging_context`, `rfc7807`, `cors`, `gzip`, `admin_guard`, `auth`, `tenant_context`, `ensure_gzip_wildcard`, `access_log`, `concurrency`, `rate_limit`, `vary_headers`, `response_headers`.

### Testing

Use `aurum.api.middleware.testing.build_test_app()` to construct an app with custom enable/disable sets. For unit tests of ordering, call `MiddlewareManager.describe_order()`.


