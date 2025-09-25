# Fast Local API Tests

For a quick, dependency-light test loop on API middleware and HTTP behaviors (gzip, headers, routing skeletons), enable light initialization to avoid heavy imports and database connections.

Environment variable:

- `AURUM_API_LIGHT_INIT=1` — loads minimal fallback routers for curves/metadata and skips heavy app wiring. Useful for tests that only need routing/middleware behavior.

Examples:

- Run a targeted module without coverage gating:

```
PYTHONPATH=src AURUM_API_LIGHT_INIT=1 python3 -m pytest --no-cov -q tests/api/test_gzip_and_content_negotiation.py
```

- Run the broader API test subset (may still skip tests requiring full wiring):

```
PYTHONPATH=src AURUM_API_LIGHT_INIT=1 python3 -m pytest --no-cov -q tests/api
```

Notes & limitations:

- Light init installs minimal fallback endpoints and bypasses DB/Trino/Redis connections; it’s intended for middleware/headers, router registry, and basic contract checks.
- For full integration behavior (e.g., golden query cache, scenario models, OpenAPI contract generation), run without `AURUM_API_LIGHT_INIT` and ensure backing services are available, or use the docker-compose stack documented in README.
- Coverage thresholds in CI may still apply when not using `--no-cov`.

