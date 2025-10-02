## Testing

- Unit tests: `pytest -m "unit"`
- Integration tests: `pytest -m "integration"`
- Dead fixtures report: `make dead-fixtures`
- Coverage: `pytest -m "unit" --cov=src/aurum --cov-report=xml`

CI Full enforces lint, types, security, coverage >=85% and uploads coverage.xml.

## Testing strategy

Test layers:
- Unit (`@pytest.mark.unit`): isolated logic, fakes/mocks for IO; target <30s suite.
- Integration (`@pytest.mark.integration`): service boundaries, docker-compose harness.
- E2E (`docker-compose.e2e.yml`): full stack, slowest; run on demand.
- Contract (`schemathesis`): OpenAPI conformance.
- Perf (`k6`): latency budgets.

Fast loop commands:
- `make unit-fast`
- `make unit-watch`
- `make unit-changed`

Markers & selection:
```bash
pytest -m "unit and not slow and not integration"
pytest -m integration -v
```

Parallelism:
- `-n auto` via xdist is enabled in fast targets; avoid shared global state.

Coverage policy:
- Fast lane: no coverage (speed). CI full: `--cov-fail-under=85`.

Writing fast tests:
- Use fakes (e.g., `fakeredis`), avoid network/DB.
- Keep assertions focused; prefer pure functions.
- Prefer dependency injection to enable mocking.

