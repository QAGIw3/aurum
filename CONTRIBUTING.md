# Contributing to Aurum

Thanks for taking the time to contribute! This guide explains how to set up your environment, run tests, follow code style, and update documentation.

## Development Setup

- Python: 3.11+
- Create a virtualenv: `make install` (installs into `.venv`)
- Enable pre-commit hooks: `pip install pre-commit && pre-commit install`

Common targets:
- Lint/format: `make lint` / `make format`
- Unit tests: `make test` (or `make test-docker` for a containerized run)
- Type checks: `make typecheck` (if configured)

## Running the Stack (Dev)

- Minimal services: `COMPOSE_PROFILES=core docker compose -f compose/docker-compose.dev.yml up -d`
- API only: add `api` to the profile or run `make api-up`
- Scenario worker: add `worker` to the profile (see README.md for details)

## Code Style

- Follow PEP 8 and keep functions small and cohesive.
- Prefer descriptive names; avoid single-letter variables except in tight loops.
- Keep changes minimal and scoped to the task at hand.
- Docstrings: use concise module/class/function docstrings describing purpose, key inputs/outputs, and assumptions. Examples are welcome for public APIs.
- Logging: use structured logging helpers (`aurum.telemetry.context.log_structured`) and include `tenant_id`, `request_id`, and `user_id` when available.

## Testing

- Add or update unit tests near changed code (e.g., `tests/api`, `tests/parsers`).
- Keep tests deterministic and fast. Avoid external network calls.
- For integration tests requiring services, prefer dockerized test targets.

## Documentation

- Update relevant docs under `docs/` when changing behavior, endpoints, or configuration.
- API docs: regenerate OpenAPI after changing routes/models:
  - `make docs-openapi` (updates `docs/api/openapi-spec.*`)
  - `make docs-build` (updates `docs/api/api-docs.md` and `docs/api/openapi.json`)
- Keep `docs/README.md` index links current when adding new docs.

## Commits & PRs

- Write clear, focused commits; reference issues when applicable.
- Summarize motivation, approach, and any trade-offs in the PR description.
- Include before/after notes for user-visible changes (APIs, CLIs, config).

## Security & Tenancy

- Enforce tenant scoping in APIs and storage; never bypass RLS in production code.
- Avoid logging secrets or PII. Use placeholders or redact selectively.
- Follow `docs/security/tenant-rls.md` for RLS guidance and `docs/auth/oidc-forward-auth.md` for auth wiring.

## Questions

If you’re unsure about approach or scope, open a draft PR early or start a discussion. Thanks again for contributing!

