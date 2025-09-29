# ADR-000X: Async Execution and Background Task Standards

## Status
Accepted

## Context
We have inconsistent async patterns across the API (bare `asyncio.create_task`, ad-hoc retry loops, blocking `run_in_executor`, missing timeouts) and multiple background task helpers (Celery stubs, custom loops). This makes behavior unreliable, hard to observe, and difficult to test.

## Decision
- Adopt Python `asyncio` primitives as the default concurrency model (`TaskGroup`, `asyncio.to_thread`, `asyncio.timeout`).
- Standardize background task lifecycle via `BackgroundTaskSupervisor` with explicit startup/shutdown hooks.
- Require explicit timeouts for all external I/O and async operations.
- Provide retry/backoff utilities with jitter and caps; avoid bespoke loops.
- Instrument async operations with metrics, tracing, and structured logs.
- Supply test helpers for deterministic async testing (timeouts, cancellation, retry assertions).
- Document configuration (pool sizes, concurrency caps, timeouts) in `config/async_runtime.json`.

## Consequences
- **Benefits**: consistent patterns, safer cancellation, clearer observability, repeatable tests.
- **Costs**: initial refactor effort, learning curve for new utilities, need to update existing modules.
- **Follow-up**: implement foundation primitives (`executor.py`, `queues.py`, `retry_circuit.py`, `supervisor.py`, `monitoring.py`, `testing.py`), refactor `async_service.py`, migrate call sites, update linting and documentation.
