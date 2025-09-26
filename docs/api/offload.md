# Async Offload Runbook

Async offloading lets the Aurum API keep HTTP requests fast while delegating
long-running work to background workers. The API enqueues a job, returns `202
Accepted` with a task identifier, and clients poll a lightweight status
endpoint until the job completes.

This document covers configuration, worker operations, and code patterns for
using the offload helpers under `aurum.api.async_exec`.

## Architecture Overview

```
┌──────────────┐      enqueue job       ┌──────────────┐
│ FastAPI route│ ─────────────────────▶ │ Celery broker│
└──────┬───────┘                       └──────┬───────┘
       │                                     │
   poll status                               │ delivers task
       │                                     ▼
┌──────▼───────┐      execute job       ┌──────────────┐
│ Status route │ ◀────────────────────  │ Celery worker│
└──────────────┘                         └──────────────┘
```

During local development or in environments without Celery the module falls
back to an in-memory stub that preserves the public API (`run_job_async`,
`fetch_job_result`, `register_job`).

## Environment Configuration

| Variable | Description | Default |
| --- | --- | --- |
| `AURUM_API_OFFLOAD_ENABLED` | Toggle async offloading. When `false` the stub implementation is used even if Celery is installed. | `false` |
| `AURUM_API_OFFLOAD_USE_STUB` | Force the stub implementation regardless of other settings. Handy for local development. | `true` in dev environments, otherwise `false`. |
| `AURUM_API_OFFLOAD_CELERY_BROKER_URL` | Celery broker connection string. | `redis://localhost:6379/0` (falls back to `AURUM_CELERY_BROKER_URL` if present). |
| `AURUM_API_OFFLOAD_CELERY_RESULT_BACKEND` | Celery result backend. | `redis://localhost:6379/1` (falls back to `AURUM_CELERY_RESULT_BACKEND`). |
| `AURUM_API_OFFLOAD_DEFAULT_QUEUE` | Default Celery queue for offloaded work. | `default` (falls back to `AURUM_CELERY_TASK_DEFAULT_QUEUE`). |

> **Tip:** When using Helm or Kubernetes, set these values in the API
> ConfigMap so workers and the API share a consistent broker configuration.

The values above are exposed on `settings.async_offload` for runtime code:

```python
settings = request.app.state.settings
cfg = settings.async_offload
if cfg.enabled and not cfg.use_stub:
    ...  # Celery is active
```

## Worker Runbook

1. Configure the environment variables listed above (broker, backend, queue).
2. Ensure the job modules are importable by the worker (e.g. the API package is
   on `PYTHONPATH`).
3. Start a worker:
   ```bash
   celery      -A aurum.api.async_exec.celery_app      worker      --loglevel=info
   ```
4. (Optional) Run an additional beat process if your jobs schedule periodic
   tasks.

When the worker starts it will import the API package. Register jobs during API
startup so they are available to both the API and the worker process.

## Registering Jobs

```python
from aurum.api.async_exec import register_job, run_job_async

async def startup_event():
    register_job("heavy_compute", heavy_compute)


def heavy_compute(payload: dict[str, object]) -> dict[str, object]:
    # Perform CPU intensive work
    return {"ok": True, "input": payload}

@router.post("/v1/heavy", status_code=202)
async def enqueue_heavy(request: Request):
    task_id = run_job_async("heavy_compute", {"tenant": request.headers["X-Aurum-Tenant"]})
    return {"task_id": task_id}
```

## Polling Status

Expose a lightweight route that wraps `fetch_job_result`:

```python
from aurum.api.async_exec import fetch_job_result

@router.get("/v1/heavy/{task_id}")
async def job_status(task_id: str):
    status = fetch_job_result(task_id)
    # Optionally translate Celery states into API friendly enums
    return status
```

The stub implementation returns deterministic responses (`state=PENDING`) and
synthetic task ids, which keeps tests fast and deterministic while exercising
identical control flow.

## Local Development

With the default configuration (`AURUM_API_OFFLOAD_ENABLED=false`) the stub is
active. It records registered jobs and returns fake task ids without hitting a
broker. Enable the real pipeline by setting the following environment
variables:

```bash
export AURUM_API_OFFLOAD_ENABLED=true
export AURUM_API_OFFLOAD_USE_STUB=false
export AURUM_API_OFFLOAD_CELERY_BROKER_URL=redis://localhost:6379/0
export AURUM_API_OFFLOAD_CELERY_RESULT_BACKEND=redis://localhost:6379/1
```

Developers can also toggle the stub at runtime by setting
`AURUM_API_OFFLOAD_USE_STUB=true`, which leaves the rest of the configuration
untouched.

## Troubleshooting

- **ImportError: `celery`** – ensure Celery is installed in the worker and API
  environments. The module will fall back to the stub and log a warning when
  Celery cannot be imported.
- **Task stuck in `PENDING`** – verify the worker process is running and
  connected to the same broker specified by the API.
- **Repeated 202 responses** – ensure the status route uses `fetch_job_result`
  and that the worker stores results (Redis backend configured correctly).
