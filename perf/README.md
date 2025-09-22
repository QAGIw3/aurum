# Aurum Performance Test Kit

This directory contains lightweight performance harnesses for the highest traffic
API paths. Two flavours are supported:

- `locustfile.py` – Python Locust swarm to drive concurrent requests against
  `/v2/curves`, `/v2/curves/export`, and the Trino admin endpoints. Useful when a
  stateful load profile is needed.
- `k6/curves.js` – Declarative k6 script with latency/error thresholds suitable
  for CI smoke runs.

## Usage

### Locust

```bash
AURUM_PERF_BASE_URL=https://api.dev.aurum.energy \
AURUM_API_TOKEN=... \
locust -f perf/locustfile.py
```

### k6 (CI friendly)

```bash
AURUM_PERF_BASE_URL=https://api.dev.aurum.energy \
AURUM_PERF_DURATION=2m \
k6 run perf/k6/curves.js
```

### Environment variables

| Variable | Description | Default |
| --- | --- | --- |
| `AURUM_PERF_BASE_URL` | API base URL | `http://localhost:8000` |
| `AURUM_API_TOKEN` | Bearer token for authenticated routes | _blank_ |
| `AURUM_PERF_ISO` | ISO filter for curve requests | `ISO-NE` |
| `AURUM_PERF_MARKET` | Market filter | `DAY_AHEAD` |
| `AURUM_PERF_LOCATION` | Location filter | `HUB` |
| `AURUM_PERF_ASOF` | Optional as-of date | _blank_ |
| `AURUM_PERF_DURATION` | k6 scenario duration | `1m` |
| `AURUM_PERF_CURVE_RPS` | RPS for `/v2/curves` in k6 | `5` |
| `AURUM_PERF_EXPORT_RPS` | RPS for `/v2/curves/export` in k6 | `1` |

Thresholds defined in the k6 script will fail the run when p95 latency exceeds
500 ms for standard curve queries or 2 s for streaming exports, providing quick
feedback in CI.
