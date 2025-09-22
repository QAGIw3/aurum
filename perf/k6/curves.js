import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.AURUM_PERF_BASE_URL || 'http://localhost:8000';
const TOKEN = __ENV.AURUM_API_TOKEN;
const ISO = __ENV.AURUM_PERF_ISO || 'ISO-NE';
const MARKET = __ENV.AURUM_PERF_MARKET || 'DAY_AHEAD';
const LOCATION = __ENV.AURUM_PERF_LOCATION || 'HUB';
const ASOF = __ENV.AURUM_PERF_ASOF;

export const options = {
  scenarios: {
    curves_read: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.AURUM_PERF_CURVE_RPS || 5),
      timeUnit: '1s',
      duration: __ENV.AURUM_PERF_DURATION || '1m',
      preAllocatedVUs: 5,
      tags: { endpoint: '/v2/curves' },
      exec: 'curvesScenario',
    },
    curves_export: {
      executor: 'constant-arrival-rate',
      startTime: '10s',
      rate: Number(__ENV.AURUM_PERF_EXPORT_RPS || 1),
      timeUnit: '1s',
      duration: __ENV.AURUM_PERF_DURATION || '1m',
      preAllocatedVUs: 3,
      tags: { endpoint: '/v2/curves/export' },
      exec: 'exportScenario',
    },
  },
  thresholds: {
    'http_req_duration{endpoint:/v2/curves}': ['p(95)<500'],
    'http_req_duration{endpoint:/v2/curves/export}': ['p(95)<2000'],
    'http_req_failed': ['rate<0.05'],
  },
};

function authHeaders() {
  const headers = { 'Accept': 'application/json' };
  if (TOKEN) {
    headers['Authorization'] = `Bearer ${TOKEN}`;
  }
  return headers;
}

export function curvesScenario() {
  const params = {
    headers: authHeaders(),
    tags: { endpoint: '/v2/curves' },
  };

  const query = {
    iso: ISO,
    market: MARKET,
    location: LOCATION,
    limit: __ENV.AURUM_PERF_LIMIT || 200,
  };
  if (ASOF) {
    query.asof = ASOF;
  }

  const res = http.get(`${BASE_URL}/v2/curves`, query, params);
  check(res, {
    'curves status 200': (r) => r.status === 200,
    'curves returns data': (r) => r.json('data.length') > 0,
  });
  sleep(0.1);
}

export function exportScenario() {
  const params = {
    headers: authHeaders(),
    responseType: 'binary',
    tags: { endpoint: '/v2/curves/export' },
  };

  const query = {
    iso: ISO,
    market: MARKET,
    location: LOCATION,
    chunk_size: __ENV.AURUM_PERF_CHUNK || 1500,
  };
  if (ASOF) {
    query.asof = ASOF;
  }

  const res = http.get(`${BASE_URL}/v2/curves/export`, query, params);
  check(res, {
    'export status 200': (r) => r.status === 200,
  });
  sleep(0.5);
}
