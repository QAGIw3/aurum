import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');
const apiRequestDuration = new Trend('api_request_duration');
const healthCheckDuration = new Trend('health_check_duration');
const curvesListDuration = new Trend('curves_list_duration');

// Test configuration
export const options = {
  stages: [
    { duration: '2m', target: 10 },   // Ramp up to 10 users over 2 minutes
    { duration: '5m', target: 10 },   // Stay at 10 users for 5 minutes
    { duration: '2m', target: 50 },   // Ramp up to 50 users over 2 minutes
    { duration: '5m', target: 50 },   // Stay at 50 users for 5 minutes
    { duration: '2m', target: 100 },  // Ramp up to 100 users over 2 minutes
    { duration: '10m', target: 100 }, // Stay at 100 users for 10 minutes
    { duration: '2m', target: 0 },    // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests should be below 500ms
    http_req_failed: ['rate<0.01'],   // Error rate should be below 1%
    errors: ['rate<0.01'],            // Custom error rate below 1%
    api_request_duration: ['p(95)<1000'], // API requests below 1s
  },
};

// Test data
const BASE_URL = __ENV.AURUM_API_URL || 'http://localhost:8000';
const TEST_TENANT_ID = __ENV.TEST_TENANT_ID || 'test-tenant';

// Setup function
export function setup() {
  console.log(`Target URL: ${BASE_URL}`);
  console.log(`Test Tenant ID: ${TEST_TENANT_ID}`);

  // Verify API is accessible
  const healthResponse = http.get(`${BASE_URL}/health/ready`);
  if (healthResponse.status !== 200) {
    throw new Error(`Health check failed: ${healthResponse.status}`);
  }

  return { startTime: new Date() };
}

// Main test function
export default function(data) {
  const startTime = new Date();

  // Health check (20% of requests)
  if (Math.random() < 0.2) {
    const healthStart = new Date();
    const healthResponse = http.get(`${BASE_URL}/health/ready`);
    const healthDuration = new Date() - healthStart;

    healthCheckDuration.add(healthDuration);
    apiRequestDuration.add(healthDuration);

    const healthCheck = check(healthResponse, {
      'health check status is 200': (r) => r.status === 200,
      'health check response time < 100ms': (r) => healthDuration < 100,
    });

    errorRate.add(!healthCheck);
    return;
  }

  // List curves (40% of requests)
  if (Math.random() < 0.4) {
    const curvesStart = new Date();
    const curvesResponse = http.get(`${BASE_URL}/v1/curves`, {
      headers: {
        'X-Tenant-ID': TEST_TENANT_ID,
        'Content-Type': 'application/json',
      }
    });
    const curvesDuration = new Date() - curvesStart;

    curvesListDuration.add(curvesDuration);
    apiRequestDuration.add(curvesDuration);

    const curvesCheck = check(curvesResponse, {
      'curves list status is 200 or 401': (r) => r.status === 200 || r.status === 401,
      'curves list response time < 500ms': (r) => curvesDuration < 500,
      'curves list has valid JSON': (r) => {
        try {
          JSON.parse(r.body);
          return true;
        } catch (e) {
          return false;
        }
      },
    });

    errorRate.add(!curvesCheck);
    return;
  }

  // Get curve details (30% of requests)
  if (Math.random() < 0.3) {
    const curveId = 'test-curve-' + Math.floor(Math.random() * 1000);
    const curveStart = new Date();
    const curveResponse = http.get(`${BASE_URL}/v1/curves/${curveId}`, {
      headers: {
        'X-Tenant-ID': TEST_TENANT_ID,
        'Content-Type': 'application/json',
      }
    });
    const curveDuration = new Date() - curveStart;

    apiRequestDuration.add(curveDuration);

    const curveCheck = check(curveResponse, {
      'curve detail status is 200, 404, or 401': (r) => [200, 404, 401].includes(r.status),
      'curve detail response time < 300ms': (r) => curveDuration < 300,
    });

    errorRate.add(!curveCheck);
    return;
  }

  // Create scenario (10% of requests)
  const scenarioData = {
    name: `test-scenario-${Math.floor(Math.random() * 10000)}`,
    description: 'Load test scenario',
    parameters: {
      start_date: '2024-01-01',
      end_date: '2024-12-31',
      model_type: 'test'
    }
  };

  const scenarioStart = new Date();
  const scenarioResponse = http.post(
    `${BASE_URL}/v1/scenarios`,
    JSON.stringify(scenarioData),
    {
      headers: {
        'X-Tenant-ID': TEST_TENANT_ID,
        'Content-Type': 'application/json',
      }
    }
  );
  const scenarioDuration = new Date() - scenarioStart;

  apiRequestDuration.add(scenarioDuration);

  const scenarioCheck = check(scenarioResponse, {
    'scenario create status is 201 or 401': (r) => r.status === 201 || r.status === 401,
    'scenario create response time < 1000ms': (r) => scenarioDuration < 1000,
  });

  errorRate.add(!scenarioCheck);

  // Sleep to simulate realistic user behavior
  sleep(Math.random() * 2); // 0-2 seconds between requests
}

// Teardown function
export function teardown(data) {
  const endTime = new Date();
  const duration = endTime - data.startTime;

  console.log(`Test completed in ${duration}ms`);
  console.log(`Total virtual users: ${__VU}`);
  console.log(`Iteration count: ${__ITER}`);
}
