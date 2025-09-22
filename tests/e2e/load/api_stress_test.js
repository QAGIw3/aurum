import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('stress_test_errors');
const responseTime = new Trend('stress_test_response_time');
const throughput = new Trend('stress_test_throughput');

// Test configuration for stress testing
export const options = {
  vus: 100,           // 100 virtual users
  duration: '5m',     // 5 minutes
  thresholds: {
    http_req_duration: ['p(99)<2000'], // 99% of requests should be below 2s
    http_req_failed: ['rate<0.05'],    // Allow up to 5% error rate under stress
    stress_test_errors: ['rate<0.05'], // Custom error rate below 5%
  },
  // Disable connection reuse to simulate real-world stress
  noConnectionReuse: true,
  // Randomize user agent to simulate different clients
  userAgent: `Aurum-Stress-Test-${Math.random()}`,
};

// Test data
const BASE_URL = __ENV.AURUM_API_URL || 'http://localhost:8000';
const TEST_TENANT_ID = __ENV.TEST_TENANT_ID || 'stress-test-tenant';

// Request patterns for stress testing
const REQUEST_PATTERNS = [
  { path: '/health/ready', weight: 30, method: 'GET' },           // Health checks
  { path: '/v1/curves', weight: 25, method: 'GET' },             // List operations
  { path: '/v1/curves/test-curve', weight: 20, method: 'GET' },  // Detail operations
  { path: '/v1/scenarios', weight: 15, method: 'POST' },         // Create operations
  { path: '/metrics', weight: 10, method: 'GET' },               // Monitoring
];

// Weighted random selection
function getRandomRequestPattern() {
  const totalWeight = REQUEST_PATTERNS.reduce((sum, pattern) => sum + pattern.weight, 0);
  let random = Math.random() * totalWeight;

  for (const pattern of REQUEST_PATTERNS) {
    random -= pattern.weight;
    if (random <= 0) {
      return pattern;
    }
  }

  return REQUEST_PATTERNS[0]; // Fallback
}

// Main test function
export default function() {
  const pattern = getRandomRequestPattern();
  const startTime = new Date();

  let response;
  let duration;

  try {
    // Build request
    const url = `${BASE_URL}${pattern.path}`;
    const params = {
      headers: {
        'X-Tenant-ID': TEST_TENANT_ID,
        'Content-Type': 'application/json',
        'User-Agent': `Aurum-Stress-Test-${__VU}-${__ITER}`,
      },
      timeout: '30s', // 30 second timeout
    };

    // Add request body for POST requests
    let body = undefined;
    if (pattern.method === 'POST' && pattern.path.includes('/scenarios')) {
      body = JSON.stringify({
        name: `stress-test-scenario-${__VU}-${__ITER}`,
        description: 'Stress test scenario',
        parameters: {
          start_date: '2024-01-01',
          end_date: '2024-12-31',
          model_type: 'stress_test'
        }
      });
    }

    // Make request
    response = http.request(pattern.method, url, body, params);
    duration = new Date() - startTime;

    // Record metrics
    responseTime.add(duration);
    throughput.add(1);

    // Check response
    const success = check(response, {
      'status is not 5xx': (r) => r.status < 500,
      'response time < 5s': (r) => duration < 5000,
      'response has valid JSON for non-health endpoints': (r) => {
        if (pattern.path === '/health/ready') return true;
        if (pattern.path === '/metrics') return true;
        try {
          JSON.parse(r.body);
          return true;
        } catch (e) {
          return false;
        }
      },
    });

    errorRate.add(!success);

  } catch (error) {
    duration = new Date() - startTime;
    responseTime.add(duration);
    errorRate.add(true);

    console.log(`Request failed: ${error.message}`);
  }

  // Adaptive sleep based on error rate
  const currentErrorRate = errorRate.value;
  if (currentErrorRate > 0.1) {
    // High error rate - slow down
    sleep(Math.random() * 5);
  } else if (currentErrorRate > 0.05) {
    // Moderate error rate - moderate delay
    sleep(Math.random() * 2);
  } else {
    // Low error rate - minimal delay
    sleep(Math.random() * 0.5);
  }
}

// Handle summary generation
export function handleSummary(data) {
  const summary = {
    'stdout': textSummary(data, { indent: ' ', enableColors: false }),
    'stress-test-summary.json': JSON.stringify({
      metrics: data.metrics,
      timestamp: new Date().toISOString(),
      test: 'stress_test',
      duration: data.metrics.iteration_duration.values.avg,
      throughput: data.metrics.http_reqs.values.rate,
      error_rate: data.metrics.http_req_failed.values.rate,
      p95_response_time: data.metrics.http_req_duration.values['p(95)'],
      p99_response_time: data.metrics.http_req_duration.values['p(99)'],
    }, null, 2),
  };

  // Add additional analysis
  const analysis = analyzeResults(data);
  summary['stress-test-analysis.json'] = JSON.stringify(analysis, null, 2);

  return summary;
}

function analyzeResults(data) {
  const analysis = {
    summary: {
      total_requests: data.metrics.http_reqs.values.count,
      total_errors: data.metrics.http_req_failed.values.count,
      error_rate: data.metrics.http_req_failed.values.rate,
      avg_response_time: data.metrics.http_req_duration.values.avg,
      p95_response_time: data.metrics.http_req_duration.values['p(95)'],
      p99_response_time: data.metrics.http_req_duration.values['p(99)'],
      throughput: data.metrics.http_reqs.values.rate,
    },
    recommendations: [],
    issues: [],
  };

  // Analyze performance
  if (data.metrics.http_req_duration.values['p(99)'] > 2000) {
    analysis.issues.push({
      severity: 'high',
      type: 'performance',
      description: '99th percentile response time exceeds 2 seconds',
      recommendation: 'Consider scaling up resources or optimizing slow endpoints'
    });
  }

  if (data.metrics.http_req_failed.values.rate > 0.05) {
    analysis.issues.push({
      severity: 'high',
      type: 'reliability',
      description: 'Error rate exceeds 5%',
      recommendation: 'Investigate failing endpoints and implement better error handling'
    });
  }

  if (data.metrics.http_req_duration.values['p(95)'] < 500) {
    analysis.recommendations.push({
      type: 'optimization',
      description: 'System is performing well under stress',
      action: 'Consider reducing resource allocation to optimize costs'
    });
  }

  // Analyze throughput
  const throughput = data.metrics.http_reqs.values.rate;
  if (throughput < 50) {
    analysis.issues.push({
      severity: 'medium',
      type: 'throughput',
      description: 'Throughput is below expected levels',
      recommendation: 'Check for bottlenecks in request processing pipeline'
    });
  } else {
    analysis.recommendations.push({
      type: 'capacity',
      description: `System handled ${throughput.toFixed(1)} requests/second`,
      action: 'Monitor this capacity for production planning'
    });
  }

  return analysis;
}

// Utility function for text summary
function textSummary(data, options) {
  const {
    indent = ' ',
    enableColors = true
  } = options || {};

  let output = 'Stress Test Results\n';
  output += `${indent}Duration: ${data.metrics.iteration_duration.values.avg.toFixed(2)}s\n`;
  output += `${indent}Total Requests: ${data.metrics.http_reqs.values.count}\n`;
  output += `${indent}Throughput: ${data.metrics.http_reqs.values.rate.toFixed(2)} req/s\n`;
  output += `${indent}Error Rate: ${(data.metrics.http_req_failed.values.rate * 100).toFixed(2)}%\n`;
  output += `${indent}P95 Response Time: ${data.metrics.http_req_duration.values['p(95)'].toFixed(2)}ms\n`;
  output += `${indent}P99 Response Time: ${data.metrics.http_req_duration.values['p(99)'].toFixed(2)}ms\n`;

  return output;
}
