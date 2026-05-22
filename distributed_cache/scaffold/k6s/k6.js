// Run with Prometheus remote write:
// K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
// K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM=false \
// k6 run -o experimental-prometheus-rw k6s/k6.js
//
// Improve cycle:
// 1. Run k6 → watch Grafana (http://localhost:3000) for hot nodes, eviction storms
// 2. Tune CAPACITY, VIRTUAL_NODES, HASH_STRATEGY in docker-compose.env
// 3. Restart: ./scripts/start.sh rebuild
// 4. Re-run k6 and compare hit rate and latency

import http from "k6/http";
import { check, group } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";

// Per-operation latency trends for granular Grafana panels.
const getDuration    = new Trend("cache_get_duration",    true);
const postDuration   = new Trend("cache_post_duration",   true);
const deleteDuration = new Trend("cache_delete_duration", true);

// 404 on GET/DELETE is a valid cache-miss, not an error — track separately.
const errorRate   = new Rate("cache_error_rate");
const cacheMisses = new Counter("cache_miss_count");
const cacheHits   = new Counter("cache_hit_count");

export const options = {
  scenarios: {
    cache_load: {
      executor: "ramping-arrival-rate",
      // Little's Law: 1000 RPS × ~20ms avg ≈ 20 VUs; 1200 is generous headroom.
      preAllocatedVUs: 200,
      maxVUs: 1200,
      startRate: 0,
      timeUnit: "1s",
      stages: [
        { duration: "30s",  target: 50   }, // warm up
        { duration: "60s",  target: 200  }, // ramp
        { duration: "60s",  target: 500  }, // stress
        { duration: "60s",  target: 1000 }, // find limit
        { duration: "30s",  target: 0    }, // ramp down
      ],
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<200"],   // p95 < 200ms
    cache_error_rate:  ["rate<0.05"],   // actual errors (not 404 cache misses) < 5%
  },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function randKey() {
  return `key-${Math.floor(Math.random() * 200)}`;
}

function randTTL() {
  // TTL between 30 and 300 seconds
  return Math.floor(30 + Math.random() * 270);
}

// ---------------------------------------------------------------------------
// Operation functions
// ---------------------------------------------------------------------------

function doGet() {
  const key = randKey();
  const res = http.get(
    `${BASE_URL}/cache/${key}`,
    { tags: { endpoint: "cache_get" } }
  );
  getDuration.add(res.timings.duration);

  const ok = check(res, {
    "GET /cache/{key} status 200 or 404": (r) =>
      r.status === 200 || r.status === 404,
  });

  if (res.status === 404) {
    cacheMisses.add(1);
  } else if (res.status === 200) {
    cacheHits.add(1);
  }

  // Only count as error if status is not 200 or 404
  errorRate.add(!ok);
}

function doPost() {
  const key = randKey();
  const ttl = randTTL();
  const res = http.post(
    `${BASE_URL}/cache/${key}`,
    JSON.stringify({ value: `val-${__VU}-${__ITER}`, ttl }),
    {
      headers: { "Content-Type": "application/json" },
      tags: { endpoint: "cache_post" },
    }
  );
  postDuration.add(res.timings.duration);

  const ok = check(res, {
    "POST /cache/{key} status 200": (r) => r.status === 200,
  });
  errorRate.add(!ok);
}

function doDelete() {
  const key = randKey();
  const res = http.del(
    `${BASE_URL}/cache/${key}`,
    null,
    { tags: { endpoint: "cache_delete" } }
  );
  deleteDuration.add(res.timings.duration);

  const ok = check(res, {
    "DELETE /cache/{key} status 200 or 404": (r) =>
      r.status === 200 || r.status === 404,
  });
  errorRate.add(!ok);
}

// ---------------------------------------------------------------------------
// Default function — weighted traffic mix (70% GET, 20% POST, 10% DELETE)
// ---------------------------------------------------------------------------

export default function () {
  const roll = Math.floor(Math.random() * 100);

  if (roll < 70) {
    group("cache_get",    () => doGet());
  } else if (roll < 90) {
    group("cache_post",   () => doPost());
  } else {
    group("cache_delete", () => doDelete());
  }
}
