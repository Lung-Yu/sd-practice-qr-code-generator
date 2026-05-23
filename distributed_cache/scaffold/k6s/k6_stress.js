// High-pressure stress test — ramps to 3000 RPS to find the ceiling.
// Use this to compare Python nodes vs Go nodes at the same load level.
//
// Python nodes (2w):  p95 ≈ 217ms, FAIL SLO
// Go nodes (1 proc):  p95 ≈ 117ms, PASS SLO
//
// Run with Prometheus remote write:
// K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
// K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM=false \
// k6 run -o experimental-prometheus-rw k6s/k6_stress.js

import http from "k6/http";
import { check, group } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";

const getDuration    = new Trend("cache_get_duration",    true);
const postDuration   = new Trend("cache_post_duration",   true);
const deleteDuration = new Trend("cache_delete_duration", true);

const errorRate   = new Rate("cache_error_rate");
const cacheMisses = new Counter("cache_miss_count");
const cacheHits   = new Counter("cache_hit_count");

export const options = {
  scenarios: {
    cache_load: {
      executor: "ramping-arrival-rate",
      preAllocatedVUs: 200,
      maxVUs: 1200,
      startRate: 0,
      timeUnit: "1s",
      stages: [
        { duration: "30s",  target: 200  }, // warm up
        { duration: "60s",  target: 500  }, // ramp
        { duration: "60s",  target: 1000 }, // ramp
        { duration: "60s",  target: 2000 }, // stress
        { duration: "60s",  target: 3000 }, // find limit
        { duration: "30s",  target: 0    }, // ramp down
      ],
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<200"],   // p95 < 200ms
    cache_error_rate:  ["rate<0.05"],   // actual errors < 5%
  },
};

function randKey() { return `key-${Math.floor(Math.random() * 200)}`; }
function randTTL() { return Math.floor(30 + Math.random() * 270); }

function doGet() {
  const key = randKey();
  const res = http.get(`${BASE_URL}/cache/${key}`, { tags: { endpoint: "cache_get" } });
  getDuration.add(res.timings.duration);
  const ok = check(res, { "GET status 200 or 404": (r) => r.status === 200 || r.status === 404 });
  if (res.status === 404) cacheMisses.add(1);
  else if (res.status === 200) cacheHits.add(1);
  errorRate.add(!ok);
}

function doPost() {
  const key = randKey();
  const res = http.post(
    `${BASE_URL}/cache/${key}`,
    JSON.stringify({ value: `val-${__VU}-${__ITER}`, ttl: randTTL() }),
    { headers: { "Content-Type": "application/json" }, tags: { endpoint: "cache_post" } }
  );
  postDuration.add(res.timings.duration);
  errorRate.add(!check(res, { "POST status 200": (r) => r.status === 200 }));
}

function doDelete() {
  const key = randKey();
  const res = http.del(`${BASE_URL}/cache/${key}`, null, { tags: { endpoint: "cache_delete" } });
  deleteDuration.add(res.timings.duration);
  errorRate.add(!check(res, { "DELETE status 200 or 404": (r) => r.status === 200 || r.status === 404 }));
}

export default function () {
  const roll = Math.floor(Math.random() * 100);
  if (roll < 70)       group("cache_get",    () => doGet());
  else if (roll < 90)  group("cache_post",   () => doPost());
  else                 group("cache_delete", () => doDelete());
}
