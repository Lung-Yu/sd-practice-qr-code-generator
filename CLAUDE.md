# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

A collection of independent system design exercises. Each exercise is a self-contained directory with its own infrastructure, implementation, and load tests. Exercises are built with Python + FastAPI and run via Podman + podman-compose.

## Key Commands

### Shared Monitoring (start this first)
```bash
# From repo root
./scripts/monitoring.sh start    # Prometheus :9090 + Grafana :3000
./scripts/monitoring.sh stop
```

### notification_system
```bash
cd notification_system
./scripts/start.sh start         # start service (auto-creates sd_monitoring network)
./scripts/start.sh rebuild       # rebuild image + start
./scripts/start.sh stop

# Load test (with live Grafana output)
K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM=false \
k6 run -o experimental-prometheus-rw k6s/k6.js

# Load test in 4-worker high-throughput mode (FAILURE_RATE=0)
podman-compose -f docker-compose.yml -f k6s/docker-compose.loadtest.yml up -d --build
```

### qr_code_generator
```bash
cd qr_code_generator
./scripts/start.sh               # full stack (postgres, redis, nginx, varnish, 4 app instances)
./scripts/k6s.sh                 # k6 load test → Prometheus remote write
```

### chatgpt_task
```bash
cd chatgpt_task/scaffold

# Build images + start all three services (postgres, api, mcp-server)
./scripts/start.sh start

# Stop all services
./scripts/start.sh stop

# REST API (FastAPI, :8000)
curl http://localhost:8000/tasks
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"description":"...", "scheduled_at":"2025-01-01T00:00:00"}'
curl http://localhost:8000/tasks/1
curl -X DELETE http://localhost:8000/tasks/1

# MCP inspector via SSE (opens browser at http://localhost:5173)
npx @modelcontextprotocol/inspector http://localhost:8001/sse

# Stdio MCP for PROMPT.md testing (hangs on stdin — correct)
podman-compose run --rm \
  -e DATABASE_URL=postgresql://taskuser:taskpass@postgres:5432/taskdb \
  mcp-server python -m app.mcp_server
```

All credentials and config live in `docker-compose.env` — do not hardcode them elsewhere.

### Adding a new exercise
```
/new-exercise <topic_name>
```
This slash command scaffolds `PROMPT.md`, `README.md`, updates the root README table, and commits.

## Exercise Tracks

Every exercise supports two tracks:

- **Challenge Track** — read `PROMPT.md`, answer design questions, build from scratch.
- **Guided Track** — go to `scaffold/`, fill in `TODO`-marked functions; structure and boilerplate are pre-written.

## Architecture

### Project-level Shared Monitoring

All exercises share a single Prometheus + Grafana instance via the `sd_monitoring` Docker network:

```
docker-compose.monitoring.yml     ← creates sd_monitoring network + Prometheus + Grafana
monitoring/
  prometheus.yml                  ← scrapes qr_code app1-4 via sd_monitoring; accepts k6 remote write
  grafana/dashboards/             ← one JSON per exercise (qr-code-gen, k6-notification)
```

Exercise docker-composes reference `sd_monitoring` as an external network. Each `start.sh` auto-creates the network (`podman network create sd_monitoring`) if monitoring isn't running yet, so exercises can start independently.

- **notification_system** pushes k6 metrics via Prometheus remote write (`--out experimental-prometheus-rw`)
- **qr_code_generator** app instances expose `/metrics` scraped by Prometheus; app1–4 join `sd_monitoring` so Prometheus can reach them by service name

### notification_system

Async delivery pipeline — `POST /send` returns 202 immediately; a background worker delivers:

```
routes.py → idempotency check → store.py (save PENDING) → queue.py (XADD to Redis Stream)
                                                                     ↓
worker.py (XREADGROUP) → store.py (HGETALL batch) → delivery.py → channels/registry.py
                                                                     ↓
                                                   EmailChannel / SMSChannel / PushChannel (stdout)
```

**Fallback (no REDIS_URL):** `routes.py` calls `delivery.py` synchronously inline — useful for local dev without Redis.

- **Idempotency key**: `sha256(user_id|topic|message)` — duplicate POST returns existing record without re-delivering
- **State machine**: `PENDING → SENT | FAILED`; transitions happen in `delivery.py` inside the worker
- **Failure simulation**: `FAILURE_RATE` env var (default 0.20); `MAX_RETRIES` (default 3)
- **Store polymorphism**: `store._make_store()` returns `NotificationStore` (in-memory dict, thread-safe) when `REDIS_URL` is unset, or `RedisNotificationStore` when set. Both implement the same async interface so routes and worker are store-agnostic.
  - Redis key layout: `notification:{id}` HASH, `idempotency:{sha256}` STRING (TTL 24 h), `user:{uid}:notifications` ZSET
- **Two Redis instances** (Tier 7): `REDIS_URL` — notification state (HASH/ZSET/rate-limit); `DELIVERY_REDIS_URL` — Streams + DLQ. Falls back to `REDIS_URL` when `DELIVERY_REDIS_URL` is unset.
- **Priority streams**: `notifications:critical` drained first (non-blocking), then `notifications:delivery` (blocking). Set `priority="critical"` in the POST body.
- **Circuit breaker**: one `CircuitBreaker` instance per channel in `circuit_breaker.py`; CLOSED → OPEN after 5 consecutive failures, HALF_OPEN after 30 s.
- **PEL recovery** (Tier 10A): each worker periodically runs `XAUTOCLAIM` to reclaim messages from dead consumers (idle > `PEL_CLAIM_TIMEOUT_MS`, default 60 s).
- **Fanout**: `POST /fanout` broadcasts one message to N users in 3 Redis round-trips (pipeline dedup check → pipeline save → pipeline enqueue). Max 10 000 users per call.
- **Admin endpoints**: `GET /admin/health/channels` (circuit breaker state), `GET /admin/dlq` (DLQ depth), `POST /admin/dlq/retry` (re-enqueue DLQ entries).
- **Adding a channel**: one line in `channels/registry.py._REGISTRY`; implement `BaseChannel.send()`

**Horizontal scaling** (Tier 9D):
```bash
# 4 API replicas + 4 workers, all traffic via nginx :8080
podman-compose -f docker-compose.yml -f docker-compose.scaled.yml \
  up -d --scale notification-api=4 --scale delivery-worker=4
```

### qr_code_generator

Multi-tier production-like stack:

```
nginx-global (:8100) → nginx-site1/site2 → app1–4 (FastAPI)
                                                  ↓
varnish (:8200) → nginx-origin → app1–4     postgres (primary + replica via pgbouncer)
                                                  ↓
                                             redis (cache)
```

App code is in `scaffold/app/`. Core logic lives in `routes.py` (redirect flow), `token_gen.py`, `url_validator.py`, `cache.py`, and `metrics.py` (Prometheus instrumentation).

## Exercise Template

Every exercise follows this pattern:
- `PROMPT.md` — design questions (answered in-place) + curl verification tests
- `docker-compose.yml` — full infrastructure; joins `sd_monitoring` external network
- `scripts/start.sh` — start/stop helpers
- `k6s/` or `k6/` — load test scripts targeting 5000 RPS, p95 < 500ms

### chatgpt_task

Three persistent containers (all visible in `podman ps`):

```
REST client / curl
    ↓ HTTP :8000
api (FastAPI) ── scheduler (watcher + worker threads)
    ↓                          ↓
    └──────────────────────────┤
                               ↓
                          PostgreSQL :5432
                               ↑
mcp_sse.py (SSE MCP :8001) ───┘
    ↑
MCP client (Claude Desktop / Claude Code / inspector)

mcp_server.py (stdio — for PROMPT.md / inspector stdio testing)
```

**Services** (`scaffold/docker-compose.yml`):
| Service | Port | Role |
|---------|------|------|
| `postgres` | 5432 | Database |
| `api` | 8000 | FastAPI REST + scheduler (watcher/worker threads) |
| `mcp-server` | 8001 | SSE MCP server (reuses `TOOL_REGISTRY` from `mcp_server.py`) |

**Infrastructure** (`scaffold/`):
- `docker-compose.env` — single source of truth for all credentials/config
- `Dockerfile` — `python:3.11-slim`; one image, `api` and `mcp-server` use different `command`
- `app/api.py` — FastAPI with `POST/GET/DELETE /tasks`; starts scheduler on lifespan
- `app/mcp_sse.py` — SSE transport wrapper; imports `server` from `mcp_server.py` so it inherits all registered handlers
- `app/mcp_server.py` — stdio transport; used directly for PROMPT.md inspector testing

**MCP client config via SSE** (Claude Desktop / Claude Code):
```json
{
  "url": "http://localhost:8001/sse"
}
```

- **Scheduler ownership**: `api` service runs watcher + worker threads; `mcp-server` container connects to same DB but does not run the scheduler.
- **Time bucket**: `scheduled_at.strftime("%Y%m%d%H")` stored as partition key; watcher queries `time_bucket <= current_bucket` to catch overdue jobs from past buckets.
- **Registry pattern**: `TOOL_REGISTRY` maps `"task.create"` → `handle_create_task` etc.; `route_tool_call()` is the single dispatch point. Adding a tool is one registry entry + one handler.
- **Async bridge**: `call_tool()` is async (MCP requirement) but handlers are sync; bridged via `asyncio.to_thread()`.
- **Scaffold TODOs** (Guided Track): `get_time_bucket()`, `find_due_jobs()`, `TOOL_REGISTRY` dict, `route_tool_call()` body.

## Podman Notes

The repo uses `podman-compose` (not `docker-compose`). The `qr_code_generator/scripts/start.sh` bridges to `docker-compose` CLI via `DOCKER_HOST` pointing at the Podman socket — this is already handled in that script. All other scripts use `podman-compose` directly.
