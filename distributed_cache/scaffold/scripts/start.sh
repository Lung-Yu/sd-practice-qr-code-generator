#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCAFFOLD_DIR="$(dirname "$SCRIPT_DIR")"

case "${1:-start}" in
  start)
    podman network create sd_monitoring 2>/dev/null || true
    cd "$SCAFFOLD_DIR"
    podman-compose up -d
    echo ""
    echo "Services running:"
    echo "  Router:  http://localhost:8000"
    echo "  Node 1:  http://localhost:8001"
    echo "  Node 2:  http://localhost:8002"
    echo "  Node 3:  http://localhost:8003"
    echo "  Metrics: http://localhost:8000/metrics"
    echo ""
    echo "Test:"
    echo "  curl -X POST http://localhost:8000/cache/hello -H 'Content-Type: application/json' -d '{\"value\":\"world\",\"ttl\":60}'"
    echo "  curl http://localhost:8000/cache/hello"
    echo "  curl http://localhost:8000/ring/hello"
    echo "  curl http://localhost:8000/stats"
    ;;
  stop)
    cd "$SCAFFOLD_DIR"
    podman-compose down
    ;;
  rebuild)
    cd "$SCAFFOLD_DIR"
    podman-compose down
    podman-compose up -d --build
    ;;
  *)
    echo "Usage: $0 [start|stop|rebuild]"
    exit 1
    ;;
esac
