#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCAFFOLD_DIR="$(dirname "$SCRIPT_DIR")"

case "${1:-start}" in
  start)
    cd "$SCAFFOLD_DIR"
    podman-compose build
    podman-compose up -d postgres api mcp-server
    echo ""
    echo "Services running:"
    echo "  REST API  → http://localhost:8000"
    echo "  MCP (SSE) → http://localhost:8001/sse"
    echo "  Postgres  → localhost:5432"
    echo ""
    echo "MCP inspector:"
    echo "  npx @modelcontextprotocol/inspector http://localhost:8001/sse"
    echo ""
    echo "Stdio MCP (for PROMPT.md testing):"
    echo "  podman-compose run --rm -e DATABASE_URL=postgresql://taskuser:taskpass@postgres:5432/taskdb mcp-server python -m app.mcp_server"
    ;;
  stop)
    cd "$SCAFFOLD_DIR"
    podman-compose down
    ;;
  *)
    echo "Usage: $0 [start|stop]"
    ;;
esac
