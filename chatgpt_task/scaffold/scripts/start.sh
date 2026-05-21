#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCAFFOLD_DIR="$(dirname "$SCRIPT_DIR")"

case "${1:-start}" in
  start)
    cd "$SCAFFOLD_DIR"
    podman-compose build mcp-server
    podman-compose up -d postgres
    echo "Postgres running, mcp-server image built."
    echo ""
    echo "Run the MCP server interactively (stdio test):"
    echo "  ./scripts/start.sh run"
    echo ""
    echo "MCP client config (Claude Desktop / Claude Code):"
    echo "  command: podman-compose"
    echo "  args:    [\"-f\", \"$SCAFFOLD_DIR/docker-compose.yml\", \"run\", \"--rm\", \"mcp-server\"]"
    echo "  cwd:     $SCAFFOLD_DIR"
    ;;
  stop)
    cd "$SCAFFOLD_DIR"
    podman-compose down
    ;;
  run)
    # Runs the MCP server container attached to stdin/stdout — same as an MCP client would
    cd "$SCAFFOLD_DIR"
    podman-compose run --rm mcp-server
    ;;
  *)
    echo "Usage: $0 [start|stop|run]"
    ;;
esac
