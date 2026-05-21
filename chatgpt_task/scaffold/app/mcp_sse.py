"""SSE MCP server for docker-compose deployment (persistent container on :8001).

Reuses the same Server instance and TOOL_REGISTRY from mcp_server.py.
Does NOT start the scheduler — the `api` service owns watcher + worker threads.

MCP inspector: npx @modelcontextprotocol/inspector http://localhost:8001/sse

Claude Desktop / Claude Code config:
  {
    "url": "http://localhost:8001/sse"
  }
"""

import uvicorn
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount, Route

from .database import Base, engine
from .mcp_server import server  # reuses registered list_tools + call_tool handlers

sse_transport = SseServerTransport("/messages/")


async def handle_sse(request: Request) -> None:
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await server.run(streams[0], streams[1], server.create_initialization_options())


starlette_app = Starlette(
    routes=[
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse_transport.handle_post_message),
    ]
)


def main() -> None:
    Base.metadata.create_all(bind=engine)
    uvicorn.run(starlette_app, host="0.0.0.0", port=8001)


if __name__ == "__main__":
    main()
