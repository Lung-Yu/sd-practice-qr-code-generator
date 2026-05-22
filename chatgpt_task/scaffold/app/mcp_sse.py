"""SSE/Streamable-HTTP MCP server for docker-compose deployment (:8001).

Uses FastMCP + Streamable HTTP transport (MCP protocol 2025-11-25) so
Claude.ai remote connectors can reach it directly.

Claude.ai connector URL: https://<ngrok-host>/
"""

import uvicorn
from mcp.server.fastmcp import FastMCP

from .database import Base, SessionLocal, engine
from .mcp_server import (
    handle_cancel_task,
    handle_create_task,
    handle_get_status,
    handle_list_tasks,
)

fastmcp = FastMCP("task-scheduler", streamable_http_path="/")


def _db():
    db = SessionLocal()
    try:
        return db
    finally:
        pass  # caller must close


@fastmcp.tool()
def task_create(description: str, scheduled_at: str) -> dict:
    """Create a scheduled task. scheduled_at must be ISO-8601 (e.g. 2025-01-01T09:00:00)."""
    db = SessionLocal()
    try:
        return handle_create_task(db, description=description, scheduled_at=scheduled_at)
    finally:
        db.close()


@fastmcp.tool()
def task_list() -> dict:
    """List all tasks."""
    db = SessionLocal()
    try:
        return handle_list_tasks(db)
    finally:
        db.close()


@fastmcp.tool()
def task_status(job_id: int) -> dict:
    """Get the status of a task by job_id."""
    db = SessionLocal()
    try:
        return handle_get_status(db, job_id=job_id)
    finally:
        db.close()


@fastmcp.tool()
def task_cancel(job_id: int) -> dict:
    """Cancel a pending task by job_id."""
    db = SessionLocal()
    try:
        return handle_cancel_task(db, job_id=job_id)
    finally:
        db.close()


def main() -> None:
    Base.metadata.create_all(bind=engine)
    app = fastmcp.streamable_http_app()
    uvicorn.run(app, host="0.0.0.0", port=8001)


if __name__ == "__main__":
    main()
