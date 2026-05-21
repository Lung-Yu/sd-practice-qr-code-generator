"""REST API for the task scheduler.

Run via: python -m app.api  (or via docker-compose `api` service on :8000)

Exposes the same business logic as the MCP server over HTTP.
Owns the scheduler — watcher + worker threads start with the app.
"""

from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine
from .mcp_server import (
    handle_cancel_task,
    handle_create_task,
    handle_get_status,
    handle_list_tasks,
)
from .scheduler import start_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    start_scheduler()
    yield


app = FastAPI(title="Task Scheduler API", lifespan=lifespan)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class CreateTaskRequest(BaseModel):
    description: str
    scheduled_at: str


@app.post("/tasks", status_code=201)
def create_task(req: CreateTaskRequest, db: Session = Depends(get_db)):
    return handle_create_task(db, description=req.description, scheduled_at=req.scheduled_at)


@app.get("/tasks")
def list_tasks(db: Session = Depends(get_db)):
    return handle_list_tasks(db)


@app.get("/tasks/{job_id}")
def get_task(job_id: int, db: Session = Depends(get_db)):
    result = handle_get_status(db, job_id=job_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.delete("/tasks/{job_id}")
def cancel_task(job_id: int, db: Session = Depends(get_db)):
    result = handle_cancel_task(db, job_id=job_id)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
