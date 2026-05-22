from pydantic import BaseModel


class CacheSetRequest(BaseModel):
    value: str
    ttl: int | None = None


class CacheSetResponse(BaseModel):
    key: str
    node: str
    status: str = "ok"


class CacheGetResponse(BaseModel):
    key: str
    value: str
    ttl_remaining: int | None  # seconds remaining, None if no TTL


class CacheDeleteResponse(BaseModel):
    key: str
    status: str = "deleted"


class RingResponse(BaseModel):
    key: str
    node: str
    virtual_nodes: int


class StatsResponse(BaseModel):
    hits: int
    misses: int
    evictions: int
    size: int
    capacity: int


class ErrorResponse(BaseModel):
    error: str   # "miss", "expired", or "node_unreachable"
    key: str
    node: str | None = None
