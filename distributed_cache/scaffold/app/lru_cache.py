import time
from dataclasses import dataclass, field


class CacheMiss(Exception):
    pass


class CacheExpired(Exception):
    pass


@dataclass
class _Node:
    key: str
    value: str
    expires_at: float | None   # absolute Unix timestamp, or None for no TTL
    prev: "_Node | None" = field(default=None, repr=False)
    next: "_Node | None" = field(default=None, repr=False)


class LRUCache:
    """In-memory LRU cache with optional per-key TTL."""

    def __init__(self, capacity: int = 100) -> None:
        self._capacity = capacity
        self._cache: dict[str, _Node] = {}
        # Sentinel nodes — _head.next is MRU, _tail.prev is LRU
        self._head = _Node("__head__", "", None)
        self._tail = _Node("__tail__", "", None)
        self._head.next = self._tail
        self._tail.prev = self._head
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    # ── private helpers (pre-written) ────────────────────────────────────

    def _remove(self, node: _Node) -> None:
        """Unlink node from the doubly-linked list."""
        prev, nxt = node.prev, node.next
        prev.next = nxt  # type: ignore[union-attr]
        nxt.prev = prev  # type: ignore[union-attr]
        node.prev = node.next = None

    def _prepend(self, node: _Node) -> None:
        """Insert node immediately after head (MRU position)."""
        node.prev = self._head
        node.next = self._head.next
        self._head.next.prev = node  # type: ignore[union-attr]
        self._head.next = node

    def delete(self, key: str) -> bool:
        """Remove key from cache. Returns True if the key existed."""
        if key not in self._cache:
            return False
        self._remove(self._cache.pop(key))
        return True

    def stats(self) -> dict:
        """Return current cache statistics."""
        return {
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
            "size": len(self._cache),
            "capacity": self._capacity,
        }

    # ── TODOs for guided track ────────────────────────────────────────────

    def get(self, key: str) -> tuple[str, int | None]:
        """
        TODO: Retrieve a value from the cache.

        Returns (value, ttl_remaining_seconds) on hit.
        ttl_remaining is None when the key was stored without a TTL.

        Raises:
            CacheMiss    — key is not in the cache
            CacheExpired — key existed but its TTL has elapsed (also deletes it)

        Steps:
        1. If key not in self._cache → increment self._misses, raise CacheMiss
        2. node = self._cache[key]
        3. If node.expires_at is not None and time.time() > node.expires_at:
               call self.delete(key), increment self._misses, raise CacheExpired
        4. Move node to head: call self._remove(node) then self._prepend(node)
        5. Increment self._hits
        6. Compute ttl_remaining = int(node.expires_at - time.time()) if node.expires_at else None
        7. Return (node.value, ttl_remaining)
        """
        if key not in self._cache:
            self._misses += 1
            raise CacheMiss
        node = self._cache[key]
        if node.expires_at is not None and time.time() > node.expires_at:
            self.delete(key)
            self._misses += 1
            raise CacheExpired
        self._remove(node)
        self._prepend(node)
        self._hits += 1
        ttl_remaining = int(node.expires_at - time.time()) if node.expires_at else None
        return (node.value, ttl_remaining)

    def set(self, key: str, value: str, ttl: int | None = None) -> None:
        """
        TODO: Insert or update a key in the cache.

        Steps:
        1. If key already exists in self._cache:
               update node.value and node.expires_at (= time.time() + ttl if ttl else None)
               move to head (self._remove then self._prepend)
               return
        2. If len(self._cache) >= self._capacity:
               evict the LRU item (node = self._tail.prev)
               call self.delete(node.key)
               increment self._evictions
        3. Create new _Node(key=key, value=value, expires_at=time.time()+ttl if ttl else None)
        4. Add to self._cache[key] = node
        5. Call self._prepend(node)
        """
        if key in self._cache:
            node = self._cache[key]
            node.value = value
            node.expires_at = time.time() + ttl if ttl else None
            self._remove(node)
            self._prepend(node)
            return
        if len(self._cache) >= self._capacity:
            lru = self._tail.prev
            self.delete(lru.key)  # type: ignore[union-attr]
            self._evictions += 1
        node = _Node(key=key, value=value, expires_at=time.time() + ttl if ttl else None)
        self._cache[key] = node
        self._prepend(node)
