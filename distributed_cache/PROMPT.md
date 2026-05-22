# Distributed Cache Prototype

## System Requirements

Build a distributed key-value cache where:
- Clients can `SET key value [ttl_seconds]`, `GET key`, and `DELETE key` via HTTP
- Each key has an optional TTL; after expiry the key is invisible (GET returns 404)
- The cache evicts the least-recently-used key when it reaches its capacity limit
- Keys are distributed across multiple cache nodes using consistent hashing — adding or removing a node remaps only `1/N` of the keys
- A cache miss (key not found or expired) is distinguishable from a node error in the response body

## Design Questions

Answer these before you start coding:

1. **Storage model:** Each cache entry needs to store the value, the expiry timestamp, and enough metadata for eviction. Should you store entries as a flat dict `{key: (value, expires_at)}` plus a separate LRU doubly-linked list, or use a single `OrderedDict` that combines both? What does one entry look like in memory, and what is the primary key for lookup?

-> flat dict

2. **Sync vs async:** When a client calls `SET`, the node that owns the key (via consistent hashing) must write the value. Should the API route block until the owning node confirms the write, or return 202 immediately and replicate in the background? What does the client see if the owning node is temporarily unreachable?

-> block unitl the owning node confirms the write

3. **Eviction policy:** LRU evicts the key that was accessed least recently; LFU evicts the key accessed least frequently. LRU is simpler but favours recency over popularity — a key accessed 1000 times yesterday but not today gets evicted before a key accessed once today. Which do you implement first, and what data structure backs it?

-> LRU

4. **Failure semantics:** A GET request can fail in three distinct ways: key never existed, key existed but expired, or the node that owns the key is unreachable. Should all three return `404 Not Found`, or should expired keys return `410 Gone` and node errors return `503 Service Unavailable`? What does the response body look like in each case?

-> follow RFC 7807 (Problem Details for HTTP APIs)

5. **Core abstraction:** The hash ring needs to map keys to nodes, and adding/removing nodes must reroute keys without a full rebuild. Should the ring be a sorted list of virtual nodes (each physical node gets `N` positions on the ring) queried with `bisect`, or a rendezvous/highest-random-weight scheme where each key independently scores every node? What interface does the rest of the code call into the ring through?

-> ring  first , but imple toggle to switch to  rendezvous/highest-random-weight 

## Verification

Your prototype should pass all of these:

```bash
# SET a key with a 10-second TTL
curl -s -X POST http://localhost:8000/cache/session:abc \
  -H "Content-Type: application/json" \
  -d '{"value": "user-data-xyz", "ttl": 10}'
# → {"key": "session:abc", "node": "node-1", "status": "ok"}

# GET the key — should return value and remaining TTL
curl -s http://localhost:8000/cache/session:abc
# → {"key": "session:abc", "value": "user-data-xyz", "ttl_remaining": 9}

# GET a key that was never set
curl -s http://localhost:8000/cache/no-such-key
# → 404 {"error": "miss", "key": "no-such-key"}

# DELETE the key
curl -s -X DELETE http://localhost:8000/cache/session:abc
# → {"key": "session:abc", "status": "deleted"}

# GET after delete
curl -s http://localhost:8000/cache/session:abc
# → 404 {"error": "miss", "key": "session:abc"}

# Inspect the hash ring — which node owns a given key?
curl -s http://localhost:8000/ring/session:abc
# → {"key": "session:abc", "node": "node-1", "virtual_nodes": 150}

# Inspect cache stats (hit rate, eviction count, current size)
curl -s http://localhost:8000/stats
# → {"hits": 1, "misses": 2, "evictions": 0, "size": 0, "capacity": 1000}
```

## Suggested Tech Stack

Python + FastAPI recommended, but any language/framework is fine.

---

## Later Phases (do not implement yet)

- Replication: each key is written to a primary node and `R` replicas for fault tolerance
- Persistence: periodic RDB-style snapshots so the cache survives restarts
- Monitoring: hit rate, eviction rate, per-node key distribution via Prometheus
- Cluster membership: gossip protocol for nodes to detect and announce failures
- Read-through / write-through: cache coordinates with a backing store automatically
