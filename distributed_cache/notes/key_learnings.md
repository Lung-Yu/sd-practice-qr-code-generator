# Key Learnings — distributed_cache

> 本文整理從零建起一個 Distributed In-Memory Cache 過程中最值得帶走的知識點。
> 涵蓋：Consistent Hashing、LRU 資料結構、TTL 設計、Back-pressure、Python GIL 限制、Prometheus 分散式監控。

---

## 1. Consistent Hash Ring — 為什麼不用取模 (mod)

**取模的問題**：
```
3 個節點：key → node = hash(key) % 3
加到 4 個節點：key → node = hash(key) % 4
→ 幾乎所有 key 都換節點，N=3→4 時 75% 的 key 需要搬移
```

**Consistent Hash Ring 的做法**：
```
把 hash space 想成一個圓圈（0 ~ 2^128）
每個節點在圓圈上佔 150 個虛擬位置（virtual nodes）
key 的 hash 落在圓圈上，順時針找到第一個節點就是 owner
```

```python
def add_node(self, node_id: str) -> None:
    for i in range(self.virtual_nodes):            # 150 個虛擬節點
        h = self._hash(f"{node_id}:{i}")
        bisect.insort(self._ring, h)               # 維持排序
        self._ring_map[h] = node_id

def get_node(self, key: str) -> str:
    h = self._hash(key)
    idx = bisect.bisect_right(self._ring, h) % len(self._ring)  # 找右邊第一個，wrap 到 0
    return self._ring_map[self._ring[idx]]
```

**加/移除節點時只動 1/N 的 key**：
```
N=3 時，加第 4 個節點 → 只有 ~25% 的 key 換節點
```

**帶走的原則**：水平擴展要用 Consistent Hashing，不要用 mod。`bisect.insort` + `bisect.bisect_right` 是實作的核心，兩行程式碼搞定 O(log N) lookup。

---

## 2. 虛擬節點 (Virtual Nodes) 解決 Hot Node 問題

沒有虛擬節點，3 個物理節點在環上只有 3 個點，key 分佈可能嚴重不均：

```
node1 管了 hash space 的 60%
node2 管了 30%
node3 管了 10%   ← hot node，很快 evict 完
```

加入 150 個虛擬節點後，每個物理節點均勻散布 → 每個節點接近 33% 的 key。

**實驗方法**：把 `VIRTUAL_NODES` 從 150 改到 3，跑 k6，在 Grafana 觀察 `cache_size{node}` 分佈差異。

---

## 3. Rendezvous Hashing (HRW) — 更簡單的替代方案

Consistent Hash Ring 需要維護排序 list，實作有一定複雜度。Highest Random Weight 只需要：

```python
def rendezvous_node(self, key: str) -> str:
    return max(self._nodes, key=lambda node_id: self._hash(f"{node_id}:{key}"))
```

**比較**：

| 特性 | Consistent Hash Ring | Rendezvous (HRW) |
|------|---------------------|------------------|
| Lookup 複雜度 | O(log N) | O(N) |
| 加/移除節點影響 | 1/N keys 搬移 | 1/N keys 搬移 |
| 實作複雜度 | 中（bisect + 虛擬節點） | 低（一行 max） |
| N 很大時 | 快 | 慢 |

**帶走的原則**：節點數 < 100 時 HRW 夠用；節點很多時才需要 Ring。用 `HASH_STRATEGY=rendezvous` env var 切換，不改程式碼。

---

## 4. LRU Cache 的 O(1) 實作：dict + 雙向鏈表

最直覺的 LRU 是 `OrderedDict.move_to_end()`，但理解底層原理更重要：

```
_head ←→ [MRU node] ←→ [node] ←→ ... ←→ [LRU node] ←→ _tail
```

```python
@dataclass
class _Node:
    key: str
    value: str
    expires_at: float | None
    prev: "_Node | None" = None
    next: "_Node | None" = None

class LRUCache:
    _cache: dict[str, _Node]   # O(1) 按 key 找到 node
    _head: _Node               # sentinel，_head.next = MRU
    _tail: _Node               # sentinel，_tail.prev = LRU
```

**三個操作全部 O(1)**：

```python
# get：找到 → 移到 head → 回傳
node = self._cache[key]          # O(1) dict lookup
self._remove(node)               # O(1) pointer 操作
self._prepend(node)              # O(1) 插到 head 後

# set：滿了先 evict tail.prev，再插到 head
lru = self._tail.prev            # O(1) 找 LRU
self.delete(lru.key)             # O(1) 移除

# evict 和 get 用同一套 _remove / _prepend — 沒有重複邏輯
```

**帶走的原則**：LRU 的本質是 hashmap + doubly-linked list。`dict` 解決 O(1) 查找，linked list 解決 O(1) 移動順序。sentinel node（dummy head/tail）讓邊界條件消失，不需要判斷 `if prev is None`。

---

## 5. TTL 用絕對時間戳，不用剩餘秒數

**壞的設計（相對秒數）**：
```python
expires_in: int  # 還有幾秒
# 問題：存進去後每次 get 都要重算，而且不精確（如果 CPU busy 一秒呢？）
```

**好的設計（絕對 Unix timestamp）**：
```python
expires_at: float | None  # time.time() + ttl，存的是「到期時刻」
```

```python
# set 時
expires_at = time.time() + ttl if ttl else None

# get 時
if node.expires_at is not None and time.time() > node.expires_at:
    self.delete(key)
    raise CacheExpired

# 回傳剩餘秒數
ttl_remaining = int(node.expires_at - time.time()) if node.expires_at else None
```

**帶走的原則**：所有「未來事件」的時間都存絕對時間戳（epoch seconds）。相對時間只在介面層（API response）計算。

---

## 6. CacheMiss vs CacheExpired — 用 Exception 區分兩種「找不到」

cache miss 和 TTL 過期對 caller 的語意不同，但 Python 的 `None` 無法區分：

```python
# 壞：caller 不知道為什麼是 None
def get(key) -> str | None:
    ...

# 好：明確的語意
class CacheMiss(Exception): pass
class CacheExpired(Exception): pass

def get(key) -> tuple[str, int | None]:
    ...
    raise CacheMiss     # key 從來就不在
    raise CacheExpired  # key 曾在，但過期了
```

呼叫端可以各自處理：
```python
try:
    value, ttl_remaining = cache.get(key)
    return CacheGetResponse(...)
except CacheExpired:
    return JSONResponse(404, {"error": "expired", "key": key})
except CacheMiss:
    return JSONResponse(404, {"error": "miss", "key": key})
```

**帶走的原則**：當函式有多種「找不到」情況時，用不同的 Exception class，而不是多個 sentinel value（`None`, `-1`, `""` 等）。

---

## 7. Back-pressure：Sync 的線程池意外比 Async 更穩定

這是這次最反直覺的發現：

```
Sync router (httpx.Client + FastAPI thread pool)  → 893 RPS，0% 錯誤
Async router (httpx.AsyncClient，無 semaphore)    → 213 RPS，31% 錯誤 (timeout)
Async router + asyncio.Semaphore(150)              → 496 RPS，0% 錯誤
```

**為什麼 Sync 反而更好？**

FastAPI 的 sync handler 跑在一個有上限的線程池（`min(32, cpu_count + 4)` 個 thread）：
```
同時最多 32 個請求打到 nodes
每個 ~2ms → 理論上限 32 / 0.002 = 16,000 RPS
但 uvicorn connection acceptance rate ≈ 900 RPS 才是真正瓶頸
```

Async handler 沒有這個「自然節流」，把所有連線都接進來後，3,000 個 VU 同時等 httpx，node 的線程池爆掉，latency 飆到 60s。

**Back-pressure 的本質**：當下游處理速度跟不上上游請求速度，系統需要一個機制告訴上游「慢一點」。Sync thread pool 是隱式的 back-pressure；Async 系統需要顯式實作（Semaphore、Rate Limiter、Queue with bounded capacity）。

**帶走的原則**：`async def` 不保證更快，要看整條鏈的瓶頸在哪。引入 Async 時必須同時考慮 back-pressure 設計。

---

## 8. Python GIL 是單進程的天花板

這個系統的真正上限：

```
理論最大吞吐：
  3 個 node，每個 ~32 threads，每個 request ~1ms
  → 3 × 32 / 0.001 = 96,000 RPS

實測上限：~900 RPS
```

差距原因：

```
GIL（Global Interpreter Lock）：任何時刻只有一個 Python thread 在執行 bytecode
LRU cache 操作是 pure Python（dict + linked list）→ 每次操作都要搶 GIL
32 個 thread 中只有 1 個在真正執行，其他在等
```

**突破方法**：

```yaml
# 方法 1：多 uvicorn workers（多進程，每個有自己的 GIL）
command: uvicorn app.node_api:app --workers 4 --host 0.0.0.0 --port 8001
# 4 個進程 × 3 nodes = 12 個獨立 GIL → 理論上吞吐量 ×4

# 方法 2：節點水平擴展（多個 node replica）
# 需要路由層知道每個 replica 的 node_id
```

**帶走的原則**：Python 單進程的 I/O-bound 上限遠高於 CPU-bound，但 pure Python 資料結構（dict, list, custom objects）因為 GIL 無法真正並行。CPU-bound 的 hot path 改用 C extension（`redis-py`、`msgpack` 等）或多進程。

---

## 9. Prometheus 分散式監控：label 是關鍵

每個 metric 都加 `node` label，Grafana 才能做 per-node 比較：

```python
# node_api.py
_hits_counter    = Counter("cache_hits_total",    "Cache hits",    ["node"])
_size_gauge      = Gauge("cache_size",            "Current size",  ["node"])

# 使用時帶 label
_hits_counter.labels(node=NODE_ID).inc()
_size_gauge.labels(node=NODE_ID).set(cache.stats()["size"])
```

**觀察 hot node 的 PromQL**：
```promql
# 各節點命中率
sum by (node) (rate(cache_hits_total[1m]))
  / (sum by (node) (rate(cache_hits_total[1m])) + sum by (node) (rate(cache_misses_total[1m])))

# 各節點 cache 使用率
cache_size / cache_capacity
```

**帶走的原則**：分散式系統的 metric 一定要有識別維度（`node`, `instance`, `region`）。`sum by (label)` 才能找到異常節點；沒有 label 就只能看整體，找不到誰出問題。

---

## 10. k6 的 http_req_failed 會把 404 算進去

k6 預設的 `http_req_failed` 計算所有非 2xx 回應，包括 404（cache miss）：

```javascript
// 壞：cache miss 被算成 error，threshold 永遠 fail
thresholds: {
  http_req_failed: ["rate<0.05"],  // 404 miss 佔 25%，永遠超標
}

// 好：自訂只算真正的 error
const errorRate = new Rate("cache_error_rate");

// GET：404 是 miss，不是 error
errorRate.add(res.status !== 200 && res.status !== 404);

thresholds: {
  cache_error_rate: ["rate<0.05"],  // 只算 5xx 和 timeout
}
```

**帶走的原則**：k6 的 threshold 要配合業務語意設計，不要直接用框架的預設值。「404」在 cache 系統是正常行為，不是 error。

---

## 11. podman-compose 1.5.0 需要顯式宣告 default 網路

標準 Docker Compose 不需要宣告 `default` 網路，但 podman-compose 1.5.0 比較嚴格：

```yaml
# 壞（podman-compose 1.5.0 報錯）
networks:
  sd_monitoring:
    external: true

services:
  dc-router:
    networks:
      - default         # ← "missing networks: default"
      - sd_monitoring

# 好
networks:
  default: {}           # ← 顯式宣告
  sd_monitoring:
    name: sd_monitoring
    external: true
```

**帶走的原則**：每個 exercise 的 `docker-compose.yml` 都要加 `default: {}`，讓 container 之間能用 service name 互連，同時加入 `sd_monitoring` 讓 Prometheus 可以 scrape。

---

## 系統設計總結

```
Client
  ↓
Router (Consistent Hash Ring)
  ↓         ↓         ↓
Node 1    Node 2    Node 3
(LRU+TTL)(LRU+TTL)(LRU+TTL)
  ↓
/metrics → Prometheus → Grafana
```

**每一層的設計決策**：

| 層 | 問題 | 決策 | 原因 |
|----|------|------|------|
| 路由 | 如何分配 key 給節點？ | Consistent Hash Ring (150 vnodes) | 加/移節點只動 1/N 的 key |
| 路由 | Sync or Async HTTP client？ | Sync (`httpx.Client`) | Thread pool 提供隱式 back-pressure |
| 節點 | 怎麼做 LRU？ | dict + doubly-linked list | 全部操作 O(1) |
| 節點 | TTL 如何存？ | 絕對 timestamp (`expires_at`) | 精確，計算 ttl_remaining 方便 |
| 節點 | miss vs expired 如何區分？ | 兩個不同的 Exception class | 讓 caller 能各自處理語意 |
| 錯誤 | HTTP error format？ | RFC 7807 簡化版 `{error, key, node}` | 統一格式，client 好解析 |
| 監控 | 如何看各節點差異？ | Prometheus label `node=node{1,2,3}` | `sum by (node)` 找熱點 |

**系統上限（單進程 uvicorn）**：~900 RPS
**突破方向**：`--workers 4`（多進程）或節點水平擴展
