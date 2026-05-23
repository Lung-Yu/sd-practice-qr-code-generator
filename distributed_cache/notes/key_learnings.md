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

## 12. Python router × Go nodes：換節點語言，吞吐量不變但 p95 減半

把三個 cache node 從 Python (uvicorn, 2 workers) 換成 Go (net/http, 1 process, goroutines)，路由層保持 Python 8 workers 不變。相同的 k6 腳本（0→3000 RPS），跑出以下對比：

| 指標 | Python node (2w each) | Go node (1 proc each) |
|------|:---------------------:|:---------------------:|
| 總請求數 | 352,786 | 353,797 |
| 平均 RPS | 1,176 | 1,179 |
| p95 latency | **216.9ms ❌** | **116.8ms ✓** |
| dropped_iterations | 1,213 | 203 |
| 最高 VU 數 | 1,165（latency 積壓） | ~10（乾淨） |

**為什麼吞吐量不變？** 路由層是 Python 8w，每個 request 都要經過 FastAPI + Pydantic + GIL。換 node 語言不影響路由層的 CPU 開銷，上限不動。

**為什麼 p95 減半？** Go node 的每個回應從 ~2ms → ~0.5ms，router 的 thread pool 不積壓，VU 從 1,165 降到 ~10。

**帶走的原則**：換掉非瓶頸層的語言只影響 latency，不影響吞吐量天花板。要突破天花板，必須改掉瓶頸層本身。

---

## 13. Go router + Go nodes：吞吐量突破 5.5×，p95 改善 100×

把路由層也從 Python 8 workers 換成 Go 單進程，k6 ceiling test（0→10,000 RPS）：

| 指標 | Python 8w + Python node (2w) | Python 8w + Go node | **Go router + Go node** |
|------|:---:|:---:|:---:|
| 吞吐量天花板 | ~1,823 RPS | ~1,823 RPS | **~10,000 RPS** |
| 平均 RPS（測試中） | 1,176 | 1,179 | **4,147** |
| p95 at 3000 target | 217ms ❌ | 117ms ✓ | **1.19ms ✓✓** |
| p95 at max load | ~2ms | — | **22ms ✓** |
| 最高 VU 數 | 1,165 | ~10 | **35** |
| dropped_iterations | 1,213 | 203 | **56** |

**為什麼 Go router 能突破 Python 的 1,823 RPS 天花板？**

Python router 的瓶頸是每個 request 都要走 FastAPI request parsing → Pydantic → GIL → httpx sync call → GIL。8 個 worker process × ~32 threads，但 GIL 讓 pure Python 開銷（hash 計算、JSON 解析）無法真正並行。

Go router 的模型完全不同：
```
每個 incoming connection → 一個 goroutine（~2 KB stack, OS thread共享）
goroutine 直接做：ring hash → http.Client.Do() → io.Copy 回傳
全程 non-blocking，沒有 GIL，沒有 thread pool 限制
```

Go 的 `net/http.Client` 有連線池（`MaxIdleConnsPerHost: 200`），keepalive 復用 TCP connection，不像 httpx 在高並發下還有 thread 切換開銷。

**Peak 9,997 RPS 的意義**：

測試中 k6 在 10,000 target 階段達到 9,997 iter/s，之後進入 ramp down。系統沒有報錯（cache_error_rate 0%），p95 仍在 22ms（SLO 200ms 內）。真正的天花板可能更高，受限於 k6 的 maxVUs=3000 和 macOS 網路 stack。

**Go router 的關鍵設計**：
```go
// 連線池：高並發下避免 TCP 頻繁建立/關閉
proxyClient = &http.Client{
    Timeout: 5 * time.Second,
    Transport: &http.Transport{
        MaxIdleConnsPerHost: 200,
        DisableCompression:  true,  // cache values 是純文字，不需要再壓縮
    },
}

// /stats 並行查詢三個 node（goroutine + WaitGroup）
for _, base := range nodeURLs {
    go func(base string) {
        defer wg.Done()
        resp, _ := proxyClient.Get(base + "/stats")
        // merge results with mutex
    }(base)
}
wg.Wait()
```

**帶走的原則**：
1. 瓶頸在哪一層，就改那一層的語言。換非瓶頸層只影響 latency。
2. Go 的 goroutine 比 Python thread 輕量 ~500×（2 KB vs 1 MB），支撐更高並發而不需要多進程。
3. HTTP 連線池（`MaxIdleConnsPerHost`）在高 RPS 時至關重要——每個請求建新 TCP 連線的開銷可以讓吞吐量腰斬。
4. 從 1,823 → 10,000 RPS（5.5×）：語言選擇在路由層的影響遠大於在節點層。

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

**系統上限進化**：

| 配置 | 天花板 RPS | p95 at 3k RPS | p95 at max |
|------|:----------:|:-------------:|:----------:|
| Python router ×1 + Python node ×1w | ~900 | — | ~3ms |
| Python router ×4 + Python node ×1w | ~1,484 | — | ~3ms |
| Python router ×8 + Python node ×2w | ~1,823 | 217ms ❌ | ~2ms |
| Python router ×8 + Go node ×1 | ~1,823* | 117ms ✓ | — |
| **Go router + Go node ×1** | **~10,000** | **1.19ms ✓✓** | **22ms ✓** |

\* 平均 RPS 不變是因為路由層仍是 Python；Go node 的貢獻是 p95 latency 減半。

完整 Go stack 的 k6 ceiling test（0→10,000 RPS）：
- 4,146 RPS 平均，峰值達 **9,997 RPS**
- p95 = 22ms（仍在 200ms SLO 內），cache_error_rate = 0%
- 最高 VU = 3000（k6 本身的並發上限），而 Python stack 在 1,823 RPS 時就需要 1,165 VU
- 結論：**全 Go 比全 Python 快 ~5.5×**，p95 latency 改善超過 100×

**突破方向**：
- 短期：`--workers N`（多進程，打破 Python 單進程 GIL 天花板）→ ~1,823 RPS
- 中期：Go router + Go nodes（移除解釋器開銷，goroutine 無 GIL）→ ~10,000 RPS
- 長期：節點水平擴展（多個 replica）→ 理論上線性擴展

---

## 14. Node Failure + Health Check：AP 行為的具體表現

### 實作

Router 每 2 秒對所有節點的 `/health` 發一個 HTTP GET。連續 2 次失敗（≈4s 偵測窗口）→ 將該節點從 consistent hash ring 移除；1 次成功 → 加回。Ring 操作用 `sync.RWMutex` 保護，request handler 與 health checker goroutine 不互相阻塞。

### 測試觀察（`scripts/test_node_failure.sh`）

```
Seed: key1→node3, key5→node1, key10→node2 …

Stop dc-node2 → wait 6s

Health: node2 alive=False, failures=3  (degraded)

Access 30 keys:
  hits=25, misses=5, errors=0   ← 0 個 503

Ring after failure:
  key10 → node1  (原本 node2，自動 reroute)

Restart dc-node2 → wait 6s

Health: node2 alive=True  (ok)
Re-seed: nodes used = node1 node2 node3  ← node2 回到輪換
```

### 為什麼是 AP，不是 CP？

**CAP 定理**：分散式系統在網路分割（Partition）發生時，只能二選一：

| 選擇 | 行為 | 這個系統 |
|------|------|:--------:|
| **CP**（一致性優先） | node2 不可達 → 拒絕服務 node2 的 key，回傳 503 | ✗ |
| **AP**（可用性優先） | node2 不可達 → key 自動 reroute 給其他節點，繼續服務 | ✓ |

這個系統選 AP：Router 把 node2 的 key 「悄悄」reroute 給 ring 上的下一個節點。那個節點沒有這些 key（in-memory，node2 掛掉資料就丟了），所以回傳 404 cache miss，讓 client 自己回 source DB 拿。**系統不停服，但資料暫時不一致（遺失）**。

### AP 的代價：Cache Miss Storm

node2 下線後，原本 node2 負責的 ~1/3 key 全部 cache miss。如果 cache miss 需要回 DB，DB 會瞬間承受 3× 的流量（所有這些 key 的請求都直打 DB）。這就是所謂的 **thundering herd**。

緩解方式：
- **Circuit breaker**（已實作，見 §15）：node 死後 3 次失敗即快速拒絕，DB 前不再積壓重試
- **Probabilistic early expiration**（PER）：在 TTL 到期前 stochastic 地 refresh，分散 miss 時機
- **Replica（寫兩個節點）**：node2 掛掉時從 replica 讀，代價是寫入成本加倍、複雜度上升

### CP 的代價：部分不可用

如果改成 CP 行為（不 reroute，直接 503），系統在 node2 下線期間有 ~1/3 的 key 完全無法讀寫，直到 node2 回來或人工介入（把那批 key 遷移走）。對某些場景（e.g. 金融交易、session token）這是正確選擇；對純 cache（miss 就回 DB）通常不必要。

### Consistent Hashing 在 AP 裡的角色

Ring 移除 node2 後，那些 key 不是「消失」，而是自動轉移到 ring 上的 **下一個節點**。這是 consistent hashing 的核心優勢：加/移節點只影響 1/N 的 key（不是全部），且路由邏輯完全在 router 端，節點本身不感知彼此存在。

**帶走的原則**：
1. 在設計 cache 系統時，要明確選擇 AP 或 CP，並把代價（miss storm vs 部分 503）暴露給上游。
2. Health check 的偵測窗口（這裡 4s）= interval × threshold，這段時間內 in-flight 請求仍可能 503，是不可避免的 gap。要縮短 gap 就要縮短 interval（成本：更多空請求），或用 passive 偵測（直接捕捉 proxy 的連線錯誤，第一次失敗就移環）。
3. Node 恢復後 ring 加回，但 node 上的 cache 是空的。如果下游不能承受 miss storm，需要 **warm-up**（重新把 key 搬回去）再讓 router 把流量切回來。

---

## 15. Circuit Breaker：填補 Health Check 的 4 秒偵測窗口

### 問題：Health Checker 有偵測延遲

Health checker 每 2 秒 poll 一次，需要 2 次連續失敗（≈4s）才把節點從 ring 移除。這段空窗期內，proxy 每次都要發起 TCP 連線、等 timeout（5s），大量請求積壓。

```
t=0    node2 死掉
t=0~4s 健康檢查還沒觸發，每個 request 都嘗試連 node2
       → 等 5s timeout → 503 node_unreachable
t=4s   health checker 把 node2 移出 ring，key 自動 reroute
```

這段空窗期沒有 back-pressure，如果 cache miss 需要回 DB，這 4 秒會讓 DB 瞬間收到大量重試請求（thundering herd）。

### 解法：Circuit Breaker（三態狀態機）

```
CLOSED ──(3 次連線失敗)──→ OPEN ──(15s 後)──→ HALF_OPEN ──(成功)──→ CLOSED
                                                     └──(失敗)──→ OPEN
```

- **CLOSED**：正常狀態，所有請求通過
- **OPEN**：快速拒絕，不嘗試 TCP 連線，立即回傳 `{"error":"circuit_open"}` 503
- **HALF_OPEN**：放行一個試探請求；成功→CLOSED，失敗→回 OPEN

```go
func (cb *CircuitBreaker) Allow() bool {
    switch cb.state {
    case cbClosed:   return true
    case cbOpen:
        if time.Since(cb.lastFailure) >= cb.openTimeout {
            cb.state = cbHalfOpen   // 到期，放一個試探
            return true
        }
        return false                // 快速拒絕
    case cbHalfOpen: return false   // 試探已在飛行中，其他人等
    }
}
```

### 什麼算「失敗」？什麼不算？

**算失敗**（觸發 RecordFailure）：
- TCP 連線錯誤（node 死掉 / 網路中斷）
- HTTP 5xx（node 內部錯誤）

**不算失敗**（觸發 RecordSuccess）：
- HTTP 200 ok
- HTTP 404（cache miss）← **這是關鍵**

404 是正常的 cache 行為，不代表節點有問題。如果把 miss 算進失敗，recovery 後的冷 cache 會重新打開 CB，形成死鎖：節點活著但 CB 永遠不 close。

### 測試觀察（`scripts/test_node_failure.sh` Phase 4）

```
node2 剛被停掉，health checker 尚未觸發（t < 4s）：

req 1: HTTP 503  error=node_unreachable   ← TCP 連線失敗，failures=1
req 2: HTTP 503  error=node_unreachable   ← failures=2
req 3: HTTP 503  error=node_unreachable   ← failures=3 → CB OPENS
req 4: HTTP 503  error=circuit_open       ← 快速拒絕，無 TCP 嘗試
req 5: HTTP 503  error=circuit_open       ← 同上
...
req 10: HTTP 503  error=circuit_open

Summary:
  503:node_unreachable → 3 times   (真正的連線錯誤)
  503:circuit_open     → 7 times   (fast-fail，無 TCP 嘗試)
```

3 次 TCP 失敗後，剩下 7 次全部是 circuit_open（每次 < 1ms，不再等 timeout）。Health checker 6 秒後把 node2 移出 ring，之後 key 自動 reroute，所有 30 個 key 正常服務（hits=30, errors=0）。

### CB vs Health Checker 的分工

| 機制 | 類型 | 觸發時機 | 作用 |
|------|------|---------|------|
| **Health Checker** | Proactive（主動 poll） | 每 2s 固定觸發 | 修改 ring：決定流量路由給誰 |
| **Circuit Breaker** | Reactive（被動偵測） | 第 3 次請求失敗後立即觸發 | 阻止後續請求去嘗試已知壞節點 |

兩者互補：CB 在 health checker 還沒動作的 4s 空窗內提供 fast-fail；health checker 修完 ring 後，CB 的 fast-fail 就消失（key 不再路由到 node2），CB 本身也由 health checker 在 recovery 時 Reset。

### Recovery 流程

```
node2 重啟 → health checker 成功 ping 到 /health
  → ring.addNode("node2")    ← 流量重新路由給 node2
  → cb.Reset()               ← CB 強制回 CLOSED，不等 openTimeout

這樣避免了：node2 已經健康，但 CB 還在 OPEN，新流量
仍被拒絕 15s 的問題。
```

### Prometheus 監控

```go
circuitOpenTotal = promauto.NewCounterVec(prometheus.CounterOpts{
    Name: "cache_circuit_open_total",
    Help: "Requests rejected because the node's circuit breaker is open",
}, []string{"node"})
```

PromQL 查看各節點 circuit open 速率：
```promql
rate(cache_circuit_open_total[1m])
```

spike 代表某節點正在故障；spike 消失代表 health checker 已移除或 CB 自然恢復。

### 帶走的原則

1. **Circuit breaker 補 health check 的盲區**：health check 是秒級的，CB 是毫秒級的。兩者結合才能把故障的影響時間從「秒」壓到「3 個請求」。
2. **404 ≠ 失敗**：cache miss 是正常業務行為，不能算進 CB 失敗計數。設計 CB 時要明確定義「什麼是失敗」，而不是「所有非 200」。
3. **CB 與 ring 的生命週期要連動**：recovery 時要同時 `ring.addNode` + `cb.Reset()`，否則流量切回來但 CB 還開著，保護機制變成阻塞。
4. **HALF_OPEN 的「一個試探」語意**：只放行一個請求，其他人繼續 fast-fail，直到那個試探有結果。這確保 recovery 是漸進的，不是「timeout 到了全部湧進來」的二次衝擊。
