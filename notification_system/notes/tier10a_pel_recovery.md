# Tier 10A — PEL Recovery（恰好一次語義）

## 問題：Redis Streams 的隱藏 bug

Redis Consumer Group 的語義是 **at-least-once delivery**，不是 exactly-once。

```
正常流程：
  Worker A  →  XREADGROUP (claim)  →  deliver()  →  XACK  ✓

崩潰流程：
  Worker A  →  XREADGROUP (claim)  →  deliver()  →  [CRASH]
  ↑ 訊息永遠卡在 PEL（Pending Entry List），沒有 worker 會再處理它
```

**Pending Entry List (PEL)**：每個 consumer group 維護一個已 claim 但未 ACK 的訊息清單。
已 claimed 的訊息不會被 `XREADGROUP ... >` 取走（`>` 只取新訊息）。
如果 worker 崩潰，它的 PEL entries 變成孤兒，永久積壓。

---

## 解法：XAUTOCLAIM（Redis 6.2+）

```python
next_id, claimed, deleted = await r.xautoclaim(
    stream, GROUP_NAME, CONSUMER_NAME,
    min_idle_time=PEL_CLAIM_TIMEOUT_MS,  # 超過此時間視為死亡 worker 的訊息
    start_id="0-0",                       # 從 PEL 頭開始掃
    count=BATCH_SIZE,
)
```

- `min_idle_time`：訊息在 PEL 中閒置超過此時間（ms）才會被 claim
- `next_id`：下次繼續掃描的游標；`"0-0"` 表示已掃完整個 PEL
- `claimed`：被接管的 `[(msg_id, data), ...]`
- `deleted`：已不存在於 stream 的 PEL entries（stream 被 trim 後的殘留）

---

## Timeout 設定原則

```
PEL_CLAIM_TIMEOUT_MS > max_delivery_time = ATTEMPT_TIMEOUT_S × MAX_RETRIES
```

預設值：
- `ATTEMPT_TIMEOUT_S = 5.0`
- `MAX_RETRIES = 3`
- `max_delivery_time = 15s`
- `PEL_CLAIM_TIMEOUT_MS = 60000`（60s，留 4× 安全邊際）

太短 → 把正在交付中的訊息誤判為死亡（重複交付）  
太長 → 死亡 worker 的訊息等很久才被 recovery

---

## 實作：`_recover_pending()` in worker.py

```python
async def _recover_pending(r, loop):
    total = 0
    for stream in (STREAM_KEY_CRITICAL, STREAM_KEY):
        start = "0-0"
        while True:
            next_id, claimed, _ = await r.xautoclaim(
                stream, GROUP_NAME, CONSUMER_NAME,
                min_idle_time=config.PEL_CLAIM_TIMEOUT_MS,
                start_id=start,
                count=BATCH_SIZE,
            )
            if claimed:
                await _process_batch(r, claimed, loop, stream_key=stream)
                pel_recovered.labels(stream=stream).inc(len(claimed))
                total += len(claimed)
            if next_id == "0-0":
                break
            start = next_id
    return total
```

`_process_batch()` 是 normal 和 recovery 共用的路徑，確保恢復邏輯一致。

---

## 主迴圈整合

```python
# 每個 worker 的初始 PEL check 加入隨機 jitter，避免所有 worker 同時 sweep
last_pel_check = time.monotonic() - random.uniform(0, PEL_CHECK_INTERVAL_S)

while running:
    now = time.monotonic()
    if now - last_pel_check >= PEL_CHECK_INTERVAL_S:
        await _recover_pending(r, loop)
        last_pel_check = time.monotonic()
    
    # normal message processing...
```

**Jitter 的重要性**：4 個 worker 同時 XAUTOCLAIM 會互相競爭 PEL entries。
Jitter 讓 worker 的 sweep 時間錯開，但即使競爭也是安全的（最終只有一個人 claim 成功）。

---

## 實測結果

### 正常啟動後 PEL recovery 生效

系統重啟後，前幾個工作 cycle 看到 log：
```
[worker] PEL recovery: reclaiming 20 from notifications:delivery
[worker] PEL recovery: reclaiming 20 from notifications:delivery
[worker] PEL recovery: reclaiming 20 from notifications:delivery
[worker] PEL recovery: reclaiming 20 from notifications:delivery
[worker] PEL recovery: 80 messages recovered
```
這是前次測試中 worker 重啟前未 ACK 的訊息，重啟後正確 recovery。

### 手動模擬死亡 worker

```python
# 1. 注入訊息
sid = await r.xadd("notifications:delivery", {"notification_id": "pel-fake-0004"})
# → 1778996217036-0

# 2. zombie consumer 取走但不 ACK
await r.xreadgroup("delivery-workers", "zombie-DEAD",
                    {"notifications:delivery": ">"}, count=10)
# PEL: [{message_id: 1778996217036-0, consumer: zombie-DEAD, idle: 37229ms}]

# 3. 等 5s，XAUTOCLAIM with min_idle_time=5000
next_id, claimed, _ = await r.xautoclaim(
    "notifications:delivery", "delivery-workers", "recovery-worker",
    min_idle_time=5000, start_id="0-0", count=10,
)
# claimed: [(1778996217036-0, {notification_id: pel-fake-0004})]
# PEL after: 0 zombie messages ✓
```

---

## 設計邊界

### 重複交付（At-Least-Once 的代價）

如果 worker 在 `deliver()` 完成後、`XACK` 之前崩潰：
- 訊息已交付（channel.send() 成功）
- PEL recovery 會再交付一次

**這是 at-least-once 語義的固有特性**。對於通知系統（email/SMS），重複是可接受的邊際情況。
Exactly-once 需要：2-phase commit 或冪等接收端（channel 端去重）。

### deliver() 的冪等性

目前 `deliver()` 沒有冪等保護。如果 recovery 再次交付，用戶會收到重複通知。
生產建議：在 channel.send() 前檢查 notification.status：

```python
if notification.status != NotificationStatus.PENDING:
    return notification  # 已交付，跳過
```

### PEL 積壓監控

```
XPENDING notifications:delivery delivery-workers - + 1
```
回傳 PEL 總數。Alert 建議：`pel_depth > 1000` → 表示有大量 worker 崩潰或 PEL_CLAIM_TIMEOUT 設太長。

---

## Prometheus 指標

```python
pel_recovered = Counter(
    "pel_recovered_total",
    "Messages reclaimed from dead consumers via XAUTOCLAIM, by stream",
    ["stream"],
)
```

Grafana alert：`rate(pel_recovered_total[5m]) > 100` → 頻繁 recovery 表示 worker 不穩定。

---

## 檔案變更清單

| 檔案 | 變更 |
|------|------|
| `config.py` | `PEL_CLAIM_TIMEOUT_MS`、`PEL_CHECK_INTERVAL_S` |
| `metrics.py` | `pel_recovered` Counter（by stream label） |
| `worker.py` | `_recover_pending()`；主迴圈加 PEL check（含 jitter） |
