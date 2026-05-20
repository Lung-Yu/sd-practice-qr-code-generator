# Tier 10D — Kafka 遷移分析

## 前提：Redis Streams 撐到哪裡？

現有系統（Redis Streams）的量化瓶頸：

```
實測吞吐量上限（本機，4 workers × BS=20）：
  POST /send p95 ≈ 430ms @ ~2000 RPS（POST + GET + LIST 混合）
  純 POST /send  ≈ 1500 RPS（benchmarked at c=300）

Redis Streams 的理論上限（單節點）：
  XADD：約 1M ops/s（100B payload）
  XREADGROUP：約 400K msgs/s（pipeline batch）
  → 對通知系統的瓶頸是 state ops（HSET+SET+ZADD），不是 stream ops
```

**結論：Redis Streams 在數萬 RPS 前不是瓶頸。**  
真正的瓶頸是單節點 Redis 的 primary state ops（notification HASH + idempotency）。

---

## 為什麼要考慮 Kafka？（觸發條件）

| 觸發條件 | 說明 |
|---------|------|
| **> 50K msg/s** 持續吞吐 | Redis Streams 單節點 XADD 接近飽和 |
| **長期留存（> 7 天）** | Kafka 支援無限 retention；Redis Streams 依賴 MAXLEN trim |
| **多消費群組** | Kafka 天然支援 N 個 consumer group 各自獨立消費；Redis Streams 也支援但管理複雜 |
| **回放（replay）** | Kafka offset 可重設；Redis Streams 只能 XREVRANGE 有限查詢 |
| **跨資料中心** | Kafka MirrorMaker 2 支援跨 DC 複製；Redis Streams 沒有內建 |
| **稽核/合規** | Kafka 作為不可變 event log；Redis Streams 可被 XTRIM 覆蓋 |

---

## 架構比對

### 現有：Redis Streams

```
POST /send → XADD notifications:delivery → XREADGROUP (worker) → deliver() → XACK
                                                                            → XADD notifications:dlq (on failure)
POST /fanout → XADD notifications:critical / notifications:delivery (batch)
```

**Redis Streams 的消費模型：**
```
Stream: notifications:delivery
  Consumer Group: delivery-workers
    Consumer 1: worker-a  (claims msg 1-20 via XREADGROUP)
    Consumer 2: worker-b  (claims msg 21-40)
    Consumer 3: worker-c  (claims msg 41-60)
  PEL: {1: worker-a, 21: worker-b, ...}  ← 未 ACK 的訊息
```

### 目標：Kafka

```
POST /send → Producer → Topic: notifications (partitioned by user_id)
           → Consumer Group: delivery-workers
             → Partition 0: worker-a
             → Partition 1: worker-b
             → Partition 2: worker-c
             → Partition 3: worker-d
→ deliver() → commit offset
```

---

## Topic/Partition 設計

### Topic 結構

| Topic | Partitions | Retention | 說明 |
|-------|-----------|-----------|------|
| `notifications.normal` | 16 | 7 days | 一般通知 |
| `notifications.critical` | 8 | 7 days | 高優先通知 |
| `notifications.dlq` | 4 | 30 days | 死信，長期保留 |

**為什麼 critical partition 數量較少？**  
Critical 訊息通常量少但時效高。16 個 partition 表示 16 個 consumer，但 critical 流量可能只需要 4 個。partition 數 = 最大 consumer 平行度。

### Partition Key 選擇

```python
# 現在：XADD 進入單一 stream，consumer group 按訊息順序競爭
# Kafka：partition key 決定哪個 partition（哪個 consumer）處理

partition_key = user_id  # 確保同一 user 的通知有序
```

**User ID as partition key 的取捨：**
- ✅ 同一 user 的通知保序（不需要 client-side sorting）
- ✅ user 的通知集中在一個 consumer → 減少 Redis state 競爭
- ❌ hot user（大量通知）→ 單 partition 成為熱點
- 熱點緩解：Kafka 支援 Custom Partitioner，對 VIP user 額外 spread

---

## Consumer Group 語義對比

| 特性 | Redis Streams | Kafka |
|------|--------------|-------|
| ACK 機制 | XACK (per message) | offset commit (per partition) |
| 未 ACK 重試 | XPENDING + XAUTOCLAIM（需自實作，Tier 10A） | 自動 re-deliver（rebalance 或 restart） |
| Consumer 數量上限 | 無限（但 PEL 管理複雜） | = partition 數量 |
| 順序保證 | 全域 stream 有序（單 consumer 時）| 同 partition 內有序 |
| Exactly-once | 需外部 2PC 或冪等接收 | Kafka transactions + idempotent producer |
| 多 consumer group 獨立消費 | ✅ 支援 | ✅ 原生支援 |

**Kafka 的最大優勢：不需要 Tier 10A 的 PEL recovery。**  
Consumer crash 後重啟，從最後一個 committed offset 繼續消費。沒有 PEL 的概念。

---

## Delivery Semantics（交付語義）

### Redis Streams（at-least-once）
```
XREADGROUP → [deliver()] → [XACK]
如果在 deliver() 後、XACK 前崩潰 → PEL recovery 重新交付 → 重複
```

### Kafka（at-least-once, 可升級為 exactly-once）

**At-least-once（預設）：**
```
poll() → [deliver()] → [commitSync()]
如果在 deliver() 後、commit 前崩潰 → 重啟後從舊 offset 再消費 → 重複
```

**Exactly-once（Kafka Transactions）：**
```python
producer.init_transactions()
with producer.transaction():
    producer.produce("notifications.dlq", ...)  # publish DLQ
    consumer.commit(offsets, transaction=True)   # atomic commit
# 只有 transaction commit 後，DLQ 消息才可見且 offset 才更新
```
代價：Kafka broker 需要 `transaction.state.log.replication.factor=3`，延遲增加 ~2ms。

---

## 遷移路徑（零停機）

### Phase 1：雙寫（2-4 週）

```python
async def aenqueue(notification_id: str, priority: str = "normal") -> None:
    # 現有 Redis Streams（保持）
    stream = STREAM_KEY_CRITICAL if priority == "critical" else STREAM_KEY
    await redis_client.xadd(stream, {"notification_id": notification_id})
    
    # 新增 Kafka（shadow write，不影響現有 consumer）
    if config.KAFKA_ENABLED:
        await kafka_producer.send(
            topic=f"notifications.{priority}",
            key=notification_id.encode(),
            value=json.dumps({"notification_id": notification_id}).encode(),
        )
```

此時 Kafka consumer 可以在 shadow 模式下消費（只記錄，不實際交付）。

### Phase 2：Kafka consumer 接管（切換日）

1. 停止 Redis Streams worker（SIGTERM → 等 graceful shutdown）
2. Redis Streams worker 處理完所有 PEL
3. 啟動 Kafka consumer
4. 驗證 delivery rate 正常（Grafana `notifications_sent_total`）

### Phase 3：移除 Redis Streams 寫入

確認 Kafka 穩定 7 天後，移除雙寫中的 Redis Streams 部分。

---

## 成本/複雜度分析

| 指標 | Redis Streams | Kafka |
|------|--------------|-------|
| 基礎設施複雜度 | 低（已有 Redis）| 高（ZooKeeper 或 KRaft + 3 broker） |
| 運維負擔 | 低 | 高（partition rebalance、lag monitoring、schema registry） |
| 開發成本 | PEL recovery 需自實作 | 原生支援，但 exactly-once 設定複雜 |
| 吞吐量上限 | ~1M msg/s（單節點）| ~數十M msg/s（cluster） |
| 留存 | 受 Redis memory 限制 | 幾乎無限（disk-based） |
| Replay | 有限（XRANGE 查詢）| 完整（offset reset） |
| 跨 DC | 無原生支援 | MirrorMaker 2 |

**結論：Redis Streams 在 < 100K msg/s 且 7 天內留存的場景，是更輕量的選擇。**  
觸發 Kafka 遷移的門檻：

```
if (peak_throughput > 100K msg/s)
    OR (retention_requirement > 7 days)
    OR (multiple_independent_consumers > 3)
    OR (cross_DC_replication_required)
    OR (exactly_once_required AND business_critical):
    → 考慮 Kafka 遷移
```

---

## Kafka 架構草圖（如果遷移）

```
notification-api (×4)
    ↓ kafka-python aiokafka Producer
    ↓ key=user_id (partition by user)

Kafka Cluster（3 brokers, KRaft mode）
    Topic: notifications.normal    (16 partitions, RF=3)
    Topic: notifications.critical  ( 8 partitions, RF=3)
    Topic: notifications.dlq       ( 4 partitions, RF=3, retention=30d)

delivery-worker (×16, one per partition)
    ↓ aiokafka Consumer Group: delivery-workers
    ↓ deliver() → commitSync()
    ↓ on permanent failure → producer.send("notifications.dlq")

Schema Registry（optional）
    → Avro/Protobuf schema for notification payload
    → ensures backward compatibility across rolling deploys
```

---

## 關鍵學習

1. **Redis Streams 的 PEL = Kafka 的 uncommitted offset 積壓**  
   兩者都面對「claim but crash」問題，解法不同（XAUTOCLAIM vs offset reset）。

2. **Partition = 平行度上限**  
   Kafka consumer 數量不能超過 partition 數量；超出的 consumer 空轉。  
   Redis Streams 沒有此限制，但 PEL 管理隨 consumer 數增加而複雜化。

3. **Retention 是核心差異**  
   Redis Streams 的訊息必須 trim（MAXLEN）避免記憶體耗盡。  
   Kafka 的訊息存在 disk，可保留幾個月，支援完整 replay。

4. **「用 Kafka 代替一切」是反模式**  
   Kafka 的複雜性（ZooKeeper/KRaft、consumer rebalance、schema evolution）對低吞吐系統是過度設計。  
   正確的問題是：「Redis Streams 的哪個具體限制擋住了我們？」
