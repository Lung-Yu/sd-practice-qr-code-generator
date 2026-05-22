# Key Learnings — chatgpt_task (MCP Job Scheduler)

> 本文整理從零建起一個 MCP Job Scheduler 過程中最值得帶走的知識點。
> 涵蓋：MCP 協議架構、排程設計、容器化、Claude.ai 遠端連線。

---

## 1. MCP 有三種 Transport，適合不同場景

```
stdio          ← 本機測試、MCP Inspector、PROMPT.md 驗證
               Claude Code 啟動時用 subprocess 管理，session 結束即消滅

SSE (舊)       ← mcp.server.sse.SseServerTransport
               GET /sse 建立 stream，client 監聽 event
               問題：只支援 GET，Claude.ai 2025-11-25 協議送 POST → 405

Streamable     ← mcp.server.fastmcp.FastMCP.streamable_http_app()
HTTP (新)      協議版本：2025-11-25
               POST endpoint → 回應 SSE stream
               這是 Claude.ai remote connector 唯一支援的格式
```

**帶走的原則**：給 Claude.ai remote connector 用的 MCP server，一定要用 Streamable HTTP，不能用舊 SSE。

---

## 2. FastMCP vs 低階 Server API

```python
# 低階（mcp.server.Server）
server = Server("name")

@server.list_tools()
async def list_tools(): ...

@server.call_tool()
async def call_tool(name, arguments): ...

# 高階（FastMCP）
mcp = FastMCP("name")

@mcp.tool()
def my_tool(param: str) -> dict: ...
```

低階 API 讓你完全控制，適合 stdio + SSE 組合；
高階 FastMCP 內建 `streamable_http_app()`，直接給 Claude.ai 用。

**這次的選擇**：`mcp_server.py` 保留低階 Server（stdio 測試用），`mcp_sse.py` 用 FastMCP 包住同一批 handler 暴露 HTTP。

---

## 3. TOOL_REGISTRY 是最乾淨的 dispatch 模式

```python
TOOL_REGISTRY: dict = {
    "task.create": handle_create_task,
    "task.list":   handle_list_tasks,
    "task.status": handle_get_status,
    "task.cancel": handle_cancel_task,
}

def route_tool_call(tool_name: str, arguments: dict, db: Session) -> dict:
    handler = TOOL_REGISTRY.get(tool_name)
    if handler is None:
        return {"error": f"Unknown tool: {tool_name}"}
    return handler(db, **arguments)
```

**好處**：新增工具只要一行 registry entry + 一個 handler function，dispatch 邏輯不需要動。
這是 Open/Closed Principle 在 MCP server 的直接應用。

---

## 4. Time Bucket 是避免全表掃描的分區鍵

排程任務最直接的 `find_due_jobs()` 是 `SELECT * WHERE scheduled_at <= now AND status = 'pending'`。

問題：百萬任務時這是 full scan，即使有 index 也要掃大量過去資料。

**Time Bucket 方案**：
```python
def get_time_bucket(scheduled_at: datetime) -> str:
    return scheduled_at.strftime("%Y%m%d%H")  # 以小時為粒度

# 查詢只看當前小時及之前的 bucket
db.query(Job).filter(
    Job.time_bucket <= current_bucket,  # 不是 ==，要抓過去未執行的
    Job.scheduled_at <= current_time,
    Job.status == "pending",
)
```

`<=` 而不是 `==` 是關鍵：過去的 bucket（歷史遺留任務）也要被撿回來執行。
`==` 的話，2025 年建立的任務在 2026 年永遠不會執行到。

---

## 5. Scheduler 的 Watcher / Worker 分工

```
watcher thread
  └─ 每 N 秒 poll DB (find_due_jobs)
  └─ 放進 in-memory queue

worker thread(s)
  └─ 從 queue 取任務
  └─ 執行（這裡是 print，實際是 HTTP call / email 等）
  └─ 更新 DB status = "completed" | "failed"
```

**為什麼分開**：watcher 負責「找」，worker 負責「做」。兩者解耦後，可以有多個 worker 並行，而 watcher 保持單一、避免重複 poll。

**Scheduler 的 ownership**：在 `api` service 啟動（lifespan），`mcp-server` 不跑 scheduler。兩個 service 連同一個 DB 但只有一個 scheduler，避免雙重執行。

---

## 6. 一個 Image，兩種角色

```yaml
# docker-compose.yml
api:
  image: chatgpt-task:latest
  command: ["python", "-m", "app.api"]       # REST + scheduler

mcp-server:
  image: chatgpt-task:latest
  command: ["python", "-m", "app.mcp_sse"]   # MCP Streamable HTTP
```

同一個 Dockerfile build 出的 image，透過 `command` override 決定跑哪個進入點。

**好處**：只維護一份 Dockerfile、一次 build。環境、dependency、版本完全一致，不會出現「api 有裝但 mcp-server 沒裝」的問題。

---

## 7. Claude.ai Remote Connector 的四個坑

Claude.ai 不是一般 MCP client，它有特定的連線行為：

**坑 1 — 用 POST，不用 GET**
舊 SSE transport 只接 GET → 405。必須用 FastMCP Streamable HTTP。

**坑 2 — 打 `/`，不是 `/mcp`**
FastMCP 預設 endpoint 是 `/mcp`。Claude.ai 打你填入的 URL 的根路徑。
```python
FastMCP("name", streamable_http_path="/")
```

**坑 3 — DNS Rebinding Protection 擋 ngrok**
FastMCP 預設只接受 `localhost / 127.0.0.1`，ngrok host 被拒 → 421 Misdirected Request。
```python
from mcp.server.transport_security import TransportSecuritySettings
FastMCP("name", transport_security=TransportSecuritySettings(
    enable_dns_rebinding_protection=False
))
```

**坑 4 — 不會主動送 Bearer Token**
Claude.ai 用 OAuth discovery 決定如何認證（先打 `/.well-known/oauth-protected-resource`）。
如果你只在 SSE handler 檢查 `Authorization: Bearer`，Claude.ai 永遠不會送，永遠 401。
> 短期解法：不做 auth，用 ngrok URL 的隱秘性保護。
> 長期解法：實作完整 OAuth 2.0 server。

---

## 8. ngrok 是「臨時公網」的標準工具

```
本機 :8001 ──── ngrok ──── https://<random>.ngrok-free.dev
```

**必要的前置步驟**：
```bash
ngrok config add-authtoken <token>   # 一次性，需要帳號
ngrok http 8001                      # 開 tunnel
```

**Free tier 限制**：
- URL 每次重啟都換
- 不能跑多個 tunnel
- ngrok v3 強制要求 authtoken，無法匿名

**檢查是否在跑**：
```bash
curl -s http://localhost:4040/api/tunnels | python3 -c \
  "import sys,json; t=json.load(sys.stdin)['tunnels']; print(t[0]['public_url'])"
```

---

## 9. `podman-compose restart` ≠ 更新環境變數

修改 `docker-compose.env` 後：

```bash
# 這樣不行 — container 原地重啟，env 不更新
podman-compose restart mcp-server

# 正確 — 銷毀再建，env 重新注入
podman stop scaffold_mcp-server_1
podman rm scaffold_mcp-server_1
podman-compose up -d mcp-server
```

原理：`restart` 對既有 container 送 SIGTERM + SIGSTART，env 是 container 建立時固定的。
`up -d` 在偵測到 config 差異時重新 `create` container。

---

## 10. async bridge：MCP 是 async，handler 是 sync

MCP SDK 的 `call_tool()` 是 `async def`，但業務 handler（SQLAlchemy 操作）是同步的。

```python
# 錯誤：在 async context 直接呼叫 blocking sync → 卡住 event loop
result = handle_create_task(db, ...)

# 正確：把 sync 推進 thread pool，不阻塞 event loop
result = await asyncio.to_thread(handle_create_task, db, ...)
```

FastMCP 的 `@mcp.tool()` 裝飾的 sync function 會自動被 `asyncio.to_thread()` 包住，不需要手動處理。

---

## 架構全貌

```
REST client / curl
    ↓ HTTP :8000
api (FastAPI)
    ├── /tasks CRUD
    └── scheduler (watcher thread + worker thread)
         ↓ find_due_jobs()
         ↓ update status
         ↓
      PostgreSQL :5432
         ↑
mcp-server (FastMCP, Streamable HTTP :8001)
    ├── task_create → handle_create_task()
    ├── task_list   → handle_list_tasks()
    ├── task_status → handle_get_status()
    └── task_cancel → handle_cancel_task()
         ↑
Claude.ai remote connector
    via ngrok https://xxx.ngrok-free.dev/
```
