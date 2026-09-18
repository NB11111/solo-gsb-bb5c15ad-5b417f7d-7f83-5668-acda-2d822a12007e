# 跨时区可用时段聚合 API

基于 **Python + FastAPI + 标准库 `zoneinfo`** 的会议候选时段聚合服务，
并附带 **SQLite 持久化的会议预约接口**（幂等创建、乐观锁版本改期、
软删除取消、单事务冲突检测、**可校验的 SHA-256 哈希链变更历史**）。

每位参与者按**本地星期与钟面时刻**填写每周重复的可用区间并给出 IANA 时区
（如 `Asia/Shanghai`），服务按具体日期应用夏令时（DST）规则，统一换算到 UTC，
用**扫描线**算法计算所有满足最低出席人数与会议时长的候选时段。

- 交互式文档（Swagger UI）：<http://localhost:8000/docs>
- ReDoc：<http://localhost:8000/redoc>
- 健康检查：`GET /health`

服务默认监听 **8000 端口**（Docker Compose 一键启动：`docker compose up --build`，
随后访问 <http://localhost:8000>；端口被占用时可用 `HOST_PORT` 改宿主机映射）。

## 语义说明

- **搜索区间**：`[start_date 00:00 UTC, end_date + 1 天 00:00 UTC)`，起止日期均含（以 UTC 日期为界）。
  这意味着 UTC+14 时区里落在本地周一凌晨的窗口，其 UTC 时刻可能处于周日、从而被裁剪在区间之外。
- **跨午夜窗口**：`end_local <= start_local` 时视为跨午夜，自动拆成“当日起点 → 次日终点”的半开区间后换算 UTC。
- **DST 处理**（按每个具体日期分别计算）：
  - **不存在的本地时刻**（春季跳变被跳过的钟面时间，如美东 2026-03-08 02:30）→ 返回 `400` 错误；
  - **歧义时刻**（秋季回退出现两次的钟面时间）→ 统一选择 `fold=0`，即 UTC 偏移量较大、较早出现的那一次。
- **候选段**：出席者集合保持恒定的最大区间；时长必须 `>= meeting_minutes`。
- **排序**：出席人数**降序**，人数相同时按 UTC 开始时间**升序**（稳定排序）。
- 同一参与者互相重叠的窗口会先按人合并，扫描线中一人在任一时刻至多计一份出席。

## 目录结构

```
app/
  scheduling.py    # 聚合核心：本地时刻换算、跨午夜拆分、扫描线、排序
  reservations.py  # 预约核心：规范化、幂等创建、乐观锁版本改期/取消、单事务冲突检测、历史读取
  audit.py         # 审计事件：canonical JSON、SHA-256 哈希链追加与整链重验
  db.py            # SQLite 连接管理、建表与无损迁移（补列 / 补 baseline 事件）
  schemas.py       # Pydantic 请求/响应模型
  main.py          # FastAPI 路由
tests/             # pytest 测试（DST、跨午夜、排序、预约冲突/幂等/并发、哈希链/篡改/迁移）
Dockerfile
docker-compose.yml
requirements.txt  # 运行依赖
requirements-dev.txt  # 测试依赖（含 pytest、httpx）
```

## 配置

| 配置项 | 位置 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `APP_PORT` | 容器环境变量 | `8000` | 容器内 uvicorn 监听端口 |
| `HOST_PORT` | 宿主机环境变量 / `.env` | `8000` | 映射到宿主机的端口 |
| `RESERVATIONS_DB_PATH` | 容器环境变量 / `.env` | `/data/reservations.db`（Compose）<br>`./data/reservations.db`（本地） | SQLite 数据库文件路径 |
| `TZ` | 容器环境变量 | `UTC` | 容器系统时区（不影响按 IANA 时区进行的换算） |

构建时镜像内会安装 `tzdata`（IANA 时区数据库），Python 的 `tzdata` pip 包也作为兜底依赖安装。

## 用 Docker Compose 一条命令启动

```bash
docker compose up --build
```

启动时应用会**自动初始化 SQLite 数据库**（建表幂等）；数据库文件落在命名卷
`reservations-data`（容器内 `/data`）上，容器重建后预约数据不丢失。
升级到带 `version` / 取消能力的版本后首次启动时，会为**既有数据无损补齐**
`version=1`、`status='active'`、`canceled_at=NULL`
（`ALTER TABLE ADD COLUMN ... DEFAULT ...`），无需手工迁移。
升级到带**变更历史**的版本后首次启动时，还会为每条尚无审计事件的既有预约
在同一迁移事务内补写一条 `baseline` 事件（序号 1、首哈希为 64 个 0，
快照如实记录初始化时观测到的预约状态，事件时间取迁移时刻——**不伪造**
过去的创建/改期/取消操作）；迁移可重复执行，已有事件不会重复补写。

服务启动后访问 <http://localhost:8000>（Swagger UI 在 <http://localhost:8000/docs>）。
若 8000 端口被占用：

```bash
HOST_PORT=9000 docker compose up --build   # 访问 http://localhost:9000
```

后台运行与停止：

```bash
docker compose up -d --build
docker compose logs -f
docker compose down          # 加 -v 会连同数据库卷一起删除
```

## 本地开发（不使用 Docker）

需要 Python 3.11+。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# 启动开发服务器（端口 8000）
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 运行测试
pytest -q
```

## API

### `POST /api/v1/availability/aggregate`

请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `start_date` / `end_date` | `YYYY-MM-DD` | 搜索日期范围，均含（UTC 日期为界） |
| `meeting_minutes` | 整数 > 0 | 会议时长（分钟） |
| `min_attendees` | 整数 > 0 | 最低出席人数（不能超过参与者总数） |
| `participants[].id` | 字符串 | 参与者唯一标识 |
| `participants[].timezone` | 字符串 | IANA 时区名，如 `Europe/Berlin` |
| `participants[].availability[].weekday` | `1..7` 或名称 | `1`=周一 … `7`=周日；也接受 `Monday`/`mon` |
| `...start_local` / `end_local` | `HH:MM[:SS]` | 本地钟面时刻；`end <= start` 表示跨午夜 |

请求示例（东京周一 22:00 跨午夜到次日 02:00，纽约周一 09:00–12:00）：

```bash
curl -s -X POST http://localhost:8000/api/v1/availability/aggregate \
  -H 'Content-Type: application/json' \
  -d '{
    "start_date": "2026-03-09",
    "end_date": "2026-03-09",
    "meeting_minutes": 60,
    "min_attendees": 2,
    "participants": [
      {"id": "tokyo", "timezone": "Asia/Tokyo",
       "availability": [{"weekday": 1, "start_local": "22:00", "end_local": "02:00"}]},
      {"id": "ny", "timezone": "America/New_York",
       "availability": [{"weekday": 1, "start_local": "09:00", "end_local": "12:00"}]}
    ]
  }'
```

响应（共同可用段为 UTC 13:00–16:00；东京本地已跨到周二凌晨）：

```json
{
  "start_date": "2026-03-09",
  "end_date": "2026-03-09",
  "meeting_minutes": 60,
  "min_attendees": 2,
  "segments": [
    {
      "start_utc": "2026-03-09T13:00:00Z",
      "end_utc": "2026-03-09T16:00:00Z",
      "duration_minutes": 180,
      "attendees": ["ny", "tokyo"],
      "attendee_count": 2,
      "local_times": [
        {
          "participant_id": "ny",
          "timezone": "America/New_York",
          "start_local": "2026-03-09T09:00:00-04:00",
          "end_local": "2026-03-09T12:00:00-04:00",
          "start_weekday": "Monday",
          "end_weekday": "Monday"
        },
        {
          "participant_id": "tokyo",
          "timezone": "Asia/Tokyo",
          "start_local": "2026-03-09T22:00:00+09:00",
          "end_local": "2026-03-10T01:00:00+09:00",
          "start_weekday": "Monday",
          "end_weekday": "Tuesday"
        }
      ]
    }
  ]
}
```

错误：

- `400`：本地时刻因 DST 春跳变不存在，或 IANA 时区名未知；
- `422`：请求结构校验失败（日期倒置、人数非法、id 重复、weekday 越界等）。

### `POST /api/v1/reservations`

创建会议预约（SQLite 持久化）。请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `request_id` | 非空字符串 | 客户端幂等请求标识 |
| `start_utc` / `end_utc` | ISO 8601（须带时区偏移，如 `...Z`） | UTC 起止时刻；`end_utc` 必须晚于 `start_utc` |
| `participant_ids` | 非空字符串数组 | 参与者 ID；**去重并排序后**存储并参与幂等比较 |

语义：

- **原子占用**：检查与写入在单个 `BEGIN IMMEDIATE` 事务内完成，并发请求被串行化；
  只要任一参与者与已有预约存在**半开区间重叠**（`existing.start < new.end` 且
  `new.start < existing.end`），整个请求被拒绝，不会留下部分写入。
  **相邻区间不冲突**（`end == start` 首尾相接可以共存）。
- **版本号**：每条预约带单调递增的 `version`，创建时为 `1`，每次成功改期或取消加 `1`。
- **状态**：`status` 为 `active`（进行中）或 `canceled`（已取消，软删除终态）；
  未取消时 `canceled_at` 为 `null`。
- **幂等**：相同 `request_id` + 相同规范化内容（起止时刻、去重排序后的参与者）
  重试 → `200` 返回原预约，不新增记录；同一 `request_id` 内容不同 → `409`。
- **审计**：仅成功创建时在**同一事务**追加 `created` 哈希链事件（见
  [变更历史](#get-apiv1reservationsreservation_idhistory)）；
  冲突/幂等失败与重放不产生事件。

请求示例：

```bash
curl -s -X POST http://localhost:8000/api/v1/reservations \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-9f3a",
    "start_utc": "2026-03-09T13:00:00Z",
    "end_utc": "2026-03-09T14:00:00Z",
    "participant_ids": ["alice", "bob"]
  }'
```

成功响应（`201`，幂等重放为 `200`）：

```json
{
  "reservation_id": 1,
  "request_id": "req-9f3a",
  "start_utc": "2026-03-09T13:00:00Z",
  "end_utc": "2026-03-09T14:00:00Z",
  "participants": ["alice", "bob"],
  "version": 1,
  "status": "active",
  "canceled_at": null,
  "created_at": "2026-09-17T20:42:39.634009Z"
}
```

冲突响应（`409`，含冲突预约 ID 与参与者）：

```json
{
  "detail": "reservation conflict: participant 'bob' overlaps reservation #1",
  "conflicts": [{"reservation_id": 1, "participant_id": "bob"}]
}
```

`request_id` 被不同内容占用时（`409`）：

```json
{
  "detail": "request_id is already used by reservation #1 with different content",
  "existing_reservation_id": 1
}
```

其他错误：`422`（`end_utc` 不晚于 `start_utc`、时间缺时区偏移、参与者列表为空等）。

### `PUT /api/v1/reservations/{reservation_id}`

改期：原子替换预约的起止时间与参与者占用，并推进乐观锁版本。请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `expected_version` | 正整数 | 客户端上次读到的版本号；与当前版本一致才执行 |
| `start_utc` / `end_utc` | ISO 8601（须带时区偏移） | 新的 UTC 起止时刻；沿用相同校验（`end > start`、等价偏移归一） |
| `participant_ids` | 非空字符串数组 | 新的参与者列表；**同样去重并排序**后存储 |

语义：

- 整个流程（存在性 → 状态 → 版本匹配 → 冲突检查 → 替换）在单个 `BEGIN IMMEDIATE`
  事务内完成；**任一步失败都整体回滚**，预约与参与者占用保持原状。
- 预约不存在 → `404`。
- 预约已取消 → `409`：取消是终态，改期**不会**使其复活。
- `expected_version` 与当前版本不一致（已被别人改过）→ `409`，响应给出
  `current_version`；客户端可用它重新读取后重试。
- 冲突检查沿用半开区间规则，并**排除自身**（`reservation_id != 当前预约`）：
  自己改期前的旧占用不算冲突，相邻区间也不冲突；与其他预约重叠 → `409`
  及冲突明细。
- 成功时 `request_id` 与 `created_at` 不变，`version` 加 `1`，返回更新后的预约
  （结构同创建响应）。
- **审计**：成功改期在同一事务追加 `rescheduled` 事件（快照为改期后的新状态）；
  任一步失败或并发落败都不产生事件。

请求示例（把预约 #1 改到 15:00–16:00，并把参与者调整为 alice / carol）：

```bash
curl -s -X PUT http://localhost:8000/api/v1/reservations/1 \
  -H 'Content-Type: application/json' \
  -d '{
    "expected_version": 1,
    "start_utc": "2026-03-09T15:00:00Z",
    "end_utc": "2026-03-09T16:00:00Z",
    "participant_ids": ["carol", "alice"]
  }'
```

成功响应（`200`，参与者已按字典序规范化，版本推进到 2）：

```json
{
  "reservation_id": 1,
  "request_id": "req-9f3a",
  "start_utc": "2026-03-09T15:00:00Z",
  "end_utc": "2026-03-09T16:00:00Z",
  "participants": ["alice", "carol"],
  "version": 2,
  "status": "active",
  "canceled_at": null,
  "created_at": "2026-09-17T20:42:39.634009Z"
}
```

版本过期（`409`，可用 `current_version` 重试）：

```json
{
  "detail": "expected_version does not match current version of reservation #1",
  "current_version": 2
}
```

预约不存在（`404`）：

```json
{ "detail": "reservation #999 not found" }
```

参与者冲突时返回的 `409` 结构与创建接口一致（`detail` + `conflicts` 明细）。
其他错误：`422`（`expected_version < 1`、时间/参与者非法等）。

### `DELETE /api/v1/reservations/{reservation_id}`

取消预约（**软删除**，乐观并发控制）。查询参数：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `expected_version` | 正整数，**必填** | 客户端上次读到的版本号；与当前版本一致才执行取消 |

语义：

- 整个流程（存在性 → 状态 → 版本匹配 → 取消 → 释放占用）在单个
  `BEGIN IMMEDIATE` 事务内完成；任一步失败都整体回滚。
- 预约不存在 → `404`。
- `expected_version` 与当前版本不一致 → `409`，响应给出 `current_version`。
- 成功 → `200`：`status` 置为 `canceled`、记录 `canceled_at`、`version` 加 `1`，
  并**同事务删除全部参与者占用**——时段提交后立即可被再次预约；
  预约行本身保留（软删除），`request_id`、`created_at` 等不变。
- **幂等重试**：相同 `reservation_id` + 原 `expected_version` 重试 → `200`
  返回**同一取消结果**（同样的 `canceled_at` 与版本），不重复变更；
  取消后使用其他版本号 → `409` + `current_version`。
- 取消是**终态**：之后 `PUT` 改期一律 `409`，预约不会复活；
  创建请求的迟到重试（同 `request_id` + 同内容）幂等返回该已取消记录。
- **审计**：成功取消在同一事务追加 `canceled` 事件（快照记录取消终态）；
  版本不符等失败与幂等重试不产生事件。

请求示例：

```bash
curl -s -X DELETE 'http://localhost:8000/api/v1/reservations/1?expected_version=2'
```

成功/幂等重放响应（`200`）：

```json
{
  "reservation_id": 1,
  "request_id": "req-9f3a",
  "start_utc": "2026-03-09T15:00:00Z",
  "end_utc": "2026-03-09T16:00:00Z",
  "participants": ["alice", "carol"],
  "version": 3,
  "status": "canceled",
  "canceled_at": "2026-09-17T21:05:12.345678Z",
  "created_at": "2026-09-17T20:42:39.634009Z"
}
```

版本不匹配（`409`）：

```json
{
  "detail": "expected_version does not match current version of reservation #1",
  "current_version": 3
}
```

其他错误：`404`（预约不存在）、`422`（缺少 `expected_version` 或其值 `< 1`）。

### `GET /api/v1/reservations/{reservation_id}/history`

返回预约的**可校验变更历史**：按序（`seq` 升序）列出审计事件，服务端在返回前
**重验整条哈希链**。

- 预约不存在 → `404`；
- 哈希链任一环重验失败（序号断档、`prev_hash` 不衔接、`canonical_event`
  被篡改导致哈希不符、快照列与链上内容不一致等）→ `409`，
  **响应不包含任何历史**，仅在 `detail` 中给出失败位置；
- 校验通过 → `200`。

**什么时候产生事件**：仅在创建/改期/取消**成功提交**时，于**同一数据库事务**
内追加一条事件（与业务写入同生共死，不会出现有预约无事件或反之）；
失败请求（冲突、版本不符等）与**幂等重放**（相同 `request_id` 重试创建、
原 `expected_version` 重试取消）**不产生事件**。

**事件字段与哈希链规则**：

| 字段 | 说明 |
| --- | --- |
| `seq` | 该预约内**从 1 递增**的序号 |
| `action` | `created`（创建）、`rescheduled`（改期）、`canceled`（取消）、`baseline`（初始化时为既有预约补写） |
| `timestamp` | 事件时间（UTC ISO 8601；与业务写入同一时钟读数） |
| `snapshot` | 事件发生后的预约完整快照（结构同预约响应：id、request_id、起止时刻、参与者、version、status、canceled_at、created_at） |
| `prev_hash` | 上一条事件的哈希（64 位十六进制）；**首条为 64 个 `0`** |
| `hash` | 本条事件哈希 |
| `canonical_event` | 实际参与哈希的规范化 JSON |

- `canonical_event` 为**不含哈希字段**（无 `prev_hash`/`hash`）、**键按字典序
  排序**、**非 ASCII 不转义**（中文原样保留）、**无任何多余空白**的紧凑 JSON：
  `json.dumps(event, sort_keys=True, ensure_ascii=False, separators=(",", ":"))`；
- 链公式：`hash = SHA-256(prev_hash 的 ASCII 字节 + canonical_event 的 UTF-8 字节)`；
- 每条事件的 `snapshot.version` 与当时的预约版本一致：`created` 后为 1，
  每次 `rescheduled`/`canceled` 递增；`baseline` 记录迁移时观测到的版本。

请求示例：

```bash
curl -s http://localhost:8000/api/v1/reservations/1/history
```

成功响应（`200`，创建 → 改期 → 取消后）：

```json
{
  "reservation_id": 1,
  "events": [
    {
      "seq": 1,
      "action": "created",
      "timestamp": "2026-09-17T20:42:39.634009Z",
      "snapshot": {
        "reservation_id": 1,
        "request_id": "req-9f3a",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participants": ["alice", "bob"],
        "version": 1,
        "status": "active",
        "canceled_at": null,
        "created_at": "2026-09-17T20:42:39.634009Z"
      },
      "prev_hash": "0000000000000000000000000000000000000000000000000000000000000000",
      "hash": "e917cbcc0f0032480091db37122254e3f71d7808ca2c79da9fde57b40be4d69b",
      "canonical_event": "{\"action\":\"created\",\"seq\":1,\"snapshot\":{...},\"timestamp\":\"...\"}"
    },
    { "seq": 2, "action": "rescheduled", "prev_hash": "e917cb…", "hash": "4f1ad5…", "...": "..." },
    { "seq": 3, "action": "canceled",    "prev_hash": "4f1ad5…", "hash": "db3ccb…", "...": "..." }
  ]
}
```

验链异常（`409`，无 `events` 字段）：

```json
{ "detail": "reservation history chain verification failed: hash mismatch at seq 1" }
```

客户端可用任意语言按同一规则独立复核（Python 示例）：

```python
import hashlib, json

prev = "0" * 64
for ev in history["events"]:
    assert ev["prev_hash"] == prev
    digest = hashlib.sha256(
        prev.encode("ascii") + ev["canonical_event"].encode("utf-8")
    ).hexdigest()
    assert digest == ev["hash"]          # canonical_event 被任何改动都会失配
    prev = ev["hash"]
```

**升级迁移语义**：升级前已存在的预约没有真实操作历史，初始化时补写的
`baseline` 事件只表示“启动时观测到的快照”——其 `timestamp` 是迁移时刻
（晚于预约 `created_at`），快照里保留既有的 `version`/`status`/`canceled_at`，
不会伪造出过去的 `created`/`rescheduled`/`canceled` 事件；迁移后新的成功操作
正常追加为 seq=2、3……并与 baseline 的哈希衔接。

## 测试覆盖要点

- **春季跳变**：不存在的钟面时刻报错；跨越跳变间隙的窗口 UTC 时长缩短（柏林 2026-03-29 00:00–04:00 实际只有 3 小时）。
- **秋季回退**：歧义窗口取 `fold=0`（美东 2025-11-02 01:00–02:00 → UTC 05:00–07:00，共 2 小时）。
- **跨午夜**：窗口拆分、与另一时区窗口求交、各自本地时刻回显。
- 扫描线排序（人数降序、开始时间升序）、同参与者重叠窗口合并、UTC 边界裁剪、时长过滤。
- **预约**：半开区间各方向重叠均 409、相邻区间不冲突、冲突无部分写入、
  幂等重放返回原预约且不新增记录、`request_id` 内容不同 409、
  多线程并发下同一时段仅一个赢家、并发重放只建一条记录；
  **改期**：不存在 404、版本过期 409 并回传 `current_version`、排除自身的
  半开区间冲突 409、失败不改预约/占用、成功原子替换且版本递增、
  并发改期仅一个赢家；
  **取消**：软删除置 `canceled` 并记录 `canceled_at`、版本递增、
  占用同事务释放使时段立即可再约、原 `expected_version` 重试幂等返回
  同一结果、其他版本 409、取消后改期 409 不复活、并发取消仅一次实际变更；
  以及**旧版数据库启动时无损补齐 `version=1`、`status='active'`、
  `canceled_at=NULL`** 的迁移。
- **变更历史（哈希链）**：成功创建/改期/取消各在同事务追加事件、序号从 1
  连续递增、快照与版本跟随；`canonical_event` 紧凑/键排序/非 ASCII 不转义；
  首条 `prev_hash` 为 64 个 0，可按公式独立重算整链；GET 历史按序返回、
  不存在 404、篡改哈希/事件/快照列/序号断档均 409 且不返回历史；
  失败请求、幂等重放（创建重放、取消重试）不新增事件；事件写入失败时业务
  写入同事务回滚；非 ASCII 参与者端到端验签；并发改期后链仍连续；
  **旧版库（含无 version 列的最旧 schema）初始化时无损补写 `baseline`**、
  迁移幂等不重复补写、baseline 如实记录迁移时刻与取消态、迁移后改期正常接链。

```bash
pytest -q
```
