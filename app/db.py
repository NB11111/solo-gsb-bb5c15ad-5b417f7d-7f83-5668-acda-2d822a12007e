"""SQLite 存储层：连接管理、模式初始化与数据库路径配置。

数据库文件路径通过环境变量 ``RESERVATIONS_DB_PATH`` 配置；
未设置时默认 ``./data/reservations.db``（容器内由 Compose 指向挂载卷的 /data）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path

from . import audit

DEFAULT_DB_PATH = "data/reservations.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS reservations (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id       TEXT NOT NULL UNIQUE,
    start_utc        TEXT NOT NULL,
    end_utc          TEXT NOT NULL,
    participants_key TEXT NOT NULL,  -- 规范化参与者列表（去重排序后的 JSON），用于幂等比较
    version          INTEGER NOT NULL DEFAULT 1,  -- 乐观锁版本：创建为 1，每次改期/取消 +1
    status           TEXT NOT NULL DEFAULT 'active',  -- active=进行中；canceled=已取消（软删除）
    canceled_at      TEXT,  -- 取消时刻（UTC 定宽文本）；未取消为 NULL
    created_at       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservation_participants (
    reservation_id INTEGER NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL,
    start_utc      TEXT NOT NULL,
    end_utc        TEXT NOT NULL,
    PRIMARY KEY (reservation_id, participant_id)
);
CREATE INDEX IF NOT EXISTS idx_rp_participant_window
    ON reservation_participants (participant_id, start_utc, end_utc);
CREATE TABLE IF NOT EXISTS reservation_events (
    reservation_id  INTEGER NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,  -- 该预约维度下从 1 递增的事件序号
    action          TEXT NOT NULL,     -- create / reschedule / cancel / baseline
    event_at        TEXT NOT NULL,     -- 事件时间（UTC 定宽文本）
    snapshot        TEXT NOT NULL,     -- 事件落定后预约的完整快照（规范化 JSON）
    prev_hash       TEXT NOT NULL,     -- 上一条事件哈希；首条为 64 个 0
    event_hash      TEXT NOT NULL,     -- SHA-256(prev_hash ASCII 字节 + canonical_event UTF-8 字节)
    canonical_event TEXT NOT NULL,     -- 被摘要的规范事件 JSON（无哈希字段/键排序/无空白）
    PRIMARY KEY (reservation_id, seq)
);
"""

_RESERVATION_COLUMNS = (
    "id, request_id, start_utc, end_utc, participants_key, version,"
    " status, canceled_at, created_at"
)

_init_lock = threading.Lock()
_initialized: set[str] = set()


def get_db_path() -> str:
    """数据库文件路径，``RESERVATIONS_DB_PATH`` 可覆盖默认值。"""
    return os.environ.get("RESERVATIONS_DB_PATH", DEFAULT_DB_PATH)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    """打开一个连接（自动提交模式；事务由调用方用 BEGIN/COMMIT 显式控制）。

    WAL + busy_timeout 保证并发读写时锁等待而非立即报错。
    """
    path = db_path or get_db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def snapshot_from_row(row: sqlite3.Row) -> dict:
    """从 ``reservations`` 行构造审计事件里的预约快照。

    快照直接取存储值（UTC 定宽文本），字段与预约对外响应一一对应；
    事件由同事务内刚写定的行构造，保证“事件记录的就是已提交结果”。
    """
    return {
        "reservation_id": row["id"],
        "request_id": row["request_id"],
        "start_utc": row["start_utc"],
        "end_utc": row["end_utc"],
        "participants": json.loads(row["participants_key"]),
        "version": row["version"],
        "status": row["status"],
        "canceled_at": row["canceled_at"],
        "created_at": row["created_at"],
    }


def _backfill_baseline_events(conn: sqlite3.Connection) -> None:
    """为升级前已存在、但尚无事件的预约补写 ``baseline`` 事件。

    - 基线事件按预约 id 顺序逐条补写，各自独立从序号 1 起链（哈希链是
      **每条预约**一条），事件时间取迁移当时——只声明“迁移时刻观察到该
      预约处于此状态”，**不伪造**其过去真实发生过的 create/reschedule/
      cancel 操作；
    - 已存在事件的预约（含重复执行本迁移）一律跳过，因此幂等；
    - 全程在单个事务内，失败整体回滚，既有预约数据不受影响。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            f"SELECT {_RESERVATION_COLUMNS} FROM reservations ORDER BY id"
        ).fetchall()
        for row in rows:
            exists = conn.execute(
                "SELECT 1 FROM reservation_events WHERE reservation_id = ? LIMIT 1",
                (row["id"],),
            ).fetchone()
            if exists is not None:
                continue
            audit.append_event(
                conn,
                reservation_id=row["id"],
                action=audit.ACTION_BASELINE,
                snapshot=snapshot_from_row(row),
            )
        conn.execute("COMMIT")
    except BaseException:
        conn.rollback()
        raise


def init_db(db_path: str | None = None) -> None:
    """建表（幂等）。应用启动时调用一次即可。

    对升级前已存在的 SQLite 数据库执行**无损在线迁移**：

    - 若 ``reservations`` 表缺少 ``version`` / ``status`` / ``canceled_at`` 列，
      逐列以 ``ALTER TABLE ADD COLUMN`` 补齐——既有行自动得到
      ``version=1``、``status='active'``、``canceled_at=NULL``；
    - 新建 ``reservation_events`` 审计表（不存在时），并为每条**既有预约**
      补写一条 ``baseline`` 事件作为哈希链起点：序号 1、prev_hash 为
      64 个 ``0``，快照为该预约迁移当时的完整状态；不伪造任何旧操作，
      此后的改期/取消事件自然续在基线之后。
    """
    path = db_path or get_db_path()
    with _init_lock:
        conn = connect(path)
        try:
            conn.executescript(_SCHEMA)
            columns = {row["name"] for row in conn.execute(
                "PRAGMA table_info(reservations)")}
            # ALTER TABLE ADD COLUMN 带 NOT NULL DEFAULT：
            # 既有行立即获得默认值，不重写/删除任何数据
            if "version" not in columns:
                conn.execute(
                    "ALTER TABLE reservations"
                    " ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
            if "status" not in columns:
                conn.execute(
                    "ALTER TABLE reservations"
                    " ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
            if "canceled_at" not in columns:
                # 可空列：既有行自动为 NULL（未取消）
                conn.execute(
                    "ALTER TABLE reservations ADD COLUMN canceled_at TEXT")
            # 既有预约补写基线事件（仅对没有事件的预约；幂等且无损）
            _backfill_baseline_events(conn)
        finally:
            conn.close()
        _initialized.add(path)


def ensure_initialized(db_path: str | None = None) -> str:
    """按路径惰性初始化一次（测试客户端不触发启动事件时也能正常工作）。"""
    path = db_path or get_db_path()
    if path not in _initialized:
        init_db(path)
    return path
