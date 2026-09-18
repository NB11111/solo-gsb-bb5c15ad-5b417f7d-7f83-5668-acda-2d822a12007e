"""SQLite 存储层：连接管理、模式初始化与数据库路径配置。

数据库文件路径通过环境变量 ``RESERVATIONS_DB_PATH`` 配置；
未设置时默认 ``./data/reservations.db``（容器内由 Compose 指向挂载卷的 /data）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
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
    seq             INTEGER NOT NULL,  -- 预约内从 1 递增
    action          TEXT NOT NULL,     -- created / rescheduled / canceled / baseline
    timestamp       TEXT NOT NULL,     -- 事件时间（UTC 定宽文本）
    snapshot        TEXT NOT NULL,     -- 预约快照（JSON），冗余列便于直接读取
    prev_hash       TEXT NOT NULL,     -- 上一条事件哈希；首条为 64 个 0
    hash            TEXT NOT NULL,     -- SHA-256(prev_hash ASCII + canonical_event UTF-8)
    canonical_event TEXT NOT NULL,     -- 实际参与哈希的规范化 JSON（不含哈希字段）
    PRIMARY KEY (reservation_id, seq)
);
"""

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


def init_db(db_path: str | None = None) -> None:
    """建表（幂等）。应用启动时调用一次即可。

    对升级前已存在的 SQLite 数据库执行**无损在线迁移**：

    - 若 ``reservations`` 表缺少 ``version`` / ``status`` / ``canceled_at`` 列，
      逐列以 ``ALTER TABLE ADD COLUMN`` 补齐——既有行自动得到
      ``version=1``、``status='active'``、``canceled_at=NULL``，
      其余数据（含参与者占用）保持不变；
    - 新建 ``reservation_events`` 审计表后，为**每条尚无事件的既有预约**
      在同一迁移事务内补写一条 ``baseline`` 事件：序号 1、prev_hash 为
      64 个 0，快照如实记录迁移初始化时观测到的预约状态（含历史
      version/status/canceled_at），事件时间取迁移时刻——不伪造过去的
      创建/改期/取消操作。重复执行幂等（已有事件的预约不会重复补写）。
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
            _backfill_baseline_events(conn)
        finally:
            conn.close()
        _initialized.add(path)


def _backfill_baseline_events(conn: sqlite3.Connection) -> None:
    """为所有没有审计事件的既有预约补写 ``baseline``（事务内幂等）。"""
    rows = conn.execute(
        "SELECT r.id, r.request_id, r.start_utc, r.end_utc, r.participants_key,"
        "        r.version, r.status, r.canceled_at, r.created_at"
        " FROM reservations r"
        " WHERE NOT EXISTS ("
        "     SELECT 1 FROM reservation_events e WHERE e.reservation_id = r.id)"
        " ORDER BY r.id"
    ).fetchall()
    if not rows:
        return

    # 迁移时刻只取一次：批量补写的 baseline 共享同一时间戳，
    # 明确表示“这是初始化时的快照”，而非预约的真实历史操作时刻。
    baseline_time = datetime.now(timezone.utc).strftime(audit.CANON_FMT)
    conn.execute("BEGIN IMMEDIATE")
    try:
        for row in rows:
            snapshot = audit.build_snapshot(
                reservation_id=row["id"],
                request_id=row["request_id"],
                start_utc=row["start_utc"],
                end_utc=row["end_utc"],
                participants=json.loads(row["participants_key"]),
                version=row["version"],
                status=row["status"],
                canceled_at=row["canceled_at"],
                created_at=row["created_at"],
            )
            audit.append_event(
                conn,
                reservation_id=row["id"],
                action="baseline",
                timestamp=baseline_time,
                snapshot=snapshot,
            )
        conn.execute("COMMIT")
    except BaseException:
        conn.rollback()
        raise


def ensure_initialized(db_path: str | None = None) -> str:
    """按路径惰性初始化一次（测试客户端不触发启动事件时也能正常工作）。"""
    path = db_path or get_db_path()
    if path not in _initialized:
        init_db(path)
    return path
