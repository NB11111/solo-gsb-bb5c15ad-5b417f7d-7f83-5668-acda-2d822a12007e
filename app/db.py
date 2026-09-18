"""SQLite 存储层：连接管理、模式初始化与数据库路径配置。

数据库文件路径通过环境变量 ``RESERVATIONS_DB_PATH`` 配置；
未设置时默认 ``./data/reservations.db``（容器内由 Compose 指向挂载卷的 /data）。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

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
    若 ``reservations`` 表缺少 ``version`` / ``status`` / ``canceled_at`` 列，
    则逐列以 ``ALTER TABLE ADD COLUMN`` 补齐——既有行自动得到
    ``version=1``、``status='active'``、``canceled_at=NULL``，
    其余数据（含参与者占用）保持不变。
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
        finally:
            conn.close()
        _initialized.add(path)


def ensure_initialized(db_path: str | None = None) -> str:
    """按路径惰性初始化一次（测试客户端不触发启动事件时也能正常工作）。"""
    path = db_path or get_db_path()
    if path not in _initialized:
        init_db(path)
    return path
