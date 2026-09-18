"""预约变更历史（哈希链审计）测试。

覆盖：
- create / reschedule / cancel 各追加一条事件，序号从 1 递增、快照即结果状态；
- 失败请求（422/409/404）与幂等重放不新增事件；
- canonical_event 形式（无哈希字段、键排序、无空白、非 ASCII 不转义）；
- 哈希链规则（首条 prev_hash 为 64 个 0、SHA-256(prev ASCII + canonical UTF-8)）；
- GET history：顺序、200/404/409，409 不返回历史；
- 各类篡改（event_hash / prev_hash / canonical_event / snapshot / 序号断裂 / 空链）409；
- 同事务原子性：审计写入失败时预约/占用/事件整体回滚；
- 旧版 SQLite 初始化无损补写 baseline，并可与后续操作续链。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import audit
from app import db as db_module
from app import reservations as core
from app.main import app


@pytest.fixture()
def db_path(tmp_path, monkeypatch) -> str:
    path = str(tmp_path / "reservations.db")
    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    return path


@pytest.fixture()
def client(db_path) -> TestClient:
    return TestClient(app)


def _payload(**overrides):
    payload = {
        "request_id": "req-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return payload


def _put(client, rid, expected_version=1, **overrides):
    payload = {
        "expected_version": expected_version,
        "start_utc": "2026-03-09T15:00:00Z",
        "end_utc": "2026-03-09T16:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return client.put(f"/api/v1/reservations/{rid}", json=payload)


def _delete(client, rid, expected_version):
    return client.delete(
        f"/api/v1/reservations/{rid}",
        params={"expected_version": expected_version},
    )


def _create(client, **overrides) -> dict:
    resp = client.post("/api/v1/reservations", json=_payload(**overrides))
    assert resp.status_code == 201, resp.text
    return resp.json()


def _history(client, rid) -> list[dict]:
    resp = client.get(f"/api/v1/reservations/{rid}/history")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["reservation_id"] == rid
    return data["events"]


# ----------------------------------------------------------- 追加与顺序

def test_create_appends_create_event(client):
    created = _create(client)
    rid = created["reservation_id"]
    events = _history(client, rid)
    assert len(events) == 1
    e = events[0]
    assert e["seq"] == 1
    assert e["action"] == "create"
    assert e["event_at"]
    assert e["prev_hash"] == "0" * 64
    # 快照即创建落定后的预约（快照使用与存储一致的定宽 UTC 文本）
    snap = e["snapshot"]
    assert snap["reservation_id"] == rid
    assert snap["request_id"] == "req-1"
    assert snap["start_utc"] == "2026-03-09T13:00:00.000000Z"
    assert snap["end_utc"] == "2026-03-09T14:00:00.000000Z"
    assert snap["participants"] == ["alice", "bob"]
    assert snap["version"] == 1
    assert snap["status"] == "active"
    assert snap["canceled_at"] is None
    assert snap["created_at"] == created["created_at"]
    # 事件时间即创建时刻，与快照 created_at 一致
    assert e["event_at"] == snap["created_at"]


def test_full_lifecycle_records_three_events_in_order(client):
    created = _create(client)
    rid = created["reservation_id"]
    assert _put(client, rid).status_code == 200
    assert _delete(client, rid, 2).status_code == 200

    events = _history(client, rid)
    assert [e["seq"] for e in events] == [1, 2, 3]
    assert [e["action"] for e in events] == ["create", "reschedule", "cancel"]

    # 每条快照即当时落定状态
    assert events[0]["snapshot"]["version"] == 1
    assert events[0]["snapshot"]["status"] == "active"
    assert events[1]["snapshot"]["version"] == 2
    assert events[1]["snapshot"]["start_utc"] == "2026-03-09T15:00:00.000000Z"
    assert events[1]["snapshot"]["status"] == "active"
    assert events[2]["snapshot"]["version"] == 3
    assert events[2]["snapshot"]["status"] == "canceled"
    # 事件时间与快照内 canceled_at 复用同一事务时间戳
    assert events[2]["snapshot"]["canceled_at"] == events[2]["event_at"]
    # 业务时间不随取消改变
    assert events[2]["snapshot"]["request_id"] == created["request_id"]
    assert events[2]["snapshot"]["created_at"] == created["created_at"]
    assert events[1]["event_at"]                      # 改期事件有独立时间戳


def test_reschedule_changes_participants_reflected_in_snapshot(client):
    rid = _create(client)["reservation_id"]
    assert _put(client, rid, participant_ids=["carol", "alice", "carol"]).status_code == 200
    events = _history(client, rid)
    assert events[1]["snapshot"]["participants"] == ["alice", "carol"]


def test_events_per_reservation_are_independent_chains(client):
    a = _create(client, request_id="req-a", participant_ids=["alice"])["reservation_id"]
    b = _create(client, request_id="req-b", participant_ids=["bob"])["reservation_id"]
    for rid in (a, b):
        events = _history(client, rid)
        assert len(events) == 1
        assert events[0]["seq"] == 1
        assert events[0]["prev_hash"] == "0" * 64


# ----------------------------------------------------------- 失败与重放

def test_failed_create_conflict_adds_no_event(client):
    assert client.post("/api/v1/reservations", json=_payload()).status_code == 201
    # 冲突 409
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
    ))
    assert resp.status_code == 409
    # request_id 不同内容 409
    assert client.post("/api/v1/reservations",
                       json=_payload(end_utc="2026-03-09T15:00:00Z")).status_code == 409
    # 422 校验失败
    assert client.post("/api/v1/reservations",
                       json=_payload(end_utc="2026-03-09T13:00:00Z")).status_code == 422

    rid = 1
    events = _history(client, rid)
    assert [e["action"] for e in events] == ["create"]


def test_failed_reschedule_and_cancel_add_no_events(client):
    rid = _create(client)["reservation_id"]
    assert _put(client, rid).status_code == 200  # -> v2

    # 版本过期 409
    assert _put(client, rid, expected_version=1,
                start_utc="2026-03-10T15:00:00Z",
                end_utc="2026-03-10T16:00:00Z").status_code == 409
    # 422
    assert _put(client, rid, expected_version=2,
                end_utc="2026-03-09T15:00:00Z").status_code == 422
    # 取消版本不匹配 409
    assert _delete(client, rid, expected_version=9).status_code == 409
    # 不存在的预约
    assert _put(client, 999, expected_version=1).status_code == 404
    assert _delete(client, 999, 1).status_code == 404

    # 取消后改期 409
    assert _delete(client, rid, 2).status_code == 200
    assert _put(client, rid, expected_version=3).status_code == 409
    assert _delete(client, rid, expected_version=9).status_code == 409

    events = _history(client, rid)
    assert [e["action"] for e in events] == ["create", "reschedule", "cancel"]


def test_idempotent_replays_add_no_events(client):
    first = client.post("/api/v1/reservations", json=_payload())
    assert first.status_code == 201
    rid = first.json()["reservation_id"]
    # 创建幂等重放
    assert client.post("/api/v1/reservations",
                       json=_payload(participant_ids=["bob", "alice", "bob"])).status_code == 200
    # 改期到同窗口（版本推进，时间不变，便于之后按原内容重放创建）
    assert _put(client, rid,
                start_utc="2026-03-09T13:00:00Z",
                end_utc="2026-03-09T14:00:00Z").status_code == 200
    # 取消
    assert _delete(client, rid, 2).status_code == 200
    # 取消幂等重试（原 expected_version）
    retry = _delete(client, rid, 2)
    assert retry.status_code == 200
    assert retry.json()["version"] == 3
    # 创建请求迟到重放（内容须与存储一致）返回已取消记录
    replay = client.post("/api/v1/reservations", json=_payload())
    assert replay.status_code == 200
    assert replay.json()["status"] == "canceled"

    events = _history(client, rid)
    assert [e["action"] for e in events] == ["create", "reschedule", "cancel"]


# ----------------------------------------------------------- 规范形式与哈希

def test_canonical_event_form_and_hash(client):
    rid = _create(client, participant_ids=["alice", "张三😀"])["reservation_id"]
    events = _history(client, rid)
    e = events[0]
    canonical = e["canonical_event"]

    # 无空白（紧凑分隔符；参与者 id 本身也不含空白）
    assert all(ch not in canonical for ch in " \n\r\t")
    # 非 ASCII 不转义：中文与 emoji 原样出现
    assert "张三" in canonical and "😀" in canonical
    assert "\\u" not in canonical

    payload = json.loads(canonical)
    # 不含任何哈希字段，且恰好四个键
    assert set(payload) == {"action", "seq", "event_at", "snapshot"}
    assert set(payload["snapshot"]) == {
        "reservation_id", "request_id", "start_utc", "end_utc",
        "participants", "version", "status", "canceled_at", "created_at",
    }
    # 键排序：重新规范化必须逐字节一致
    assert json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ) == canonical


def test_hash_chain_independently_verifiable(client):
    rid = _create(client)["reservation_id"]
    assert _put(client, rid).status_code == 200
    assert _put(client, rid, expected_version=2,
                start_utc="2026-03-09T17:00:00Z",
                end_utc="2026-03-09T18:00:00Z").status_code == 200
    assert _delete(client, rid, 3).status_code == 200

    events = _history(client, rid)
    prev_hash = "0" * 64
    for e in events:
        assert e["prev_hash"] == prev_hash
        digest = hashlib.sha256(
            e["prev_hash"].encode("ascii")
            + e["canonical_event"].encode("utf-8")
        ).hexdigest()
        assert digest == e["event_hash"]
        assert len(e["event_hash"]) == 64
        prev_hash = e["event_hash"]


def test_core_chain_matches_spec():
    canonical = audit.canonical_event(
        "create", 1, "2026-03-09T13:00:00.000000Z", {"reservation_id": 1, "x": "中"},
    )
    assert canonical == (
        '{"action":"create","event_at":"2026-03-09T13:00:00.000000Z",'
        '"seq":1,"snapshot":{"reservation_id":1,"x":"中"}}'
    )
    h = audit.chain_hash("0" * 64, canonical)
    assert h == hashlib.sha256(
        ("0" * 64).encode("ascii") + canonical.encode("utf-8")
    ).hexdigest()
    assert audit.chain_hash(audit.GENESIS_HASH, canonical) == h


# ----------------------------------------------------------- 404 / 409 语义

def test_history_unknown_reservation_returns_404(client):
    resp = client.get("/api/v1/reservations/999/history")
    assert resp.status_code == 404
    assert "999" in resp.json()["detail"]


def _raw_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    return conn


@pytest.mark.parametrize("sql,bind_hash", [
    ("UPDATE reservation_events SET event_hash = ? WHERE seq = 1", True),
    ("UPDATE reservation_events SET prev_hash = ? WHERE seq = 2", True),
    ("UPDATE reservation_events SET canonical_event = '{\"x\":1}' WHERE seq = 1", False),
    ("UPDATE reservation_events SET snapshot = '{\"reservation_id\":1,\"version\":99}'"
     " WHERE seq = 1", False),
    # 只改快照列但让 canonical 里的 snapshot 仍是旧的：列与载荷不一致
    ("UPDATE reservation_events SET snapshot = '{\"reservation_id\":1}'"
     " WHERE seq = 1", False),
    ("DELETE FROM reservation_events WHERE seq = 1", False),
    ("UPDATE reservation_events SET action = 'hacked' WHERE seq = 1", False),
    ("DELETE FROM reservation_events", False),
])
def test_tampered_chain_returns_409_without_history(client, db_path, sql, bind_hash):
    rid = _create(client)["reservation_id"]
    assert _put(client, rid).status_code == 200
    # 基线状态：历史可读
    assert client.get(f"/api/v1/reservations/{rid}/history").status_code == 200

    conn = _raw_connect(db_path)
    conn.execute(sql, ("f" * 64,) if bind_hash else ())
    conn.commit()
    conn.close()

    resp = client.get(f"/api/v1/reservations/{rid}/history")
    assert resp.status_code == 409
    body = resp.json()
    assert "events" not in body          # 验链异常不返回历史
    assert "verification failed" in body["detail"]


def test_hash_mismatch_detail_identifies_reason(client, db_path):
    rid = _create(client)["reservation_id"]
    conn = _raw_connect(db_path)
    conn.execute("UPDATE reservation_events SET event_hash = ? WHERE seq = 1",
                 ("a" * 64,))
    conn.commit()
    conn.close()
    resp = client.get(f"/api/v1/reservations/{rid}/history")
    assert resp.status_code == 409
    assert "hash mismatch" in resp.json()["detail"]


def test_self_consistent_but_reshaped_payload_still_rejected(client, db_path):
    """攻击者按新 payload 重算哈希也无法通过：载荷形状（含快照键集）被固定。"""
    rid = _create(client)["reservation_id"]
    forged = json.dumps({
        "action": "create", "seq": 1, "event_at": "2026-01-01T00:00:00.000000Z",
        "snapshot": {"reservation_id": rid, "extra": True},
    }, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    forged_hash = hashlib.sha256(
        ("0" * 64).encode("ascii") + forged.encode("utf-8")
    ).hexdigest()
    conn = _raw_connect(db_path)
    conn.execute(
        "UPDATE reservation_events SET canonical_event = ?, event_hash = ?,"
        " action = 'create', event_at = '2026-01-01T00:00:00.000000Z' WHERE seq = 1",
        (forged, forged_hash),
    )
    conn.commit()
    conn.close()
    resp = client.get(f"/api/v1/reservations/{rid}/history")
    assert resp.status_code == 409
    assert "events" not in resp.json()


# ----------------------------------------------------------- 同事务原子性

def test_audit_failure_rolls_back_entire_create_transaction(client, db_path, monkeypatch):
    # 先完成惰性初始化（可能为既有数据补写 baseline），使下面的补丁只作用于新 create
    db_module.ensure_initialized(db_path)

    def boom(*args, **kwargs):
        raise RuntimeError("simulated audit failure")

    with monkeypatch.context() as m:
        m.setattr(audit, "canonical_event", boom)
        with pytest.raises(RuntimeError):
            core.create_reservation(
                request_id="req-x",
                start_utc=datetime(2026, 4, 1, 13, tzinfo=timezone.utc),
                end_utc=datetime(2026, 4, 1, 14, tzinfo=timezone.utc),
                participant_ids=["zoe"],
                db_path=db_path,
            )

    conn = _raw_connect(db_path)
    try:
        assert conn.execute(
            "SELECT count(*) FROM reservations WHERE request_id='req-x'"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM reservation_participants WHERE participant_id='zoe'"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM reservation_events").fetchone()[0] == 0
    finally:
        conn.close()

    # 同参与者/时段立刻可约（无残留占用）
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-y",
        start_utc="2026-04-01T13:00:00Z",
        end_utc="2026-04-01T14:00:00Z",
        participant_ids=["zoe"],
    ))
    assert resp.status_code == 201, resp.text


def test_audit_failure_rolls_back_reschedule(client, db_path, monkeypatch):
    rid = _create(client)["reservation_id"]
    db_module.ensure_initialized(db_path)

    def boom(*args, **kwargs):
        raise RuntimeError("simulated audit failure")

    with monkeypatch.context() as m:
        m.setattr(audit, "canonical_event", boom)
        with pytest.raises(RuntimeError):
            core.reschedule_reservation(
                reservation_id=rid, expected_version=1,
                start_utc=datetime(2026, 3, 9, 15, tzinfo=timezone.utc),
                end_utc=datetime(2026, 3, 9, 16, tzinfo=timezone.utc),
                participant_ids=["alice", "bob"],
                db_path=db_path,
            )

    # 预约保持原状：仍只有 create 一条事件、version=1、旧占用仍在
    events = _history(client, rid)
    assert [e["action"] for e in events] == ["create"]
    replay = client.post("/api/v1/reservations", json=_payload()).json()
    assert replay["version"] == 1
    assert replay["start_utc"] == "2026-03-09T13:00:00Z"
    blocked = client.post("/api/v1/reservations", json=_payload(
        request_id="req-z",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T13:45:00Z",
        participant_ids=["alice"],
    ))
    assert blocked.status_code == 409


# ----------------------------------------------------------- 并发一致性

def test_concurrent_reschedules_leave_contiguous_chain(db_path):
    """8 个并发改期抢同一版本：恰一个成功；历史链序号连续且可重验。"""
    reservation, _ = core.create_reservation(
        request_id="req-base",
        start_utc=datetime(2026, 3, 9, 13, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 9, 14, tzinfo=timezone.utc),
        participant_ids=["alice"],
        db_path=db_path,
    )
    barrier = threading.Barrier(8)

    def worker(i: int):
        barrier.wait()
        try:
            return core.reschedule_reservation(
                reservation_id=reservation.id,
                expected_version=1,
                start_utc=datetime(2026, 3, 10 + i, 13, tzinfo=timezone.utc),
                end_utc=datetime(2026, 3, 10 + i, 14, tzinfo=timezone.utc),
                participant_ids=["alice"],
                db_path=db_path,
            )
        except core.VersionConflictError:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(worker, range(8)))
    assert sum(1 for r in results if r is not None) == 1

    events = core.get_reservation_history(reservation.id, db_path=db_path)
    assert [e["seq"] for e in events] == [1, 2]
    assert [e["action"] for e in events] == ["create", "reschedule"]
    assert events[1]["prev_hash"] == events[0]["event_hash"]
    # 最新快照即唯一赢家的版本
    assert events[-1]["snapshot"]["version"] == 2


def test_concurrent_creates_each_get_independent_genesis_event(db_path):
    """并发创建不同预约：各自独立从 seq=1、prev_hash=0 起链。"""
    barrier = threading.Barrier(8)

    def worker(i: int):
        barrier.wait()
        return core.create_reservation(
            request_id=f"req-{i}",
            start_utc=datetime(2026, 3, 10 + i, 13, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 10 + i, 14, tzinfo=timezone.utc),
            participant_ids=[f"p{i}"],
            db_path=db_path,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(worker, range(8)))
    assert all(created for _, created in outcomes)

    for reservation, _ in outcomes:
        events = core.get_reservation_history(reservation.id, db_path=db_path)
        assert len(events) == 1
        assert events[0]["seq"] == 1
        assert events[0]["prev_hash"] == "0" * 64
        assert events[0]["action"] == "create"


# --------------------------------------------------- 旧数据 baseline 补写

_PRE_AUDIT_SCHEMA = """\
CREATE TABLE reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    start_utc TEXT NOT NULL,
    end_utc TEXT NOT NULL,
    participants_key TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'active',
    canceled_at TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE reservation_participants (
    reservation_id INTEGER NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL,
    start_utc TEXT NOT NULL,
    end_utc TEXT NOT NULL,
    PRIMARY KEY (reservation_id, participant_id)
);
CREATE INDEX idx_rp_participant_window
    ON reservation_participants (participant_id, start_utc, end_utc);
"""


def _seed_pre_audit_db(path: str, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_PRE_AUDIT_SCHEMA)
        for (rid, req, start, end, pids, version, status,
             canceled_at, created) in rows:
            conn.execute(
                "INSERT INTO reservations"
                " (id, request_id, start_utc, end_utc, participants_key, version,"
                "  status, canceled_at, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (rid, req, start, end, json.dumps(pids), version, status,
                 canceled_at, created),
            )
            for pid in pids:
                conn.execute(
                    "INSERT INTO reservation_participants"
                    " (reservation_id, participant_id, start_utc, end_utc)"
                    " VALUES (?, ?, ?, ?)",
                    (rid, pid, start, end),
                )
        conn.commit()
    finally:
        conn.close()


def test_startup_backfills_baseline_for_existing_reservations(tmp_path, monkeypatch):
    path = str(tmp_path / "pre-audit.db")
    _seed_pre_audit_db(path, [
        (1, "req-1",
         "2026-03-09T13:00:00.000000Z", "2026-03-09T14:00:00.000000Z",
         ["alice", "bob"], 2, "canceled",
         "2026-03-05T10:00:00.000000Z", "2026-03-01T00:00:00.000000Z"),
        (2, "req-2",
         "2026-03-10T09:00:00.000000Z", "2026-03-10T10:00:00.000000Z",
         ["carol"], 1, "active", None,
         "2026-03-02T00:00:00.000000Z"),
    ])
    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)

    db_module.init_db(path)
    db_module.init_db(path)  # 幂等：再初始化不重复补写

    e1 = core.get_reservation_history(1, db_path=path)
    e2 = core.get_reservation_history(2, db_path=path)
    assert len(e1) == len(e2) == 1
    assert e1[0]["action"] == e2[0]["action"] == "baseline"
    assert e1[0]["seq"] == e2[0]["seq"] == 1
    assert e1[0]["prev_hash"] == "0" * 64
    # 快照如实反映迁移当时状态，不伪造旧操作
    assert (e1[0]["snapshot"]["version"], e1[0]["snapshot"]["status"]) == (
        2, "canceled")
    assert e1[0]["snapshot"]["canceled_at"] == "2026-03-05T10:00:00.000000Z"
    assert (e2[0]["snapshot"]["version"], e2[0]["snapshot"]["status"]) == (
        1, "active")
    assert e2[0]["snapshot"]["canceled_at"] is None

    # 基线之后的真实操作自然续链（序号 2，prev 指向 baseline 的哈希）
    moved = core.reschedule_reservation(
        2, 1,
        datetime(2026, 3, 10, 11, tzinfo=timezone.utc),
        datetime(2026, 3, 10, 12, tzinfo=timezone.utc),
        ["carol"], db_path=path,
    )
    assert moved.version == 2
    events = core.get_reservation_history(2, db_path=path)
    assert [e["action"] for e in events] == ["baseline", "reschedule"]
    assert events[1]["prev_hash"] == events[0]["event_hash"]
    assert events[1]["seq"] == 2

    # 既有数据无损：参与者占用仍在/按改期迁移
    conn = sqlite3.connect(path)
    try:
        assert conn.execute(
            "SELECT start_utc FROM reservation_participants"
            " WHERE reservation_id=1 AND participant_id='alice'"
        ).fetchone()[0] == "2026-03-09T13:00:00.000000Z"
    finally:
        conn.close()


def test_legacy_table_without_version_columns_also_backfills(tmp_path, monkeypatch):
    """最老版本表（无 version/status/canceled_at）：列补齐 + baseline 一并完成。"""
    path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(path)
    try:
        conn.executescript("""\
CREATE TABLE reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    start_utc TEXT NOT NULL,
    end_utc TEXT NOT NULL,
    participants_key TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE reservation_participants (
    reservation_id INTEGER NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL,
    start_utc TEXT NOT NULL,
    end_utc TEXT NOT NULL,
    PRIMARY KEY (reservation_id, participant_id)
);
""")
        conn.execute(
            "INSERT INTO reservations"
            " (request_id, start_utc, end_utc, participants_key, created_at)"
            " VALUES ('old-req', '2026-03-09T13:00:00.000000Z',"
            " '2026-03-09T14:00:00.000000Z', '[\"alice\"]',"
            " '2026-03-01T00:00:00.000000Z')")
        conn.execute(
            "INSERT INTO reservation_participants"
            " (reservation_id, participant_id, start_utc, end_utc)"
            " VALUES (1, 'alice', '2026-03-09T13:00:00.000000Z',"
            " '2026-03-09T14:00:00.000000Z')")
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    db_module.init_db(path)
    events = core.get_reservation_history(1, db_path=path)
    assert [e["action"] for e in events] == ["baseline"]
    snap = events[0]["snapshot"]
    assert (snap["version"], snap["status"], snap["canceled_at"]) == (
        1, "active", None)
