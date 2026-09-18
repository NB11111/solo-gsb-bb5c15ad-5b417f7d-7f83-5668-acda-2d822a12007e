"""预约变更历史：在同一数据库事务内追加的可校验哈希链审计事件。

每次成功的创建（``create``）、改期（``reschedule``）、取消（``cancel``）
都在**调用方已开启的同一事务**内追加一条审计事件，记录：

- ``action``：动作（``create`` / ``reschedule`` / ``cancel``，
  旧数据初始化补写为 ``baseline``）；
- ``seq``：该预约维度下从 1 递增、无间断的序号；
- ``event_at``：事件时间（UTC 定宽文本）；
- ``snapshot``：事件落定后预约的完整快照。

事件的规范形式 ``canonical_event`` 为**不含哈希字段、键按字典序排列、
非 ASCII 字符不转义、无任何空白**的 JSON：

    {"action":...,"event_at":...,"seq":...,"snapshot":{...}}

每条事件的哈希为

    SHA-256(prev_hash 的 ASCII 字节 + canonical_event 的 UTF-8 字节)

的十六进制摘要；同一预约的事件由此串成哈希链，首条事件的 ``prev_hash``
为 64 个 ``0``。读取历史时重放整条链（序号连续性、前后哈希衔接、
摘要重算、规范形式重现、列与载荷一致性），任一环节不符即抛
:class:`ChainVerificationError`，由 API 层映射为 ``409`` 且不返回历史。
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

ACTION_CREATE = "create"
ACTION_RESCHEDULE = "reschedule"
ACTION_CANCEL = "cancel"
# 升级前已存在的预约：初始化时补写一条基线事件，不伪造其过去的创建/改期/取消
ACTION_BASELINE = "baseline"

ACTIONS = frozenset({
    ACTION_CREATE, ACTION_RESCHEDULE, ACTION_CANCEL, ACTION_BASELINE,
})

# 首条事件的前驱哈希：64 个 ASCII "0"
GENESIS_HASH = "0" * 64

# 与 reservations 表一致的定宽 UTC 文本格式（字典序即时间序）
_CANON_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

_EVENT_PAYLOAD_KEYS = frozenset({"action", "seq", "event_at", "snapshot"})
_SNAPSHOT_KEYS = frozenset({
    "reservation_id", "request_id", "start_utc", "end_utc", "participants",
    "version", "status", "canceled_at", "created_at",
})
_EVENT_COLUMNS = (
    "reservation_id, seq, action, event_at, snapshot,"
    " prev_hash, event_hash, canonical_event"
)

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


class ChainVerificationError(Exception):
    """审计哈希链重验失败：序号断裂、前后哈希不接、摘要不符或载荷被改动。"""


def canonical_time(dt: datetime) -> str:
    """统一成定宽 UTC 字符串。"""
    return dt.astimezone(timezone.utc).strftime(_CANON_FMT)


def _canonical_json(value: Any) -> str:
    """键排序、非 ASCII 不转义、无空白分隔符的紧凑 JSON。"""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def canonical_event(action: str, seq: int, event_at: str,
                    snapshot: dict[str, Any]) -> str:
    """构造不含哈希字段的规范事件 JSON（哈希链的被摘要内容）。"""
    payload = {
        "action": action,
        "seq": seq,
        "event_at": event_at,
        "snapshot": snapshot,
    }
    return _canonical_json(payload)


def chain_hash(prev_hash: str, canonical: str) -> str:
    """``SHA-256(prev_hash 的 ASCII 字节 + canonical_event 的 UTF-8 字节)``。"""
    return hashlib.sha256(
        prev_hash.encode("ascii") + canonical.encode("utf-8")
    ).hexdigest()


def append_event(
    conn: sqlite3.Connection,
    reservation_id: int,
    action: str,
    snapshot: dict[str, Any],
    event_at: str | None = None,
) -> dict[str, Any]:
    """在**调用方当前事务**内追加一条审计事件并返回其记录。

    - 序号取该预约当前最大 ``seq`` + 1（无事件时为 1）；
    - ``prev_hash`` 取上一条事件的 ``event_hash``（首条为 64 个 ``0``）；
    - ``event_at`` 可由调用方显式给出（创建/取消时复用同一事务内为
      ``created_at`` / ``canceled_at`` 戳定的时刻，保证事件时间与快照内
      对应时间戳完全一致）；省略时在事务内当场戳定（UTC 定宽文本）。

    本函数不自行开关事务：它依赖调用方的 ``BEGIN IMMEDIATE`` 上下文，
    因而与预约行/占用行的写入同生共死——任一步失败，外层统一回滚，
    审计事件不会单独留下或缺失。
    """
    if action not in ACTIONS:
        raise ValueError(f"unknown audit action: {action!r}")

    last = conn.execute(
        "SELECT seq, event_hash FROM reservation_events"
        " WHERE reservation_id = ?"
        " ORDER BY seq DESC LIMIT 1",
        (reservation_id,),
    ).fetchone()
    if last is None:
        seq = 1
        prev_hash = GENESIS_HASH
    else:
        seq = int(last["seq"]) + 1
        prev_hash = last["event_hash"]

    if event_at is None:
        event_at = canonical_time(datetime.now(timezone.utc))
    canonical = canonical_event(action, seq, event_at, snapshot)
    event_hash = chain_hash(prev_hash, canonical)
    # 快照列独立存一份规范 JSON；它不参与摘要（摘要内容以 canonical_event 为准），
    # 读取时仍会与载荷里的 snapshot 做一致性比对。
    snapshot_json = _canonical_json(snapshot)

    conn.execute(
        "INSERT INTO reservation_events"
        " (reservation_id, seq, action, event_at, snapshot,"
        "  prev_hash, event_hash, canonical_event)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (reservation_id, seq, action, event_at, snapshot_json,
         prev_hash, event_hash, canonical),
    )

    return {
        "reservation_id": reservation_id,
        "seq": seq,
        "action": action,
        "event_at": event_at,
        "snapshot": snapshot,
        "prev_hash": prev_hash,
        "event_hash": event_hash,
        "canonical_event": canonical,
    }


def _fail(reservation_id: int, reason: str) -> None:
    raise ChainVerificationError(
        f"reservation #{reservation_id} audit chain verification failed: {reason}"
    )


def verify_chain(
    reservation_id: int,
    rows: list[sqlite3.Row],
) -> list[dict[str, Any]]:
    """按序重放并校验某预约的全部事件，返回事件记录列表（含快照）。

    校验内容：

    1. 链非空（预约存在却没有任何事件，说明审计被破坏或未正确初始化）；
    2. ``seq`` 从 1 开始严格 +1，无重复、无间断；
    3. 每条 ``prev_hash`` 等于上一条 ``event_hash``，首条为 64 个 ``0``，
       哈希字段均为 64 位小写十六进制；
    4. 用存储的 ``canonical_event`` 与 ``prev_hash`` 重算 SHA-256，
       与存储的 ``event_hash`` 完全一致；
    5. 重新解析 ``canonical_event`` 后再规范化，结果必须与存储逐字节一致
       （保证键排序、无空白、非 ASCII 不转义、且不含任何哈希字段）；
    6. 载荷中的 ``action`` / ``seq`` / ``event_at`` 与独立列一致，
       ``snapshot`` 列与载荷内嵌快照一致，快照属于该预约。
    """
    if not rows:
        _fail(reservation_id, "chain is empty")

    events: list[dict[str, Any]] = []
    expected_prev_hash = GENESIS_HASH
    expected_seq = 1

    for row in rows:
        seq = row["seq"]
        action = row["action"]
        event_at = row["event_at"]
        stored_prev = row["prev_hash"]
        stored_hash = row["event_hash"]
        canonical = row["canonical_event"]

        if seq != expected_seq:
            _fail(reservation_id,
                  f"sequence gap: expected seq {expected_seq}, got {seq}")
        if not _SHA256_HEX_RE.match(stored_prev or ""):
            _fail(reservation_id, f"seq {seq}: malformed prev_hash")
        if not _SHA256_HEX_RE.match(stored_hash or ""):
            _fail(reservation_id, f"seq {seq}: malformed event_hash")
        if stored_prev != expected_prev_hash:
            _fail(reservation_id,
                  f"seq {seq}: prev_hash does not link to previous event")
        if action not in ACTIONS:
            _fail(reservation_id, f"seq {seq}: unknown action {action!r}")

        try:
            payload = json.loads(canonical)
        except (TypeError, ValueError) as exc:
            _fail(reservation_id, f"seq {seq}: canonical_event is not JSON ({exc})")

        if not isinstance(payload, dict) or set(payload) != _EVENT_PAYLOAD_KEYS:
            _fail(reservation_id,
                  f"seq {seq}: canonical_event must contain exactly "
                  "action/seq/event_at/snapshot and no hash fields")

        p_action, p_seq, p_event_at, p_snapshot = (
            payload["action"], payload["seq"], payload["event_at"], payload["snapshot"]
        )
        if not isinstance(p_seq, int) or isinstance(p_seq, bool) or p_seq != seq:
            _fail(reservation_id, f"seq {seq}: payload seq mismatch")
        if p_action != action or p_event_at != event_at:
            _fail(reservation_id, f"seq {seq}: payload columns mismatch")
        if not isinstance(p_event_at, str) or not p_event_at:
            _fail(reservation_id, f"seq {seq}: invalid event_at")
        if not isinstance(p_snapshot, dict):
            _fail(reservation_id, f"seq {seq}: snapshot must be an object")
        if set(p_snapshot) != _SNAPSHOT_KEYS:
            _fail(reservation_id,
                  f"seq {seq}: snapshot keys missing or unexpected")
        if p_snapshot.get("reservation_id") != reservation_id:
            _fail(reservation_id, f"seq {seq}: snapshot belongs to another reservation")

        # 重新规范化必须与存储逐字节相同：键排序 / 无空白 / 非 ASCII 原样
        try:
            reproduced = canonical_event(p_action, p_seq, p_event_at, p_snapshot)
        except (TypeError, ValueError) as exc:
            _fail(reservation_id, f"seq {seq}: non-canonical payload ({exc})")
        if reproduced != canonical:
            _fail(reservation_id, f"seq {seq}: canonical_event is not in canonical form")

        # 摘要重算（prev_hash 按 ASCII、canonical_event 按 UTF-8 取字节）
        if chain_hash(stored_prev, canonical) != stored_hash:
            _fail(reservation_id, f"seq {seq}: hash mismatch")

        try:
            stored_snapshot = json.loads(row["snapshot"])
        except (TypeError, ValueError) as exc:
            _fail(reservation_id, f"seq {seq}: snapshot column is not JSON ({exc})")
        if stored_snapshot != p_snapshot:
            _fail(reservation_id, f"seq {seq}: snapshot column mismatch")

        events.append({
            "reservation_id": reservation_id,
            "seq": seq,
            "action": action,
            "event_at": event_at,
            "snapshot": p_snapshot,
            "prev_hash": stored_prev,
            "event_hash": stored_hash,
            "canonical_event": canonical,
        })
        expected_prev_hash = stored_hash
        expected_seq += 1

    return events
