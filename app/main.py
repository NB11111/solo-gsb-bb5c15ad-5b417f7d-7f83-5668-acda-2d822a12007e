"""FastAPI 应用：跨时区可用时段聚合 + SQLite 会议预约。

启动（容器内）：uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse

from . import db as db_module
from . import reservations as reservation_core
from . import scheduling
from .audit import CANON_FMT as _AUDIT_CANON_FMT, ChainVerificationError
from .reservations import (
    RequestIdConflictError,
    ReservationCanceledError,
    ReservationConflictError,
    ReservationNotFoundError,
    VersionConflictError,
)
from .scheduling import SchedulingError
from .schemas import (
    AggregationRequest,
    AggregationResponse,
    HistoryEventOut,
    LocalTimeOut,
    ParticipantIn,
    ReservationHistoryOut,
    ReservationIn,
    ReservationOut,
    ReservationRescheduleIn,
    SegmentOut,
    WEEKDAY_NAMES,
)
from .scheduling import Participant, Window as CoreWindow


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Compose 启动时自动初始化 SQLite（建表幂等）；
    # 数据库文件路径由 RESERVATIONS_DB_PATH 配置，默认落在挂载卷上
    db_module.init_db()
    yield


app = FastAPI(
    title="跨时区可用时段聚合 API",
    description=(
        "接收日期范围、会议时长、最低出席人数及每位参与者按本地星期/时刻填写的 "
        "重复可用区间与 IANA 时区，返回可安排会议的 UTC 候选时段及各自本地时刻。"
        "DST 按具体日期应用：不存在的本地时刻返回 400，歧义时刻统一取 fold=0。"
        "另提供 SQLite 持久化的会议预约接口：单事务冲突检测 + request_id 幂等，"
        "支持带乐观版本号的改期（PUT）与软删除取消（DELETE，取消后时段立即释放）；"
        "每次成功的创建/改期/取消都在同一事务追加 SHA-256 哈希链审计事件，"
        "可通过 GET 历史接口按序获取并由服务端重验整链。"
    ),
    version="1.4.0",
    lifespan=lifespan,
)


@app.exception_handler(SchedulingError)
async def scheduling_error_handler(request, exc: SchedulingError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(ReservationConflictError)
async def reservation_conflict_handler(request, exc: ReservationConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "conflicts": exc.conflicts,
        },
    )


@app.exception_handler(RequestIdConflictError)
async def request_id_conflict_handler(request, exc: RequestIdConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "existing_reservation_id": exc.existing_reservation_id,
        },
    )


@app.exception_handler(ReservationNotFoundError)
async def reservation_not_found_handler(request, exc: ReservationNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(VersionConflictError)
async def version_conflict_handler(request, exc: VersionConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "current_version": exc.current_version,
        },
    )


@app.exception_handler(ReservationCanceledError)
async def reservation_canceled_handler(request, exc: ReservationCanceledError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "current_version": exc.current_version,
        },
    )


@app.exception_handler(ChainVerificationError)
async def chain_verification_error_handler(
    request, exc: ChainVerificationError
) -> JSONResponse:
    # 验链失败：不返回任何历史内容，仅告知链已损坏的位置/原因
    return JSONResponse(
        status_code=409,
        content={"detail": f"reservation history chain verification failed: {exc}"},
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _to_core_participant(p: ParticipantIn) -> Participant:
    return Participant(
        participant_id=p.id,
        timezone=p.timezone,
        windows=tuple(
            CoreWindow(weekday=w.weekday,
                       start_local=w.start_local,
                       end_local=w.end_local)
            for w in p.availability
        ),
    )


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _segment_to_out(seg: scheduling.Segment,
                    tz_by_id: dict[str, ZoneInfo]) -> SegmentOut:
    local_times: list[LocalTimeOut] = []
    for pid in seg.attendees:
        tzinfo = tz_by_id[pid]
        start_local = seg.start_utc.astimezone(tzinfo)
        end_local = seg.end_utc.astimezone(tzinfo)
        local_times.append(LocalTimeOut(
            participant_id=pid,
            timezone=str(tzinfo),
            start_local=start_local.isoformat(),
            end_local=end_local.isoformat(),
            start_weekday=WEEKDAY_NAMES[start_local.isoweekday()],
            end_weekday=WEEKDAY_NAMES[end_local.isoweekday()],
        ))
    return SegmentOut(
        start_utc=_iso_utc(seg.start_utc),
        end_utc=_iso_utc(seg.end_utc),
        duration_minutes=int(seg.duration.total_seconds() // 60),
        attendees=list(seg.attendees),
        attendee_count=len(seg.attendees),
        local_times=local_times,
    )


@app.post("/api/v1/availability/aggregate", response_model=AggregationResponse)
async def aggregate_availability(req: AggregationRequest) -> AggregationResponse:
    participants = [_to_core_participant(p) for p in req.participants]
    # 提前解析所有时区，未知名称统一返回 400
    tz_by_id = {p.participant_id: scheduling.get_timezone(p.timezone)
                for p in participants}

    segments = scheduling.aggregate(
        participants=participants,
        start_date=req.start_date,
        end_date=req.end_date,
        meeting_minutes=req.meeting_minutes,
        min_attendees=req.min_attendees,
    )

    return AggregationResponse(
        start_date=req.start_date,
        end_date=req.end_date,
        meeting_minutes=req.meeting_minutes,
        min_attendees=req.min_attendees,
        segments=[_segment_to_out(seg, tz_by_id) for seg in segments],
    )


# ---------------------------------------------------------------- reservations


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _reservation_to_out(r: reservation_core.Reservation) -> ReservationOut:
    return ReservationOut(
        reservation_id=r.id,
        request_id=r.request_id,
        start_utc=_iso_z(r.start_utc),
        end_utc=_iso_z(r.end_utc),
        participants=list(r.participants),
        version=r.version,
        status=r.status,
        canceled_at=_iso_z(r.canceled_at) if r.canceled_at is not None else None,
        created_at=_iso_z(r.created_at),
    )


def _snapshot_to_out(snapshot: dict) -> ReservationOut:
    """审计事件中的快照（定宽 UTC 文本）转 API 输出（ISO 8601 带 Z）。"""
    def _z(ts: str) -> str:
        return _iso_z(datetime.strptime(ts, _AUDIT_CANON_FMT)
                      .replace(tzinfo=timezone.utc))

    return ReservationOut(
        reservation_id=snapshot["reservation_id"],
        request_id=snapshot["request_id"],
        start_utc=_z(snapshot["start_utc"]),
        end_utc=_z(snapshot["end_utc"]),
        participants=list(snapshot["participants"]),
        version=snapshot["version"],
        status=snapshot["status"],
        canceled_at=_z(snapshot["canceled_at"])
            if snapshot["canceled_at"] is not None else None,
        created_at=_z(snapshot["created_at"]),
    )


@app.post("/api/v1/reservations", response_model=ReservationOut, status_code=201)
def create_reservation(req: ReservationIn, response: Response) -> ReservationOut:
    """创建会议预约（同步函数，在线程池中执行以避免阻塞事件循环）。

    - 任一参与者与已有预约半开区间重叠 → 409 + 冲突预约/参与者，整体不写库；
    - 相同 request_id + 相同规范化内容 → 200 返回原预约，不新增记录；
    - 相同 request_id + 不同内容 → 409。
    """
    reservation, created = reservation_core.create_reservation(
        request_id=req.request_id,
        start_utc=req.start_utc,
        end_utc=req.end_utc,
        participant_ids=req.participant_ids,
    )
    if not created:
        # 幂等重放：返回原预约
        response.status_code = 200
    return _reservation_to_out(reservation)


@app.put("/api/v1/reservations/{reservation_id}", response_model=ReservationOut)
def reschedule_reservation(
    reservation_id: int, req: ReservationRescheduleIn
) -> ReservationOut:
    """改期（乐观并发控制）。

    - 预约不存在 → 404；
    - 预约已取消 → 409（取消是终态，改期不会使其复活）；
    - ``expected_version`` 与当前版本不一致 → 409，响应给出 ``current_version``；
    - 任一新参与者与**其他**预约半开区间重叠（自动排除自身）→ 409 + 冲突明细；
    - 成功 → 原子替换时间与参与者占用，``version`` 加 1，返回更新后的预约。

    所有检查与写入在单个 ``BEGIN IMMEDIATE`` 事务内完成；任一步失败，
    预约及其参与者占用均保持原状。
    """
    reservation = reservation_core.reschedule_reservation(
        reservation_id=reservation_id,
        expected_version=req.expected_version,
        start_utc=req.start_utc,
        end_utc=req.end_utc,
        participant_ids=req.participant_ids,
    )
    return _reservation_to_out(reservation)


@app.delete("/api/v1/reservations/{reservation_id}", response_model=ReservationOut)
def cancel_reservation(
    reservation_id: int,
    expected_version: int = Query(
        ge=1,
        description="客户端上次读到的预约版本；与当前版本一致才执行取消",
    ),
) -> ReservationOut:
    """取消预约（软删除，乐观并发控制）。

    - 预约不存在 → 404；
    - ``expected_version`` 与当前版本不一致 → 409，响应给出 ``current_version``；
    - 成功 → 单个 ``BEGIN IMMEDIATE`` 事务内把预约置为 ``canceled``、记录
      ``canceled_at``、``version`` 加 1，并删除全部参与者占用，
      时段提交后立即可再次预约；
    - 相同 ``reservation_id`` + 原 ``expected_version`` 的重试 → 幂等返回
      同一取消结果（同样的 ``canceled_at`` 与版本），不重复变更；
    - 取消是终态：之后 PUT 改期一律 409，不会复活。
    """
    reservation, _ = reservation_core.cancel_reservation(
        reservation_id=reservation_id,
        expected_version=expected_version,
    )
    return _reservation_to_out(reservation)


@app.get(
    "/api/v1/reservations/{reservation_id}/history",
    response_model=ReservationHistoryOut,
)
def get_reservation_history(reservation_id: int) -> ReservationHistoryOut:
    """预约变更历史：按 ``seq`` 升序返回哈希链审计事件，返回前重验整链。

    - 预约不存在 → ``404``；
    - 哈希链任一环重验失败（序号断档、``prev_hash`` 不衔接、
      ``canonical_event`` 与哈希不符等）→ ``409``，**不返回任何历史**；
    - 校验通过 → ``200``，事件包含动作（``created`` / ``rescheduled`` /
      ``canceled`` / ``baseline``）、序号、时间、预约快照、前后哈希与
      canonical_event。``baseline`` 为升级初始化时为既有预约补写的快照，
      不是伪造的历史操作。

    仅成功的创建/改期/取消会产生事件；失败请求与幂等重放不产生事件。
    """
    events = reservation_core.get_reservation_history(reservation_id)
    return ReservationHistoryOut(
        reservation_id=reservation_id,
        events=[
            HistoryEventOut(
                seq=e["seq"],
                action=e["action"],
                timestamp=_iso_z(datetime.strptime(e["timestamp"], _AUDIT_CANON_FMT)
                                 .replace(tzinfo=timezone.utc)),
                snapshot=_snapshot_to_out(e["snapshot"]),
                prev_hash=e["prev_hash"],
                hash=e["hash"],
                canonical_event=e["canonical_event"],
            )
            for e in events
        ],
    )
