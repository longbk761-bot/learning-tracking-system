"""
API Endpoints – Learning Behavior Tracker
"""
import uuid
import random
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from cassandra.query import BatchStatement, BatchType, ConsistencyLevel

from app.database import DB
from app.models import LearningEvent, EventBatch, BulkSeedRequest

router = APIRouter()

EVENT_TYPES   = ["video_watch", "quiz_submit", "click", "login", "logout"]
COURSE_IDS    = ["CS101", "DB201", "AI301", "NET401", "SE501"]
STUDENT_NAMES = {
    "SV001": "Nguyễn Văn An",   "SV002": "Trần Thị Bình",
    "SV003": "Lê Minh Cường",   "SV004": "Phạm Thu Dung",
    "SV005": "Hoàng Văn Em",    "SV006": "Vũ Thị Phương",
    "SV007": "Đinh Quang Hải",  "SV008": "Bùi Lan Anh",
    "SV009": "Ngô Đức Minh",    "SV010": "Đặng Thị Lan",
}


# ────────────────────────────────────────────────
# POST /events – Ghi 1 sự kiện
# ────────────────────────────────────────────────
@router.post("/events", status_code=201)
async def create_event(evt: LearningEvent, bg: BackgroundTasks):
    """Ghi 1 sự kiện học tập. Prepared Statement → latency ~2-5ms."""
    now = datetime.utcnow()
    eid = uuid.uuid4()
    DB.session().execute_async(
        DB.stmt("insert_event"),
        (
            evt.student_id, now, eid,
            evt.event_type, evt.course_id, evt.lesson_id,
            evt.duration_sec, evt.score, evt.progress_pct,
            {str(k): str(v) for k, v in (evt.metadata or {}).items()},
        )
    )
    bg.add_task(_update_summary, evt, now)
    return {"event_id": str(eid), "ts": now.isoformat(), "status": "ok"}


# ────────────────────────────────────────────────
# POST /events/batch – Ghi nhiều sự kiện
# ────────────────────────────────────────────────
@router.post("/events/batch", status_code=201)
async def create_batch(payload: EventBatch):
    """
    UNLOGGED BATCH – chia theo partition (student_id).
    Không block response, ghi bất đồng bộ.
    """
    now = datetime.utcnow()
    partitions: dict = {}
    for e in payload.events:
        partitions.setdefault(e.student_id, []).append(e)

    for sid, evts in partitions.items():
        batch = BatchStatement(
            batch_type=BatchType.UNLOGGED,
            consistency_level=ConsistencyLevel.ANY,
        )
        for e in evts:
            batch.add(DB.stmt("insert_event"), (
                sid, now, uuid.uuid4(),
                e.event_type, e.course_id, e.lesson_id,
                e.duration_sec, e.score, e.progress_pct,
                {str(k): str(v) for k, v in (e.metadata or {}).items()},
            ))
        DB.session().execute_async(batch)

    return {
        "accepted": len(payload.events),
        "partitions": len(partitions),
        "ts": now.isoformat(),
    }


# ────────────────────────────────────────────────
# GET /students/{id}/history – Lịch sử sinh viên
# ────────────────────────────────────────────────
@router.get("/students/{student_id}/history")
async def get_history(
    student_id: str,
    days:  int = Query(30, ge=1, le=90),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Partition Key Lookup – O(1).
    Clustering Key (event_time DESC) → không cần ORDER BY tốn CPU.
    """
    since = datetime.utcnow() - timedelta(days=days)
    rows = DB.session().execute(
        DB.stmt("get_history"), (student_id, since, limit)
    )
    events = []
    for r in rows:
        events.append({
            "event_time":   r.event_time.isoformat(),
            "event_id":     str(r.event_id),
            "event_type":   r.event_type,
            "course_id":    r.course_id,
            "duration_sec": r.duration_sec,
            "score":        r.score,
            "progress_pct": r.progress_pct,
        })
    return {
        "student_id":  student_id,
        "name":        STUDENT_NAMES.get(student_id, "Unknown"),
        "days":        days,
        "total":       len(events),
        "events":      events,
    }


# ────────────────────────────────────────────────
# GET /students/{id}/summary – Tổng hợp theo tháng
# ────────────────────────────────────────────────
@router.get("/students/{student_id}/summary")
async def get_summary(student_id: str):
    rows = DB.session().execute(DB.stmt("get_summary"), (student_id,))
    return {
        "student_id": student_id,
        "name":       STUDENT_NAMES.get(student_id, "Unknown"),
        "monthly":    [
            {
                "month":          r.month_bucket,
                "total_events":   r.total_events,
                "total_video_sec":r.total_video_sec,
                "total_quiz_done":r.total_quiz_done,
                "avg_quiz_score": round(r.avg_quiz_score or 0, 1),
                "active_days":    r.active_days,
                "video_skip_pct": round((r.video_skip_pct or 0) * 100, 1),
                "risk_score":     round(r.risk_score or 0, 1),
                "risk_label":     r.risk_label or "LOW",
            }
            for r in rows
        ],
    }


# ────────────────────────────────────────────────
# GET /students – Danh sách sinh viên demo
# ────────────────────────────────────────────────
@router.get("/students")
async def list_students():
    return {"students": [
        {"student_id": sid, "name": name}
        for sid, name in STUDENT_NAMES.items()
    ]}


# ────────────────────────────────────────────────
# POST /seed – Tạo dữ liệu demo
# ────────────────────────────────────────────────
@router.post("/seed", status_code=201)
async def seed_data(req: BulkSeedRequest):
    """
    Tạo dữ liệu ngẫu nhiên để demo.
    Ghi trực tiếp vào AstraDB – chứng minh tốc độ batch write.
    """
    now = datetime.utcnow()
    total = 0
    students = list(STUDENT_NAMES.keys())[:req.num_students]

    for sid in students:
        batch = BatchStatement(
            batch_type=BatchType.UNLOGGED,
            consistency_level=ConsistencyLevel.ANY,
        )
        for _ in range(req.events_per_student):
            days_ago = random.uniform(0, 30)
            ts = now - timedelta(days=days_ago)
            etype = random.choice(EVENT_TYPES)
            batch.add(DB.stmt("insert_event"), (
                sid, ts, uuid.uuid4(),
                etype,
                random.choice(COURSE_IDS),
                f"lesson_{random.randint(1, 20)}",
                random.randint(30, 3600) if etype == "video_watch" else random.randint(5, 300),
                random.uniform(40, 100) if etype == "quiz_submit" else None,
                random.uniform(0.1, 1.0) if etype == "video_watch" else None,
                {"device": random.choice(["mobile", "desktop", "tablet"])},
            ))
            total += 1
        DB.session().execute(batch)

    return {
        "seeded_students": len(students),
        "total_events":    total,
        "message":         f"✅ Đã tạo {total} sự kiện demo vào AstraDB",
    }


# ────────────────────────────────────────────────
# GET /analytics/stream – Giả lập live stream stats
# ────────────────────────────────────────────────
@router.get("/analytics/stream")
async def stream_stats():
    """Trả về thống kê hiện tại (dùng cho biểu đồ live)."""
    return {
        "events_per_sec": random.randint(8, 28),
        "active_students": random.randint(15, 45),
        "write_latency_ms": random.uniform(1.2, 6.8),
        "read_latency_ms":  random.uniform(2.0, 9.5),
        "ts": datetime.utcnow().isoformat(),
    }


# ────────────────────────────────────────────────
# Background task helper
# ────────────────────────────────────────────────
def _update_summary(evt: LearningEvent, ts: datetime):
    month = ts.strftime("%Y-%m")
    is_video = 1 if evt.event_type == "video_watch" else 0
    is_quiz  = 1 if evt.event_type == "quiz_submit"  else 0
    vid_sec  = evt.duration_sec if is_video else 0
    try:
        DB.session().execute(
            DB.stmt("upsert_summary"),
            (1, vid_sec or 0, is_quiz, ts, evt.student_id, month),
        )
    except Exception:
        pass  # Không ảnh hưởng API response
