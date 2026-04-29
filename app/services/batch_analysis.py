"""
BATCH ANALYSIS – Phát hiện mẫu hành vi bất thường
Hệ thống Tracking Hành vi Học tập

Chạy định kỳ (VD: mỗi đêm 2:00 AM qua cron hoặc Celery Beat):
    python batch_analysis.py --month 2025-06 --course CS101

Thuật toán:
  Tính Risk Score (0–100) dựa trên 5 tín hiệu hành vi.
  Sinh viên risk cao → ghi vào bảng high_risk_students → UI cảnh báo sớm.
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import ConsistencyLevel
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Ngưỡng phát hiện bất thường
# ─────────────────────────────────────────────
THRESHOLDS = {
    "min_events_per_week":  5,      # < 5 sự kiện/tuần → inactive
    "min_video_ratio":      0.20,   # < 20% video trong tổng events → ít xem video
    "max_skip_ratio":       0.60,   # > 60% video bị bỏ qua (progress < 0.3)
    "min_avg_quiz_score":   50.0,   # Điểm quiz trung bình < 50
    "max_inactive_days":    5,      # > 5 ngày liên tiếp không đăng nhập
    "high_risk_threshold":  65,     # Risk Score >= 65 → HIGH RISK
    "medium_risk_threshold":35,     # Risk Score 35–64 → MEDIUM RISK
}

# Trọng số của từng tín hiệu trong Risk Score
WEIGHTS = {
    "w_inactive":    0.30,   # Số ngày không tương tác
    "w_video_skip":  0.25,   # Tỷ lệ bỏ qua video
    "w_low_event":   0.20,   # Ít sự kiện
    "w_quiz_score":  0.15,   # Điểm thấp
    "w_no_video":    0.10,   # Không xem video
}


@dataclass
class StudentBehavior:
    """Tổng hợp hành vi 1 sinh viên trong 1 tháng."""
    student_id: str
    events:            List[dict] = field(default_factory=list)

    # Tính toán sau khi aggregate
    total_events:      int   = 0
    active_days:       int   = 0
    max_inactive_days: int   = 0
    video_count:       int   = 0
    video_skip_count:  int   = 0    # video có progress < 0.3
    quiz_count:        int   = 0
    quiz_score_sum:    float = 0.0
    risk_score:        float = 0.0
    risk_label:        str   = "LOW"
    anomalies:         List[str] = field(default_factory=list)


# ─────────────────────────────────────────────
# Kết nối DB
# ─────────────────────────────────────────────
def connect_db():
    auth = PlainTextAuthProvider(
        username=os.getenv("ASTRA_DB_CLIENT_ID"),
        password=os.getenv("ASTRA_DB_CLIENT_SECRET"),
    )
    cluster = Cluster(
        cloud={"secure_connect_bundle": os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH")},
        auth_provider=auth,
    )
    session = cluster.connect(os.getenv("ASTRA_DB_KEYSPACE", "learning_tracker"))
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    log.info("✅  AstraDB connected")
    return cluster, session


# ─────────────────────────────────────────────
# Bước 1: Quét dữ liệu log từ Cassandra
# ─────────────────────────────────────────────
def scan_events(session, month: str, course_id: str = None) -> Dict[str, StudentBehavior]:
    """
    Đọc toàn bộ sự kiện của tháng từ course_event_log.
    Sử dụng token-based pagination để không OOM với dữ liệu lớn.
    """
    year, mon = map(int, month.split("-"))
    since = datetime(year, mon, 1)
    until = (since + timedelta(days=32)).replace(day=1)

    log.info(f"📥  Scanning events: {since.date()} → {until.date()}")

    if course_id:
        cql = """
            SELECT student_id, event_time, event_type, duration_sec, score, progress_pct
            FROM course_event_log
            WHERE course_id = %s AND event_time >= %s AND event_time < %s
        """
        rows = session.execute(cql, (course_id, since, until), timeout=60)
    else:
        # Full scan – chỉ dùng cho môi trường dev/test nhỏ
        # Production: dùng Spark Cassandra Connector cho full table scan
        cql = """
            SELECT student_id, event_time, event_type, duration_sec, score, progress_pct
            FROM learning_events
            WHERE event_time >= %s AND event_time < %s
            ALLOW FILTERING
        """
        rows = session.execute(cql, (since, until), timeout=120)

    behaviors: Dict[str, StudentBehavior] = {}
    total_rows = 0

    for row in rows:
        sv = row.student_id
        if sv not in behaviors:
            behaviors[sv] = StudentBehavior(student_id=sv)
        behaviors[sv].events.append({
            "ts":       row.event_time,
            "type":     row.event_type,
            "dur":      row.duration_sec or 0,
            "score":    row.score,
            "progress": row.progress_pct,
        })
        total_rows += 1

    log.info(f"📊  Loaded {total_rows:,} events for {len(behaviors)} students")
    return behaviors


# ─────────────────────────────────────────────
# Bước 2: Tính toán tín hiệu hành vi
# ─────────────────────────────────────────────
def compute_signals(beh: StudentBehavior) -> None:
    """
    Phân tích 5 tín hiệu hành vi bất thường từ raw events.
    """
    if not beh.events:
        beh.risk_score = 100.0
        beh.risk_label = "HIGH"
        beh.anomalies  = ["Không có sự kiện nào trong tháng"]
        return

    beh.total_events = len(beh.events)
    dates = sorted({e["ts"].date() for e in beh.events})
    beh.active_days = len(dates)

    # ── Tín hiệu 1: Số ngày bất hoạt liên tiếp ──
    if len(dates) > 1:
        gaps = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
        beh.max_inactive_days = max(gaps)
    else:
        beh.max_inactive_days = 30  # Chỉ hoạt động 1 ngày

    # ── Tín hiệu 2: Tỷ lệ xem video & skip ──
    videos = [e for e in beh.events if e["type"] == "video_watch"]
    beh.video_count = len(videos)
    beh.video_skip_count = sum(
        1 for v in videos if (v["progress"] or 0) < 0.3
    )

    # ── Tín hiệu 3: Điểm quiz ──
    quizzes = [e for e in beh.events if e["type"] == "quiz_submit"]
    beh.quiz_count = len(quizzes)
    beh.quiz_score_sum = sum(q["score"] or 0 for q in quizzes)


def compute_risk_score(beh: StudentBehavior) -> None:
    """
    Risk Score = tổng có trọng số của 5 tín hiệu bất thường.
    Output: 0 (an toàn) → 100 (nguy cơ cao).
    """
    scores = {}

    # S1: Inactive days (0–30) → normalize về 0–100
    s1 = min(beh.max_inactive_days / 14.0, 1.0) * 100
    scores["inactive"] = s1
    if beh.max_inactive_days > THRESHOLDS["max_inactive_days"]:
        beh.anomalies.append(
            f"Bất hoạt {beh.max_inactive_days} ngày liên tiếp"
        )

    # S2: Video skip ratio
    if beh.video_count > 0:
        skip_ratio = beh.video_skip_count / beh.video_count
        s2 = min(skip_ratio / THRESHOLDS["max_skip_ratio"], 1.0) * 100
        if skip_ratio > THRESHOLDS["max_skip_ratio"]:
            beh.anomalies.append(
                f"Bỏ qua {skip_ratio*100:.0f}% video (< 30% progress)"
            )
    else:
        s2 = 100  # Không xem video nào → điểm tối đa
        beh.anomalies.append("Không xem video nào")
    scores["video_skip"] = s2

    # S3: Tần suất sự kiện thấp (< 5/tuần = < 21.5/tháng)
    expected = THRESHOLDS["min_events_per_week"] * 4.3  # ~4.3 tuần/tháng
    s3 = max(0, (1.0 - beh.total_events / expected)) * 100
    if beh.total_events < expected:
        beh.anomalies.append(
            f"Ít tương tác ({beh.total_events} sự kiện/tháng)"
        )
    scores["low_event"] = s3

    # S4: Điểm quiz thấp
    if beh.quiz_count > 0:
        avg_score = beh.quiz_score_sum / beh.quiz_count
        s4 = max(0, (1.0 - avg_score / 100.0)) * 100
        if avg_score < THRESHOLDS["min_avg_quiz_score"]:
            beh.anomalies.append(
                f"Điểm quiz trung bình thấp ({avg_score:.1f}/100)"
            )
    else:
        s4 = 60  # Chưa làm quiz – rủi ro trung bình
    scores["quiz_score"] = s4

    # S5: Tỷ lệ video trong tổng sự kiện
    video_ratio = beh.video_count / max(beh.total_events, 1)
    s5 = max(0, (1.0 - video_ratio / THRESHOLDS["min_video_ratio"])) * 100
    scores["no_video"] = s5

    # Tổng hợp có trọng số
    beh.risk_score = round(
        scores["inactive"]   * WEIGHTS["w_inactive"] +
        scores["video_skip"] * WEIGHTS["w_video_skip"] +
        scores["low_event"]  * WEIGHTS["w_low_event"] +
        scores["quiz_score"] * WEIGHTS["w_quiz_score"] +
        scores["no_video"]   * WEIGHTS["w_no_video"],
        2
    )

    # Phân loại
    if beh.risk_score >= THRESHOLDS["high_risk_threshold"]:
        beh.risk_label = "HIGH"
    elif beh.risk_score >= THRESHOLDS["medium_risk_threshold"]:
        beh.risk_label = "MEDIUM"
    else:
        beh.risk_label = "LOW"


# ─────────────────────────────────────────────
# Bước 3: Ghi kết quả vào Cassandra
# ─────────────────────────────────────────────
INSERT_SUMMARY_CQL = """
    INSERT INTO student_monthly_summary
      (student_id, month_bucket, total_events, total_video_sec, total_quiz_done,
       avg_quiz_score, active_days, video_skip_pct, risk_score, risk_label, last_active)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_HIGH_RISK_CQL = """
    INSERT INTO high_risk_students
      (course_id, risk_label, risk_score, student_id, detected_at,
       inactive_days, video_skip_pct, recommendation)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

def write_results(session, behaviors: Dict[str, StudentBehavior],
                  month: str, course_id: str = "ALL") -> Tuple[int, int]:
    """Ghi kết quả phân tích vào Cassandra. Trả về (total, high_risk_count)."""
    now = datetime.utcnow()
    high_risk_count = 0

    for beh in behaviors.values():
        avg_score = (beh.quiz_score_sum / beh.quiz_count) if beh.quiz_count else 0
        skip_pct  = (beh.video_skip_count / beh.video_count) if beh.video_count else 0
        vid_sec   = sum(e["dur"] for e in beh.events if e["type"] == "video_watch")

        # Ghi summary
        session.execute(INSERT_SUMMARY_CQL, (
            beh.student_id, month, beh.total_events, vid_sec,
            beh.quiz_count, round(avg_score, 2),
            beh.active_days, round(skip_pct, 4),
            beh.risk_score, beh.risk_label, now,
        ))

        # Ghi cảnh báo nếu nguy cơ cao
        if beh.risk_label in ("HIGH", "MEDIUM"):
            high_risk_count += 1
            rec = _generate_recommendation(beh)
            session.execute(INSERT_HIGH_RISK_CQL, (
                course_id, beh.risk_label, beh.risk_score / 100.0,
                beh.student_id, now, beh.max_inactive_days,
                round(skip_pct, 4), rec,
            ))

    return len(behaviors), high_risk_count


def _generate_recommendation(beh: StudentBehavior) -> str:
    """Tạo khuyến nghị can thiệp dựa trên mẫu bất thường."""
    if not beh.anomalies:
        return "Theo dõi thêm"
    primary = beh.anomalies[0]
    if "Bất hoạt" in primary:
        return "Liên hệ sinh viên qua email – nhắc nhở học tập"
    if "video" in primary.lower():
        return "Gợi ý xem lại bài giảng – gửi tóm tắt nội dung"
    if "Điểm quiz" in primary:
        return "Mời tham gia buổi ôn tập – kết nối với tutor"
    return "Tư vấn học tập – kiểm tra khó khăn cá nhân"


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def run(month: str, course_id: str = None):
    log.info(f"🚀  Batch Analysis bắt đầu – month={month}, course={course_id or 'ALL'}")
    t0 = datetime.utcnow()

    cluster, session = connect_db()

    try:
        # Bước 1: Quét log
        behaviors = scan_events(session, month, course_id)

        # Bước 2: Phân tích từng sinh viên
        for beh in behaviors.values():
            compute_signals(beh)
            compute_risk_score(beh)

        # Bước 3: Ghi kết quả
        total, high = write_results(session, behaviors, month, course_id or "ALL")

        elapsed = (datetime.utcnow() - t0).total_seconds()
        log.info(f"✅  Hoàn thành: {total} sinh viên, {high} nguy cơ cao – {elapsed:.1f}s")

        # In báo cáo
        _print_report(behaviors)

    finally:
        cluster.shutdown()


def _print_report(behaviors: Dict[str, StudentBehavior]):
    high   = [b for b in behaviors.values() if b.risk_label == "HIGH"]
    medium = [b for b in behaviors.values() if b.risk_label == "MEDIUM"]
    low    = [b for b in behaviors.values() if b.risk_label == "LOW"]

    print("\n" + "═"*60)
    print("  BÁO CÁO PHÂN TÍCH HÀNH VI HỌC TẬP")
    print("═"*60)
    print(f"  Tổng sinh viên phân tích : {len(behaviors)}")
    print(f"  🔴 Nguy cơ cao (HIGH)    : {len(high)}")
    print(f"  🟡 Cần theo dõi (MEDIUM) : {len(medium)}")
    print(f"  🟢 Bình thường (LOW)     : {len(low)}")
    print("─"*60)
    if high:
        print("  TOP 5 sinh viên cần can thiệp ngay:")
        for b in sorted(high, key=lambda x: -x.risk_score)[:5]:
            print(f"    [{b.risk_score:.0f}/100] {b.student_id} – {b.anomalies[0] if b.anomalies else ''}")
    print("═"*60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Analysis – Learning Behavior")
    parser.add_argument("--month",  required=True, help="VD: 2025-06")
    parser.add_argument("--course", default=None,  help="Course ID (bỏ trống = tất cả)")
    args = parser.parse_args()
    run(args.month, args.course)
