-- =============================================================
-- HỆ THỐNG TRACKING HÀNH VI HỌC TẬP
-- Database: AstraDB (Apache Cassandra)
-- Thiết kế: Query-First / Partition Key tối ưu hóa
-- =============================================================

-- Tạo Keyspace (trên AstraDB thực tế được tạo qua giao diện)
-- CREATE KEYSPACE IF NOT EXISTS learning_tracker
--   WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}
--   AND durable_writes = true;

USE learning_tracker;

-- =============================================================
-- BẢNG 1: learning_events
-- Mục đích: Ghi nhận TOÀN BỘ sự kiện học tập theo từng sinh viên
--
-- Partition Key:  student_id  → Tất cả sự kiện của 1 SV nằm cùng node
-- Clustering Key: event_time DESC → Lấy sự kiện mới nhất trước
--                 event_id     → Phân biệt 2 sự kiện cùng mili-giây
--
-- Query mục tiêu:
--   SELECT * FROM learning_events
--   WHERE student_id = 'SV001'       -- Partition Key lookup (O(1))
--     AND event_time > '2025-05-01'  -- Clustering Key range scan
--   ORDER BY event_time DESC
--   LIMIT 100;
-- =============================================================

CREATE TABLE IF NOT EXISTS learning_events (
    student_id      TEXT,
    event_time      TIMESTAMP,
    event_id        UUID,

    -- Loại sự kiện
    event_type      TEXT,   -- 'video_watch' | 'quiz_submit' | 'click' | 'login' | 'logout'

    -- Ngữ cảnh học tập
    course_id       TEXT,
    lesson_id       TEXT,
    chapter         INT,

    -- Dữ liệu định lượng
    duration_sec    INT,        -- Thời lượng (giây)
    score           FLOAT,      -- Điểm quiz (0–100), null nếu không phải quiz
    progress_pct    FLOAT,      -- % hoàn thành video (0.0–1.0)
    click_target    TEXT,       -- CSS selector hoặc element id

    -- Metadata mở rộng (schema-flexible cho Cassandra)
    metadata        MAP<TEXT, TEXT>,  -- {"device":"mobile","ip":"...","browser":"Chrome"}

    -- Phân vùng kỹ thuật
    PRIMARY KEY ((student_id), event_time, event_id)
)
WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
AND default_time_to_live = 7776000    -- TTL 90 ngày tự động xóa
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
}
AND comment = 'Log sự kiện học tập – Partition by student_id, Cluster by time DESC';


-- =============================================================
-- BẢNG 2: student_monthly_summary
-- Mục đích: Tổng hợp hành vi theo tháng phục vụ batch analysis
--
-- Query mục tiêu:
--   SELECT * FROM student_monthly_summary
--   WHERE student_id = 'SV001'
--   ORDER BY month_bucket DESC
--   LIMIT 6;  -- 6 tháng gần nhất
-- =============================================================

CREATE TABLE IF NOT EXISTS student_monthly_summary (
    student_id      TEXT,
    month_bucket    TEXT,       -- '2025-06' → dễ group by tháng

    total_events    INT,
    total_video_sec INT,
    total_quiz_done INT,
    avg_quiz_score  FLOAT,
    active_days     INT,        -- Số ngày có tương tác trong tháng
    video_skip_pct  FLOAT,      -- % video bị bỏ qua (progress < 0.3)
    risk_score      FLOAT,      -- Output của batch analysis (0.0–1.0)
    risk_label      TEXT,       -- 'LOW' | 'MEDIUM' | 'HIGH'
    last_active     TIMESTAMP,

    PRIMARY KEY ((student_id), month_bucket)
)
WITH CLUSTERING ORDER BY (month_bucket DESC)
AND comment = 'Tổng hợp hành vi hàng tháng – batch analysis output';


-- =============================================================
-- BẢNG 3: course_event_log
-- Mục đích: Phân tích engagement theo khóa học
-- (Bảng phụ – denormalization theo Cassandra pattern)
--
-- Query mục tiêu:
--   SELECT * FROM course_event_log
--   WHERE course_id = 'CS101'
--     AND event_time > '2025-05-01'
--   LIMIT 500;
-- =============================================================

CREATE TABLE IF NOT EXISTS course_event_log (
    course_id       TEXT,
    event_time      TIMESTAMP,
    student_id      TEXT,
    event_id        UUID,
    event_type      TEXT,
    duration_sec    INT,
    score           FLOAT,

    PRIMARY KEY ((course_id), event_time, student_id, event_id)
)
WITH CLUSTERING ORDER BY (event_time DESC, student_id ASC, event_id ASC)
AND default_time_to_live = 7776000;


-- =============================================================
-- BẢNG 4: high_risk_students
-- Mục đích: Danh sách SV có nguy cơ chán học – real-time alert
--
-- Query mục tiêu:
--   SELECT * FROM high_risk_students
--   WHERE course_id = 'CS101' AND risk_label = 'HIGH'
--   LIMIT 50;
-- =============================================================

CREATE TABLE IF NOT EXISTS high_risk_students (
    course_id       TEXT,
    risk_label      TEXT,       -- 'HIGH' | 'MEDIUM'
    risk_score      FLOAT,
    student_id      TEXT,
    student_name    TEXT,
    detected_at     TIMESTAMP,
    inactive_days   INT,
    video_skip_pct  FLOAT,
    recommendation  TEXT,

    PRIMARY KEY ((course_id, risk_label), risk_score, student_id)
)
WITH CLUSTERING ORDER BY (risk_score DESC, student_id ASC);


-- =============================================================
-- INDEX PHỤ (Materialized View) – Tìm kiếm theo event_type
-- =============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS events_by_type AS
    SELECT student_id, event_type, event_time, course_id, duration_sec, score
    FROM learning_events
    WHERE student_id IS NOT NULL
      AND event_type IS NOT NULL
      AND event_time IS NOT NULL
      AND event_id IS NOT NULL
    PRIMARY KEY ((event_type), event_time, student_id, event_id)
WITH CLUSTERING ORDER BY (event_time DESC);
