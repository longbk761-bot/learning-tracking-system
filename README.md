# Learning Behavior Tracker
Hệ thống tracking hành vi học tập – **AstraDB (Apache Cassandra) + FastAPI + Web GUI**

> Project môn **Modern Database Applications** – minh chứng sức mạnh của Partition Key và Clustering Key trong Cassandra.

---

## Tính năng

| Tính năng | Mô tả |
|-----------|-------|
| **Live Stream** | Biểu đồ luồng sự kiện học tập real-time |
| **Tra cứu sinh viên** | Partition Key lookup < 10ms |
| **Phát hiện bất thường** | Batch analysis 5 tín hiệu hành vi |
| **Data Model** | So sánh Cassandra vs MySQL |
| **Seed Data** | Ghi dữ liệu demo vào AstraDB |

---

## Cài đặt nhanh

### 1. Clone repo
```bash
git clone https://github.com/longbk761-bot/learning-tracking-system.git
cd learning-tracking-system
```

### 2. Cài thư viện
```bash
pip install -r requirements.txt
```

### 3. Cấu hình .env
```bash
cp .env.example .env
# Điền credentials AstraDB vào file .env
```

### 4. Khởi tạo schema trên AstraDB
Mở **CQL Console** trên AstraDB → paste toàn bộ nội dung `schema.cql` → chạy.

### 5. Chạy
```bash
python -m uvicorn app.main:app --reload --port 8000
```

Mở trình duyệt: **http://localhost:8000**

---

## Cấu trúc project
learning-tracking-system/
├── app/
│   ├── main.py           # FastAPI entry point
│   ├── database.py       # AstraDB connection + Prepared Statements
│   ├── models.py         # Pydantic schemas
│   ├── api/
│   │   └── endpoints.py  # /events, /students, /seed, /analytics
│   ├── services/
│   │   └── batch_analysis.py
│   └── static/
│       ├── index.html    # Web GUI
│       └── tracker.js    # Client SDK
├── schema.cql            # CQL schema
├── requirements.txt
├── .env.example
└── README.md
---

## API Endpoints

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `POST` | `/api/events` | Ghi 1 sự kiện |
| `POST` | `/api/events/batch` | Ghi nhiều sự kiện (max 500) |
| `POST` | `/api/seed` | Tạo dữ liệu demo |
| `GET`  | `/api/students/{id}/history` | Lịch sử 30 ngày |
| `GET`  | `/api/analytics/batch/{month}` | Phân tích bất thường |
| `GET`  | `/api/analytics/stream` | Thống kê real-time |
| `GET`  | `/health` | Kiểm tra kết nối DB |
| `GET`  | `/docs` | Swagger UI |

---

## Data Model – Query-First Design

```sql
CREATE TABLE learning_events (
    student_id   TEXT,      -- PARTITION KEY → cùng node vật lý
    event_time   TIMESTAMP, -- CLUSTERING KEY DESC → sắp xếp sẵn
    event_id     UUID,
    event_type   TEXT,
    course_id    TEXT,
    duration_sec INT,
    score        FLOAT,
    progress_pct FLOAT,
    metadata     MAP<TEXT, TEXT>,
    PRIMARY KEY ((student_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);
```

### Phân tích Trade-off

**Tại sao `student_id` làm Partition Key?**
Query chính là "lấy toàn bộ lịch sử 1 sinh viên" → gom data của 1 SV vào cùng 1 node → read O(1), không scan toàn bảng.

**Tại sao `event_time DESC` làm Clustering Key?**
Cassandra lưu data đã sắp xếp sẵn trên đĩa → truy vấn "30 ngày gần nhất" không tốn CPU sort, lấy từ đầu là xong.

**Trade-off cần lưu ý:**
- **Hot Partition**: Nếu 1 SV có hàng triệu sự kiện → 1 node bị quá tải. Giải pháp: thêm `month_bucket` vào Partition Key.
- **Denormalization**: Phải tạo 4 bảng riêng vì Cassandra không có JOIN. Tốn storage hơn nhưng read nhanh hơn.
- **Append-only**: Cassandra tối ưu cho ghi mới, không nên UPDATE log events.

**Cassandra vs MySQL:**

| Tiêu chí | Cassandra | MySQL |
|----------|-----------|-------|
| Write throughput | ~10.000/s | ~2.000/s |
| Partition read | <5ms | 350ms+ |
| Scalability | Horizontal | Vertical |
| Schema change | Không lock | Lock table |

---

## Batch Analysis – Phát hiện bất thường

Risk Score = tổng có trọng số của 5 tín hiệu:

| Tín hiệu | Trọng số |
|----------|----------|
| Số ngày bất hoạt liên tiếp | 30% |
| Tỷ lệ bỏ qua video (progress < 30%) | 25% |
| Tần suất sự kiện thấp | 20% |
| Điểm quiz trung bình thấp | 15% |
| Không xem video | 10% |

- **HIGH** (≥65): Liên hệ can thiệp ngay
- **MEDIUM** (35–64): Theo dõi thêm
- **LOW** (<35): Bình thường

---

## Tech Stack

- **Database**: AstraDB (Apache Cassandra) – Serverless, Free tier
- **Backend**: FastAPI + cassandra-driver (Prepared Statements)
- **Frontend**: Vanilla HTML/CSS/JS + Chart.js
- **Consistency**: LOCAL_QUORUM cho read/write