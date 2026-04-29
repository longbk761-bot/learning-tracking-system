# Hệ thống Tracking Hành vi Học tập
# AstraDB (Apache Cassandra) + FastAPI

## Cài đặt

```bash
pip install fastapi uvicorn cassandra-driver python-dotenv pydantic
```

## Cấu hình .env

```
ASTRA_DB_SECURE_BUNDLE_PATH=./secure-connect-learning.zip
ASTRA_DB_CLIENT_ID=your_client_id
ASTRA_DB_CLIENT_SECRET=your_client_secret
ASTRA_DB_KEYSPACE=learning_tracker
```

## Khởi tạo schema

```bash
# Trên AstraDB Console hoặc cqlsh
cqlsh -f schema.cql
```

## Chạy API

```bash
# Dev
uvicorn main:app --reload --port 8000

# Production (4 workers)
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

## Batch Analysis

```bash
python batch_analysis.py --month 2025-06 --course CS101
```

## API Endpoints

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| POST | /events | Ghi 1 sự kiện |
| POST | /events/batch | Ghi nhiều sự kiện (max 500) |
| GET | /students/{id}/history | Lịch sử sinh viên |
| GET | /courses/{id}/events | Sự kiện theo khóa học |
| GET | /health | Kiểm tra kết nối |

## Ví dụ gọi API

```bash
# Ghi sự kiện
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "student_id": "SV001",
    "event_type": "video_watch",
    "course_id": "CS101",
    "duration_sec": 720,
    "progress_pct": 0.85
  }'

# Lịch sử sinh viên
curl http://localhost:8000/students/SV001/history?days=30&limit=100

# Ghi batch
curl -X POST http://localhost:8000/events/batch \
  -H "Content-Type: application/json" \
  -d '{"events": [...]}'
```
