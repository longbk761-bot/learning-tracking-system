from typing import List, Optional
from pydantic import BaseModel, Field


class LearningEvent(BaseModel):
    student_id:   str   = Field(..., example="SV001")
    event_type:   str   = Field(..., example="video_watch",
                                description="video_watch | quiz_submit | click | login | logout")
    course_id:    str   = Field(..., example="CS101")
    lesson_id:    Optional[str]   = None
    duration_sec: Optional[int]   = Field(None, ge=0)
    score:        Optional[float] = Field(None, ge=0, le=100)
    progress_pct: Optional[float] = Field(None, ge=0.0, le=1.0)
    metadata:     Optional[dict]  = None


class EventBatch(BaseModel):
    events: List[LearningEvent] = Field(..., max_length=500)


class BulkSeedRequest(BaseModel):
    """Seed dữ liệu demo – tạo ngẫu nhiên N sự kiện."""
    num_students: int = Field(10, ge=1, le=50)
    events_per_student: int = Field(50, ge=10, le=200)
