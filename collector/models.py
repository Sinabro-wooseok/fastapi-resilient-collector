"""수집 데이터와 실행 기록 모델."""
from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field


class ChartEntry(BaseModel):
    """일별 차트 한 줄. (chart_date, track_id)가 자연 키다."""

    chart_date: date
    rank: int = Field(ge=1)
    track_id: str
    title: str
    artist: str
    streams: int = Field(ge=0)


class RunStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    INVALID = "invalid"  # 받아 왔지만 정합성 검사에서 걸림 — 저장하지 않는다


class RunRecord(BaseModel):
    chart_date: date
    status: RunStatus
    rows: int = 0
    attempts: int = 0
    error: str | None = None
    finished_at: datetime
