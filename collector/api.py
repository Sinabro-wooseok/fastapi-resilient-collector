"""수집 서비스 API — 단건 실행, 백필, 실행 기록, 신선도 확인."""
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .models import RunRecord, RunStatus
from .pipeline import Notifier, backfill, collect_day
from .upstream import ChartClient

MAX_BACKFILL_DAYS = 400


class RunRequest(BaseModel):
    chart_date: date


class BackfillRequest(BaseModel):
    start: date
    end: date
    concurrency: int = 3
    skip_done: bool = True


def create_app(make_client: Callable[[], ChartClient], store, notifier: Notifier,
               stale_after: timedelta = timedelta(hours=26)) -> FastAPI:
    app = FastAPI(title="Resilient chart collector")

    @app.post("/runs", response_model=RunRecord)
    async def run_one(req: RunRequest):
        return await collect_day(make_client(), store, notifier, req.chart_date)

    @app.post("/backfill", response_model=list[RunRecord])
    async def run_backfill(req: BackfillRequest):
        days = (req.end - req.start).days + 1
        if days < 1 or days > MAX_BACKFILL_DAYS:
            raise HTTPException(422, f"기간은 1~{MAX_BACKFILL_DAYS}일이어야 합니다")
        if not 1 <= req.concurrency <= 10:
            raise HTTPException(422, "concurrency는 1~10")
        return await backfill(make_client, store, notifier, req.start, req.end,
                              req.concurrency, req.skip_done)

    @app.get("/runs", response_model=list[RunRecord])
    async def list_runs(status: RunStatus | None = None, limit: int = 50):
        return store.runs(status, min(max(limit, 1), 500))

    @app.get("/health")
    async def health():
        # 「배치가 조용히 멈춘 것」도 실패로 본다: 마지막 성공이 오래되면 503
        last = store.last_success_at()
        if last and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        stale = last is None or datetime.now(timezone.utc) - last > stale_after
        body = {"last_success_at": last.isoformat() if last else None, "stale": stale}
        if stale:
            raise HTTPException(503, body)
        return body

    return app
