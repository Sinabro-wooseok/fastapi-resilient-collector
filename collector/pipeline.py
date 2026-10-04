"""수집 → 검사 → 저장 → 기록 → (실패 시) 알림."""
import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Protocol

import httpx

from .models import RunRecord, RunStatus
from .upstream import ChartClient, UpstreamError
from .validate import check_day

log = logging.getLogger(__name__)


class Notifier(Protocol):
    async def send(self, text: str) -> None: ...


class SlackNotifier:
    """Slack 수신 웹후크. 주소는 환경 변수로만 받는다."""

    def __init__(self, webhook_url: str, http: httpx.AsyncClient | None = None):
        self.url = webhook_url
        self.http = http or httpx.AsyncClient(timeout=5)

    async def send(self, text: str) -> None:
        try:
            await self.http.post(self.url, json={"text": text})
        except httpx.HTTPError as e:
            # 알림 실패가 수집 결과를 덮어쓰지 않게 기록만 남긴다
            log.error("알림 전송 실패: %s", e)


class ListNotifier:
    """테스트·로컬용: 보낸 알림을 메모리에 쌓는다."""

    def __init__(self):
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def collect_day(client: ChartClient, store, notifier: Notifier, day: date) -> RunRecord:
    try:
        entries, total = await client.fetch_day(day)
    except UpstreamError as e:
        run = RunRecord(chart_date=day, status=RunStatus.FAILED, attempts=client.attempts,
                        error=str(e), finished_at=_now())
    else:
        problems = check_day(entries, total)
        if problems:
            run = RunRecord(chart_date=day, status=RunStatus.INVALID, rows=len(entries),
                            attempts=client.attempts, error="; ".join(problems),
                            finished_at=_now())
        else:
            store.replace_day(day, entries)
            run = RunRecord(chart_date=day, status=RunStatus.SUCCESS, rows=len(entries),
                            attempts=client.attempts, finished_at=_now())
    store.record_run(run)
    if run.status is not RunStatus.SUCCESS:
        await notifier.send(f"[차트 수집 {run.status.value}] {day}: {run.error}")
    log.info("%s %s rows=%d attempts=%d", day, run.status.value, run.rows, run.attempts)
    return run


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


async def backfill(make_client, store, notifier: Notifier, start: date, end: date,
                   concurrency: int = 3, skip_done: bool = True) -> list[RunRecord]:
    """기간을 채운다. 이미 성공한 날은 건너뛰어 중단 뒤 다시 돌려도 이어서 진행된다."""
    done = store.successful_dates() if skip_done else set()
    todo = [d for d in date_range(start, end) if d not in done]
    sem = asyncio.Semaphore(concurrency)

    async def one(day: date) -> RunRecord:
        async with sem:
            # 날짜마다 클라이언트를 따로 써서 attempts 집계가 섞이지 않게 한다
            return await collect_day(make_client(), store, notifier, day)

    return list(await asyncio.gather(*(one(d) for d in todo)))
