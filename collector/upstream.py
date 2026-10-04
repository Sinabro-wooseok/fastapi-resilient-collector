"""외부 차트 API 클라이언트 — 인증 갱신, 재시도·백오프, 페이지 순회."""
import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import date

import httpx

from .models import ChartEntry

log = logging.getLogger(__name__)

RETRYABLE = {429, 500, 502, 503, 504}


class UpstreamError(Exception):
    """재시도를 다 써도 실패했거나, 재시도해도 소용없는 응답."""


@dataclass
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 8.0

    def delay(self, attempt: int, retry_after: str | None) -> float:
        # 서버가 Retry-After를 주면 그 값을 따르고, 아니면 지수 백오프 + 지터
        if retry_after and retry_after.isdigit():
            return min(float(retry_after), self.max_delay)
        backoff = min(self.base_delay * 2 ** (attempt - 1), self.max_delay)
        return backoff * random.uniform(0.5, 1.0)


class ChartClient:
    def __init__(self, http: httpx.AsyncClient, api_key: str,
                 policy: RetryPolicy | None = None, sleep=asyncio.sleep):
        self.http = http
        self.api_key = api_key
        self.policy = policy or RetryPolicy()
        self.sleep = sleep
        self.token: str | None = None
        self.attempts = 0  # 마지막 fetch_day 동안의 총 요청 수(실행 기록용)

    async def _login(self) -> None:
        r = await self.http.post("/auth/token", json={"api_key": self.api_key})
        if r.status_code != 200:
            raise UpstreamError(f"로그인 실패: {r.status_code}")
        self.token = r.json()["token"]

    async def _get(self, path: str, params: dict) -> dict:
        refreshed = False
        attempt = 0
        while attempt < self.policy.max_attempts:
            attempt += 1
            if self.token is None:
                await self._login()
            self.attempts += 1
            try:
                r = await self.http.get(path, params=params,
                                        headers={"Authorization": f"Bearer {self.token}"})
            except httpx.TransportError as e:
                log.warning("연결 오류 %s (시도 %d)", e, attempt)
                await self.sleep(self.policy.delay(attempt, None))
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401 and not refreshed:
                # 세션 만료 — 한 번만 다시 로그인하고 시도 횟수는 쓰지 않는다
                self.token, refreshed = None, True
                attempt -= 1
                continue
            if r.status_code in RETRYABLE:
                wait = self.policy.delay(attempt, r.headers.get("Retry-After"))
                log.warning("%s → %d, %.2fs 뒤 재시도", path, r.status_code, wait)
                await self.sleep(wait)
                continue
            raise UpstreamError(f"{path} 재시도 불가 응답 {r.status_code}")
        raise UpstreamError(f"{path} 재시도 {self.policy.max_attempts}회 초과")

    async def fetch_day(self, chart_date: date) -> tuple[list[ChartEntry], int]:
        """하루치 전 페이지를 받아 (항목, 서버가 알려 준 전체 개수)를 돌려준다."""
        self.attempts = 0
        entries: list[ChartEntry] = []
        cursor: str | None = None
        total = 0
        while True:
            params = {"date": chart_date.isoformat()}
            if cursor:
                params["cursor"] = cursor
            page = await self._get("/charts/daily", params)
            total = page["total"]
            entries += [ChartEntry(chart_date=chart_date, **row) for row in page["items"]]
            cursor = page.get("next_cursor")
            if not cursor:
                return entries, total
