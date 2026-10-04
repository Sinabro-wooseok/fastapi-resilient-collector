"""테스트 공용: 가짜 외부 API에 붙은 클라이언트를 만든다(실제 대기 없음)."""
import httpx

from collector.fake_upstream import Faults, create_fake_upstream
from collector.upstream import ChartClient, RetryPolicy


class Sleeps:
    def __init__(self):
        self.waits: list[float] = []

    async def __call__(self, s: float) -> None:
        self.waits.append(s)


def make_env(faults: Faults | None = None, **kw):
    upstream = create_fake_upstream(faults, **kw)
    sleeps = Sleeps()

    def make_client() -> ChartClient:
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream),
                                 base_url="http://upstream", timeout=10)
        return ChartClient(http, "test-key", RetryPolicy(max_attempts=4), sleep=sleeps)

    return upstream, make_client, sleeps
