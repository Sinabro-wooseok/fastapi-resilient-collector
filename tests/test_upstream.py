import asyncio
from datetime import date

import pytest

from collector.fake_upstream import Faults
from collector.upstream import RetryPolicy, UpstreamError
from tests.helpers import make_env

D = date(2026, 10, 6)


def test_pages_are_followed_until_cursor_ends():
    upstream, make_client, _ = make_env(size=25, page_size=10)
    entries, total = asyncio.run(make_client().fetch_day(D))
    assert total == 25 and len(entries) == 25
    assert upstream.state.calls == 3


def test_429_uses_retry_after_and_503_backs_off():
    faults = Faults(rate_limit={D.isoformat(): 1}, server_error={D.isoformat(): 2})
    _, make_client, sleeps = make_env(faults)
    entries, _ = asyncio.run(make_client().fetch_day(D))
    assert len(entries) == 25
    assert sleeps.waits[0] == 1.0          # Retry-After 그대로
    assert len(sleeps.waits) == 3


def test_expired_session_relogs_without_spending_attempts():
    faults = Faults(expire_token_once={D.isoformat()}, server_error={D.isoformat(): 3})
    upstream, make_client, _ = make_env(faults)
    asyncio.run(make_client().fetch_day(D))  # 401 1회 + 503 3회 + 성공 = 최대 4회 안
    assert upstream.state.logins == 2


def test_gives_up_after_max_attempts():
    _, make_client, _ = make_env(Faults(server_error={D.isoformat(): 99}))
    with pytest.raises(UpstreamError, match="초과"):
        asyncio.run(make_client().fetch_day(D))


def test_non_retryable_status_fails_fast():
    _, make_client, sleeps = make_env(Faults(forbidden={D.isoformat()}))
    with pytest.raises(UpstreamError, match="403"):
        asyncio.run(make_client().fetch_day(D))
    assert sleeps.waits == []


def test_backoff_is_capped():
    p = RetryPolicy(base_delay=1, max_delay=4)
    assert all(p.delay(n, None) <= 4 for n in range(1, 10))
