import asyncio
from datetime import date

from collector.fake_upstream import Faults
from collector.models import RunStatus
from collector.pipeline import ListNotifier, backfill, collect_day
from collector.store_sqlite import SQLiteStore
from tests.helpers import make_env

D = date(2026, 10, 6)


def run(faults=None, day=D):
    _, make_client, _ = make_env(faults)
    store, note = SQLiteStore(), ListNotifier()
    rec = asyncio.run(collect_day(make_client(), store, note, day))
    return rec, store, note


def test_success_saves_and_stays_quiet():
    rec, store, note = run()
    assert rec.status is RunStatus.SUCCESS and store.count_day(D) == 25
    assert note.sent == []


def test_missing_row_is_not_saved_and_alerts():
    rec, store, note = run(Faults(drop_row={D.isoformat()}))
    assert rec.status is RunStatus.INVALID and "개수 불일치" in rec.error
    assert store.count_day(D) == 0
    assert "invalid" in note.sent[0]


def test_rank_inversion_is_caught():
    rec, _, _ = run(Faults(swap_streams={D.isoformat()}))
    assert rec.status is RunStatus.INVALID and "순위 역전" in rec.error


def test_upstream_failure_is_recorded_and_alerts():
    rec, store, note = run(Faults(server_error={D.isoformat(): 99}))
    assert rec.status is RunStatus.FAILED and rec.attempts == 4
    assert store.runs(RunStatus.FAILED)[0].chart_date == D
    assert note.sent


def test_rerun_replaces_day_without_duplicates():
    _, make_client, _ = make_env()
    store, note = SQLiteStore(), ListNotifier()
    for _ in range(3):
        asyncio.run(collect_day(make_client(), store, note, D))
    assert store.count_day(D) == 25


def test_backfill_resumes_and_skips_done_days():
    faults = Faults(server_error={"2026-10-03": 99})
    upstream, make_client, _ = make_env(faults)
    store, note = SQLiteStore(), ListNotifier()
    first = asyncio.run(backfill(make_client, store, note, date(2026, 10, 1), date(2026, 10, 5)))
    assert sorted(r.status.value for r in first).count("success") == 4
    faults.server_error.clear()
    calls = upstream.state.calls
    second = asyncio.run(backfill(make_client, store, note, date(2026, 10, 1), date(2026, 10, 5)))
    assert [r.chart_date for r in second] == [date(2026, 10, 3)]   # 실패한 날만 다시
    assert upstream.state.calls - calls == 3                       # 3페이지만 추가 호출
