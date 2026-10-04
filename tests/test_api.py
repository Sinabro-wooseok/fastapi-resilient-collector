from datetime import timedelta

from fastapi.testclient import TestClient

from collector.api import create_app
from collector.pipeline import ListNotifier
from collector.store_sqlite import SQLiteStore
from tests.helpers import make_env


def client(stale_after=timedelta(hours=26)):
    _, make_client, _ = make_env()
    store = SQLiteStore()
    return TestClient(create_app(make_client, store, ListNotifier(), stale_after)), store


def test_health_is_503_before_first_success():
    c, _ = client()
    assert c.get("/health").status_code == 503


def test_run_then_health_ok_and_runs_listed():
    c, _ = client()
    r = c.post("/runs", json={"chart_date": "2026-10-06"})
    assert r.status_code == 200 and r.json()["status"] == "success"
    assert c.get("/health").json()["stale"] is False
    assert len(c.get("/runs", params={"status": "success"}).json()) == 1


def test_health_turns_stale_when_batch_silently_stops():
    c, _ = client(stale_after=timedelta(seconds=-1))
    c.post("/runs", json={"chart_date": "2026-10-06"})
    assert c.get("/health").status_code == 503


def test_backfill_rejects_bad_ranges():
    c, _ = client()
    assert c.post("/backfill", json={"start": "2026-10-06", "end": "2026-10-01"}).status_code == 422
    assert c.post("/backfill", json={"start": "2024-01-01", "end": "2026-10-01"}).status_code == 422
