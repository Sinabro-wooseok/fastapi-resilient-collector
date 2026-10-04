"""실행 진입점.

UPSTREAM_URL이 없으면 같은 프로세스 안의 가짜 외부 API에 붙어 바로 데모가 돈다.
  uvicorn main:app --reload
환경 변수: UPSTREAM_URL, UPSTREAM_API_KEY, SLACK_WEBHOOK_URL, BQ_DATASET, SQLITE_PATH
"""
import logging
import os

import httpx

from collector.api import create_app
from collector.fake_upstream import Faults, create_fake_upstream
from collector.pipeline import ListNotifier, SlackNotifier
from collector.store_sqlite import SQLiteStore
from collector.upstream import ChartClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

UPSTREAM_URL = os.getenv("UPSTREAM_URL")
API_KEY = os.getenv("UPSTREAM_API_KEY", "test-key")


def _transport():
    if UPSTREAM_URL:
        return None, UPSTREAM_URL
    # 데모: 10/3은 503이 계속, 10/4는 한 줄이 빠진 응답 — 실패 기록·알림을 바로 볼 수 있다
    fake = create_fake_upstream(Faults(server_error={"2026-10-03": 99}, drop_row={"2026-10-04"}))
    return httpx.ASGITransport(app=fake), "http://fake-upstream"


def make_client() -> ChartClient:
    transport, base = _transport_cached
    http = httpx.AsyncClient(transport=transport, base_url=base, timeout=10)
    return ChartClient(http, API_KEY)


def make_store():
    dataset = os.getenv("BQ_DATASET")
    if dataset:
        from google.cloud import bigquery

        from collector.store_bigquery import BigQueryStore
        return BigQueryStore(bigquery.Client(), dataset)
    return SQLiteStore(os.getenv("SQLITE_PATH", "collector.db"))


_transport_cached = _transport()
webhook = os.getenv("SLACK_WEBHOOK_URL")
app = create_app(make_client, make_store(), SlackNotifier(webhook) if webhook else ListNotifier())
