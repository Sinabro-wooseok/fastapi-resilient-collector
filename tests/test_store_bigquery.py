from datetime import date, datetime, timezone

from google.cloud import bigquery

from collector.models import ChartEntry, RunRecord, RunStatus
from collector.store_bigquery import BigQueryStore


class FakeJob:
    def result(self):
        return []


class FakeBQ:
    def __init__(self):
        self.loads, self.inserts, self.queries = [], [], []

    def load_table_from_json(self, rows, dest, job_config):
        self.loads.append((rows, dest, job_config))
        return FakeJob()

    def insert_rows_json(self, table, rows):
        self.inserts.append((table, rows))
        return []

    def query(self, sql, job_config):
        self.queries.append((sql, job_config))
        return FakeJob()


def test_replace_day_truncates_only_that_partition():
    bq = FakeBQ()
    e = ChartEntry(chart_date=date(2026, 10, 6), rank=1, track_id="t1", title="a", artist="b", streams=5)
    BigQueryStore(bq, "charts").replace_day(date(2026, 10, 6), [e])
    rows, dest, cfg = bq.loads[0]
    assert dest == "charts.chart_entries$20261006"
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert rows[0]["chart_date"] == "2026-10-06"


def test_queries_use_parameters_not_string_formatting():
    bq = FakeBQ()
    store = BigQueryStore(bq, "charts")
    store.record_run(RunRecord(chart_date=date(2026, 10, 6), status=RunStatus.FAILED,
                               error="x", finished_at=datetime.now(timezone.utc)))
    store.runs(RunStatus.FAILED, 10)
    assert bq.loads[0][1] == "charts.runs"                     # 스트리밍 삽입이 아니라 적재 작업
    assert bq.loads[0][2].write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    sql, cfg = bq.queries[0]
    assert "@status" in sql and "failed" not in sql
    assert {p.name for p in cfg.query_parameters} == {"status", "limit"}
