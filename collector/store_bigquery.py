"""BigQuery 저장소.

chart_entries는 chart_date로 파티션된 테이블이라고 가정한다. 하루치를
`table$YYYYMMDD` 파티션에 WRITE_TRUNCATE로 적재하면 그 파티션만 원자적으로
교체되어, 재실행·백필을 여러 번 해도 중복이 생기지 않는다.
"""
from datetime import date, datetime

from google.cloud import bigquery

from .models import ChartEntry, RunRecord, RunStatus


class BigQueryStore:
    def __init__(self, client: bigquery.Client, dataset: str):
        self.client = client
        self.entries_table = f"{dataset}.chart_entries"
        self.runs_table = f"{dataset}.runs"

    def replace_day(self, chart_date: date, entries: list[ChartEntry]) -> None:
        dest = f"{self.entries_table}${chart_date:%Y%m%d}"
        config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        rows = [e.model_dump(mode="json") for e in entries]
        self.client.load_table_from_json(rows, dest, job_config=config).result()

    def record_run(self, run: RunRecord) -> None:
        errors = self.client.insert_rows_json(self.runs_table, [run.model_dump(mode="json")])
        if errors:
            raise RuntimeError(f"실행 기록 저장 실패: {errors}")

    def _query(self, sql: str, params: list) -> list:
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        return list(job.result())

    def runs(self, status: RunStatus | None = None, limit: int = 50) -> list[RunRecord]:
        where = "WHERE status = @status" if status else ""
        params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
        if status:
            params.append(bigquery.ScalarQueryParameter("status", "STRING", status.value))
        rows = self._query(
            f"SELECT * FROM `{self.runs_table}` {where} ORDER BY finished_at DESC LIMIT @limit",
            params)
        return [RunRecord(**dict(r)) for r in rows]

    def successful_dates(self) -> set[date]:
        rows = self._query(
            f"SELECT DISTINCT chart_date FROM `{self.runs_table}` WHERE status = @status",
            [bigquery.ScalarQueryParameter("status", "STRING", RunStatus.SUCCESS.value)])
        return {r["chart_date"] for r in rows}

    def last_success_at(self) -> datetime | None:
        rows = self._query(
            f"SELECT MAX(finished_at) AS t FROM `{self.runs_table}` WHERE status = @status",
            [bigquery.ScalarQueryParameter("status", "STRING", RunStatus.SUCCESS.value)])
        return rows[0]["t"] if rows else None
