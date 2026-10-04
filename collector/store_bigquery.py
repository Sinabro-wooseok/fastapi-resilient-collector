"""BigQuery 저장소.

chart_entries는 chart_date로 파티션된 테이블이라고 가정한다. 하루치를
`table$YYYYMMDD` 파티션에 WRITE_TRUNCATE로 적재하면 그 파티션만 원자적으로
교체되어, 재실행·백필을 여러 번 해도 중복이 생기지 않는다.
실행 기록도 스트리밍 삽입 대신 적재 작업(WRITE_APPEND)으로 넣어, 결제 등록이
없는 BigQuery 샌드박스에서도 그대로 돈다.
"""
from datetime import date, datetime

from google.cloud import bigquery

from .models import ChartEntry, RunRecord, RunStatus


ENTRY_SCHEMA = [
    bigquery.SchemaField("chart_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("rank", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("track_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("artist", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("streams", "INT64", mode="REQUIRED"),
]
RUN_SCHEMA = [
    bigquery.SchemaField("chart_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rows", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("attempts", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("error", "STRING"),
    bigquery.SchemaField("finished_at", "TIMESTAMP", mode="REQUIRED"),
]


class BigQueryStore:
    def __init__(self, client: bigquery.Client, dataset: str):
        self.client = client
        self.entries_table = f"{dataset}.chart_entries"
        self.runs_table = f"{dataset}.runs"

    def ensure_tables(self, partition_expiration_days: int | None = None) -> None:
        """chart_date 파티션·artist 클러스터링 테이블과 실행 기록 테이블을 만든다(있으면 그대로).

        샌드박스는 파티션 보관 기간이 60일 미만이어야 해서 값을 받을 수 있게 했다.
        """
        entries = bigquery.Table(self.entries_table, schema=ENTRY_SCHEMA)
        expiration = partition_expiration_days and partition_expiration_days * 86_400_000
        entries.time_partitioning = bigquery.TimePartitioning(field="chart_date",
                                                              expiration_ms=expiration)
        entries.clustering_fields = ["artist"]
        self.client.create_table(entries, exists_ok=True)
        self.client.create_table(bigquery.Table(self.runs_table, schema=RUN_SCHEMA), exists_ok=True)

    def _load(self, rows: list[dict], dest: str, disposition: str, schema) -> None:
        config = bigquery.LoadJobConfig(
            write_disposition=disposition, schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        self.client.load_table_from_json(rows, dest, job_config=config).result()

    def replace_day(self, chart_date: date, entries: list[ChartEntry]) -> None:
        self._load([e.model_dump(mode="json") for e in entries],
                   f"{self.entries_table}${chart_date:%Y%m%d}",
                   bigquery.WriteDisposition.WRITE_TRUNCATE, ENTRY_SCHEMA)

    def record_run(self, run: RunRecord) -> None:
        self._load([run.model_dump(mode="json")], self.runs_table,
                   bigquery.WriteDisposition.WRITE_APPEND, RUN_SCHEMA)

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
