"""실제 BigQuery(샌드박스 가능)에 붙여 적재·재실행·백필을 확인한다.

  GOOGLE_CLOUD_PROJECT=내-프로젝트 python -m scripts.bq_live_check
데이터셋 collector_demo를 만들고(시험 테이블은 매번 새로 만들며 59일 뒤 만료), 가짜 외부 API로 6일치를 백필한 뒤
같은 날을 세 번 다시 돌려 행 수가 그대로인지 본다.
"""
import asyncio
import logging
import os
from datetime import date

from google.cloud import bigquery

from collector.fake_upstream import Faults
from collector.pipeline import ListNotifier, backfill, collect_day
from collector.store_bigquery import BigQueryStore
from tests.helpers import make_env

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("bq_live_check")

DATASET = "collector_demo"


def counts(client: bigquery.Client, project: str) -> dict:
    sql = (f"SELECT chart_date, COUNT(*) AS n, COUNT(DISTINCT track_id) AS u "
           f"FROM `{project}.{DATASET}.chart_entries` GROUP BY chart_date ORDER BY chart_date")
    return {str(r["chart_date"]): (r["n"], r["u"]) for r in client.query(sql).result()}


def main() -> None:
    project = os.environ["GOOGLE_CLOUD_PROJECT"]
    client = bigquery.Client(project=project)
    ds = bigquery.Dataset(f"{project}.{DATASET}")
    ds.location = "asia-northeast3"
    # 샌드박스는 데이터셋 기본 만료·파티션 만료가 모두 60일 미만이어야 한다
    ds.default_table_expiration_ms = ds.default_partition_expiration_ms = 59 * 86_400_000
    ds = client.create_dataset(ds, exists_ok=True)
    ds.default_table_expiration_ms = ds.default_partition_expiration_ms = 59 * 86_400_000
    client.update_dataset(ds, ["default_table_expiration_ms", "default_partition_expiration_ms"])
    for t in ("chart_entries", "runs"):  # 시험 전용 데이터셋 — 이전 결과를 지우고 시작
        client.delete_table(f"{project}.{DATASET}.{t}", not_found_ok=True)

    store = BigQueryStore(client, f"{project}.{DATASET}")
    store.ensure_tables(partition_expiration_days=59)
    faults = Faults(server_error={"2026-10-03": 99}, drop_row={"2026-10-04"})
    _, make_client, _ = make_env(faults)
    note = ListNotifier()

    first = asyncio.run(backfill(make_client, store, note, date(2026, 10, 1), date(2026, 10, 6)))
    log.info("1차 백필: %s", [(str(r.chart_date), r.status.value) for r in first])

    for _ in range(3):
        asyncio.run(collect_day(make_client(), store, note, date(2026, 10, 6)))
    after_rerun = counts(client, project)
    log.info("10/6 세 번 재실행 뒤 날짜별 (행, 고유 곡): %s", after_rerun)

    faults.server_error.clear()
    faults.drop_row.clear()
    second = asyncio.run(backfill(make_client, store, note, date(2026, 10, 1), date(2026, 10, 6)))
    log.info("2차 백필(장애 해제 뒤) 다시 받은 날: %s", [str(r.chart_date) for r in second])
    final = counts(client, project)
    log.info("최종 날짜별 (행, 고유 곡): %s", final)
    log.info("알림 %d건, 마지막 성공 %s", len(note.sent), store.last_success_at())

    assert after_rerun["2026-10-06"] == (25, 25), "재실행 뒤 중복 발생"
    assert sorted(str(r.chart_date) for r in second) == ["2026-10-03", "2026-10-04"]
    assert all(v == (25, 25) for v in final.values()) and len(final) == 6
    log.info("확인 완료: 재실행 중복 없음, 실패한 날만 다시 수집, 6일 모두 25행")


if __name__ == "__main__":
    main()
