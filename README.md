# fastapi-resilient-collector

외부 API에서 일별 데이터를 모아 BigQuery(또는 로컬 SQLite)에 적재하는 FastAPI 수집기 예제입니다.
「수집이 실패하거나 데이터가 빠졌을 때 누가 어떻게 알게 되는가」까지 기능의 일부로 설계했습니다.

[English below](#english)

## 무엇을 보여 주나

| 문제 | 처리 |
|---|---|
| 세션 만료(401) | 한 번만 다시 로그인하고 재시도 횟수는 쓰지 않음 |
| 요청 제한(429) | 서버의 `Retry-After`를 따름 |
| 일시 장애(5xx·연결 오류) | 지수 백오프 + 지터, 상한 있음, 최대 횟수 넘으면 실패로 기록 |
| 재시도해도 소용없는 응답(403 등) | 기다리지 않고 바로 실패 |
| 페이지 일부 누락·순위 역전·중복 곡 | 저장 전 정합성 검사에서 막고 `invalid`로 기록 — 틀린 데이터를 저장하지 않음 |
| 재실행·백필 중복 | 날짜 단위 교체 저장(BigQuery는 `table$YYYYMMDD` 파티션 `WRITE_TRUNCATE`) — 몇 번 돌려도 결과가 같음 |
| 백필 중간 실패 | 성공한 날은 건너뛰고 실패한 날만 다시 수집 |
| 배치가 조용히 멈춤 | `/health`가 마지막 성공 시각을 보고 기준 시간이 지나면 503 — 외부 모니터가 잡을 수 있음 |
| 실패 알림 | `failed`·`invalid`면 Slack 웹후크로 알림(주소는 환경 변수) |

## 구조

```
collector/
  upstream.py        외부 API 클라이언트 — 인증 갱신, 재시도·백오프, 페이지 순회
  validate.py        저장 전 정합성 검사
  pipeline.py        수집 → 검사 → 저장 → 실행 기록 → 알림, 백필(동시 실행 제한)
  store_sqlite.py    로컬·테스트 저장소
  store_bigquery.py  BigQuery 저장소(파티션 교체 적재, 파라미터 바인딩 쿼리)
  api.py             POST /runs, POST /backfill, GET /runs, GET /health
  fake_upstream.py   장애를 골라 넣을 수 있는 가짜 외부 API(테스트·데모)
main.py              실행 진입점
scripts/bq_live_check.py  실제 BigQuery 확인
tests/               18개 테스트
```

## 실행

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest -q
.venv/bin/uvicorn main:app
curl -X POST localhost:8000/backfill -H 'content-type: application/json' \
  -d '{"start":"2026-10-01","end":"2026-10-06"}'
```

`UPSTREAM_URL`이 없으면 가짜 외부 API에 붙습니다. 데모에서는 10/3에 503이 계속 나고 10/4는 한 줄이 빠진 응답이라,
결과에 `failed`·`invalid`가 하나씩 나오고 그날은 저장되지 않습니다.

환경 변수: `UPSTREAM_URL`, `UPSTREAM_API_KEY`, `SLACK_WEBHOOK_URL`, `BQ_DATASET`(지정하면 BigQuery 사용), `SQLITE_PATH`.

## 실제 BigQuery 확인

`scripts/bq_live_check.py`는 실제 BigQuery(결제 등록 없는 샌드박스도 가능)에 붙여 같은 시나리오를 돌립니다.

```bash
GOOGLE_CLOUD_PROJECT=내-프로젝트 .venv/bin/python -m scripts.bq_live_check
```

2026-10-04 샌드박스 프로젝트(서울 리전)에서 돌린 결과:

| 단계 | 결과 |
|---|---|
| 1차 백필 10/1~10/6 | 성공 4, 10/3 `failed`(503 계속), 10/4 `invalid`(한 줄 누락) — 두 날은 저장 안 됨, 알림 2건 |
| 10/6을 세 번 다시 실행 | 10/6 파티션 25행 그대로(중복 없음) |
| 장애를 풀고 2차 백필 | 10/3·10/4만 다시 수집 |
| 최종 | 파티션 6개 × 25행, 실행 기록 success 9 · failed 1 · invalid 1 |

샌드박스는 스트리밍 삽입을 막고 만료 기간을 60일 미만으로 요구해서, 실행 기록도 적재 작업으로 넣고
`ensure_tables(partition_expiration_days=59)`로 만료를 지정합니다.

## 한계
- 실행 기록과 데이터 교체는 한 트랜잭션이 아닙니다. 교체 성공 뒤 기록 저장이 실패하면 다음 백필이 그날을 한 번 더 수집하는데, 날짜 단위 교체라 결과는 같습니다.

---

<a id="english"></a>
## English

A FastAPI collector that pulls daily data from an external API and loads it into BigQuery (or SQLite locally).
The design treats "who finds out, and how, when a run fails or data goes missing" as part of the feature.

- **Auth & retries:** re-login once on 401 without spending a retry; honor `Retry-After` on 429; capped exponential backoff with jitter on 5xx and transport errors; fail fast on non-retryable responses.
- **Validation before write:** row count vs. server `total`, contiguous ranks, duplicate tracks, rank/stream inversion. Bad days are recorded as `invalid` and never written.
- **Idempotent loads:** each day replaces its own partition (`table$YYYYMMDD` with `WRITE_TRUNCATE` on BigQuery), so reruns and backfills never duplicate rows.
- **Resumable backfill:** successful days are skipped; only failed days are fetched again, with a concurrency limit.
- **Silent-stop detection:** `/health` returns 503 when the last success is older than a threshold.
- **Alerts:** Slack webhook on `failed` / `invalid` runs.

Run `python -m pytest -q` (18 tests) or `uvicorn main:app` for a demo against the built-in fake upstream.
`scripts/bq_live_check.py` runs the same scenario against a real BigQuery project (sandbox works). On 2026-10-04 in a Seoul-region sandbox: rerunning a day three times kept its partition at 25 rows, and the second backfill re-fetched only the failed and invalid days, ending with 6 partitions × 25 rows. Run records use load jobs instead of streaming inserts so the sandbox accepts them.
