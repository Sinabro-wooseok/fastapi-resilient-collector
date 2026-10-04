"""로컬·테스트용 저장소. 하루치를 통째로 바꿔 끼워 재실행해도 결과가 같다."""
import sqlite3
from datetime import date, datetime

from .models import ChartEntry, RunRecord, RunStatus

SCHEMA = """
CREATE TABLE IF NOT EXISTS chart_entries (
  chart_date TEXT NOT NULL, rank INTEGER NOT NULL, track_id TEXT NOT NULL,
  title TEXT NOT NULL, artist TEXT NOT NULL, streams INTEGER NOT NULL,
  PRIMARY KEY (chart_date, track_id)
);
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, chart_date TEXT NOT NULL, status TEXT NOT NULL,
  rows INTEGER NOT NULL, attempts INTEGER NOT NULL, error TEXT, finished_at TEXT NOT NULL
);
"""


class SQLiteStore:
    def __init__(self, path: str = ":memory:"):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(SCHEMA)

    def replace_day(self, chart_date: date, entries: list[ChartEntry]) -> None:
        # 차트에서 빠진 곡이 남지 않도록 업서트가 아니라 날짜 단위 교체
        with self.db:
            self.db.execute("DELETE FROM chart_entries WHERE chart_date = ?",
                            (chart_date.isoformat(),))
            self.db.executemany(
                "INSERT INTO chart_entries VALUES (?, ?, ?, ?, ?, ?)",
                [(e.chart_date.isoformat(), e.rank, e.track_id, e.title, e.artist, e.streams)
                 for e in entries])

    def count_day(self, chart_date: date) -> int:
        row = self.db.execute("SELECT COUNT(*) FROM chart_entries WHERE chart_date = ?",
                              (chart_date.isoformat(),)).fetchone()
        return row[0]

    def record_run(self, run: RunRecord) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO runs (chart_date, status, rows, attempts, error, finished_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (run.chart_date.isoformat(), run.status.value, run.rows, run.attempts,
                 run.error, run.finished_at.isoformat()))

    def runs(self, status: RunStatus | None = None, limit: int = 50) -> list[RunRecord]:
        sql = "SELECT chart_date, status, rows, attempts, error, finished_at FROM runs"
        args: tuple = ()
        if status:
            sql, args = sql + " WHERE status = ?", (status.value,)
        sql += " ORDER BY id DESC LIMIT ?"
        rows = self.db.execute(sql, args + (limit,)).fetchall()
        return [RunRecord(chart_date=r[0], status=r[1], rows=r[2], attempts=r[3],
                          error=r[4], finished_at=r[5]) for r in rows]

    def successful_dates(self) -> set[date]:
        rows = self.db.execute("SELECT DISTINCT chart_date FROM runs WHERE status = ?",
                               (RunStatus.SUCCESS.value,)).fetchall()
        return {date.fromisoformat(r[0]) for r in rows}

    def last_success_at(self) -> datetime | None:
        row = self.db.execute("SELECT MAX(finished_at) FROM runs WHERE status = ?",
                              (RunStatus.SUCCESS.value,)).fetchone()
        return datetime.fromisoformat(row[0]) if row[0] else None
