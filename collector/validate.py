"""저장 전 정합성 검사. 하나라도 걸리면 그날 데이터는 저장하지 않는다."""
from .models import ChartEntry


def check_day(entries: list[ChartEntry], expected_total: int) -> list[str]:
    problems: list[str] = []
    if len(entries) != expected_total:
        problems.append(f"개수 불일치: 받은 {len(entries)}건, 서버 total {expected_total}건")
    if not entries:
        return problems or ["빈 응답"]

    track_ids = [e.track_id for e in entries]
    dup = {t for t in track_ids if track_ids.count(t) > 1}
    if dup:
        problems.append(f"중복 곡: {sorted(dup)[:5]}")

    ranks = sorted(e.rank for e in entries)
    if ranks != list(range(1, len(entries) + 1)):
        problems.append("순위가 1부터 빈틈없이 이어지지 않음")

    # 순위가 낮을수록 스트리밍 수가 많아야 한다(같을 수는 있음)
    by_rank = sorted(entries, key=lambda e: e.rank)
    for upper, lower in zip(by_rank, by_rank[1:]):
        if lower.streams > upper.streams:
            problems.append(f"순위 역전: {upper.rank}위 {upper.streams} < {lower.rank}위 {lower.streams}")
            break
    return problems
