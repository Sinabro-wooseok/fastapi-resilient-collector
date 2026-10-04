"""시험·데모용 가짜 차트 API. 장애를 골라 넣을 수 있다."""
from dataclasses import dataclass, field

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse


@dataclass
class Faults:
    """날짜별로 넣을 장애. 값은 남은 횟수(0이 되면 정상 응답)."""

    rate_limit: dict[str, int] = field(default_factory=dict)   # 429 + Retry-After
    server_error: dict[str, int] = field(default_factory=dict)  # 503
    expire_token_once: set[str] = field(default_factory=set)    # 첫 요청에 401
    drop_row: set[str] = field(default_factory=set)             # 한 줄을 빼고 total은 그대로
    swap_streams: set[str] = field(default_factory=set)         # 순위와 스트리밍 수 역전
    forbidden: set[str] = field(default_factory=set)            # 403 — 재시도 불가


def build_rows(day: str, size: int) -> list[dict]:
    seed = sum(map(ord, day))
    return [{"rank": i, "track_id": f"t{(seed + i * 7) % 997:03d}",
             "title": f"Song {i}", "artist": f"Artist {i % 5}",
             "streams": 1_000_000 - i * 1_000} for i in range(1, size + 1)]


def create_fake_upstream(faults: Faults | None = None, size: int = 25,
                         page_size: int = 10, api_key: str = "test-key") -> FastAPI:
    app = FastAPI()
    app.state.faults = faults or Faults()
    app.state.tokens = set()
    app.state.calls = 0
    app.state.logins = 0

    @app.post("/auth/token")
    async def token(body: dict):
        if body.get("api_key") != api_key:
            raise HTTPException(403, "bad key")
        app.state.logins += 1
        tok = f"tok{app.state.logins}"
        app.state.tokens.add(tok)
        return {"token": tok}

    @app.get("/charts/daily")
    async def daily(request: Request, date: str, cursor: int = 0,
                    authorization: str = Header("")):
        app.state.calls += 1
        f: Faults = app.state.faults
        tok = authorization.removeprefix("Bearer ")
        if date in f.expire_token_once:
            f.expire_token_once.discard(date)
            app.state.tokens.discard(tok)
        if tok not in app.state.tokens:
            return JSONResponse({"error": "session expired"}, status_code=401)
        if date in f.forbidden:
            return JSONResponse({"error": "forbidden"}, status_code=403)
        for kind, status, headers in (("rate_limit", 429, {"Retry-After": "1"}),
                                      ("server_error", 503, {})):
            left = getattr(f, kind)
            if left.get(date, 0) > 0:
                left[date] -= 1
                return JSONResponse({"error": kind}, status_code=status, headers=headers)
        rows = build_rows(date, size)
        if date in f.swap_streams:
            rows[0]["streams"], rows[1]["streams"] = rows[1]["streams"], rows[0]["streams"]
        total = len(rows)
        if date in f.drop_row:
            rows = rows[:-1]
        chunk = rows[cursor:cursor + page_size]
        nxt = cursor + page_size
        return {"total": total, "items": chunk,
                "next_cursor": str(nxt) if nxt < len(rows) else None}

    return app
