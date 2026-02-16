import json
import os
import sqlite3
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(__file__).parent / "data" / "marketcap.db"
STATIC_DIR = Path(__file__).parent / "static"
COMPANY_NAME_KO_PATH = Path(__file__).parent / "data" / "company_names_ko.json"
DEFAULT_LIMIT = 260
MAX_LIMIT = 260
MAX_EVENT_DAYS = 5475
MAX_EVENT_ITEMS = 500


def env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


APP_ENV = os.getenv("APP_ENV", "production").strip().lower()
ENABLE_DOCS = env_bool("ENABLE_DOCS", default=APP_ENV != "production")
RATE_LIMIT_WINDOW_SECONDS = max(1, int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60")))
RATE_LIMIT_MAX_REQUESTS = max(1, int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "90")))
API_CACHE_TTL_SECONDS = max(0, int(os.getenv("API_CACHE_TTL_SECONDS", "90")))
API_CACHE_MAX_ENTRIES = max(32, int(os.getenv("API_CACHE_MAX_ENTRIES", "256")))
TRUST_PROXY_HEADERS = env_bool("TRUST_PROXY_HEADERS", default=False)
GZIP_MIN_SIZE = max(256, int(os.getenv("GZIP_MIN_SIZE", "1024")))
STATIC_CACHE_SECONDS = max(0, int(os.getenv("STATIC_CACHE_SECONDS", "86400")))
API_PUBLIC_CACHE_SECONDS = max(0, int(os.getenv("API_PUBLIC_CACHE_SECONDS", "30")))

CSP_POLICY = "; ".join(
    [
        "default-src 'self'",
        "script-src 'self'",
        "style-src 'self'",
        "img-src 'self' data:",
        "font-src 'self' data:",
        "connect-src 'self'",
        "object-src 'none'",
        "base-uri 'self'",
        "frame-ancestors 'none'",
    ]
)

_rate_limit_log: Dict[str, Deque[float]] = defaultdict(deque)
_rate_limit_lock = threading.Lock()
_api_cache: Dict[Tuple[Any, ...], Tuple[float, Any]] = {}
_api_cache_lock = threading.Lock()
_company_names_cache: Dict[str, str] = {}
_company_names_mtime_ns: int = -1
_company_names_lock = threading.Lock()

app = FastAPI(
    title="US Market Cap Tracker (Yahoo Edition)",
    version="0.1.0",
    docs_url="/docs" if ENABLE_DOCS else None,
    redoc_url="/redoc" if ENABLE_DOCS else None,
    openapi_url="/openapi.json" if ENABLE_DOCS else None,
)
app.add_middleware(GZipMiddleware, minimum_size=GZIP_MIN_SIZE)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def client_ip(request: Request) -> str:
    if TRUST_PROXY_HEADERS:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.split(",")[0].strip() or "unknown"
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def is_rate_limited(ip: str) -> bool:
    now = time.time()
    min_allowed = now - RATE_LIMIT_WINDOW_SECONDS
    with _rate_limit_lock:
        history = _rate_limit_log[ip]
        while history and history[0] < min_allowed:
            history.popleft()
        if len(history) >= RATE_LIMIT_MAX_REQUESTS:
            return True
        history.append(now)
    return False


def cache_key(*parts: Any) -> Tuple[Any, ...]:
    db_mtime = int(DB_PATH.stat().st_mtime) if DB_PATH.exists() else 0
    return (db_mtime, *parts)


def cache_get(key: Tuple[Any, ...]) -> Optional[Any]:
    if API_CACHE_TTL_SECONDS <= 0:
        return None
    now = time.time()
    with _api_cache_lock:
        entry = _api_cache.get(key)
        if entry is None:
            return None
        expire_at, value = entry
        if expire_at < now:
            _api_cache.pop(key, None)
            return None
        return value


def cache_set(key: Tuple[Any, ...], value: Any) -> None:
    if API_CACHE_TTL_SECONDS <= 0:
        return
    with _api_cache_lock:
        _api_cache[key] = (time.time() + API_CACHE_TTL_SECONDS, value)
        if len(_api_cache) <= API_CACHE_MAX_ENTRIES:
            return

        # Prefer pruning expired entries first.
        now = time.time()
        for stale_key in [k for k, (expire_at, _) in _api_cache.items() if expire_at < now]:
            _api_cache.pop(stale_key, None)
            if len(_api_cache) <= API_CACHE_MAX_ENTRIES:
                return

        # Fallback: evict oldest inserted key.
        oldest_key = next(iter(_api_cache))
        _api_cache.pop(oldest_key, None)


@app.middleware("http")
async def add_security_headers_and_limit(request: Request, call_next):
    if not TRUST_PROXY_HEADERS:
        if (
            "x-forwarded-for" in request.headers
            or "x-real-ip" in request.headers
            or "forwarded" in request.headers
        ):
            return JSONResponse(
                status_code=400,
                content={"detail": "Forwarded headers are not accepted on this server."},
            )

    if request.url.path.startswith("/api/") and is_rate_limited(client_ip(request)):
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Please retry later."},
        )

    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
    response.headers["Content-Security-Policy"] = CSP_POLICY

    if request.url.path == "/health":
        response.headers["Cache-Control"] = "no-store"
    elif request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = f"public, max-age={STATIC_CACHE_SECONDS}"
    elif request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = (
            f"public, max-age={API_PUBLIC_CACHE_SECONDS}, stale-while-revalidate=60"
        )
    else:
        response.headers["Cache-Control"] = "no-store"

    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail="Database not found. Run fetch_and_store.py first.")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def get_latest_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(as_of_date) AS d FROM ranks").fetchone()
    return row["d"] if row and row["d"] else None


def fetch_snapshot(conn: sqlite3.Connection, as_of_date: str, limit: int) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT as_of_date, symbol, market_cap, price, rank
        FROM ranks
        WHERE as_of_date = ?
        ORDER BY rank ASC
        LIMIT ?
        """,
        (as_of_date, limit),
    ).fetchall()


def fetch_rank_history(conn: sqlite3.Connection, symbol: str, days: int) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT as_of_date, rank, market_cap, price
        FROM ranks
        WHERE symbol = ?
        ORDER BY as_of_date DESC
        LIMIT ?
        """,
        (symbol, days),
    ).fetchall()


def fetch_recent_dates(conn: sqlite3.Connection, days: int) -> List[str]:
    rows = conn.execute(
        "SELECT DISTINCT as_of_date FROM ranks ORDER BY as_of_date DESC LIMIT ?",
        (days,),
    ).fetchall()
    return sorted([r["as_of_date"] for r in rows])


def fetch_event_dates(conn: sqlite3.Connection, days: Optional[int]) -> List[str]:
    if days is None:
        rows = conn.execute("SELECT DISTINCT as_of_date FROM ranks ORDER BY as_of_date").fetchall()
        return [r[0] for r in rows]

    # Need previous day context for first comparison date.
    rows = conn.execute(
        "SELECT DISTINCT as_of_date FROM ranks ORDER BY as_of_date DESC LIMIT ?",
        (days + 1,),
    ).fetchall()
    return sorted([r[0] for r in rows])


def fetch_timeline_rows(conn: sqlite3.Connection, dates: List[str], limit: int) -> List[sqlite3.Row]:
    if not dates:
        return []
    return conn.execute(
        """
        SELECT as_of_date, symbol, rank, market_cap
        FROM ranks
        WHERE as_of_date >= ?
          AND as_of_date <= ?
          AND rank <= ?
        ORDER BY as_of_date ASC, rank ASC
        """,
        (dates[0], dates[-1], limit),
    ).fetchall()


def fetch_rank_maps(conn: sqlite3.Connection, dates: List[str], max_rank: int) -> Dict[str, Dict[str, int]]:
    if not dates:
        return {}
    rows = conn.execute(
        """
        SELECT as_of_date, symbol, rank
        FROM ranks
        WHERE as_of_date >= ?
          AND as_of_date <= ?
          AND rank <= ?
        ORDER BY as_of_date ASC, rank ASC
        """,
        (dates[0], dates[-1], max_rank),
    ).fetchall()
    out: Dict[str, Dict[str, int]] = {}
    for row in rows:
        out.setdefault(row["as_of_date"], {})[row["symbol"]] = row["rank"]
    return out


def load_company_names_ko() -> Dict[str, str]:
    global _company_names_cache, _company_names_mtime_ns

    if not COMPANY_NAME_KO_PATH.exists():
        with _company_names_lock:
            _company_names_cache = {}
            _company_names_mtime_ns = -1
        return {}
    mtime_ns = COMPANY_NAME_KO_PATH.stat().st_mtime_ns
    with _company_names_lock:
        if _company_names_mtime_ns == mtime_ns:
            return _company_names_cache

    try:
        raw = json.loads(COMPANY_NAME_KO_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

    if not isinstance(raw, dict):
        return {}

    out: Dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        symbol = key.strip().upper()
        if not symbol:
            continue

        if isinstance(value, str):
            name_ko = value.strip()
            if name_ko:
                out[symbol] = name_ko
            continue

        if isinstance(value, dict):
            name_ko = value.get("name_ko")
            if isinstance(name_ko, str) and name_ko.strip():
                out[symbol] = name_ko.strip()
    with _company_names_lock:
        _company_names_cache = out
        _company_names_mtime_ns = mtime_ns
    return out


def display_name(symbol: str, fallback_name: Optional[str], ko_map: Dict[str, str]) -> str:
    name_ko = ko_map.get(symbol.upper())
    if name_ko:
        return name_ko
    if fallback_name and fallback_name.strip() and fallback_name.strip().upper() != symbol.upper():
        return fallback_name.strip()
    return symbol


@app.get("/")
def root() -> FileResponse:
    index_path = STATIC_DIR / "index.html"
    return FileResponse(index_path)


@app.get("/api/ranks/latest")
def api_latest(limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT)):
    key = cache_key("latest", limit)
    cached = cache_get(key)
    if cached is not None:
        return cached

    with get_conn() as conn:
        latest_date = get_latest_date(conn)
        if not latest_date:
            raise HTTPException(status_code=404, detail="No data available")
        rows = fetch_snapshot(conn, latest_date, limit)

    ko_map = load_company_names_ko()
    out_rows = []
    for row in rows:
        item = dict(row)
        item["name"] = display_name(item["symbol"], item["symbol"], ko_map)
        out_rows.append(item)
    result = {"as_of_date": latest_date, "rows": out_rows}
    cache_set(key, result)
    return result


@app.get("/api/rank-history/{symbol}")
def api_rank_history(symbol: str, days: int = Query(365, ge=1, le=5475)):
    normalized_symbol = symbol.upper()
    key = cache_key("rank-history", normalized_symbol, days)
    cached = cache_get(key)
    if cached is not None:
        return cached

    with get_conn() as conn:
        rows = fetch_rank_history(conn, normalized_symbol, days)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history for {symbol}")
    # reverse chronological to chronological
    data = [dict(r) for r in reversed(rows)]
    result = {"symbol": normalized_symbol, "rows": data}
    cache_set(key, result)
    return result


@app.get("/api/ranks/timeline")
def api_ranks_timeline(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    days: int = Query(120, ge=2, le=5475),
):
    key = cache_key("timeline", limit, days)
    cached = cache_get(key)
    if cached is not None:
        return cached

    ko_map = load_company_names_ko()
    with get_conn() as conn:
        dates = fetch_recent_dates(conn, days)
        if not dates:
            raise HTTPException(status_code=404, detail="No data available")
        rows = fetch_timeline_rows(conn, dates, limit)
    if not rows:
        result = {"dates": dates, "series": [], "limit": limit}
        cache_set(key, result)
        return result

    idx_by_date = {d: i for i, d in enumerate(dates)}
    series_map: Dict[str, Dict] = {}
    for row in rows:
        symbol = row["symbol"]
        if symbol not in series_map:
            series_map[symbol] = {
                "symbol": symbol,
                "name": display_name(symbol, symbol, ko_map),
                "ranks": [None] * len(dates),
                "caps": [None] * len(dates),
            }
        date_idx = idx_by_date[row["as_of_date"]]
        series_map[symbol]["ranks"][date_idx] = row["rank"]
        series_map[symbol]["caps"][date_idx] = row["market_cap"]

    def sort_key(item: Dict) -> tuple:
        ranks = item["ranks"]
        latest_rank = ranks[-1] if ranks[-1] is not None else 99999
        best_rank = min([r for r in ranks if r is not None], default=99999)
        return (latest_rank, best_rank, item["symbol"])

    series = sorted(series_map.values(), key=sort_key)
    result = {
        "dates": dates,
        "series": series,
        "limit": limit,
    }
    cache_set(key, result)
    return result


@app.get("/api/events/new-entrants")
def api_new_entrants(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    days: Optional[int] = Query(None, ge=2, le=MAX_EVENT_DAYS),
    max_events: int = Query(100, ge=1, le=MAX_EVENT_ITEMS),
):
    key = cache_key("new-entrants", limit, days or "all", max_events)
    cached = cache_get(key)
    if cached is not None:
        return cached

    with get_conn() as conn:
        dates = fetch_event_dates(conn, days)
        if len(dates) < 2:
            result = {"events": [], "total_events": 0}
            cache_set(key, result)
            return result
        wide_maps = fetch_rank_maps(conn, dates, 1000)
        events = []
        for idx in range(1, len(dates)):
            prev_date = dates[idx - 1]
            date = dates[idx]
            prev_wide = wide_maps.get(prev_date, {})
            curr_wide = wide_maps.get(date, {})
            prev_top = {sym: rank for sym, rank in prev_wide.items() if rank <= limit}
            curr_set = {sym: rank for sym, rank in curr_wide.items() if rank <= limit}
            new_symbols = set(curr_set) - set(prev_top)
            for sym in new_symbols:
                events.append(
                    {
                        "date": date,
                        "symbol": sym,
                        "from_rank": prev_wide.get(sym),
                        "to_rank": curr_set[sym],
                    }
                )
    total_events = len(events)
    if max_events and total_events > max_events:
        events = events[-max_events:]
    result = {"events": events, "total_events": total_events}
    cache_set(key, result)
    return result


@app.get("/api/events/big-movers")
def api_big_movers(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    days: Optional[int] = Query(None, ge=2, le=MAX_EVENT_DAYS),
    max_events: int = Query(100, ge=1, le=MAX_EVENT_ITEMS),
    threshold: int = Query(5, ge=1, le=100),
):
    key = cache_key("big-movers", limit, days or "all", max_events, threshold)
    cached = cache_get(key)
    if cached is not None:
        return cached

    with get_conn() as conn:
        dates = fetch_event_dates(conn, days)
        if len(dates) < 2:
            result = {"events": [], "total_events": 0}
            cache_set(key, result)
            return result
        wide_maps = fetch_rank_maps(conn, dates, 1000)
        events = []
        prev_map: Dict[str, int] = wide_maps.get(dates[0], {})
        for date in dates[1:]:
            curr_map = wide_maps.get(date, {})
            for sym, curr_rank in curr_map.items():
                if curr_rank > limit:
                    continue
                prev_rank = prev_map.get(sym)
                if prev_rank and prev_rank - curr_rank >= threshold:
                    events.append(
                        {
                            "date": date,
                            "symbol": sym,
                            "from_rank": prev_rank,
                            "to_rank": curr_rank,
                        }
                    )
            prev_map = curr_map
    total_events = len(events)
    if max_events and total_events > max_events:
        events = events[-max_events:]
    result = {"events": events, "total_events": total_events}
    cache_set(key, result)
    return result


@app.get("/health")
def health():
    return {"status": "ok"}
