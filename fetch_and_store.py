import argparse
import datetime as dt
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from yfinance import EquityQuery
from dotenv import load_dotenv

DB_PATH = Path(__file__).parent / "data" / "marketcap.db"
DEFAULT_TARGET_TOP = 260
DEFAULT_UNIVERSE_SIZE = 260


class ConfigError(Exception):
    pass


RANKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ranks (
    as_of_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    market_cap REAL,
    price REAL,
    rank INTEGER,
    captured_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date, symbol)
);
"""


def get_rank_columns(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("PRAGMA table_info(ranks)").fetchall()
    return [str(row[1]) for row in rows]


def migrate_ranks_schema(conn: sqlite3.Connection, columns: List[str]) -> None:
    if "name" not in columns and "exchange" not in columns:
        return

    print("Migrating ranks table: removing legacy columns (name, exchange) ...")
    try:
        conn.execute("BEGIN")
        conn.execute(
            """
            CREATE TABLE ranks_new (
                as_of_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market_cap REAL,
                price REAL,
                rank INTEGER,
                captured_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (as_of_date, symbol)
            );
            """
        )
        conn.execute(
            """
            INSERT INTO ranks_new (as_of_date, symbol, market_cap, price, rank, captured_at)
            SELECT
                as_of_date,
                symbol,
                market_cap,
                price,
                rank,
                COALESCE(captured_at, CURRENT_TIMESTAMP)
            FROM ranks;
            """
        )
        conn.execute("DROP TABLE ranks")
        conn.execute("ALTER TABLE ranks_new RENAME TO ranks")
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
        print("Migration complete.")


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(RANKS_TABLE_SQL)
    migrate_ranks_schema(conn, get_rank_columns(conn))
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ranks_date_rank ON ranks(as_of_date, rank);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ranks_symbol ON ranks(symbol);")


def fetch_screen_symbols(size: int) -> List[str]:
    # Yahoo screener sorted by market cap gives a broader, data-driven universe.
    query = EquityQuery(
        "and",
        [
            EquityQuery("eq", ["region", "us"]),
            EquityQuery("gt", ["intradaymarketcap", 0]),
            EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
        ],
    )

    symbols: List[str] = []
    seen = set()
    page = 0
    # Yahoo screener responses are paged; keep each request bounded.
    page_size = 250
    while len(symbols) < size:
        remaining = size - len(symbols)
        current_size = page_size if remaining > page_size else remaining
        response = yf.screen(
            query=query,
            size=current_size,
            offset=page * page_size,
            sortField="intradaymarketcap",
            sortAsc=False,
        )
        quotes = response.get("quotes") or []
        if not quotes:
            break
        for row in quotes:
            symbol = (row.get("symbol") or "").upper().strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
            if len(symbols) >= size:
                break
        page += 1
    return symbols


def normalize_index(index: pd.Index) -> pd.DatetimeIndex:
    out = pd.DatetimeIndex(index)
    if out.tz is not None:
        out = out.tz_convert("UTC").tz_localize(None)
    return out.normalize()


def read_fast_info_value(ticker: yf.Ticker, key: str) -> Optional[float]:
    try:
        fast_info = ticker.fast_info
    except Exception:
        return None
    try:
        value = fast_info.get(key)
    except Exception:
        try:
            value = fast_info[key]
        except Exception:
            return None
    return value


def coerce_shares_series(raw: object) -> Optional[pd.Series]:
    if raw is None:
        return None
    if isinstance(raw, pd.Series):
        return raw
    if isinstance(raw, pd.DataFrame):
        if raw.shape[1] == 0:
            return None
        return raw.iloc[:, 0]
    return None


def build_future_split_factor(
    close_index: pd.DatetimeIndex,
    split_series: Optional[pd.Series],
) -> pd.Series:
    # Yahoo close is split-adjusted. Convert historical shares to the same basis.
    if split_series is None or split_series.empty:
        return pd.Series(1.0, index=close_index, dtype="float64")

    out = split_series.dropna().astype(float)
    out.index = normalize_index(out.index)
    out = out[~out.index.duplicated(keep="last")]
    out = out.reindex(close_index, fill_value=0.0)

    factors: Dict[pd.Timestamp, float] = {}
    cumulative = 1.0
    for timestamp in reversed(close_index):
        factors[timestamp] = cumulative
        ratio = float(out.loc[timestamp])
        if ratio > 0:
            cumulative *= ratio
    return pd.Series([factors[timestamp] for timestamp in close_index], index=close_index, dtype="float64")


def correct_split_day_share_lag(
    close_index: pd.DatetimeIndex,
    shares_series: pd.Series,
    split_series: Optional[pd.Series],
    split_factor: pd.Series,
) -> pd.Series:
    # Yahoo shares can apply split effects 1-3 trading days late.
    if split_series is None or split_series.empty or shares_series.empty or split_factor.empty:
        return split_factor

    out = split_factor.copy()
    splits = split_series.dropna().astype(float)
    splits.index = normalize_index(splits.index)
    splits = splits[~splits.index.duplicated(keep="last")]
    splits = splits.reindex(close_index, fill_value=0.0)

    ratio_tolerance = 0.20
    stable_tolerance = 0.05
    max_lag_days = 3

    for split_date, ratio in splits.items():
        ratio = float(ratio)
        if ratio <= 0 or ratio == 1.0:
            continue
        if split_date not in close_index:
            continue

        split_pos = close_index.get_loc(split_date)
        if not isinstance(split_pos, int):
            continue

        base_shares = float(shares_series.iloc[split_pos])
        if pd.isna(base_shares) or base_shares <= 0:
            continue

        matched_lag: Optional[int] = None
        for lag in range(1, max_lag_days + 1):
            future_pos = split_pos + lag
            if future_pos >= len(close_index):
                break

            future_shares = float(shares_series.iloc[future_pos])
            if pd.isna(future_shares) or future_shares <= 0:
                continue

            observed_ratio = future_shares / base_shares
            lower = ratio * (1.0 - ratio_tolerance)
            upper = ratio * (1.0 + ratio_tolerance)
            if not (lower <= observed_ratio <= upper):
                continue

            # Ensure the series stayed effectively flat until the delayed jump.
            window = shares_series.iloc[split_pos:future_pos]
            if window.empty:
                continue
            max_relative = ((window.astype(float) - base_shares).abs() / base_shares).max()
            if pd.isna(max_relative) or max_relative > stable_tolerance:
                continue

            matched_lag = lag
            break

        if matched_lag is not None:
            out.iloc[split_pos : split_pos + matched_lag] *= ratio

    return out


def fetch_symbol_history(
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
) -> List[Tuple[str, float, float]]:
    ticker = yf.Ticker(symbol)

    history = ticker.history(
        start=start_date.isoformat(),
        end=(end_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        actions=True,
    )
    if history.empty or "Close" not in history:
        return []

    close = history["Close"].dropna().astype(float)
    if close.empty:
        return []
    close.index = normalize_index(close.index)
    close = close[~close.index.duplicated(keep="last")]

    split_series: Optional[pd.Series] = None
    if "Stock Splits" in history:
        raw_splits = history["Stock Splits"]
        if isinstance(raw_splits, pd.Series):
            split_series = raw_splits

    shares_series: Optional[pd.Series] = None
    try:
        raw_shares = ticker.get_shares_full(start=start_date.isoformat(), end=end_date.isoformat())
        shares_series = coerce_shares_series(raw_shares)
    except Exception:
        shares_series = None

    if shares_series is not None and not shares_series.empty:
        shares_series = shares_series.dropna().astype(float)
        shares_series.index = normalize_index(shares_series.index)
        shares_series = shares_series[~shares_series.index.duplicated(keep="last")]
        shares_series = shares_series.reindex(close.index, method="ffill")
        shares_series = shares_series.bfill()
        if shares_series.isna().all():
            shares_series = None

    if shares_series is None:
        shares_outstanding = read_fast_info_value(ticker, "shares")
        if not shares_outstanding:
            try:
                info = ticker.info
                shares_outstanding = info.get("sharesOutstanding")
            except Exception:
                shares_outstanding = None
        if not shares_outstanding:
            return []
        shares_series = pd.Series(float(shares_outstanding), index=close.index, dtype="float64")

    split_factor = build_future_split_factor(close.index, split_series)
    split_factor = correct_split_day_share_lag(
        close_index=close.index,
        shares_series=shares_series,
        split_series=split_series,
        split_factor=split_factor,
    )
    shares_series = shares_series * split_factor

    market_cap = close * shares_series
    rows: List[Tuple[str, float, float]] = []
    for timestamp, cap in market_cap.items():
        if pd.isna(cap) or cap <= 0:
            continue
        date_str = timestamp.date().isoformat()
        rows.append((date_str, float(cap), float(close.loc[timestamp])))
    return rows


def build_rank_rows(
    histories: Dict[str, List[Tuple[str, float, float]]],
    store_limit: int,
    target_date: Optional[str] = None,
    all_dates: bool = False,
) -> List[Tuple]:
    per_date: Dict[str, List[Tuple[str, float, float]]] = defaultdict(list)
    for symbol, series in histories.items():
        for date_str, market_cap, price in series:
            if target_date and date_str != target_date:
                continue
            per_date[date_str].append((symbol, market_cap, price))

    if not per_date:
        return []

    dates = sorted(per_date.keys())
    if not all_dates and not target_date:
        dates = [dates[-1]]

    rows: List[Tuple] = []
    for date_str in dates:
        ranked = sorted(per_date[date_str], key=lambda item: item[1], reverse=True)[:store_limit]
        for rank, (symbol, market_cap, price) in enumerate(ranked, start=1):
            rows.append((date_str, symbol, market_cap, price, rank))
    return rows


def replace_rows(conn: sqlite3.Connection, rows: List[Tuple]) -> int:
    if not rows:
        return 0
    dates = sorted({row[0] for row in rows})
    conn.execute(
        "DELETE FROM ranks WHERE as_of_date BETWEEN ? AND ?",
        (dates[0], dates[-1]),
    )
    cur = conn.executemany(
        """
        INSERT INTO ranks
        (as_of_date, symbol, market_cap, price, rank)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return cur.rowcount


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch market-cap ranking history using Yahoo Finance and store snapshots in SQLite."
    )
    parser.add_argument(
        "--universe-size",
        type=int,
        default=DEFAULT_UNIVERSE_SIZE,
        help="How many top symbols to request from Yahoo screener.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Calendar lookback window in days.",
    )
    parser.add_argument(
        "--symbols-limit",
        type=int,
        default=None,
        help="Use only first N symbols.",
    )
    parser.add_argument(
        "--store-limit",
        type=int,
        default=DEFAULT_TARGET_TOP,
        help="Store only top N ranks per day after sorting by market cap.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Store only this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--all-dates",
        action="store_true",
        help="Store all dates in lookback window (default stores latest date only).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview output without DB writes.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    symbols = fetch_screen_symbols(args.universe_size)
    if not symbols:
        raise ConfigError("Yahoo screener returned no symbols.")

    if args.symbols_limit:
        symbols = symbols[: args.symbols_limit]
    if not symbols:
        raise ConfigError("No symbols loaded from Yahoo screener.")

    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=args.days)

    print(
        f"Fetching Yahoo history for {len(symbols)} symbols "
        f"({start_date.isoformat()}..{end_date.isoformat()}) ..."
    )

    histories: Dict[str, List[Tuple[str, float, float]]] = {}
    failures: Dict[str, str] = {}

    for idx, symbol in enumerate(symbols, start=1):
        try:
            rows = fetch_symbol_history(symbol, start_date=start_date, end_date=end_date)
        except Exception as exc:
            failures[symbol] = str(exc)
            print(f"{symbol}: failed ({failures[symbol][:120]})")
            continue
        if rows:
            histories[symbol] = rows
        else:
            failures[symbol] = "empty series"
        if idx % 20 == 0 or idx == len(symbols):
            print(f"... progress {idx}/{len(symbols)}")

    if not histories:
        sample = ", ".join(f"{sym}={reason}" for sym, reason in list(failures.items())[:3])
        raise ConfigError(f"No usable symbol history fetched. Sample failures: {sample}")

    rank_rows = build_rank_rows(
        histories=histories,
        store_limit=args.store_limit,
        target_date=args.date,
        all_dates=args.all_dates,
    )
    if not rank_rows:
        raise ConfigError("No ranked rows produced. Verify --date or widen --days.")

    dates = sorted({row[0] for row in rank_rows})
    preview_date = dates[-1]
    print(
        f"Prepared {len(rank_rows)} rows across {len(dates)} date(s): "
        f"{dates[0]} .. {dates[-1]} (top {args.store_limit}/day)"
    )
    print(f"Top 15 preview for {preview_date}:")
    preview = [row for row in rank_rows if row[0] == preview_date][:15]
    for row in preview:
        _, symbol, market_cap, price, rank = row
        print(f"#{rank:>2} {symbol:<6} ${price:>8.2f}  mcap={market_cap/1e9:>8.1f}B")

    if failures:
        print(f"Warning: {len(failures)} symbols had no usable data.")

    if args.dry_run:
        print("Dry run complete; nothing written.")
        return

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        ensure_db(conn)
        count = replace_rows(conn, rank_rows)
    print(f"Upserted {count} rows into {DB_PATH}")


if __name__ == "__main__":
    try:
        main()
    except ConfigError as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
