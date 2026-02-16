#!/usr/bin/env python3
import argparse
import json
import math
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://stock.naver.com/api/foreign/market/stock/global"
REFERER_URL = "https://stock.naver.com/market/stock/usa/stocklist/marketValue"

EXCHANGE_SUFFIXES = {"O", "K", "N", "A", "P", "PK"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Korean company names from Naver US market cap ranking."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=300,
        help="How many top symbols to fetch (default: 300)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=60,
        help="Naver page size per request (default: 60)",
    )
    parser.add_argument(
        "--output",
        default="data/company_names_ko.json",
        help="Output JSON path (default: data/company_names_ko.json)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace output with fetched names only (default: merge with existing).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout seconds (default: 20)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retry count per page request (default: 3)",
    )
    return parser.parse_args()


def normalize_symbol_from_symbol_code(raw: str) -> str:
    symbol = raw.strip().replace(" ", "")
    if not symbol:
        return ""
    return symbol.replace(".", "-").upper()


def normalize_symbol_from_reuters_code(raw: str) -> str:
    symbol = raw.strip().replace(" ", "")
    if not symbol:
        return ""
    if "." in symbol:
        base, suffix = symbol.rsplit(".", 1)
        if suffix.upper() in EXCHANGE_SUFFIXES:
            symbol = base
    if re.fullmatch(r"[A-Z]{1,6}[a-z]", symbol):
        # Example: BRKb -> BRK-B
        symbol = f"{symbol[:-1]}-{symbol[-1].upper()}"
    return symbol.replace(".", "-").upper()


def normalize_symbol(row: dict) -> str:
    symbol_code = row.get("symbolCode")
    if isinstance(symbol_code, str) and symbol_code.strip():
        return normalize_symbol_from_symbol_code(symbol_code)
    reuters_code = row.get("reutersCode")
    if isinstance(reuters_code, str) and reuters_code.strip():
        return normalize_symbol_from_reuters_code(reuters_code)
    return ""


def build_request_url(start_idx: int, page_size: int) -> str:
    query = urlencode(
        {
            "nation": "usa",
            "tradeType": "ALL",
            "orderType": "marketValue",
            "startIdx": start_idx,
            "pageSize": page_size,
        }
    )
    return f"{API_URL}?{query}"


def fetch_page(start_idx: int, page_size: int, timeout: float, max_retries: int) -> List[dict]:
    url = build_request_url(start_idx=start_idx, page_size=page_size)
    headers = {
        "Accept": "application/json",
        "Referer": REFERER_URL,
        "User-Agent": "Mozilla/5.0 (compatible; marketcap-tracker/1.0)",
    }
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(url, headers=headers, method="GET")
            with urlopen(request, timeout=timeout) as response:
                payload = json.load(response)
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                rows = payload.get("datas")
                if isinstance(rows, list):
                    return rows
            return []
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(0.7 * attempt)
                continue
            break

    raise RuntimeError(f"Failed to fetch page startIdx={start_idx}: {last_error}")


def load_existing(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}

    out: Dict[str, dict] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        symbol = key.strip().upper()
        if not symbol:
            continue
        if isinstance(value, dict):
            item = {}
            name_en = value.get("name_en")
            name_ko = value.get("name_ko")
            if isinstance(name_en, str) and name_en.strip():
                item["name_en"] = name_en.strip()
            if isinstance(name_ko, str) and name_ko.strip():
                item["name_ko"] = name_ko.strip()
            if item:
                out[symbol] = item
            continue
        if isinstance(value, str) and value.strip():
            out[symbol] = {"name_ko": value.strip()}
    return out


def fetch_names(limit: int, page_size: int, timeout: float, max_retries: int) -> Dict[str, dict]:
    if limit <= 0:
        return {}

    max_pages = max(1, math.ceil(limit / page_size))
    out: Dict[str, dict] = {}

    for page in range(max_pages):
        rows = fetch_page(start_idx=page, page_size=page_size, timeout=timeout, max_retries=max_retries)
        if not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = normalize_symbol(row)
            if not symbol or symbol in out:
                continue
            name_ko = row.get("koreanCodeName")
            if not isinstance(name_ko, str) or not name_ko.strip():
                continue
            item = {"name_ko": name_ko.strip()}
            name_en = row.get("englishCodeName")
            if isinstance(name_en, str) and name_en.strip():
                item["name_en"] = name_en.strip()
            out[symbol] = item
            if len(out) >= limit:
                return out

    return out


def merge_names(existing: Dict[str, dict], fetched: Dict[str, dict]) -> Dict[str, dict]:
    merged = dict(existing)
    for symbol, item in fetched.items():
        current = dict(merged.get(symbol, {}))
        current["name_ko"] = item["name_ko"]
        if "name_en" not in current and isinstance(item.get("name_en"), str):
            current["name_en"] = item["name_en"]
        merged[symbol] = current
    return dict(sorted(merged.items(), key=lambda kv: kv[0]))


def main() -> None:
    args = parse_args()
    if args.limit <= 0:
        raise SystemExit("--limit must be >= 1")
    if args.page_size <= 0:
        raise SystemExit("--page-size must be >= 1")
    if args.max_retries <= 0:
        raise SystemExit("--max-retries must be >= 1")

    fetched = fetch_names(
        limit=args.limit,
        page_size=args.page_size,
        timeout=args.timeout,
        max_retries=args.max_retries,
    )
    output_path = Path(args.output)

    if args.replace:
        result = dict(sorted(fetched.items(), key=lambda kv: kv[0]))
    else:
        existing = load_existing(output_path)
        result = merge_names(existing, fetched)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Fetched symbols: {len(fetched)}")
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
