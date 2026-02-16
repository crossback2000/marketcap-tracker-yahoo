import argparse
import runpy
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Yahoo historical backfill helper.")
    parser.add_argument("--days", type=int, default=5475, help="Calendar lookback in days.")
    parser.add_argument(
        "--universe-size",
        type=int,
        default=260,
        help="How many top symbols to request from Yahoo screener.",
    )
    parser.add_argument("--symbols-limit", type=int, default=None, help="Use only first N symbols.")
    parser.add_argument("--store-limit", type=int, default=260, help="Store top N ranks per day.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fetch_script = Path(__file__).parent / "fetch_and_store.py"

    script_args = [
        "--all-dates",
        "--days",
        str(args.days),
        "--universe-size",
        str(args.universe_size),
        "--store-limit",
        str(args.store_limit),
    ]
    if args.symbols_limit:
        script_args.extend(["--symbols-limit", str(args.symbols_limit)])
    if args.dry_run:
        script_args.append("--dry-run")

    print("Running:", " ".join([str(sys.executable), str(fetch_script), *script_args]))
    original_argv = sys.argv[:]
    try:
        sys.argv = [str(fetch_script), *script_args]
        runpy.run_path(str(fetch_script), run_name="__main__")
    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    main()
