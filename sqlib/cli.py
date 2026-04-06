from __future__ import annotations

import argparse

from sqlib.services.daily_sync import sync_daily


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sqlib")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync-daily")
    sync_parser.add_argument("--ts-code", action="append", required=True)
    sync_parser.add_argument("--start-date")
    sync_parser.add_argument("--end-date")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "sync-daily":
        result = sync_daily(
            args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        if result.failures:
            for ts_code, error in result.failures.items():
                print(f"FAIL {ts_code}: {error}")
            return 1
        for ts_code in result.successes:
            print(f"SYNCED {ts_code}")
        for ts_code in result.noops:
            print(f"NOOP {ts_code}")
        return 0

    parser.error(f"unsupported command: {args.command}")
