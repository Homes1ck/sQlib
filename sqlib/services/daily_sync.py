from __future__ import annotations

import time
from dataclasses import dataclass, field

import pandas as pd

from sqlib.config import Settings
from sqlib.data_sources.tushare.client import TushareDailyClient
from sqlib.storage.parquet_daily import ParquetDailyStore


@dataclass
class SyncResult:
    successes: list[str] = field(default_factory=list)
    noops: list[str] = field(default_factory=list)
    failures: dict[str, str] = field(default_factory=dict)


def sync_daily(
    ts_codes: str | list[str],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    settings: Settings | None = None,
    client: TushareDailyClient | None = None,
    store: ParquetDailyStore | None = None,
) -> SyncResult:
    settings = settings or Settings.from_env()
    client = client or TushareDailyClient.from_settings(settings)
    store = store or ParquetDailyStore(base_dir=settings.data_dir)
    codes = [ts_codes] if isinstance(ts_codes, str) else list(ts_codes)

    result = SyncResult()
    for ts_code in codes:
        try:
            effective_start = _resolve_start_date(store, ts_code, start_date)
            fetched = client.fetch_daily(ts_code, start_date=effective_start, end_date=end_date)
            if fetched.empty:
                result.noops.append(ts_code)
                continue

            if store.exists(ts_code):
                existing = store.read(ts_code)
                merged = store.merge_frames(existing, fetched)
            else:
                merged = fetched.sort_values("trade_date").reset_index(drop=True)

            store.write(ts_code, merged)
            result.successes.append(ts_code)
            if settings.request_sleep:
                time.sleep(settings.request_sleep)
        except Exception as exc:
            result.failures[ts_code] = str(exc)

    return result


def _resolve_start_date(
    store: ParquetDailyStore, ts_code: str, requested_start: str | None
) -> str | None:
    normalized_request = _normalize_date(requested_start)
    if not store.exists(ts_code):
        return normalized_request

    existing = store.read(ts_code)
    if existing.empty:
        return normalized_request

    latest_trade_date = pd.to_datetime(existing["trade_date"]).max()
    if pd.isna(latest_trade_date):
        return normalized_request

    derived = (latest_trade_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
    if normalized_request is None:
        return derived

    return max(derived, normalized_request)


def _normalize_date(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace("-", "")
