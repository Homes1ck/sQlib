import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import pandas as pd

from sqlib.config import Settings
from sqlib.data_sources.tushare.client import TushareDailyClient
from sqlib.storage.parquet_daily import ParquetDailyStore


@dataclass
class SyncResult:
    successes: List[str] = field(default_factory=list)
    noops: List[str] = field(default_factory=list)
    failures: Dict[str, str] = field(default_factory=dict)


def sync_daily(
    ts_codes: Union[str, List[str]],
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    settings: Optional[Settings] = None,
    client: Optional[TushareDailyClient] = None,
    store: Optional[ParquetDailyStore] = None,
) -> SyncResult:
    settings = settings or Settings.from_env()
    client = client or TushareDailyClient.from_settings(settings)
    store = store or ParquetDailyStore(base_dir=settings.data_dir)
    codes = [ts_codes] if isinstance(ts_codes, str) else list(ts_codes)

    result = SyncResult()
    for ts_code in codes:
        effective_start = _resolve_start_date(store, ts_code, start_date)
        try:
            fetched = client.fetch_daily(ts_code, start_date=effective_start, end_date=end_date)
        except Exception as exc:
            if isinstance(exc, (AttributeError, TypeError)):
                raise
            result.failures[ts_code] = str(exc)
            continue

        if fetched.empty:
            result.noops.append(ts_code)
            continue

        if store.exists(ts_code):
            existing = store.read(ts_code)
            merged = store.merge_frames(existing, fetched)
            if merged.equals(existing):
                result.noops.append(ts_code)
                continue
        else:
            merged = fetched.sort_values("trade_date").reset_index(drop=True)

        store.write(ts_code, merged)
        result.successes.append(ts_code)
        if settings.request_sleep:
            time.sleep(settings.request_sleep)

    return result


def _resolve_start_date(
    store: ParquetDailyStore, ts_code: str, requested_start: Optional[str]
) -> Optional[str]:
    normalized_request = _normalize_date(requested_start)
    if not store.exists(ts_code):
        return normalized_request

    if normalized_request is not None:
        return normalized_request

    existing = store.read(ts_code)
    if existing.empty:
        return None

    latest_trade_date = pd.to_datetime(existing["trade_date"]).max()
    if pd.isna(latest_trade_date):
        return None

    return (latest_trade_date + pd.Timedelta(days=1)).strftime("%Y%m%d")


def _normalize_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.replace("-", "")
