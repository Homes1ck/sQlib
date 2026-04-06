from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd

try:
    import tushare as ts
except ModuleNotFoundError:  # pragma: no cover - exercised only in environments without tushare
    ts = None

from sqlib.config import Settings


class TushareProApi(Protocol):
    def daily(self, **kwargs) -> pd.DataFrame: ...


@dataclass
class TushareDailyClient:
    pro_api: TushareProApi

    @classmethod
    def from_settings(cls, settings: Settings) -> "TushareDailyClient":
        if not settings.tushare_token:
            raise ValueError("TUSHARE_TOKEN is required to create a live TushareDailyClient")
        if ts is None:
            raise ModuleNotFoundError("tushare is required to create a live TushareDailyClient")
        ts.set_token(settings.tushare_token)
        return cls(pro_api=ts.pro_api())

    def fetch_daily(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        frame = self.pro_api.daily(
            ts_code=ts_code,
            start_date=self._normalize_date_arg(start_date),
            end_date=self._normalize_date_arg(end_date),
        )

        if frame.empty:
            return pd.DataFrame(
                columns=["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
            )

        ordered = frame.loc[:, ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]].copy()
        ordered["trade_date"] = pd.to_datetime(ordered["trade_date"], format="%Y%m%d")
        ordered = ordered.sort_values("trade_date").reset_index(drop=True)
        return ordered

    @staticmethod
    def _normalize_date_arg(value: str | None) -> str | None:
        if value is None:
            return None
        return value.replace("-", "")
