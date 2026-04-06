from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Protocol

import pandas as pd

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
        try:
            ts = importlib.import_module("tushare")
        except ModuleNotFoundError as exc:
            if exc.name != "tushare":
                raise
            raise ModuleNotFoundError("tushare is required to create a live TushareDailyClient") from exc
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
            return self._empty_daily_frame()

        ordered = frame.loc[:, ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]].copy()
        ordered["trade_date"] = pd.to_datetime(ordered["trade_date"], format="%Y%m%d")
        ordered = ordered.sort_values("trade_date").reset_index(drop=True)
        return ordered

    @staticmethod
    def _normalize_date_arg(value: str | None) -> str | None:
        if value is None:
            return None
        return value.replace("-", "")

    @staticmethod
    def _empty_daily_frame() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ts_code": pd.Series(dtype="object"),
                "trade_date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "vol": pd.Series(dtype="float64"),
                "amount": pd.Series(dtype="float64"),
            }
        )
