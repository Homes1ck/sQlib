from __future__ import annotations

from datetime import date, datetime
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class ParquetDailyStore:
    base_dir: Path

    @staticmethod
    def _normalize_trade_date(value: object) -> pd.Timestamp:
        if pd.isna(value):
            return pd.NaT
        if isinstance(value, pd.Timestamp):
            return value.normalize()
        if isinstance(value, datetime):
            return pd.Timestamp(value).normalize()
        if isinstance(value, date):
            return pd.Timestamp(value)
        if isinstance(value, str):
            text = value.strip()
            if len(text) == 8 and text.isdigit():
                return pd.to_datetime(text, format="%Y%m%d")
            return pd.to_datetime(text)
        return pd.Timestamp(value).normalize()

    def raw_daily_dir(self) -> Path:
        return self.base_dir / "raw" / "daily"

    def file_path(self, ts_code: str) -> Path:
        return self.raw_daily_dir() / f"{ts_code}.parquet"

    def exists(self, ts_code: str) -> bool:
        return self.file_path(ts_code).exists()

    def read(self, ts_code: str) -> pd.DataFrame:
        path = self.file_path(ts_code)
        frame = pd.read_parquet(path)
        frame["trade_date"] = frame["trade_date"].map(self._normalize_trade_date)
        return frame

    def write(self, ts_code: str, frame: pd.DataFrame) -> Path:
        path = self.file_path(ts_code)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        return path

    def merge_frames(
        self, old_frame: pd.DataFrame, new_frame: pd.DataFrame
    ) -> pd.DataFrame:
        combined = pd.concat([old_frame, new_frame], ignore_index=True)
        combined["trade_date"] = combined["trade_date"].map(self._normalize_trade_date)
        combined = combined.drop_duplicates(subset=["trade_date"], keep="last")
        combined = combined.sort_values("trade_date").reset_index(drop=True)
        return combined.loc[
            :, ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
        ]
