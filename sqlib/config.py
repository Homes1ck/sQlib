from __future__ import annotations

import os
import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    tushare_token: str | None
    data_dir: Path
    request_sleep: float = 0.0

    @classmethod
    def from_env(cls) -> "Settings":
        token = _normalize_token(os.getenv("TUSHARE_TOKEN"))
        data_dir = Path(os.getenv("SQLIB_DATA_DIR", "sqlib_data")).expanduser().resolve()
        sleep_raw = os.getenv("SQLIB_REQUEST_SLEEP", "0")
        try:
            request_sleep = float(sleep_raw)
        except ValueError as exc:
            raise ValueError("SQLIB_REQUEST_SLEEP must be a finite, non-negative number") from exc
        if not math.isfinite(request_sleep) or request_sleep < 0:
            raise ValueError("SQLIB_REQUEST_SLEEP must be a finite, non-negative number")
        return cls(
            tushare_token=token,
            data_dir=data_dir,
            request_sleep=request_sleep,
        )


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
