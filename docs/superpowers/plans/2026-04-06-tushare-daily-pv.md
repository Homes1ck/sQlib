# Tushare Daily PV Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first usable `sQlib` slice that fetches A-share daily OHLCV data from Tushare and stores one parquet file per stock with full-sync and incremental-sync support.

**Architecture:** The implementation uses a lightweight layered design. A Tushare client fetches and normalizes market data, a parquet store owns all local file IO and merge behavior, and a sync service orchestrates per-stock full and incremental updates while returning structured results. A minimal CLI and environment-based config sit on top of those layers.

**Tech Stack:** Python 3.11+, pandas, pyarrow, tushare, pytest

---

## File Structure

### New Files

- `pyproject.toml` - project metadata, runtime dependencies, pytest configuration
- `sqlib/__init__.py` - package export surface
- `sqlib/config.py` - environment-based settings
- `sqlib/data_sources/__init__.py` - package marker
- `sqlib/data_sources/tushare/__init__.py` - Tushare package marker and exports
- `sqlib/data_sources/tushare/client.py` - Tushare client and dataframe normalization
- `sqlib/storage/__init__.py` - package marker
- `sqlib/storage/parquet_daily.py` - parquet path resolution, read, merge, write
- `sqlib/services/__init__.py` - package marker
- `sqlib/services/daily_sync.py` - batch orchestration and sync results
- `sqlib/cli.py` - CLI entry point for `sync-daily`
- `tests/conftest.py` - shared fixtures for temporary data directories
- `tests/test_config.py` - config loading tests
- `tests/test_tushare_client.py` - client normalization tests
- `tests/test_parquet_daily_store.py` - local storage behavior tests
- `tests/test_daily_sync.py` - service orchestration tests
- `tests/test_cli.py` - CLI contract tests

### Modified or Removed Files

- Remove legacy directories after the new package is working: `backtest/`, `cn_data/`, `config/`, `data/`, `instruments/`, `model/`, `utils/`
- Remove legacy notebooks after package verification: `qlib_workflow.ipynb`, `sqlib_workflow.ipynb`

The implementation should not delete the legacy code until the new package and tests pass. Deletion is the last task so there is always a working checkpoint before removing the old experiment code.

### Task 1: Project Bootstrap

**Files:**
- Create: `pyproject.toml`
- Create: `sqlib/__init__.py`
- Create: `sqlib/data_sources/__init__.py`
- Create: `sqlib/data_sources/tushare/__init__.py`
- Create: `sqlib/storage/__init__.py`
- Create: `sqlib/services/__init__.py`
- Create: `tests/conftest.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing config/bootstrap test**

```python
from pathlib import Path

from sqlib.config import Settings


def test_settings_defaults_to_repo_local_data_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("SQLIB_DATA_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    settings = Settings.from_env()

    assert settings.tushare_token is None
    assert settings.data_dir == tmp_path / "sqlib_data"
    assert settings.request_sleep == 0.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py::test_settings_defaults_to_repo_local_data_dir -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sqlib'`

- [ ] **Step 3: Write the package skeleton and config implementation**

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "sqlib"
version = "0.1.0"
description = "Lightweight quant research library with Tushare daily data ingestion"
requires-python = ">=3.11"
dependencies = [
  "pandas>=2.2",
  "pyarrow>=16.0.0",
  "tushare>=1.4.18",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.2",
]

[project.scripts]
sqlib = "sqlib.cli:main"

[tool.pytest.ini_options]
testpaths = ["tests"]
```

```python
# sqlib/__init__.py
from sqlib.config import Settings

__all__ = ["Settings"]
```

```python
# sqlib/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    tushare_token: str | None
    data_dir: Path
    request_sleep: float = 0.0

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.getenv("TUSHARE_TOKEN")
        data_dir = Path(os.getenv("SQLIB_DATA_DIR", "sqlib_data")).expanduser()
        sleep_raw = os.getenv("SQLIB_REQUEST_SLEEP", "0")
        return cls(
            tushare_token=token,
            data_dir=data_dir,
            request_sleep=float(sleep_raw),
        )
```

```python
# sqlib/data_sources/__init__.py
__all__: list[str] = []
```

```python
# sqlib/data_sources/tushare/__init__.py
from sqlib.data_sources.tushare.client import TushareDailyClient

__all__ = ["TushareDailyClient"]
```

```python
# sqlib/storage/__init__.py
__all__: list[str] = []
```

```python
# sqlib/services/__init__.py
__all__: list[str] = []
```

```python
# tests/conftest.py
from pathlib import Path

import pytest


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    path = tmp_path / "sqlib_data"
    path.mkdir()
    return path
```

- [ ] **Step 4: Add the test file**

```python
from sqlib.config import Settings


def test_settings_defaults_to_repo_local_data_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("SQLIB_DATA_DIR", raising=False)
    monkeypatch.delenv("SQLIB_REQUEST_SLEEP", raising=False)
    monkeypatch.chdir(tmp_path)

    settings = Settings.from_env()

    assert settings.tushare_token is None
    assert settings.data_dir == tmp_path / "sqlib_data"
    assert settings.request_sleep == 0.0
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `pytest tests/test_config.py::test_settings_defaults_to_repo_local_data_dir -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml sqlib/__init__.py sqlib/config.py sqlib/data_sources/__init__.py sqlib/data_sources/tushare/__init__.py sqlib/storage/__init__.py sqlib/services/__init__.py tests/conftest.py tests/test_config.py
git commit -m "feat: bootstrap sqlib package"
```

### Task 2: Tushare Daily Client

**Files:**
- Create: `sqlib/data_sources/tushare/client.py`
- Create: `tests/test_tushare_client.py`
- Modify: `sqlib/__init__.py`

- [ ] **Step 1: Write the failing client normalization test**

```python
import pandas as pd

from sqlib.data_sources.tushare.client import TushareDailyClient


class DummyPro:
    def daily(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240103",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "vol": 123.0,
                    "amount": 456.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "open": 9.8,
                    "high": 10.2,
                    "low": 9.6,
                    "close": 10.0,
                    "vol": 120.0,
                    "amount": 430.0,
                },
            ]
        )


def test_fetch_daily_normalizes_schema_and_sorts():
    client = TushareDailyClient(pro_api=DummyPro())

    result = client.fetch_daily("000001.SZ", start_date="20240101", end_date="20240131")

    assert list(result.columns) == [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
    ]
    assert result["trade_date"].dt.strftime("%Y%m%d").tolist() == ["20240102", "20240103"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_tushare_client.py::test_fetch_daily_normalizes_schema_and_sorts -v`
Expected: FAIL with `ModuleNotFoundError` or `ImportError` for `TushareDailyClient`

- [ ] **Step 3: Implement the daily client**

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd
import tushare as ts

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
```

```python
# sqlib/__init__.py
from sqlib.config import Settings
from sqlib.data_sources.tushare.client import TushareDailyClient

__all__ = ["Settings", "TushareDailyClient"]
```

- [ ] **Step 4: Add the client test file**

```python
import pandas as pd

from sqlib.data_sources.tushare.client import TushareDailyClient


class DummyPro:
    def daily(self, **kwargs):
        assert kwargs["ts_code"] == "000001.SZ"
        assert kwargs["start_date"] == "20240101"
        assert kwargs["end_date"] == "20240131"
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240103",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "vol": 123.0,
                    "amount": 456.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "open": 9.8,
                    "high": 10.2,
                    "low": 9.6,
                    "close": 10.0,
                    "vol": 120.0,
                    "amount": 430.0,
                },
            ]
        )


def test_fetch_daily_normalizes_schema_and_sorts():
    client = TushareDailyClient(pro_api=DummyPro())

    result = client.fetch_daily("000001.SZ", start_date="2024-01-01", end_date="2024-01-31")

    assert list(result.columns) == [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
    ]
    assert result["trade_date"].dt.strftime("%Y%m%d").tolist() == ["20240102", "20240103"]
```

- [ ] **Step 5: Run the client test to verify it passes**

Run: `pytest tests/test_tushare_client.py::test_fetch_daily_normalizes_schema_and_sorts -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add sqlib/__init__.py sqlib/data_sources/tushare/client.py tests/test_tushare_client.py
git commit -m "feat: add tushare daily client"
```

### Task 3: Parquet Store

**Files:**
- Create: `sqlib/storage/parquet_daily.py`
- Create: `tests/test_parquet_daily_store.py`

- [ ] **Step 1: Write the failing storage merge test**

```python
import pandas as pd

from sqlib.storage.parquet_daily import ParquetDailyStore


def test_merge_frames_deduplicates_and_sorts(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    old_frame = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-02"), "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 10.0, "amount": 100.0},
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
        ]
    )
    new_frame = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-04"), "open": 10.8, "high": 11.3, "low": 10.4, "close": 11.0, "vol": 15.0, "amount": 130.0},
        ]
    )

    merged = store.merge_frames(old_frame, new_frame)

    assert merged["trade_date"].dt.strftime("%Y%m%d").tolist() == ["20240102", "20240103", "20240104"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_parquet_daily_store.py::test_merge_frames_deduplicates_and_sorts -v`
Expected: FAIL with `ImportError` for `ParquetDailyStore`

- [ ] **Step 3: Implement the parquet store**

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class ParquetDailyStore:
    base_dir: Path

    def raw_daily_dir(self) -> Path:
        return self.base_dir / "raw" / "daily"

    def file_path(self, ts_code: str) -> Path:
        return self.raw_daily_dir() / f"{ts_code}.parquet"

    def exists(self, ts_code: str) -> bool:
        return self.file_path(ts_code).exists()

    def read(self, ts_code: str) -> pd.DataFrame:
        path = self.file_path(ts_code)
        frame = pd.read_parquet(path)
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        return frame

    def write(self, ts_code: str, frame: pd.DataFrame) -> Path:
        path = self.file_path(ts_code)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        return path

    def merge_frames(self, old_frame: pd.DataFrame, new_frame: pd.DataFrame) -> pd.DataFrame:
        combined = pd.concat([old_frame, new_frame], ignore_index=True)
        combined["trade_date"] = pd.to_datetime(combined["trade_date"])
        combined = combined.drop_duplicates(subset=["trade_date"], keep="last")
        combined = combined.sort_values("trade_date").reset_index(drop=True)
        return combined.loc[:, ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]]
```

- [ ] **Step 4: Add the storage tests**

```python
import pandas as pd

from sqlib.storage.parquet_daily import ParquetDailyStore


def test_merge_frames_deduplicates_and_sorts(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    old_frame = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-02"), "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 10.0, "amount": 100.0},
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
        ]
    )
    new_frame = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-04"), "open": 10.8, "high": 11.3, "low": 10.4, "close": 11.0, "vol": 15.0, "amount": 130.0},
        ]
    )

    merged = store.merge_frames(old_frame, new_frame)

    assert merged["trade_date"].dt.strftime("%Y%m%d").tolist() == ["20240102", "20240103", "20240104"]


def test_write_and_read_roundtrip(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    frame = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-02"), "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 10.0, "amount": 100.0},
        ]
    )

    path = store.write("000001.SZ", frame)
    loaded = store.read("000001.SZ")

    assert path.exists()
    assert loaded.to_dict(orient="records")[0]["ts_code"] == "000001.SZ"
```

- [ ] **Step 5: Run the storage tests to verify they pass**

Run: `pytest tests/test_parquet_daily_store.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add sqlib/storage/parquet_daily.py tests/test_parquet_daily_store.py
git commit -m "feat: add parquet daily store"
```

### Task 4: Sync Service

**Files:**
- Create: `sqlib/services/daily_sync.py`
- Create: `tests/test_daily_sync.py`

- [ ] **Step 1: Write the failing incremental sync test**

```python
import pandas as pd

from sqlib.services.daily_sync import sync_daily
from sqlib.storage.parquet_daily import ParquetDailyStore


class DummyClient:
    def __init__(self):
        self.calls = []

    def fetch_daily(self, ts_code, start_date=None, end_date=None):
        self.calls.append((ts_code, start_date, end_date))
        return pd.DataFrame(
            [
                {"ts_code": ts_code, "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
                {"ts_code": ts_code, "trade_date": pd.Timestamp("2024-01-04"), "open": 10.8, "high": 11.3, "low": 10.4, "close": 11.0, "vol": 15.0, "amount": 130.0},
            ]
        )


def test_sync_daily_uses_next_date_for_incremental_sync(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    store.write(
        "000001.SZ",
        pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-02"), "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 10.0, "amount": 100.0},
            ]
        ),
    )
    client = DummyClient()

    result = sync_daily(["000001.SZ"], store=store, client=client)

    assert client.calls == [("000001.SZ", "20240103", None)]
    assert result.successes == ["000001.SZ"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_daily_sync.py::test_sync_daily_uses_next_date_for_incremental_sync -v`
Expected: FAIL with `ImportError` for `sync_daily`

- [ ] **Step 3: Implement the sync service**

```python
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


def _resolve_start_date(store: ParquetDailyStore, ts_code: str, requested_start: str | None) -> str | None:
    if not store.exists(ts_code):
        return _normalize_date(requested_start)
    existing = store.read(ts_code)
    latest = existing["trade_date"].max() + pd.Timedelta(days=1)
    derived = latest.strftime("%Y%m%d")
    normalized_request = _normalize_date(requested_start)
    if normalized_request is None:
        return derived
    return max(derived, normalized_request)


def _normalize_date(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace("-", "")
```

- [ ] **Step 4: Add the sync service tests**

```python
import pandas as pd

from sqlib.services.daily_sync import sync_daily
from sqlib.storage.parquet_daily import ParquetDailyStore


class DummyClient:
    def __init__(self, frame=None, error=None):
        self.calls = []
        self.frame = frame
        self.error = error

    def fetch_daily(self, ts_code, start_date=None, end_date=None):
        self.calls.append((ts_code, start_date, end_date))
        if self.error:
            raise self.error
        if self.frame is not None:
            return self.frame.copy()
        return pd.DataFrame(
            [
                {"ts_code": ts_code, "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
                {"ts_code": ts_code, "trade_date": pd.Timestamp("2024-01-04"), "open": 10.8, "high": 11.3, "low": 10.4, "close": 11.0, "vol": 15.0, "amount": 130.0},
            ]
        )


def test_sync_daily_uses_next_date_for_incremental_sync(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    store.write(
        "000001.SZ",
        pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-01-02"), "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 10.0, "amount": 100.0},
            ]
        ),
    )
    client = DummyClient()

    result = sync_daily(["000001.SZ"], store=store, client=client)

    assert client.calls == [("000001.SZ", "20240103", None)]
    assert result.successes == ["000001.SZ"]


def test_sync_daily_treats_empty_frame_as_noop(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    client = DummyClient(frame=pd.DataFrame(columns=["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]))

    result = sync_daily(["000001.SZ"], store=store, client=client)

    assert result.noops == ["000001.SZ"]
    assert result.successes == []


def test_sync_daily_keeps_processing_after_one_failure(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)

    class MixedClient:
        def fetch_daily(self, ts_code, start_date=None, end_date=None):
            if ts_code == "000001.SZ":
                raise RuntimeError("boom")
            return pd.DataFrame(
                [
                    {"ts_code": ts_code, "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
                ]
            )

    result = sync_daily(["000001.SZ", "000002.SZ"], store=store, client=MixedClient())

    assert result.failures["000001.SZ"] == "boom"
    assert result.successes == ["000002.SZ"]
```

- [ ] **Step 5: Run the sync tests to verify they pass**

Run: `pytest tests/test_daily_sync.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add sqlib/services/daily_sync.py tests/test_daily_sync.py
git commit -m "feat: add daily sync service"
```

### Task 5: CLI Entry Point

**Files:**
- Create: `sqlib/cli.py`
- Create: `tests/test_cli.py`

- [ ] **Step 1: Write the failing CLI parsing test**

```python
from sqlib.cli import build_parser


def test_build_parser_accepts_multiple_ts_codes():
    parser = build_parser()

    args = parser.parse_args(["sync-daily", "--ts-code", "000001.SZ", "--ts-code", "000002.SZ", "--start-date", "20240101"])

    assert args.command == "sync-daily"
    assert args.ts_code == ["000001.SZ", "000002.SZ"]
    assert args.start_date == "20240101"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py::test_build_parser_accepts_multiple_ts_codes -v`
Expected: FAIL with `ImportError` for `build_parser`

- [ ] **Step 3: Implement the CLI**

```python
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
        result = sync_daily(args.ts_code, start_date=args.start_date, end_date=args.end_date)
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
```

- [ ] **Step 4: Add the CLI tests**

```python
from sqlib.cli import build_parser


def test_build_parser_accepts_multiple_ts_codes():
    parser = build_parser()

    args = parser.parse_args(["sync-daily", "--ts-code", "000001.SZ", "--ts-code", "000002.SZ", "--start-date", "20240101"])

    assert args.command == "sync-daily"
    assert args.ts_code == ["000001.SZ", "000002.SZ"]
    assert args.start_date == "20240101"
```

- [ ] **Step 5: Run the CLI test to verify it passes**

Run: `pytest tests/test_cli.py::test_build_parser_accepts_multiple_ts_codes -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add sqlib/cli.py tests/test_cli.py
git commit -m "feat: add sqlib cli"
```

### Task 6: End-to-End Verification and Legacy Cleanup

**Files:**
- Modify: `sqlib/services/daily_sync.py`
- Modify: `tests/test_daily_sync.py`
- Remove: `backtest/`
- Remove: `cn_data/`
- Remove: `config/`
- Remove: `data/`
- Remove: `instruments/`
- Remove: `model/`
- Remove: `utils/`
- Remove: `qlib_workflow.ipynb`
- Remove: `sqlib_workflow.ipynb`

- [ ] **Step 1: Write the failing end-to-end idempotency test**

```python
import pandas as pd

from sqlib.services.daily_sync import sync_daily
from sqlib.storage.parquet_daily import ParquetDailyStore


class StaticClient:
    def fetch_daily(self, ts_code, start_date=None, end_date=None):
        return pd.DataFrame(
            [
                {"ts_code": ts_code, "trade_date": pd.Timestamp("2024-01-02"), "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 10.0, "amount": 100.0},
                {"ts_code": ts_code, "trade_date": pd.Timestamp("2024-01-03"), "open": 10.5, "high": 11.2, "low": 10.1, "close": 10.8, "vol": 12.0, "amount": 120.0},
            ]
        )


def test_sync_daily_is_idempotent_across_two_runs(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    client = StaticClient()

    first = sync_daily(["000001.SZ"], store=store, client=client)
    second = sync_daily(["000001.SZ"], store=store, client=client)
    loaded = store.read("000001.SZ")

    assert first.successes == ["000001.SZ"]
    assert second.successes == ["000001.SZ"] or second.noops == ["000001.SZ"]
    assert loaded["trade_date"].dt.strftime("%Y%m%d").tolist() == ["20240102", "20240103"]
```

- [ ] **Step 2: Run the end-to-end test to verify current behavior**

Run: `pytest tests/test_daily_sync.py::test_sync_daily_is_idempotent_across_two_runs -v`
Expected: PASS after the implementation is complete

- [ ] **Step 3: Tighten the sync service if needed to satisfy the idempotency test**

```python
if fetched.empty:
    result.noops.append(ts_code)
    continue

if store.exists(ts_code):
    existing = store.read(ts_code)
    merged = store.merge_frames(existing, fetched)
    if len(merged) == len(existing) and merged["trade_date"].equals(existing["trade_date"]):
        result.noops.append(ts_code)
        continue
else:
    merged = fetched.sort_values("trade_date").reset_index(drop=True)

store.write(ts_code, merged)
result.successes.append(ts_code)
```

- [ ] **Step 4: Run the full test suite**

Run: `pytest -v`
Expected: all tests PASS

- [ ] **Step 5: Remove the legacy experimental code after tests pass**

Run:

```bash
rm -rf backtest cn_data config data instruments model utils qlib_workflow.ipynb sqlib_workflow.ipynb
```

Expected: the repository root now contains the new `sqlib/`, `tests/`, `docs/`, and project metadata instead of the old experiment modules

- [ ] **Step 6: Run the full test suite again after cleanup**

Run: `pytest -v`
Expected: all tests PASS with the old directories removed

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: replace legacy sqlib prototype with tushare ingestion core"
```

## Self-Review

### Spec Coverage

- Raw A-share daily ingestion: covered by Tasks 2 and 4
- Local parquet storage per stock: covered by Task 3
- Full sync and incremental sync: covered by Task 4
- Idempotent writes and no-op handling: covered by Tasks 3, 4, and 6
- Minimal CLI and environment config: covered by Tasks 1 and 5
- Narrow scope without factors, calendars, or qlib-style providers: enforced by the file list and task boundaries

### Placeholder Scan

- No `TODO`, `TBD`, or deferred implementation markers are left inside the tasks
- Each code step includes concrete file content rather than references to prior tasks
- Each verification step includes an exact `pytest` command

### Type Consistency

- `Settings`, `TushareDailyClient`, `ParquetDailyStore`, `SyncResult`, and `sync_daily` use consistent names across all tasks
- All daily bar dataframes use the same eight-column schema
- `trade_date` is consistently normalized to datetime-like values inside the code path
