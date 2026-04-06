import pandas as pd
import pytest

from sqlib.config import Settings
from sqlib.data_sources.tushare.client import TushareDailyClient
from sqlib.data_sources.tushare import client as tushare_client_module


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


def test_fetch_daily_empty_frame_preserves_datetime_trade_date():
    class EmptyPro:
        def daily(self, **kwargs):
            return pd.DataFrame()

    client = TushareDailyClient(pro_api=EmptyPro())

    result = client.fetch_daily("000001.SZ")

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
    assert pd.api.types.is_datetime64_any_dtype(result["trade_date"])
    assert result["trade_date"].dt.strftime("%Y%m%d").tolist() == []


def test_from_settings_reraises_transitive_module_not_found(monkeypatch):
    settings = Settings(tushare_token="token", data_dir=None)  # type: ignore[arg-type]

    def raising_import(name: str):
        raise ModuleNotFoundError("no module named 'fake_dependency'", name="fake_dependency")

    monkeypatch.setattr(tushare_client_module.importlib, "import_module", raising_import)

    with pytest.raises(ModuleNotFoundError) as exc_info:
        TushareDailyClient.from_settings(settings)

    assert exc_info.value.name == "fake_dependency"
