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
