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
                {
                    "ts_code": ts_code,
                    "trade_date": pd.Timestamp("2024-01-03"),
                    "open": 10.5,
                    "high": 11.2,
                    "low": 10.1,
                    "close": 10.8,
                    "vol": 12.0,
                    "amount": 120.0,
                },
                {
                    "ts_code": ts_code,
                    "trade_date": pd.Timestamp("2024-01-04"),
                    "open": 10.8,
                    "high": 11.3,
                    "low": 10.4,
                    "close": 11.0,
                    "vol": 15.0,
                    "amount": 130.0,
                },
            ]
        )


def test_sync_daily_uses_next_date_for_incremental_sync(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    store.write(
        "000001.SZ",
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": pd.Timestamp("2024-01-02"),
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "vol": 10.0,
                    "amount": 100.0,
                },
            ]
        ),
    )
    client = DummyClient()

    result = sync_daily(["000001.SZ"], store=store, client=client)

    assert client.calls == [("000001.SZ", "20240103", None)]
    assert result.successes == ["000001.SZ"]


def test_sync_daily_treats_empty_frame_as_noop(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    client = DummyClient(
        frame=pd.DataFrame(
            columns=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
            ]
        )
    )

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
                    {
                        "ts_code": ts_code,
                        "trade_date": pd.Timestamp("2024-01-03"),
                        "open": 10.5,
                        "high": 11.2,
                        "low": 10.1,
                        "close": 10.8,
                        "vol": 12.0,
                        "amount": 120.0,
                    },
                ]
            )

    result = sync_daily(["000001.SZ", "000002.SZ"], store=store, client=MixedClient())

    assert result.failures["000001.SZ"] == "boom"
    assert result.successes == ["000002.SZ"]
