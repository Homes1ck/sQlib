import pytest
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


def test_sync_daily_honors_explicit_backfill_start_date(temp_data_dir):
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

    result = sync_daily(["000001.SZ"], start_date="2024-01-01", store=store, client=client)

    assert client.calls == [("000001.SZ", "20240101", None)]
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


def test_sync_daily_skips_write_when_merge_is_unchanged(temp_data_dir):
    class TrackingStore(ParquetDailyStore):
        def __init__(self, base_dir):
            super().__init__(base_dir=base_dir)
            self.write_count = 0

        def write(self, ts_code, frame):
            self.write_count += 1
            return super().write(ts_code, frame)

    store = TrackingStore(base_dir=temp_data_dir)
    ParquetDailyStore.write(
        store,
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
    client = DummyClient(
        frame=pd.DataFrame(
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
        )
    )

    result = sync_daily(["000001.SZ"], store=store, client=client)

    assert result.noops == ["000001.SZ"]
    assert result.successes == []
    assert store.write_count == 0


def test_sync_daily_is_idempotent_across_two_runs(temp_data_dir):
    class TrackingStore(ParquetDailyStore):
        def __init__(self, base_dir):
            super().__init__(base_dir=base_dir)
            self.write_count = 0

        def write(self, ts_code, frame):
            self.write_count += 1
            return super().write(ts_code, frame)

    class SequentialClient:
        def __init__(self):
            self.calls = []
            self.frames = [
                pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": pd.Timestamp("2024-01-03"),
                            "open": 10.5,
                            "high": 11.2,
                            "low": 10.1,
                            "close": 10.8,
                            "vol": 12.0,
                            "amount": 120.0,
                        },
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": pd.Timestamp("2024-01-04"),
                            "open": 10.8,
                            "high": 11.3,
                            "low": 10.4,
                            "close": 11.0,
                            "vol": 15.0,
                            "amount": 130.0,
                        },
                    ]
                ),
                pd.DataFrame(
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
                ),
            ]

        def fetch_daily(self, ts_code, start_date=None, end_date=None):
            self.calls.append((ts_code, start_date, end_date))
            return self.frames.pop(0)

    store = TrackingStore(base_dir=temp_data_dir)
    client = SequentialClient()

    first = sync_daily(["000001.SZ"], store=store, client=client)
    written_once = store.read("000001.SZ")
    second = sync_daily(["000001.SZ"], store=store, client=client)
    written_twice = store.read("000001.SZ")

    assert client.calls == [
        ("000001.SZ", None, None),
        ("000001.SZ", "20240105", None),
    ]
    assert first.successes == ["000001.SZ"]
    assert first.noops == []
    assert second.successes == []
    assert second.noops == ["000001.SZ"]
    assert store.write_count == 1
    pd.testing.assert_frame_equal(written_twice, written_once)


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


def test_sync_daily_does_not_mask_malformed_fetch_results(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    client = DummyClient(frame="not-a-dataframe")

    with pytest.raises(AttributeError):
        sync_daily(["000001.SZ"], store=store, client=client)
