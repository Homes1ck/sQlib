import pandas as pd

from sqlib.storage.parquet_daily import ParquetDailyStore


def test_merge_frames_deduplicates_and_sorts(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    old_frame = pd.DataFrame(
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
        ]
    )
    new_frame = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": pd.Timestamp("2024-01-03"),
                "open": 99.5,
                "high": 101.2,
                "low": 98.1,
                "close": 100.8,
                "vol": 120.0,
                "amount": 1200.0,
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
    )

    merged = store.merge_frames(old_frame, new_frame)

    assert merged["trade_date"].dt.strftime("%Y%m%d").tolist() == [
        "20240102",
        "20240103",
        "20240104",
    ]
    overwritten = merged.loc[merged["trade_date"] == pd.Timestamp("2024-01-03")].iloc[0]
    assert overwritten.to_dict() == {
        "ts_code": "000001.SZ",
        "trade_date": pd.Timestamp("2024-01-03"),
        "open": 99.5,
        "high": 101.2,
        "low": 98.1,
        "close": 100.8,
        "vol": 120.0,
        "amount": 1200.0,
    }


def test_write_and_read_roundtrip(temp_data_dir):
    store = ParquetDailyStore(base_dir=temp_data_dir)
    frame = pd.DataFrame(
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

    path = store.write("000001.SZ", frame)
    loaded = store.read("000001.SZ")

    assert path.exists()
    pd.testing.assert_frame_equal(loaded, frame, check_dtype=True)
    assert pd.api.types.is_datetime64_ns_dtype(loaded["trade_date"])
