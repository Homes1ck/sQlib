from sqlib import cli
from sqlib.cli import build_parser
from sqlib.services.daily_sync import SyncResult


def test_build_parser_accepts_multiple_ts_codes():
    parser = build_parser()

    args = parser.parse_args(
        [
            "sync-daily",
            "--ts-code",
            "000001.SZ",
            "--ts-code",
            "000002.SZ",
            "--start-date",
            "20240101",
        ]
    )

    assert args.command == "sync-daily"
    assert args.ts_code == ["000001.SZ", "000002.SZ"]
    assert args.start_date == "20240101"


def test_main_reports_all_outcomes_for_mixed_batch(monkeypatch, capsys):
    def fake_sync_daily(ts_codes, start_date=None, end_date=None):
        assert ts_codes == ["000001.SZ", "000002.SZ", "000003.SZ"]
        assert start_date == "20240101"
        assert end_date == "20240131"
        return SyncResult(
            successes=["000001.SZ"],
            noops=["000002.SZ"],
            failures={"000003.SZ": "temporary outage"},
        )

    monkeypatch.setattr(cli, "sync_daily", fake_sync_daily)

    exit_code = cli.main(
        [
            "sync-daily",
            "--ts-code",
            "000001.SZ",
            "--ts-code",
            "000002.SZ",
            "--ts-code",
            "000003.SZ",
            "--start-date",
            "20240101",
            "--end-date",
            "20240131",
        ]
    )

    assert exit_code == 1
    assert capsys.readouterr().out.splitlines() == [
        "SYNCED 000001.SZ",
        "NOOP 000002.SZ",
        "FAIL 000003.SZ: temporary outage",
    ]
