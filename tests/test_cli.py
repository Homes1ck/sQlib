from sqlib.cli import build_parser


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
