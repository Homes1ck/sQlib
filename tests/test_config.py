from sqlib.config import Settings


def test_settings_defaults_to_cwd_local_data_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("SQLIB_DATA_DIR", raising=False)
    monkeypatch.delenv("SQLIB_REQUEST_SLEEP", raising=False)
    monkeypatch.chdir(tmp_path)

    settings = Settings.from_env()

    assert settings.tushare_token is None
    assert settings.data_dir == tmp_path / "sqlib_data"
    assert settings.request_sleep == 0.0


def test_settings_uses_explicit_data_dir_and_normalizes_empty_token(tmp_path, monkeypatch):
    custom_dir = tmp_path / "custom-data"
    monkeypatch.setenv("TUSHARE_TOKEN", "   ")
    monkeypatch.setenv("SQLIB_DATA_DIR", str(custom_dir))
    monkeypatch.setenv("SQLIB_REQUEST_SLEEP", "1.5")

    settings = Settings.from_env()

    assert settings.tushare_token is None
    assert settings.data_dir == custom_dir.resolve()
    assert settings.request_sleep == 1.5


def test_settings_rejects_invalid_request_sleep(monkeypatch):
    monkeypatch.setenv("SQLIB_REQUEST_SLEEP", "nan")

    try:
        Settings.from_env()
    except ValueError as exc:
        assert str(exc) == "SQLIB_REQUEST_SLEEP must be a finite, non-negative number"
    else:
        raise AssertionError("expected ValueError for invalid SQLIB_REQUEST_SLEEP")
