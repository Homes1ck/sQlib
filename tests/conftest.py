from pathlib import Path

import pytest


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    path = tmp_path / "sqlib_data"
    path.mkdir()
    return path
