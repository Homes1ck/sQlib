import json
from pathlib import Path

from tools.codex_session.state import (
    SessionState,
    read_session_state,
    resolve_project_root,
    write_session_state,
)


def test_resolve_project_root_prefers_git_root(tmp_path):
    repo = tmp_path / "repo"
    nested = repo / "src" / "module"
    nested.mkdir(parents=True)
    (repo / ".git").mkdir()

    assert resolve_project_root(nested) == repo


def test_write_session_state_creates_expected_json(tmp_path):
    path = tmp_path / ".codex-session.json"
    state = SessionState(
        session_id="session-123",
        project_root=tmp_path,
        updated_at="2026-04-07T10:00:00Z",
    )

    write_session_state(path, state)

    payload = json.loads(path.read_text())
    assert payload == {
        "session_id": "session-123",
        "project_root": str(tmp_path.resolve()),
        "updated_at": "2026-04-07T10:00:00Z",
    }


def test_read_session_state_returns_none_for_malformed_json(tmp_path):
    path = tmp_path / ".codex-session.json"
    path.write_text("{bad json")

    assert read_session_state(path) is None
