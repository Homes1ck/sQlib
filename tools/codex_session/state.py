from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class SessionState:
    session_id: str
    project_root: Path
    updated_at: str


def resolve_project_root(cwd: Path) -> Path:
    current = cwd.resolve()
    for path in [current] + list(current.parents):
        if (path / ".git").exists():
            return path
    return current


def read_session_state(path: Path) -> Optional[SessionState]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError, TypeError):
        return None

    if not isinstance(payload, dict):
        return None

    session_id = payload.get("session_id")
    project_root = payload.get("project_root")
    updated_at = payload.get("updated_at")
    if not all(isinstance(value, str) and value for value in [session_id, project_root, updated_at]):
        return None

    return SessionState(
        session_id=session_id,
        project_root=Path(project_root),
        updated_at=updated_at,
    )


def write_session_state(path: Path, state: SessionState) -> None:
    if not state.session_id:
        raise ValueError("session_id is required")

    payload = {
        "session_id": state.session_id,
        "project_root": str(state.project_root.resolve()),
        "updated_at": state.updated_at,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp_path.replace(path)
