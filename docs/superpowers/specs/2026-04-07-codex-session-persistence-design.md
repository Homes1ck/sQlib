# Codex Session Persistence Design

## Goal

Persist the current Codex `session_id` into the active project's root directory when Codex exits, so the next Codex launch in that project can continue from the saved session.

The first version should be lightweight, local-only, and non-intrusive to repository code. It should work by wrapping Codex startup and shutdown behavior rather than modifying project application code.

## Scope

### In Scope

- Save the current Codex `session_id` on exit
- Write the state file to the current project root
- Use a structured JSON file named `.codex-session.json`
- Read the saved session file on the next launch
- Make the saved session available for continuation logic
- Keep the implementation reusable across projects

### Out of Scope

- Multi-session history
- Cross-machine sync
- Cloud persistence
- Concurrent session conflict resolution
- Storing arbitrary command history
- Project-specific application logic changes

## Storage Format

The root-level file format is:

```json
{
  "session_id": "019d633e-21b2-7591-b3cf-3845aba47868",
  "project_root": "/Users/dynam1te/sQlib",
  "updated_at": "2026-04-07T12:34:56Z"
}
```

### Field Rules

- `session_id` is the most recent Codex session identifier for this project
- `project_root` is the absolute root path used when writing the file
- `updated_at` is a UTC ISO-8601 timestamp for the write event

The first version stores only one active session per project. New exits overwrite the previous value.

## Architecture

The feature should live outside project business logic and act as a local Codex workflow utility.

### 1. Project Root Resolver

Responsibility:

- Determine the current project root from the working directory
- Prefer a git root when inside a git repository
- Fall back to the current directory if no repository root is found

### 2. Session State Reader/Writer

Responsibility:

- Read `.codex-session.json` from the resolved project root
- Validate the stored JSON shape
- Write a new `.codex-session.json` atomically

Rules:

- Do not write partial or malformed JSON
- Do not overwrite with empty or missing `session_id`
- Always write absolute `project_root`

### 3. Codex Launch/Exit Wrapper

Responsibility:

- Launch Codex through a small wrapper command or script
- Capture or infer the active `session_id`
- Persist session state on exit
- Read the saved session state before launch so continuation can be offered or triggered

Rules:

- The wrapper must be the integration point
- The first version should not require changes to project source code
- The wrapper should fail safely if session extraction is unavailable

## Integration Strategy

The implementation should use a local wrapper around Codex rather than modifying repository code.

Recommended flow:

1. User launches Codex via a wrapper command
2. Wrapper resolves the project root
3. Wrapper reads `.codex-session.json` if it exists
4. Wrapper uses the saved `session_id` to continue or offer continuation
5. On exit, wrapper extracts the active `session_id`
6. Wrapper writes `.codex-session.json` into the project root

This keeps the behavior reusable across repositories while still storing state locally inside each project.

## Session ID Source

The implementation may use local Codex state to obtain the current `session_id`, such as:

- the current launch context if Codex exposes it
- local Codex history metadata under `~/.codex/`

The implementation must not assume the project itself knows the session identifier.

## Error Handling

The first version should be conservative and explicit.

### Required Behaviors

- If no `session_id` can be determined, do not write a broken state file
- If `.codex-session.json` is malformed, ignore it and continue safely
- If project root resolution fails, fall back to the current directory
- If writing fails, surface the failure clearly instead of silently pretending persistence succeeded

### Deferred Behaviors

- auto-repair of corrupted session files
- session conflict prompts across multiple terminals
- lock files or merge behavior

## Testing Strategy

The first version needs focused tests around local persistence behavior.

### Unit Tests

- resolves project root from a git repository path
- falls back to current directory outside git
- writes valid `.codex-session.json`
- refuses to write when `session_id` is missing
- reads valid session JSON into the expected structure
- ignores malformed JSON safely

### Integration Tests

- wrapper launch in a test project reads existing `.codex-session.json`
- wrapper exit writes updated `.codex-session.json`
- repeated exits overwrite the previous `session_id`

## Evolution Path

After the first version is stable, future iterations may add:

1. optional continuation prompt behavior
2. session history retention
3. per-project helper commands like `codex-resume`
4. conflict detection for multiple active terminals

The key design constraint is that the persisted session state should remain local to the project root and not require changes to repository application code.
