# AGENTS.md

## Project Intent

`sQlib` is being rebuilt as a lightweight quant framework.

Current implementation priority:

- ingest A-share daily OHLCV data from Tushare
- persist raw daily data locally as one parquet file per stock
- keep the architecture small now and compatible with future qlib-style provider abstractions

Current preferred architecture:

- `sqlib/config.py` for runtime settings
- `sqlib/data_sources/tushare/` for upstream market data access
- `sqlib/storage/` for raw persistence
- `sqlib/services/` for orchestration
- `tests/` for automated coverage

Planning and design documents live under:

- `docs/superpowers/specs/`
- `docs/superpowers/plans/`

## Legacy Boundary

The old prototype under paths such as these is legacy:

- `data/`
- `config/`
- `model/`
- `utils/`
- `cn_data/`
- `backtest/`
- legacy notebooks in the repo root

Rules:

- do not extend the legacy architecture unless explicitly asked
- prefer adding or changing code under `sqlib/`
- delete legacy code only after replacement behavior is implemented and verified

## Coding Principles

- Keep modules small and single-purpose.
- Prefer explicit interfaces and data contracts over implicit conventions.
- Prefer simple, inspectable data flow over framework-heavy abstractions.
- Use environment variables for runtime configuration unless there is a strong reason not to.
- Preserve idempotent behavior for sync/update workflows.
- Keep commits tightly scoped to the task being worked on.

## Data Rules

- Raw daily market data must be stored as parquet.
- Use one file per `ts_code` for the raw daily layer unless the task explicitly changes this design.
- Keep field names explicit and stable across client, storage, and service layers.
- Normalize date fields consistently before merge, persistence, or comparison.
- Do not silently rewrite storage layout contracts without updating tests and docs.

## Python Environment

Python must run inside the Conda environment `qlib`.

Before running Python or Python-C++ integration code, activate:

```bash
conda activate qlib
```

Hard rules:

- never use `.venv`
- never use system Python
- install all Python dependencies into `qlib`
- any scripts, tests, or integration checks must be runnable from inside `qlib`

If there is ambiguity between interpreters, prefer the interpreter from the active `qlib` environment and state which one was used.

## C++ Build

Use CMake for C++ components.

Build rules:

- prefer `Release` mode for performance-sensitive builds
- link with MKL for numerical kernels
- keep build configuration reproducible and scriptable
- ensure produced binaries or shared libraries can be consumed from Python inside `qlib`

When adding a Python-callable native component, document:

- build target name
- output artifact path
- how Python loads or invokes it

## Python-C++ Integration

- C++ binaries or shared libraries should be callable from Python inside `qlib`.
- Do not assume standalone native executables are sufficient if the feature is meant to be used from Python workflows.
- Validate import/load behavior from Python, not just native compilation success.
- Keep boundary contracts explicit: function names, argument types, return types, ownership, and error behavior.

## Numerical Rules

- Validate numerical stability in both Python and C++ paths.
- Avoid silent `NaN`/`Inf` propagation.
- Add explicit checks, assertions, or error handling when outputs can become non-finite.
- Prefer failing loudly over silently producing corrupted numerical results.
- Be careful with dtype conversion, overflow, division-by-zero, and implicit broadcasting.
- When introducing vectorized or native numerical code, document assumptions about shape, dtype, and finite-value requirements.

## Testing And Verification

Prefer automated tests under `tests/`.

Minimum expectation for changes:

- add or update focused tests for the changed behavior
- verify critical paths directly if automated tooling is unavailable
- do not claim success without concrete evidence

Known workspace issue:

- `python -m pytest` may exit abnormally with no usable output in this environment

If that happens:

- use direct runtime verification for the changed behavior
- report clearly that pytest output was not usable
- summarize what was verified manually
- do not claim full test-suite success without evidence

## Safety

- Do not revert unrelated local changes.
- Do not delete user work unless the task explicitly allows it.
- Do not hide operational failures behind broad exception handling unless the behavior is intentional and tested.
- Do not silently swallow malformed numerical data or schema mismatches.
- When behavior changes affect storage contracts or synchronization semantics, update both tests and docs in the same task.
