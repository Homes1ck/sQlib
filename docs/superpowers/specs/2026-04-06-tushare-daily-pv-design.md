# sQlib Tushare Daily PV Design

## Goal

Rebuild the first usable slice of `sQlib` as a lightweight market data ingestion layer that fetches A-share daily OHLCV data from Tushare and stores it locally in a durable raw-data layout.

This first slice is intentionally narrow. It does not include factor generation, backtesting, model training, adjusted-price handling, suspended-trading filling, or qlib-style provider abstractions beyond what is necessary to keep the code extensible.

## Scope

### In Scope

- Fetch A-share daily market data from Tushare
- Persist raw daily data locally
- Support first-time full sync for a stock
- Support incremental sync for a stock based on the latest local `trade_date`
- Guarantee idempotent local writes through merge, sort, and deduplication
- Provide a minimal service entry point usable from Python and a CLI

### Out of Scope

- Forward-adjusted or backward-adjusted prices
- Trading calendar alignment
- Instrument universe management
- Factor computation
- Dataset/query interfaces similar to qlib
- Backtesting and model workflow
- Historical gap repair inside an already stored file

## Architecture

The first version uses a lightweight layered design with three clear boundaries.

### 1. Tushare Client Layer

Module: `sqlib/data_sources/tushare/client.py`

Responsibility:

- Initialize the Tushare client with the configured token
- Fetch daily bars for one `ts_code` and a date range
- Return a normalized `pandas.DataFrame`

Rules:

- This layer only talks to the external API
- It does not know where or how data is stored
- It returns only the raw fields needed by the first version

### 2. Parquet Storage Layer

Module: `sqlib/storage/parquet_daily.py`

Responsibility:

- Resolve the file path for a stock
- Read existing local parquet data for a stock
- Merge old and new rows
- Deduplicate by `trade_date`
- Sort rows by `trade_date`
- Write the final dataframe back to disk

Rules:

- One stock maps to one parquet file
- Files live under `sqlib/data/raw/daily/<ts_code>.parquet`
- The storage layer does not call Tushare

### 3. Sync Service Layer

Module: `sqlib/services/daily_sync.py`

Responsibility:

- Accept requested stock codes and optional date bounds
- Decide whether each stock needs full or incremental sync
- Call the client layer to fetch data
- Call the storage layer to persist data
- Continue processing remaining stocks if one stock fails
- Return a structured sync result

Rules:

- This is the orchestration layer
- It contains the workflow, but not low-level API logic or low-level file IO details

## Storage Design

### Directory Layout

```text
sqlib/
  data/
    raw/
      daily/
        000001.SZ.parquet
        600000.SH.parquet
```

### Why One File Per Stock

This is the preferred design for the raw layer in the first version because:

- Incremental updates are simple and cheap
- Re-running one failed stock is isolated
- Inspecting one stock during debugging is straightforward
- The scale of A-share daily data is manageable for this pattern

A partitioned dataset layout is intentionally deferred until later. It is more appropriate once the framework grows into broader cross-sectional processing and query optimization.

## Data Contract

Each parquet file stores one stock's raw daily bars with these columns:

- `ts_code`
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `vol`
- `amount`

### Column Rules

- `ts_code` remains in Tushare format, such as `000001.SZ`
- `trade_date` is stored as a datetime-like column and sorted ascending
- Numeric columns remain numeric and are not renamed in this version

The first version deliberately keeps Tushare-native field names. A later provider layer can translate them into a broader framework schema if needed.

## Data Flow

The sync flow for each stock is:

1. Receive a `ts_code` and optional `start_date` and `end_date`
2. Check whether `sqlib/data/raw/daily/<ts_code>.parquet` already exists
3. If no file exists, perform a full fetch within the requested range
4. If a file exists, read its latest `trade_date`
5. Compute the next fetch start as `latest_trade_date + 1 day`, unless the caller explicitly provides a later `start_date`
6. Fetch daily bars from Tushare for the required range
7. If Tushare returns an empty dataframe, treat it as a valid no-op result
8. Merge fetched rows with local rows
9. Drop duplicates on `trade_date`
10. Sort ascending by `trade_date`
11. Write the merged dataframe back to the parquet file

## Interfaces

### Python Service Interface

Primary entry point:

```python
sync_daily(ts_codes, start_date=None, end_date=None)
```

Expected behavior:

- `ts_codes` accepts a single code or a list of codes
- `start_date` and `end_date` are optional and use `YYYYMMDD` or `YYYY-MM-DD` input forms
- The return value includes per-stock status information for success, no-op, and failure cases

### CLI Interface

Primary command shape:

```bash
python -m sqlib.cli sync-daily --ts-code 000001.SZ
```

The CLI only needs to expose the minimum path for:

- one stock
- multiple explicit stock codes
- optional start and end date

No batch universe presets are needed in the first version.

## Configuration

Module: `sqlib/config.py`

The first version keeps configuration minimal:

- `TUSHARE_TOKEN`
- `SQLIB_DATA_DIR`
- optional request throttle or sleep interval

Configuration should be readable from environment variables. The first version does not need a complex settings framework.

## Error Handling

The first version uses explicit, conservative error handling.

### Required Behaviors

- Failure on one stock must not abort the whole batch
- Empty API results must not be treated as an error
- Re-running the same sync window must not create duplicate rows
- Corrupted local parquet files must surface as failures for that stock
- Storage writes must only occur after successful merge preparation

### Deferred Behaviors

These are intentionally not part of version one:

- automatic retry with exponential backoff
- historical hole detection and repair
- reconciliation against exchange calendars
- fallback providers

## Testing Strategy

The first version needs automated tests around the core workflow boundaries.

### Unit Tests

- client normalizes a Tushare daily response into the expected columns
- storage path resolution maps one stock to one parquet path
- storage merge removes duplicate `trade_date` rows and sorts ascending
- sync service chooses full sync when no local file exists
- sync service chooses incremental sync when a local file exists
- sync service treats empty fetched data as a no-op
- sync service records one stock failure without aborting the rest

### Integration-Style Tests

- syncing a new stock creates a parquet file with the expected schema
- syncing the same stock twice does not duplicate rows
- syncing an existing stock with newer rows appends only the new dates

Tests should mock the Tushare client rather than depend on live external network calls.

## Evolution Path

This design intentionally leaves room to grow toward a qlib-like architecture without pretending to be one now.

The expected path after this first slice is:

1. Stabilize raw daily data ingestion
2. Add instrument universe management
3. Add calendar and dataset access abstractions
4. Add adjusted-price and derived-feature layers
5. Add model and backtest modules against the stabilized data contracts

The main design constraint is that later provider-style abstractions should wrap this raw layer, not require this raw layer to be rewritten.
