# dmt-store-sqlite

## What this crate owns

`SqliteStore` implements the [Store contract](../dmt-store/README.md) on sqlx 0.9. It owns `SqliteOptions`, `SCHEMA_VERSION`, the embedded migration, and the test-only `test-faults` feature. Normal dependencies are `dmt-core`, `dmt-store`, `sqlx`, `async-trait`, and `serde_json`. sqlx's typed `Json` columns encode and decode the core's serialized values; fallible row conversions check statuses and integer bounds.

```sh
cargo test -p dmt-store-sqlite --features test-faults
cargo test -p dmt-store-sqlite
```

## Opening a database

Call `migrate(path)` before `open(path, options)`. Migration creates a missing file with mode `0600` on Unix, including private WAL and shared-memory sidecars, and is idempotent. Existing files keep their permissions. `open` never creates a file: it rejects unmigrated databases, missing or unknown schema versions, versions newer than `SCHEMA_VERSION`, and databases outside WAL mode. It does not write to a database it rejects. Empty paths, `:memory:`, `file:` URI paths, and non-UTF-8 paths are rejected; there is no in-memory mode. Concurrent migration calls are unsupported.

```rust,ignore
use dmt_store_sqlite::{SqliteOptions, SqliteStore};

SqliteStore::migrate("workflow.db").await?;
let store = SqliteStore::open("workflow.db", SqliteOptions::default()).await?;
// Use the dmt_store::Store methods with caller-supplied timestamps.
store.close().await;
```

`close` waits for readers, then writers. Committed data is already in the WAL before closing; closing the last connection checkpoints it.

## Options

| Builder | Default | Effect |
| --- | --- | --- |
| `busy_timeout(Duration)` | 5 seconds | SQLite lock wait |
| `synchronous_full(bool)` | `false` | Select FULL instead of NORMAL synchronization |

Each pool's acquire timeout is `max(busy_timeout, 1 second)`. A mutation can wait for pool acquisition and then for the SQLite lock: up to the acquire timeout plus `busy_timeout` (10 seconds with defaults) before returning `Busy`.

## Schema

| STRICT table | Purpose |
| --- | --- |
| `dmt_schema_meta` | Supported schema version |
| `dmt_graphs` | Definitions and hashes keyed by graph id and version |
| `dmt_runs` | Run state, version, occurrences, inputs, outputs, and event allocator |
| `dmt_events` | Append-only events keyed by run and sequence |
| `dmt_tasks` | Scheduled work, attempts, leases, outcomes, and branch references |
| `dmt_joins` | Contributions, counters, results, and satisfaction |
| `dmt_signals` | Waits, deadlines, and resolutions |

Child rows reference `dmt_runs`; runs reference registered graphs. `dmt_runs.next_seq` starts at 1 and advances in the same transaction as event inserts, so failed operations consume no sequence numbers. Timestamps are INTEGER Unix microseconds supplied by callers; the store never reads a clock. Ids are the `dmt-core` strings, with byte-wise ordering; each task id equals its unique step key.

Inspection columns absent from public Store records are `dmt_tasks.created_at`, `dmt_tasks.updated_at`, `dmt_signals.label`, and `dmt_signals.payload_json`. The schema metadata, run sequence allocator, task graph-id copy, and event kind column are also backend bookkeeping. SQL NULL and the JSON text `null` remain distinct for optional JSON fields.

The schema has no compatibility promise in milestone 1. Delete local databases created before an edit to `0001_initial.sql`; sqlx rejects changed migration checksums. Stored run versions are capped at `i64::MAX`: applying at that version returns `InvalidCommit("run version overflow")`. `MemoryStore` permits applying through expected version `u64::MAX - 1`.

## Connection model

One writer connection and up to four read-only reader connections use WAL, `foreign_keys = ON`, and the configured busy timeout. The writer uses `synchronous = NORMAL` by default. Every mutating Store method uses `BEGIN IMMEDIATE`, including graph registration and heartbeats. `claim_ready` first probes the reader pool; an idle poll acquires no write lock. Reads do not take the write lock. `load_run` reads its run, task, join, and signal rows inside one read transaction.

Claims use a JSON array bound to `json_each` for graph selection, without a graph-count limit, and fixed-size keyset pages ordered by `(run_at, task_id)`. Terminal cancellations and exhausted leases do not count toward the requested claim limit. Claims advance event sequence numbers without changing run versions or run update timestamps. The query-plan test guards indexed access on empty tables; it is not a load test or a guarantee about plans after statistics change.

## Transactions and cancellation

Each mutation is one transaction. A failure or a future dropped before commit rolls it back. Once COMMIT has been processed, cancellation can leave the entire transaction committed; it never leaves a partial commit. The checked sqlx **0.9.0** sources under `$CARGO_HOME/registry/src` explain the path:

- `sqlx-core`, `src/pool/mod.rs:391`: `Pool::begin_with` acquires a connection and starts the requested transaction.
- `sqlx-core`, `src/transaction.rs:265–279`: dropping an open `Transaction` calls `TransactionManager::start_rollback`.
- `sqlx-sqlite`, `src/transaction.rs:28–30`: the manager forwards rollback to the SQLite worker.
- `sqlx-sqlite`, `src/connection/worker.rs:406–410`: queues `Command::Rollback { tx: None }` on the connection's worker channel.
- The same worker file, `285–313`: processes that rollback before subsequent commands on the connection.
- The same worker file, `244–262`: rolls back a BEGIN whose acknowledgement was abandoned.
- The same worker file, `264–283`: a processed but unacknowledged COMMIT sets `ignore_next_start_rollback`, preventing the dropped transaction from undoing an already committed operation.

The six `*_is_atomic` tests inject an error at every statement boundary; `dropped_apply_future_rolls_back` drops an apply future at every boundary and proves the next apply on the same writer succeeds. A future dropped between COMMIT processing and acknowledgement is not tested deterministically.

## Errors

All extended `SQLITE_BUSY` codes whose low byte is 5, and pool acquire timeouts, map to `StoreError::Busy`. The store never retries; callers decide when to retry. Other sqlx failures, corrupt rows, and injected faults return `StoreError::Backend`. Decode messages begin with `corrupt row:`. Contract errors retain the Store contract's check precedence.

## Durability caveat

WAL with `synchronous = NORMAL` survives a process kill. A power loss or operating-system crash can lose recent committed transactions while preserving database consistency. Hosts needing durability across power loss should use `SqliteOptions::default().synchronous_full(true)`. See SQLite's [WAL synchronization guarantees](https://www.sqlite.org/pragma.html#pragma_synchronous).

## Limits

Use one process per database file, with any number of in-process workers. Multiple store handles inside that process are supported. Multi-process claiming is unsupported and untested in milestone 1; shared workers belong to epic `dmt-2mut`. WAL does not work on network filesystems ([SQLite WAL requirements](https://www.sqlite.org/wal.html)). Concurrent `migrate` calls are unsupported.

## test-faults

Production builds and the future facade crate must not enable this Cargo feature. It exposes hidden test APIs:

- `SqliteOptions::fault(FaultPoint::AfterStatement(n))` returns an injected Backend error after the Nth successful statement.
- `StallAfterStatement(n)` stays pending at that boundary until the caller drops the future.
- `SqliteStore::fault_fired()` reports firing; clones share the same one-shot fault state.

N is 1-based within one write transaction. BEGIN, COMMIT, and reader-pool queries (including claim probes) do not count. Zero never fires. A shorter transaction leaves the fault armed; after firing, subsequent operations on the same handle proceed normally. Without this feature the state, counter, hook, and public fault items are absent. The [workspace gate](../../AGENTS.md) checks both builds.

## Tests

| Integration binary | Evidence |
| --- | --- |
| `open_migrate` | Migration, schema rejection, WAL, private file permissions |
| `conformance` | All 24 public cases plus `run_all_sqlite`, each on fresh temp files |
| `sqlite_specific` | Graph round trips, large graph filters, paging and skips, bounds, optional JSON |
| `restart` | Mixed-state equality, migration preservation, saved lease proofs, continued execution |
| `busy` | External write-lock errors, reads and idle polls during contention, recovery |
| `atomicity` | Faults at every statement, exact seven-table dumps, retry, cancellation, pool timeout |

`reopen_from_unclean_copy_recovers_wal` copies a live database and its WAL without shared memory, then recovers and continues from that copy. It is an in-process stand-in for a crash; the process-kill proof belongs to `dmt-nvsm`.

## Manual inspection

```sh
sqlite3 <db> 'select id, status, version from dmt_runs'
sqlite3 <db> 'select seq, kind from dmt_events where run_id = "<run>" order by seq'
```

## What is not here

Busy retries, multi-process workers, PostgreSQL, archival and deletion, schema downgrade, and query-plan tuning are outside this crate's milestone-1 scope.
