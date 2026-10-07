# dmt

## What this crate owns

`dmt` is the host-facing facade: curated re-exports from `dmt::core`, `dmt::store`, and `dmt::runtime`, plus `dmt::sqlite` with the `sqlite` feature. `dmt::async_trait` lets hosts implement handlers without adding another dependency. Planner functions and `Commit` remain under `dmt::core`; lease and claim primitives remain under `dmt::store`. The facade adds no wrapper types or errors.

```sh
cargo test -p dmt
cargo test -p dmt --all-features
```

## Features

The default feature set is empty and includes no SQL driver or fault injection.

| Feature | Effect |
| --- | --- |
| `sqlite` | Adds `SqliteStore`, `SqliteOptions`, and `dmt::sqlite` |
| `test-faults` | Test-only: forwards to `dmt-runtime` and, when `sqlite` is enabled, `dmt-store-sqlite`. Production builds must not enable it. |

## Build a graph

```rust,ignore
use dmt::{EndStatus, GraphBuilder};
let graph = GraphBuilder::new("quick-start", 1)
    .start("work").task("work")
    .wait("approval", "approve", None)
    .end("done", EndStatus::Completed)
    .edge("work", "approval")
    .edge_on("approval", "done", "approved")
    .build()?;
```

`build()` validates the graph. `Graph::from_json` and `Graph::to_json` let hosts store definitions as data.

## Register handlers

```rust,ignore
use dmt::{HandlerError, NodeContext, NodeHandler, NodeInput, NodeOutcome, Outcome};
struct Work;
#[dmt::async_trait]
impl NodeHandler for Work {
    async fn run(&self, ctx: NodeContext, input: NodeInput)
        -> Result<NodeOutcome, HandlerError>
    {
        // Use ctx.step_key for idempotency; ctx.attempt is evidence, not identity.
        // Observe ctx.cancel during long work.
        Ok(NodeOutcome::Done(Outcome::done("ok")))
    }
}
```

| Node kind | Input | Returned outcome |
| --- | --- | --- |
| Task | `NodeInput::Task` | `NodeOutcome::Done` or `Fail` |
| FanOut | `NodeInput::Task` | `NodeOutcome::FanOut` or `Fail` |
| Branch | `NodeInput::Branch { index, value }` | `NodeOutcome::Done` or `Fail` |
| Join | `NodeInput::Join` with sorted branch results | `NodeOutcome::Done` or `Fail` |
| Wait / End | Never dispatched | Resolved by the planner |

`HandlerError::Retryable` follows the node retry policy; `HandlerError::Permanent` exhausts immediately. Blocking I/O belongs in `tokio::task::spawn_blocking`.

## Idempotency by step key

Handlers execute at least once; state effects commit exactly once. Hosts make external effects exactly-once by stable step key only when lookup, effect, and record are atomic with respect to concurrent attempts of that key and the external system supports that boundary.

The [example ledger](../../examples/agent-pipeline/src/ledger.rs) records every invocation, then holds one mutex across lookup, performing the fake effect, appending its `NodeOutcome`, and `sync_all`. Repeats return that recorded outcome, including `FanOut`. The durable ledger line itself is the example's fake effect. An arbitrary real external API call followed by a local append would still have a crash gap: use the remote service's idempotency key or a transactional outbox for real effects.

```rust,ignore
ledger.record_invocation(&ctx)?; // never deduplicated
// perform_once holds the mutex across lookup, perform, append, and sync_all.
let (outcome, performed) = ledger.perform_once(ctx.step_key.as_str(), || fake_effect())?;
// The line is durable before the handler returns, or crash injection fires.
Ok(outcome)
```

This ledger reads its whole file on each lookup and is intended for the small example. Its mutex coordinates one shared ledger within one process.

## Start, signal, cancel

```rust,ignore
use std::{sync::Arc, time::Duration};
use dmt::{Engine, EngineConfig, Quiescent, SignalPayload};
let handle = Engine::builder().store(store.clone()).graph(graph)
    .handler("work", Arc::new(Work)).config(EngineConfig::default())
    .build()?.start().await?;
let run = handle.start_run(&"quick-start".into(), serde_json::Value::Null).await?;
match handle.wait_quiescent(&run, Duration::from_secs(30)).await? {
    Quiescent::Parked => {
        handle.signal(&run, "approve", SignalPayload {
            label: "approved".into(), payload: serde_json::Value::Null,
        }).await?;
    }
    Quiescent::Terminal(status) => println!("{status}"),
}
// Hosts may instead request handle.cancel(&run, "host requested cancellation").await?.
handle.shutdown(Duration::from_secs(5)).await?;
```

Signal labels must match an edge label on the wait node. An unknown label fails the run when there is no matching default edge. Hosts should validate labels before sending them.

## Resume after restart

Open the same database, rebuild the same graph and handlers, then call `start()`. It registers the graph; a different definition under the same id and version returns `StoreError::GraphMismatch`. Use `Store::list_runs` to find open runs and `wait_quiescent` for each. The engine waits for a dead process's lease to expire before reclaiming its tasks. Shut down the engine and close the store before another engine reuses the file.

```rust,ignore
use dmt::{SqliteOptions, SqliteStore};
SqliteStore::migrate(&path).await?; // initial creation; idempotent
let store = Arc::new(SqliteStore::open(&path, SqliteOptions::default()).await?);
// Rebuild and start the engine as above; existing open runs resume automatically.
// After shutdown:
store.close().await;
```

## Durability contract

At-least-once handler execution and exactly-once transactional state effects survive process death. SQLite uses WAL with `synchronous = NORMAL`: committed transactions survive process crashes, but recent commits can be lost on OS crash or power loss. Use `SqliteOptions::synchronous_full(true)` when that stronger durability is needed. See the [SQLite backend](../dmt-store-sqlite/README.md).

## SQLite limits

Use one process per database file, with any number of in-process workers. Network filesystems are unsupported. Multi-process claiming and PostgreSQL are tracked in `dmt-2mut`.

## Security notes

Payloads are plaintext in the database, created with mode 0600 on Unix. Hosts copying payloads elsewhere own those files' permissions; the example creates its ledger and invocation log with mode 0600 as well. Put no secrets in payloads. Hosts authorize `signal` and `cancel`. Tests need no credentials.

## Choosing a store

Use `MemoryStore` for tests and single-process throwaway runs; enable `sqlite` and use `SqliteStore` for durable local runs. PostgreSQL is deferred to `dmt-2mut`.

## Example

```sh
cargo run -p agent-pipeline -- demo
cargo test -p agent-pipeline
```

See the [agent pipeline guide](../../examples/agent-pipeline/README.md) for its CLI, crash switches, and proof.

## What is not here

PostgreSQL (`dmt-2mut`), observability (`dmt-5vpz`), graph migration/versioning (`dmt-wjuz`), typed payloads (`dmt-bi48`), timers (`dmt-bwww`), and Restate integration are deferred. The facade adds no authorization policy or real agent integrations.
