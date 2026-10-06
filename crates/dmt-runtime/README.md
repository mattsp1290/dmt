# dmt-runtime

## What this crate owns

A backend-neutral Tokio engine driving planner commits through `Arc<dyn Store>`.
The public API includes `Engine`, `EngineBuilder`, `EngineHandle`, `EngineConfig`,
`NodeHandler`, `NodeContext`, `NodeInput`, `HandlerError`, `HandlerRegistry`,
`Quiescent`, `BuildError`, `EngineError`, and the re-exported `CancellationToken`.
Normal dependencies are dmt-core, dmt-store, Tokio, tokio-util, tracing,
async-trait, serde_json, thiserror, and rand. No SQL driver is a normal dependency.

```sh
cargo test -p dmt-runtime --features test-faults
cargo test -p dmt-runtime
```

## Quick start

```rust,ignore
use async_trait::async_trait;
use dmt_core::{EndStatus, GraphBuilder, NodeOutcome, Outcome, SignalPayload};
use dmt_runtime::{Engine, NodeHandler, NodeContext, NodeInput, HandlerError};
use dmt_store::MemoryStore;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

struct Task;
#[async_trait]
impl NodeHandler for Task {
    async fn run(&self, ctx: NodeContext, _: NodeInput)
        -> Result<NodeOutcome, HandlerError>
    {
        // Use ctx.step_key to deduplicate external side effects.
        Ok(NodeOutcome::Done(Outcome::done("ok")))
    }
}
let graph = GraphBuilder::new("example", 1)
    .start("work").task("work").wait("approval", "approve", None)
    .end("done", EndStatus::Completed)
    .edge("work", "approval").edge_on("approval", "done", "approved")
    .build()?;
let handle = Engine::builder().store(Arc::new(MemoryStore::new()))
    .graph(graph).handler("work", Arc::new(Task)).build()?.start().await?;
let run = handle.start_run(&"example".into(), Value::Null).await?;
handle.wait_quiescent(&run, Duration::from_secs(10)).await?; // Parked
handle.signal(&run, "approve", SignalPayload {
    label: "approved".into(), payload: Value::Null,
}).await?;
handle.wait_quiescent(&run, Duration::from_secs(10)).await?; // Terminal
handle.shutdown(Duration::from_secs(5)).await?;
```

## Handler contract

Handlers execute **at least once** per task. `ctx.step_key` is stable across
retries and lease reclaims and is the side-effect idempotency key. `ctx.attempt`
counts every attempt, including lease reclaims. State effects commit exactly
once for each accepted task outcome; the engine cannot deduplicate external work.

| Node kind | Input |
| --- | --- |
| Task, FanOut | `NodeInput::Task(Value)` |
| Branch | `NodeInput::Branch { index, value }` |
| Join | `NodeInput::Join(JoinInput)`; results sorted by branch index |
| Wait, End | Never dispatched; no handler needed |

`HandlerError::Retryable` follows the node retry policy; `Permanent` exhausts
immediately. Panics and timeouts become retryable failures. An incompatible
outcome or empty fan-out becomes a permanent failure. A missing handler for a
store-resolved graph version fails permanently with
`no handler registered for node <id> of graph <g>@<v>`.

The cancellation token fires for local run cancellation, heartbeat lease loss,
shutdown after the drain, and handler timeout. Handlers must await regularly and
should return promptly after cancellation. Use `spawn_blocking` for blocking
work; the engine cannot cancel work that never yields or independently spawned
work unless that work observes a cloned token.

Timeout is `NodeDef.timeout_micros`, otherwise `EngineConfig.handler_timeout`.
Tokio time measures it. On timeout the token is cancelled and the handler future
is dropped at its current await point, with no grace period. Handlers must be
cancel-safe at every await; cloned tokens notify work handed to other tasks.

A handler **must not own an `EngineHandle` clone**: that forms an ownership cycle
and prevents last-handle drop from stopping the engine. A handler **must not call
`abort()` or `shutdown()`**: those wait for the calling handler itself.

## Configuration

| Field | Default | Meaning |
| --- | --- | --- |
| workers | 4 | Claim loops; 0 disables all background tasks |
| claim_limit | 8 | In-flight dispatches per worker |
| poll_interval | 250 ms | Idle claim and quiescence polling |
| lease | 30 s | Lease length passed to the store |
| heartbeat_every | 10 s | Lease refresh cadence for the whole dispatch |
| handler_timeout | None | Fallback handler timeout |
| max_replan_attempts | 16 | Retries after the first commit attempt |
| replan_backoff | 5 ms | Doubles, capped at 200 ms; uniform jitter over half to full ceiling |
| sweep_interval | 1 s | Deadline and exhaustion sweep cadence |
| cancel_grace | 5 s | Cooperative wait after shutdown cancels tokens |
| worker_id | Fresh UUID | Shared persistence identity of this engine's workers |
| fault (test-faults only) | None | Test-only crash injection |

Concurrency is a sliding window with cap `workers × claim_limit`. A completed
dispatch frees one slot immediately; workers claim only their free slots.
`workers = 1` and `claim_limit = 1` gives strictly sequential handler runs.
`heartbeat_every` should be at most half of `lease`; a slower cadence logs a
warning. Zero poll, lease, heartbeat, or sweep durations are rejected, as are
zero replan retries and zero claim limit when workers are enabled.

## Time model

Store time comes from `Clock::now()` (`SystemClock` by default) and determines
lease expiry, scheduled `run_at`, and signal deadlines. Tokio time determines
polls, heartbeats, sweeps, handler timeouts, retry delays, and shutdown waits.
`ManualClock` permits independent control of persistence time in tests.

## Commit loop and error handling

Each outcome gets one initial attempt and at most `max_replan_attempts` retries.
Every try reloads the run and re-plans using fresh store time. A re-plan never
re-runs the handler. Heartbeats cover graph resolution, handler execution, and
commit retries.

| Result | Engine action |
| --- | --- |
| VersionConflict, JoinDrift from apply | Re-plan with jitter; debug `commit conflict` for dispatched outcomes |
| Busy, Backend from load/apply | Retry within the same budget; backend apply may have committed |
| LeaseLost from apply | Drop outcome; warn `lease lost` |
| RunTerminal, UnknownTask, TaskNotRunning at outcome planning | Drop stale outcome; debug |
| KindMismatch, EmptyFanOut | Re-plan once with permanent Fail, without charging a retry |
| Other planner/apply error | Error log and leave the task for lease expiry |
| Retry budget exhausted | Error `replan attempts exhausted`; leave task Running |
| Worker claim error | Warn and continue polling |
| Sweep stale/terminal row | Debug and continue |
| Sweep contention | Warn and retry on a later tick |
| Other sweep error | Error and continue to the next row |

Graph registration and run creation retry Busy/Backend. `start_run` mints its
run id once and recognizes its already-created run after an ambiguous Backend
response. `cancel` returns success if an ambiguous apply is followed by a
Cancelled status. `signal` returns `EngineError::Indeterminate` when an ambiguous
apply is followed by an absent signal or terminal run: read the run to decide
whether the desired state holds. Read-only store errors carry no apply ambiguity.

Fan-out width matters: more simultaneous completions than
`max_replan_attempts + 1` can leave an outcome uncommitted. After its heartbeat
ends and its lease expires, the handler runs again. Increase the retry budget
for wide fan-outs. A graph outage beyond resolution retries, missing graph, or
corrupt dispatch node is logged and also leaves the task for lease expiry.

## Signals, cancel, sweeps

| Signal result | Meaning |
| --- | --- |
| Ok | Signal resolution and wait completion committed atomically |
| SignalNotFound | No open matching signal, including a concurrent duplicate |
| RunTerminal | Planner/apply found a terminal run |
| Indeterminate | Earlier Backend apply may have committed the signal |
| Contention | Retry budget exhausted |
| Plan / Store / GraphUnavailable | Other operation failure |

| Cancel result | Meaning |
| --- | --- |
| Ok | Run is cancelled; local in-flight tokens notified |
| RunNotFound | Run does not exist |
| RunTerminal | Already terminal, with its final status |
| Contention / Plan / Store / GraphUnavailable | Operation failed |

Cancel cancels Ready and Awaiting tasks and resolves open signals. Running tasks
keep running; their later outcomes are rejected as terminal. Once their leases
expire, `claim_ready` cancels their rows without an event. A cancellation that
commits before a just-claimed task is registered can miss that handler token;
its outcome is still rejected. Other processes receive no token notification.

One sweeper handles due signal deadlines (`deadline_at <= now`) and unplanned
Exhausted tasks in batches of 64 at `sweep_interval`. Sweeps act on **every run
in the store**, resolving definitions through the catalog even for unregistered
graphs. Newly scheduled tasks wait for an engine registered for that graph id.
Sweeps run only when `workers > 0`. Lease reclaim occurs inside `claim_ready`.
Unplannable rows at the head of a sweep batch can starve later rows. A stuck
handler holds its worker slot; zero free slots means that worker cannot claim
or reclaim tasks. Use handler timeouts where necessary.

## Graph versions

An engine registers one version per graph id. Claims cover every stored version
of a registered id. The catalog resolves the registered version first, then its
cache, then `Store::load_graph`. Handler dispatch is by node id; context includes
`graph_version`. Missing handlers fail permanently with the message above.
In-flight graph migration is deferred to Beans epic `dmt-wjuz`.

## Stopping

`shutdown(drain)` stops new claim/sweep calls and closes the task tracker. It
waits for ongoing dispatches during `drain`, then cancels handler tokens and
waits `cancel_grace`. Outcomes returned during either wait still commit. If tasks
remain, it stops them hard, waits for exit, and returns
`ShutdownTimedOut { abandoned }`. A second call returns success.

`abort()` stops hard immediately and waits for every engine task to exit.
Dropping the last handle triggers the same hard stop without waiting. All three
leave unfinished tasks Running until their leases expire. A restarted engine
waits for lease expiry before reclaiming them. Hard stop waits for the next await
point; a handler blocking its thread can delay shutdown or abort indefinitely.

`abort()` guarantees no engine task runs after it returns. It does not recall a
write already handed to the backend. **Close the first store handle before
another engine reuses the database file.** A stop while `claim_ready` is in flight
can leave a Running task whose handler never ran; reclaim still costs an attempt.
See the [SQLite durability caveats](../dmt-store-sqlite/README.md); this crate
makes no power-loss guarantee.

## Worker-less handles

With `workers = 0`, no background task runs and no handlers are required. A
builder without graphs serves `signal`, `cancel`, `run`, `events`, and
`wait_quiescent` through store-resolved definitions. `start_run` requires a graph
on the builder and otherwise returns `GraphNotRegistered`. After shutdown or
abort, the handle still serves these host/store operations without workers.

## Tracing

| Kind | Name / message | Level | Fields |
| --- | --- | --- | --- |
| Span | dispatch | info | run_id, node_id, step_key, attempt |
| Event | commit conflict | debug | error, retry |
| Event | lease lost | warn | dispatch span context |
| Event | stale outcome dropped / run terminal | debug | dispatch span context |
| Event | replan attempts exhausted | error | dispatch span context |
| Event | handler panicked | error | dispatch span context |
| Event | claim_ready failed | warn | error |

Handler futures inherit the dispatch span. Payloads are never logged.

## Security notes

The host must authorize `signal` and `cancel`. Payloads are stored as plaintext
by the store; no credential belongs in a payload. Tests need no credentials.

## test-faults

The Cargo feature exposes the hidden `FaultPoint::BeforeApply { node_id, attempt }`
and `EngineConfig.fault`, aborting the process before a matching outcome apply.
Production builds and the future facade default must not enable it. Unowned
signal, cancel, and sweep commits have no fault hook.

## Tests

| Binary | Evidence |
| --- | --- |
| engine_memory | Execution order, joins, retries, leases, contention, sliding window, handlers, signals, sweeps, stops, wake-driven progress, backend ambiguity |
| engine_sqlite | Fan-out, abort/drop lease recovery, parked restart, child-process abort before apply |
| tracing | Dispatch identity fields and commit conflict events |

The workspace gate checks all-feature and feature-off Clippy and tests.

## What is not here

The facade, runnable agent pipeline, subprocess SIGKILL proof, graph migration,
multi-process SQLite claiming, cross-process cancellation notification,
priorities, rate limits, per-node limits, cron, middleware, typed payloads,
metrics, per-run commit serialization, and host-supplied run ids are deferred.
