# dmt-runtime

## What this crate owns

A backend-neutral Tokio engine driving planner commits through `Arc<dyn Store>`.
The public API includes `Engine`, `EngineBuilder`, `EngineHandle`, `EngineConfig`,
`NodeHandler`, `NodeContext`, `NodeInput`, `HandlerError`, `HandlerRegistry`,
`Quiescent`, `BuildError`, `EngineError`, and the re-exported `CancellationToken`.
Normal dependencies are dmt-core, dmt-store, Tokio, tokio-util, tracing, metrics,
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
| instrument_store | true | Wrap store calls; set false when already wrapped by the host |
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

## Telemetry

Library instrumentation uses `tracing` and `metrics` without installing a subscriber or recorder. Enable the facade's `telemetry` feature and use [dmt-telemetry](../dmt-telemetry/README.md) for Datadog transport and correlated JSON logs.

| Span | Level | Created in | `operation.name` | `resource.name` | Fields at creation | Fields recorded later | Run-scoped |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `start_run` | INFO | `crates/dmt-runtime/src/handle.rs` `EngineHandle::start_run` | `dmt.start_run` | graph id | `run_id`, `graph_id`, `graph_version` | `result` (`ok`, `contention`, `error`) | yes |
| `dispatch` | INFO | `crates/dmt-runtime/src/dispatch.rs` `run` (exists; gains fields) | `dmt.dispatch` | `{graph_id}/{node_id}` | `run_id`, `graph_id`, `graph_version`, `node_id`, `task_id`, `step_key`, `attempt` | `outcome` (handler class, below), `result` (dispatch result, below), `otel.status_code` = `ERROR` when `result` is not `applied` or `dropped` | yes |
| `handler` | INFO | `dispatch.rs` `run_task`, around the spawned handler future (replaces instrumenting it with the dispatch span; the handler span is a child of `dispatch`) | `dmt.handler` | node id | `run_id`, `graph_id`, `node_id`, `attempt` | `outcome` (handler class), `otel.status_code` = `ERROR` for `fail_*`, `panic`, `timeout`; `error.message` (truncated) for those | no (child) |
| `commit` | INFO | `crates/dmt-runtime/src/commit_loop.rs` `outcome` and `unowned_loop` | `dmt.commit` | commit kind | `run_id`, `kind` (`outcome`, `signal`, `cancel`, `timeout`, `exhausted`) | `attempts` (u32, tries used), `result` (commit result, below) | no (child) |
| `signal` | INFO | `handle.rs` `EngineHandle::signal` | `dmt.signal` | `signal` (fixed; the host-authored name is the `name` field, not the resource) | `run_id`, `name` | `result` (host result, below) | yes |
| `cancel` | INFO | `handle.rs` `EngineHandle::cancel` | `dmt.cancel` | `cancel` | `run_id` | `result` (host result) | yes |
| `sweep` | INFO | `crates/dmt-runtime/src/sweeps.rs`, one per due signal or exhausted task processed (none on idle ticks) | `dmt.sweep` | `timeout` or `exhausted` | `run_id`, `kind` | `result` (`applied`, `stale`, `contention`, `failed`) | yes |
| `store` | DEBUG | `crates/dmt-store/src/instrument.rs` `InstrumentedStore`, one per `Store` method call | `dmt.store` | method name | `backend`, `method`, `span.type` = `db`, `db.system` = backend | `result` (store result, below) | no (child or root) |

| Metric | Type | Labels | Recorded in | Meaning |
| --- | --- | --- | --- | --- |
| `dmt.runtime.runs_started_total` | counter | `graph_id` | `handle.rs` `start_run` after `create_run` succeeds (including the ambiguous-retry success path) | Runs created by this engine |
| `dmt.runtime.tasks_claimed_total` | counter | `graph_id` | `worker.rs` per `ClaimedTask` | Claims including lease reclaims |
| `dmt.runtime.in_flight` | gauge | none | `shared.rs` `register` (+1) and `InFlightGuard::drop` (-1), via `Gauge::increment`/`decrement` | Dispatches currently running in this process |
| `dmt.runtime.dispatch.duration_seconds` | histogram | `graph_id`, `node_id`, `result` | `dispatch.rs` at the end of `run` | Claim-to-commit wall time |
| `dmt.runtime.dispatch.outcomes_total` | counter | `graph_id`, `node_id`, `outcome` | `dispatch.rs` when the handler class is known | Handler outcome classes |
| `dmt.runtime.handler.duration_seconds` | histogram | `graph_id`, `node_id`, `outcome` | `dispatch.rs` around the handler future | Handler wall time including timeout |
| `dmt.runtime.lease_lost_total` | counter | `graph_id`, `node_id`, `source` (`heartbeat`, `commit`) | `dispatch.rs` heartbeat `Lost`/`NotFound` (`source = heartbeat`) and `commit_loop.rs` `LeaseLost` (`source = commit`) | Lease-loss observations; one lost lease can produce one of each source, so dashboards sum by `source` or pick one |
| `dmt.runtime.commit.attempts_total` | counter | `kind`, `result` (attempt result) | `commit_loop.rs` once per try in both loops | Commit tries and how each ended |
| `dmt.runtime.commit.duration_seconds` | histogram | `kind`, `result` (commit result) | `commit_loop.rs` at loop exit | Whole commit loop time |
| `dmt.runtime.events_total` | counter | `kind` (`RunEvent::kind()`) | `commit_loop.rs` both loops and `handle.rs` `start_run`, once per event of a commit that the store accepted | Journal growth by event kind; excludes `TaskClaimed` and reclaim `TaskExhausted` events the store writes inside `claim_ready` |
| `dmt.runtime.signals_total` | counter | `result` (host result) | `handle.rs` `signal` | Host signal calls |
| `dmt.runtime.cancels_total` | counter | `result` (host result) | `handle.rs` `cancel` | Host cancel calls |
| `dmt.runtime.sweeps_total` | counter | `kind` (`timeout`, `exhausted`), `result` (`applied`, `stale`, `contention`, `failed`) | `sweeps.rs` `classify` | Sweep actions |

Hosts may call public `dmt_runtime::telemetry::run_scope(&span, &run_id)` immediately after span creation to join host-owned operations to the same run trace, without importing an OpenTelemetry bridge. Declare the `run_id` field on the host span itself. Call before entering, instrumenting, or materializing its context.

The `otel` feature parents run-scoped spans on a deterministic run-id context; without it the hook is a no-op and normal dependencies include no OpenTelemetry crate. Handler futures run within child `handler` spans. Every error message is truncated at a UTF-8 boundary to 256 bytes. Payloads, join results, graph JSON, and machine state never appear in telemetry; handler-authored failure messages are logged.

Metric labels contain only bounded graph/node ids, kinds, results, outcomes, and sources. Graph and node ids must come from graph definitions, never per-run data. No run/task/step/signal/join/worker ids or payload values become labels. Gauges are per process across engines. The store decorator records metrics for idle polls; disable automatic wrapping when supplying an `InstrumentedStore` to avoid double counting.

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
| tracing | Every operation span, handler outcomes, errors, payload exclusion, store wrapper opt-out |
| metrics | Runtime counters, histograms, gauge, retries, contention, signals, cancels, lease loss, sweeps |
| otel_parent (otel feature) | Run-derived trace ids and handler parentage |

The workspace gate checks all-feature and feature-off Clippy and tests.

## What is not here

The facade, runnable agent pipeline, subprocess SIGKILL proof, graph migration,
multi-process SQLite claiming, cross-process cancellation notification,
priorities, rate limits, per-node limits, cron, middleware, typed payloads,
per-run commit serialization, and host-supplied run ids are deferred.
