# dmt-telemetry

## What this crate owns

Optional Datadog trace export, DogStatsD metrics, and correlated JSON logs for dmt hosts. The runtime and store emit through the `tracing` and `metrics` facades. They install no subscriber or recorder. The `dmt` facade exposes this crate as `dmt::telemetry` with the `telemetry` feature; default facade builds pull no OpenTelemetry or Datadog dependency.

The pinned chain is `datadog-opentelemetry =0.5.2`, `opentelemetry =0.32.0`, `opentelemetry_sdk =0.32.1`, `tracing-opentelemetry =0.33.0`. Upgrade all four together.

## Quick start

Enable `dmt = { version = "0.1", features = ["telemetry"] }` in the host manifest. Initialize before creating the runtime and shut down after dropping it:

```rust,ignore
let telemetry = dmt::telemetry::init(dmt::TelemetryConfig::from_env()?)?;
let runtime = tokio::runtime::Runtime::new()?;
runtime.block_on(async { /* build engine, run work, shut down engine */ });
drop(runtime);
telemetry.shutdown(std::time::Duration::from_secs(2))?;
```

Try the example against a local Datadog Agent:

```sh
DD_SERVICE=dmt-agent-pipeline DD_ENV=dev cargo run -p agent-pipeline -- --telemetry --workers 3 demo
```

Without `--telemetry`, the example installs no telemetry globals and retains its normal stderr behavior.

## Configuration

`TelemetryConfig::from_env()` delegates to `from_vars`, which accepts a lookup closure for tests. Boolean settings accept `1`, `true`, `0`, `false`, ignoring case; other values are errors. Durations are integer milliseconds. Log-file modes are octal permission bits. Fields can also be set directly on `TelemetryConfig`.

| Variable | Default | Effect |
| --- | --- | --- |
| `DD_SERVICE` | `dmt` (`agent-pipeline` in the example) | Unified service tag |
| `DD_ENV` | unset | Environment tag |
| `DD_VERSION` | unset | Deployment version tag |
| `DD_TAGS` | unset | Datadog tracer tags; not metric labels or log fields |
| `DD_AGENT_HOST` | `127.0.0.1` | Agent host for traces and DogStatsD |
| `DD_TRACE_AGENT_PORT` | `8126` | Datadog trace receiver port |
| `DD_TRACE_AGENT_URL` | unset | Override trace receiver URL |
| `DD_DOGSTATSD_PORT` | `8125` | UDP metric receiver port |
| `DD_TRACE_ENABLED` | tracer default (`true`) | Datadog tracer switch; false drops spans |
| `DD_TRACE_SAMPLE_RATE` | tracer default | Does not override sampled run contexts; see below |
| `DD_LOG_LEVEL` | tracer default | Internal console level, only with DMT opt-in |
| `DD_INSTRUMENTATION_TELEMETRY_ENABLED` | `false` | Opt in to Datadog instrumentation telemetry |
| `DD_REMOTE_CONFIGURATION_ENABLED` | `false` | Opt in to Datadog remote configuration |
| `DMT_TELEMETRY_TRACES` | `true` | Build Datadog trace layer |
| `DMT_TELEMETRY_METRICS` | `true` | Install DogStatsD recorder |
| `DMT_TELEMETRY_LOGS` | `stderr` | `stderr`, `off`, or `file:<path>` |
| `DMT_TELEMETRY_FILTER` | `info` | EnvFilter directives shared by traces and logs |
| `DMT_TELEMETRY_TRACE_SHUTDOWN_MS` | `2000` | Datadog writer force-flush timeout |
| `DMT_TELEMETRY_METRICS_FLUSH_MS` | `1000` | Metric flush cadence; clamps to at least 100 ms |
| `DMT_TELEMETRY_DATADOG_LOGS` | `false` | Explicitly enable Datadog internal console logs |
| `DMT_TELEMETRY_LOG_FILE_MODE` | `640` | Unix mode for a newly created log file |

Datadog console logs, instrumentation telemetry, and remote configuration are off by default. `DD_LOG_LEVEL` alone does not enable internal logging. Opting in can produce ANSI console text, including stdout output at non-error levels; use it only when diagnosing the tracer, since it changes the example's output.

## What Datadog shows

Each run has a deterministic 128-bit trace id: UUID bytes when possible, otherwise the first 16 bytes of SHA-256. A zero low 64-bit word is changed to one for Datadog. The parent span id is derived from SHA-256 of `dmt-run-root\0` and the run id. It is local (`is_remote = false`) so the Datadog processor correctly tracks overlapping dispatches. The mock-Agent fan-out test proves no spans are lost.

The synthetic root is never emitted: Datadog may display several top-level children or partial traces when later work arrives after a long parked interval. All run operations retain the `run_id` tag so they remain searchable. Run-scoped spans inherit `SAMPLED` and are always kept; `DD_TRACE_SAMPLE_RATE` does not sample them down.

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

`operation.name` becomes the Datadog operation; `resource.name` identifies the graph/node or operation. `handler` and `commit` are contextual children. Store spans have no `run_id` and appear only at DEBUG; use `DMT_TELEMETRY_FILTER=debug` to include them. Root store polls can then form independent traces. The in-flight gauge is per process; all engines increment/decrement it together, and the Agent adds the `host` tag.

## Metrics

Histograms arrive as Datadog distributions with percentiles. Metric labels must stay bounded: use only graph definitions' `graph_id` and `node_id`, backend, method, event kind, result, outcome, and source. Never derive graph/node ids from per-run data. Run/task/step/signal/join/worker ids and payload values never become metric labels.

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
| `dmt.store.calls_total` | counter | `backend`, `method`, `result` (store result) | `instrument.rs` per call | Store calls and outcomes; `busy` here is the SQLite contention signal |
| `dmt.store.call.duration_seconds` | histogram | `backend`, `method`, `result` | `instrument.rs` per call | Store call latency |

Every idle worker poll and sweep query records a store counter increment and latency sample. The default worker poll cadence is four times per second per worker. The exporter aggregates locally and sends per series per flush, not per call; local work is an atomic add and a reservoir insert. `events_total` counts accepted planner commits and excludes store-owned `TaskClaimed` and lease-reclaim `TaskExhausted` events. Lease-loss metrics distinguish `source:heartbeat` from `source:commit`; one lost lease can produce both observations.

## Logs

One JSON object per line, written by `DatadogJson` (new, `crates/dmt-telemetry/src/logs.rs`):

| Field | Value | Always |
| --- | --- | --- |
| `timestamp` | RFC 3339 UTC (`time::OffsetDateTime::now_utc()` formatted with `time::format_description::well_known::Rfc3339`, which emits the sub-second digits the value carries, up to nanoseconds) | yes |
| `status` | `error`, `warn`, `info`, `debug`, `trace` | yes |
| `message` | the event's `message` field, or the event name when absent | yes |
| `target` | `tracing` target | yes |
| `service` | `TelemetryConfig.service` | yes |
| `env` | `TelemetryConfig.env` | when set |
| `version` | `TelemetryConfig.version` | when set |
| `trace_id` | 32 lowercase hex characters of the active OTel trace id | when the current span has a valid, sampled OTel context |
| `span_id` | 16 lowercase hex characters of the active OTel span id | same |
| `dd.trace_id` | decimal string of the low 64 bits of the trace id | same |
| `dd.span_id` | decimal string of the span id | same |
| `dmt` | object: the merged fields of every span in scope, root first, leaf wins (for example `run_id`, `graph_id`, `node_id`, `attempt`, `operation.name`) | when at least one span is in scope |
| `span` | name of the innermost span | same |
| `fields` | object: the event's fields other than `message` | when non-empty |

The formatter reads the context activated by the OpenTelemetry layer before taking span-extension locks. `Span::current()` is suppressed inside subscriber callbacks. Span fields merge from root to leaf; leaf values win. Logs outside a sampled span omit all four ids. Numeric and boolean event fields keep their JSON types.

Payloads, graph JSON, join results, and machine state are never logged. Error messages are capped at 256 bytes at a UTF-8 boundary. Handler failure messages are host-authored and logged; keep sensitive data out of them. The formatter ships no logs itself.

## Agent requirements

The Agent's standard receivers are APM TCP 8126 and DogStatsD UDP 8125. No OTLP receiver or root Agent edit is needed for those signals. The Agent owns credentials (for example `DD_API_KEY`); hosts and tests need none.

For file logs, an operator must set `logs_enabled: true` in `datadog.yaml`, add `conf.d/dmt.d/conf.yaml`, and restart the Agent:

```yaml
logs:
  - type: file
    path: /var/tmp/dmt/agent-pipeline.log
    service: dmt-agent-pipeline
    source: dmt
```

Create `/var/tmp/dmt` with directory traversal allowed for `dd-agent`. Newly created log files use mode `0o640`: give `dd-agent` a shared group, or grant read access with `setfacl -m u:dd-agent:r /var/tmp/dmt/agent-pipeline.log`. Existing files retain their permissions. Configure the tailer's service to match `DD_SERVICE`. Container deployments can instead collect stderr through the runtime's log collector.

```sh
DD_SERVICE=dmt-agent-pipeline DD_ENV=dev DMT_TELEMETRY_LOGS=file:/var/tmp/dmt/agent-pipeline.log cargo run -p agent-pipeline -- --telemetry --workers 3 demo
```

After the command prints `RunCompleted`, allow up to five minutes for ingestion. In APM search `service:dmt-agent-pipeline @run_id:<printed-run-id>`; expect one run trace with start, at least nine dispatches, handler, commit, and signal operations, including the three review branches. In Metrics Explorer query `sum:dmt.runtime.events_total{service:dmt-agent-pipeline,kind:RunCompleted}`. In Log Explorer search `service:dmt-agent-pipeline @dd.trace_id:<decimal-low-64-trace-id>`. Use `DD_SERVICE`, `DD_ENV`, and `DD_VERSION` consistently for unified service tagging.

## Shutdown and offline behavior

`Telemetry::shutdown(timeout)` is blocking. Call it after dropping the Tokio runtime or inside `spawn_blocking`, after the engine has drained. The tracer calls `shutdown_with_timeout` once, without an additional `force_flush`. Metrics wait two flush intervals plus 50 ms because the exporter exposes neither flush nor shutdown. At defaults this adds 2.05 s to exit, plus at most the trace timeout and scheduling slack. The `trace_shutdown_timeout` setting controls the Datadog writer's internal flush bound; the shutdown argument controls the provider's bound.

The recorder and subscriber remain process globals; shutdown drains but cannot uninstall them. Exiting without shutdown can lose up to two intervals of metrics. Log files are line buffered. Agent refusal or a black-holed receiver does not panic; trace shutdown can return an error. The example emits that warning as JSON without changing its command exit code.

## Hosts with their own subscriber

`Telemetry::builder(config).build()` returns `(TelemetryLayers, Telemetry)` and installs no globals. Add these layers first to a fresh `tracing_subscriber::registry().with(layers)` before other host layers; they are typed over `Registry`. The builder supports `log_writer`, `trace_exporter`, and `extra_span_processor`. `Builder::install_metrics(&config)` separately installs the recorder and returns its flush interval; hosts using this path must wait two intervals plus 50 ms on exit themselves. `init` handles that wait automatically. Installing twice returns `AlreadyInstalled`; if recorder installation fails after subscriber installation, the subscriber remains installed.

## Tests

All tests work without credentials, internet access, or a running Agent. Loopback mock HTTP and UDP receivers are test owned.

| Binary | Evidence |
| --- | --- |
| `config` | Defaults, tags, sinks, octal mode, strict booleans, duration clamp |
| `logs_correlation` | Exported ids match JSON ids, nested spans do not deadlock, file mode |
| `run_trace` | Run trace identity and INFO/DEBUG store filtering |
| `datadog_processor` | Real Datadog processor exports every concurrent fan-out span |
| `metrics_udp` | Shutdown delivers a pending counter; only binary installing a recorder |
| `offline` | Refused and black-holed Agent shutdown stays bounded |

## What is not here

OTLP export, profiling, network propagation helpers, log shipping, dashboards, and monitors. CLI inspection, middleware, and AG-UI projection remain separate work in `dmt-5vpz`.
