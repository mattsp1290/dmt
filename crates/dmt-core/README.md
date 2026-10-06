# dmt-core

## What this crate owns

The pure core of dmt owns validated graph definitions, serializable run lifecycle state, the task transition table, append-only events, and a deterministic planner producing one atomic `Commit` per input. Hosts provide `Micros` timestamps; no clock, I/O, async runtime, or credentials are required.

## Graph model

Nodes are Task, FanOut, Branch, Join, Wait, or End. Explicit edges select an exact label before falling back to a default guard. FanOut implicitly schedules its Branch workers; their contributions eventually schedule its Join. Explicit edges cannot enter Branch or Join or leave FanOut or Branch. Exhaustion follows only `failed`, never a default success edge. `timeout` is the wait deadline route; `cancelled` is reserved for cancellation and cannot label an edge.

`GraphBuilder::build`, `Graph::from_json`, and serde deserialization all validate. The serde-derived `Graph` shape is the sole JSON format. `definition_hash` is SHA-256 of compact JSON with sorted edges and ordered node keys. No wire or hash stability promise exists in milestone 1.

## Validation rules

| # | Rule |
| --- | --- |
| 1 | `nodes` is non-empty and `start` names an existing node |
| 2 | The start node's kind is `Task`, `FanOut`, or `Wait` |
| 3 | Every edge endpoint exists |
| 4 | Every `FanOut` names a `Branch` node as `branch` and a `Join` node as `join` |
| 5 | Every `Branch` and every `Join` is named by exactly one `FanOut` |
| 6 | No explicit edge leaves a `FanOut` or `Branch`; no explicit edge enters a `Branch` or `Join` |
| 7 | Every `Task`, `Join`, and `Wait` node has at least one outgoing edge |
| 8 | At most one `Guard::Default` per `from` node |
| 9 | No duplicate `(from, label)` pair |
| 10 | `End` nodes have no outgoing edges |
| 11 | A `Wait` with `deadline_micros` has an edge with `Guard::Label("timeout")`; `deadline_micros`, when present, is greater than 0 |
| 12 | `Wait` signal names are unique per graph |
| 13 | Every node is reachable from `start` through explicit edges plus the implicit `FanOut → Branch → Join` successors |
| 14 | Node ids, signal names, and edge labels are non-empty and contain no `/` |
| 15 | `RetryPolicy`: `1 <= max_attempts <= 1000`, `initial_backoff_micros >= 0`, `max_backoff_micros >= initial_backoff_micros`, `multiplier_permille >= 1000` |
| 16 | No edge label equals `cancelled` |
| 17 | `JoinPolicy::Quorum(n)` has `n >= 1` |
| 18 | `NodeDef.timeout_micros`, when present, is greater than 0 |

Cycles are allowed; reachability includes implicit fan-out successors. Validation collects errors without cascading structural errors from unusable identifiers.

## Run lifecycle

```text
Created --Start--> Running::Active
Running::Active --Park--> Running::Parked
Running::Parked --Resume--> Running::Active
Running --Complete--> Completed
Running --Fail--> Failed
Running --Cancel--> Cancelled
```

Created rejects every event except Start. Terminal states reject every event. The initialized statig machine is persisted as JSON with `shared_storage` and `state`. Restore deserializes into an uninitialized machine, initializes it with fresh effects, then checks the stored status. Statig repeats entry actions during restoration, so this machine has no entry or exit actions; its handlers alone emit lifecycle events. Integration tests pin both the JSON shape and this behavioral constraint.

## Task status table and attempt semantics

| From | Event | To | Owner |
| --- | --- | --- | --- |
| Ready | Claim | Running | store |
| Ready | RunTerminal | Cancelled | store |
| Ready | Cancel | Cancelled | planner |
| Running | Reclaim | Running | store (attempt + 1) |
| Running | LeaseExhausted | Exhausted | store |
| Running | RunTerminal | Cancelled | store |
| Running | Done | Completed | planner |
| Running | FailRetry | Ready | planner (attempt + 1, run_at = backoff) |
| Running | FailFinal | Exhausted | planner |
| Awaiting | Signal | Completed | planner |
| Awaiting | Timeout | Completed | planner |
| Awaiting | Cancel | Cancelled | planner |


| Moment | attempt | Who |
| --- | --- | --- |
| `TaskScheduled` | 1 | planner |
| `TaskClaimed` (first claim) | unchanged | store |
| Handler fails retryably and `attempt < max_attempts` | attempt + 1, status Ready, `run_at = now + backoff(attempt)` | planner |
| Handler fails retryably and `attempt == max_attempts`, or fails with `retryable: false` | Exhausted, `TaskExhausted { HandlerFailures }` | planner |
| Lease expires and `attempt < max_attempts` | attempt + 1, Running, new lease, `TaskClaimed { attempt }` | store |
| Lease expires and `attempt == max_attempts` | Exhausted, `TaskExhausted { LeaseReclaimsExceeded }` | store |


Every other pair is rejected. Running tasks stay running on planner cancellation; the store later rejects their commits or cancels them during reclamation. Attempts count total handler runs, including lease reclaims. Retry backoff uses the failing attempt number: defaults are 1 s, 2 s, 4 s, capped at 60 s. Waits are never claimed and have one attempt. Branch and Join tasks use their own node's retry policy.

## Keys

Task and join keys are `{run}/{node}/{occurrence}`; branch keys add `/{index}`. Signal keys are `{signal}/{occurrence}`, unique per run. Occurrences start at zero. Task and Join ids equal their step key strings; Signal ids are `{run}/{signal_key}`. Only hosts mint Run and Worker ids with UUID v4. Node ids, run ids, signal names, and guard labels must be nonempty and contain no slash.

## Planner and Commit

- `plan_start` accepts a validated graph, run id, input, and timestamp, emitting RunStarted and scheduling the start node. A Wait start parks immediately.
- `plan_outcome` accepts a snapshot, task id, handler outcome, and timestamp. It completes a task, retries or exhausts it, creates a fan-out, or contributes to its join.
- `plan_signal` and `plan_timeout` accept an open signal id and timestamp. They resolve the signal, complete its awaiting task, and schedule its edge successor. Timeout selection by deadline belongs to the store.
- `plan_exhausted` marks a store-exhausted task planned and routes its failure without duplicating the store's TaskExhausted event.
- `plan_cancel` accepts a reason and timestamp, cancels ready and awaiting tasks, and resolves open signals as cancelled.

Every call returns a single Commit containing ordered events, optional lifecycle state and output, task updates and inserts, optional join creation or contribution, optional signal creation and signal resolutions, and the complete occurrence map. `Commit::check` verifies record/event consistency and key uniqueness. Missing outcome edges complete the task and fail the run in the commit.

Parking uses the post-commit task set: a run parks only with an open wait and no Ready, Running, or unplanned Exhausted task. New ready work resumes a parked run. A terminal run remains terminal. Quorum joins can complete a run while straggler branches are still Running; later commits are rejected by the store's terminal-run rule.

## Contract notes for stores and runtimes

`load_run` must include Ready, Running, Awaiting, and Exhausted tasks (including planned Exhausted rows), every join whether satisfied or not, and open signals. Additional task rows are permitted and filtered by the planner. Preserve deterministic TaskId, JoinId, and SignalId strings in storage. Runs have version 1 after create; every Commit checks its expected version before an atomic apply.

Clear leases on task updates to non-Running statuses. A Cancelled update applies only to rows still Ready or Awaiting: a concurrent claim can change a Ready row to Running without bumping run version. Use `Commit.now` for updated, recorded, resolved, and satisfied timestamps.

Late contributions to a satisfied join emit BranchContributed and complete the branch task but carry no join contribution update. For an exhausted sweep, AlreadyPlanned, UnknownTask, and RunTerminal are benign results. TaskCompleted for a FanOut carries a synthetic `fan_out` label with the input array as payload. Planner APIs take task and signal ids and look up their records in the snapshot.

The test simulator mirrors version checking, task existence, terminal rejection, step-key uniqueness, join counters, signal resolution, lease clearing, and conditional cancellation. It does not model concurrent claimers, lease expiry timing, event sequence allocation, or ignored-task reporting. Store conformance tests must cover those contracts.

## Fixtures feature

Enable the `fixtures` feature only for test support in downstream crates:

```toml
[dev-dependencies]
dmt-core = { path = "../dmt-core", features = ["fixtures"] }
```

`fixtures` exports linear, loop, fan-out, fan-out then wait, deadline wait, retry, and agent pipeline graphs. Fixture parameters must pass graph validation. These fixtures carry no API stability promise.

## What is not here

Stores, lease proofs, runtime handlers, timers beyond wait deadlines and retry delays, typed payloads, and graph migration belong to later crates or milestones.
