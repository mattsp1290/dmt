# dmt-store

## What this crate owns

`Store` is the object-safe, async persistence boundary used by every backend. This crate also owns `StoreError`, lease/claim/heartbeat/apply/event/run-query types, `Clock`, `SystemClock`, `ManualClock`, and the reference `MemoryStore`. Normal dependencies are only `dmt-core`, `async-trait`, `thiserror`, and `serde_json`; Tokio is a test dependency.

```sh
cargo test -p dmt-store
cargo tree -p dmt-store -e normal
```

## Time

Hosts supply `Micros` to every mutation and query needing time. Stores never read a clock. `SystemClock` supplies wall time for hosts; `ManualClock` supplies controllable atomic time for tests. Manual clones share time, and advancing saturates at the signed microsecond range. `Arc<C>` implements `Clock`, including `Arc<dyn Clock>`.

## Method table

Any method may return `Busy` or `Backend` in a persistent backend; `MemoryStore` never returns either.

| Method | Behavior | Contract errors |
| --- | --- | --- |
| `register_graph` | Store full graph by `(id, version)`; same hash is idempotent | `GraphMismatch` |
| `load_graph` | Return registered graph or `None` | — |
| `create_run` | Apply a start commit with version zero; new run has version 1 | `InvalidCommit`, `NotFound`, `GraphMismatch`, `VersionConflict`, record checks |
| `load_run` | Return planner snapshot or `None` | — |
| `load_task` | Return task in any status or `None` | — |
| `claim_ready` | Claim eligible rows and append claim/exhaustion events without bumping run version | — |
| `heartbeat` | Extend matching Running lease; return `Extended { lease_until }` or `Lost`; no events/version bump | `NotFound` |
| `apply` | Validate then atomically persist a planner commit | `NotFound`, `RunTerminal`, `VersionConflict`, `LeaseLost`, `JoinDrift`, `AlreadyResolved`, `InvalidCommit` |
| `find_open_signal` | Return open signal by run/name or `None` | — |
| `due_signals` | Open deadlines `<= now` on nonterminal runs, ordered by `(deadline_at, signal_id)` | — |
| `exhausted_tasks` | Unplanned Exhausted tasks on nonterminal runs, ordered by `(run_at, task_id)` | — |
| `events` | Records with `seq > after_seq`, ordered by seq; unknown run is empty | — |
| `list_runs` | Filter by optional status/graph; order by `(created_at, run_id)` | — |

All query limits cap returned rows; zero returns no rows. `ClaimedTask` contains the Running task, graph id/version, and `LeaseProof { task_id, worker_id, attempt }`. Heartbeats use `heartbeat(&proof, now, lease_micros)` with saturating deadline addition. Lease expiry alone does not invalidate a proof; reclaim or a status/owner/attempt change does.

## Error table

| Variant | Meaning | Caller action |
| --- | --- | --- |
| `VersionConflict { expected, actual }` | Run version differs; duplicate create expects zero | Reload and re-plan |
| `JoinDrift { join_id }` | Counters differ from planner expectation, or join already satisfied | Reload and re-plan |
| `LeaseLost { task_id }` | Wrong run, owner, attempt, or status | Drop dispatched outcome |
| `RunTerminal { run_id }` | Completed, Failed, or Cancelled run | Drop outcome |
| `NotFound(String)` | Required graph/run/task/join/signal missing | Investigate stale id or programming error |
| `AlreadyResolved { signal_id }` | Signal already resolved | Drop resolution |
| `GraphMismatch { graph_id, version }` | Registered key has another definition hash | Surface to host |
| `InvalidCommit(String)` | Invalid create/apply shape, duplicate signal key/join id, invalid task identity | Fix programming error |
| `Busy` | Backend lock timeout; any method can return it | Retry with backoff |
| `Backend(String)` | I/O, serialization, or schema failure | Log and retry later |

## Apply check order

MemoryStore validates these before writing. The precedence of **1 → 2 → 3 → 5** is contractual and tested; check 4's position and the relative ordering of checks 6–9 are backend choices.

1. **Run exists** (`NotFound`).
2. **Run nonterminal** (`RunTerminal`).
3. **Expected version matches** (`VersionConflict`).
4. No `new_run` in `apply` (`InvalidCommit`).
5. **If supplied, proof names an existing task** (`NotFound`) **of this run with matching owner, attempt, and Running status** (`LeaseLost`). `None` skips the proof check for sweep, signal, timeout, and cancel commits.
6. Updated tasks exist and belong to this run (`NotFound`).
7. Join exists in this run (`NotFound`), is unsatisfied, and its counters plus this contribution equal the planner expectations (`JoinDrift`).
8. Resolved signals exist in this run (`NotFound`) and are open (`AlreadyResolved`).
9. New signal keys and ids and join ids are unique; new join belongs to this run; task ids equal their step keys and cannot collide with a different key (`InvalidCommit`).

`create_run` requires `new_run` and expected version 0, a registered graph with matching hash, and no existing run. It validates records before inserting the run. Stores do not call `Commit::check`; the planner owns that check. The conformance suite deliberately modifies planner commits to exercise store validation.

## Transactional rules

1. All-or-nothing apply, including events and sequence allocation (`apply_atomic`).
2. Optimistic version check; successful create/apply sets expected version + 1 (`version_conflict`).
3. Lease proof checks task run, owner, attempt, and Running status (`lease_proof`, `stale_dispatch_same_worker`).
4. Terminal runs reject apply before stale-version checks (`run_terminal`).
5. Insert-or-ignore by step key, including repetitions within one commit; report inserted ids and ignored keys in commit order (`insert_or_ignore`). Ignored inserts never overwrite rows. All commit events still append verbatim.
6. Planner decides satisfaction; store guards join counters and prohibits contributions after satisfaction (`join_guard`, `concurrent_join`).
7. Atomic claiming, reclaiming, exhaustion, terminal cancellation, graph filtering (`claim_ready`, `concurrent_claims`, `crash_between_claim_and_apply`, `no_claim_after_terminal`, `cancel_skips_claimed`, `reclaim_exhausts`).
8. Matching heartbeat extends without an event/version bump (`heartbeat`).
9. Gap-free per-run event seq starts at 1; `EventRecord { seq, recorded_at, event }` timestamps come from the supplied commit/claim time (`event_sequence`).
10. Signal resolution is atomic within apply; stamp `resolved_at` from commit time (`signal_resolution`).
11. Graph registration is idempotent by key/hash (`register_graph`).
12. Run creation requires a registered matching graph (`create_run`).
13. Signal key includes occurrence; open-signal lookup ignores earlier resolved occurrences (`wait_loop`).
14. Every timestamp is exact microseconds (`micros_ordering`).
15. Sweeps exclude planned Exhausted rows and terminal runs (`exhausted_sweep_idempotent`).

Successful apply replaces node occurrences, applies optional run state, writes task updates, inserts tasks/joins/signals, applies guarded contributions/resolutions, appends events, and updates version and `updated_at`. Non-Running task updates clear both lease fields. Cancelled updates apply only to rows still Ready or Awaiting, preserving a concurrent claim. Join satisfaction stamps `satisfied_at` and returns `join_satisfied`; late planner outcomes append events without changing join counters. Run creation stamps `created_at = updated_at = Commit.now`.

## claim_ready semantics

Candidates belong to one of `ClaimRequest.graphs` (all versions) and are either Ready with `run_at <= now`, or Running with `lease_until < now`. Expiry is **strict**: equality does not reclaim. Sort by `(run_at, task_id)`, and walk until `limit` tasks have been returned. An empty graph list or zero limit is a no-op. Unvisited candidates stay untouched.

- A terminal run's candidate becomes Cancelled and loses its lease, without an event or counting toward the limit.
- An expired Running task at `attempt >= max_attempts` becomes Exhausted, loses its lease, and appends `TaskExhausted { reason: LeaseReclaimsExceeded }`; it does not count toward the limit.
- Other expired tasks are reclaimed with attempt + 1; Ready tasks retain their attempt. Both become Running, get owner/deadline, append `TaskClaimed`, and return a proof.

The run version never changes. Concurrent callers cannot receive the same task. Store-owned transitions use `TaskStatus::next`.

## load_run view

Snapshots include every Ready, Running, Awaiting, and Exhausted task, even planned Exhausted rows; every join, satisfied or not; and only open signals. Extra Completed/Cancelled tasks are permitted. `load_task` includes all statuses. These rules let the planner account for active dispatches, waits, and completed joins.

## MemoryStore

One `std::sync::Mutex` protects all state; there is no await while holding its guard. Validation precedes mutation in a critical section. `Clone` shares the same state, while `new`/`default` create an empty store. Poisoned locks recover their inner data. This is non-durable storage for tests and hosts that need no persistence.

## What is not here

SQL backends, runtime workers/handlers, bulk deletion, archival, graph migrations, typed payloads, and pagination cursors beyond `after_seq`/`limit` belong elsewhere.
