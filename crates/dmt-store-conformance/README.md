# dmt-store-conformance

## Purpose

This reusable suite proves a backend against the [Store contract](../dmt-store/README.md). Each case obtains its own fresh store and drives commits through the core planner. MemoryStore is the reference backend; its suite lives here to avoid a dev-dependency cycle.

```sh
cargo test -p dmt-store-conformance
cargo test -p dmt-store-conformance --test mutants
```

## Running the suite against your backend

Implement `StoreFactory` with a store that owns any resources it needs for its lifetime. Each `fresh` must return independent empty state. Example `tests/conformance.rs` (replace the example backend constructor with your own):

```rust,ignore
use async_trait::async_trait;
use dmt_store_conformance::{StoreFactory, run_all};
use my_backend::MyStore;
struct Factory;
#[async_trait]
impl StoreFactory for Factory {
    type S = MyStore;
    async fn fresh(&self) -> MyStore { MyStore::empty_temporary().await.unwrap() }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conforms() { run_all(&Factory).await; }
```

Alternatively invoke every public `case_*(&Factory)` in its own test, as [tests/memory.rs](tests/memory.rs) does. Concurrency cases and `run_all` should use a multi-thread Tokio runtime to exercise parallel workers. `run_all` awaits the cases sequentially and panics with the first failing case's name. `CASE_NAMES` and `run_all` come from the same macro list.

## Case table

The following is `CASE_NAMES` order. Every listed name exports `case_<name>`.

| Case | Rule / contract note | Fixture |
| --- | --- | --- |
| `apply_atomic` | 1 | `linear` |
| `version_conflict` | 2 | `linear` |
| `lease_proof` | 3 | `linear` |
| `run_terminal` | 4 | `linear` |
| `insert_or_ignore` | 5 | `linear` |
| `join_guard` | 6 | `fan_out(All/Quorum(1))` |
| `claim_ready` | 7 | `linear, retry_chain(3)` |
| `heartbeat` | 8 | `linear` |
| `event_sequence` | 9 | `linear` |
| `signal_resolution` | 10 | `loop_via_wait` |
| `register_graph` | 11 | `linear, changed definition` |
| `create_run` | 12 | `linear, version 2` |
| `micros_ordering` | 14 | `linear, wait_with_deadline(1)` |
| `load_run_view` | snapshot view | `fan_out_then_wait, exhaust_then_continue` |
| `list_runs` | run summaries | `linear, retry_chain(3)` |
| `wait_loop` | 13 | `loop_via_wait` |
| `crash_between_claim_and_apply` | 7 | `linear` |
| `stale_dispatch_same_worker` | 3 | `linear` |
| `no_claim_after_terminal` | 7 | `fan_out(All/Quorum(1))` |
| `cancel_skips_claimed` | conditional cancel | `fan_out(All)` |
| `reclaim_exhausts` | 7 | `retry_chain(2)` |
| `exhausted_sweep_idempotent` | 15 | `exhaust_then_continue` |
| `concurrent_claims` | 7 | `fan_out(All), 16 branches` |
| `concurrent_join` | 6 | `fan_out(All), 3 branches` |

Rule traceability: 1 → apply_atomic; 2 → version_conflict; 3 → lease_proof, stale_dispatch_same_worker; 4 → run_terminal; 5 → insert_or_ignore; 6 → join_guard, concurrent_join; 7 → claim_ready, crash_between_claim_and_apply, no_claim_after_terminal, cancel_skips_claimed, reclaim_exhausts, concurrent_claims; 8 → heartbeat; 9 → event_sequence and all contiguous assertions; 10 → signal_resolution; 11 → register_graph; 12 → create_run; 13 → wait_loop; 14 → micros_ordering; 15 → exhausted_sweep_idempotent. Contract notes: load_run_view, cancel_skips_claimed, list_runs.

## Mutant tests

[tests/mutants.rs](tests/mutants.rs) demonstrates suite sensitivity with eight wrappers: ignore apply proof, ignore heartbeat attempt, hide ignored task keys, hide reclaim attempt increment, swallow join drift, check version before terminal status, drop claim events, and duplicate concurrent branch claims. Each runs its owning case with `should_panic(expected = "case_name")`. The duplicate wrapper seeds only branch claims so the fault is tested during contention, after valid setup. To cover a new rule, add a wrapper fault and a test that fails with its owning case name; a green reference suite alone cannot prove sensitivity.

## Harness

`T0 = Micros(1_000_000_000)` and `LEASE = 10_000` provide deterministic times. Public helpers:

- `worker`, `run`, `task_id`, `branch_id`, `signal_id`, `claim_request`: deterministic ids and requests.
- `exhaust_then_continue`: custom graph for nonterminal exhaustion sweeps.
- `start_run`, `snapshot`, `task`, `claim_one`, `claim_all`: setup and required-row reads; store helpers accept `S: Store + ?Sized`.
- `plan_done`, `complete`: plan an outcome against a fresh snapshot, then optionally apply with a proof.
- `events`, `kinds`, `count`, `assert_contiguous`: event assertions.
- `ok`: unwrap a result with the case name in its panic.
- `retry_busy`: bounded lock-contention retries.

Panicking helpers take the owning case name first. Every claim's time must be at least the task's `run_at`; tests use explicit time advances. Microsecond and strict expiry boundary assertions keep rounding defects visible.

## Busy handling

Any Store method may return `StoreError::Busy`. Harness reads and mutations retry it up to 50 times with 1 ms sleeps. Concurrent claimers use `retry_busy`; concurrent joiners reload and re-plan on `VersionConflict`, `JoinDrift`, or `Busy` with a bounded retry loop. Backend tests should use the same helper for calls made during contention. Sequential direct contract assertions expect the documented success/error; backend factories should provide isolated stores with no external lock holders.

## What the suite does not cover

Backend crash/fault injection, multi-process claiming, and wall-clock behavior need backend-specific tests. Multiple open signals with the same name and the terminal-run filter of `due_signals` are unreachable through planner-built commits and are not tested. Crash-between-claim-and-apply here simulates a missing handler commit; it does not kill a process or prove durability.
