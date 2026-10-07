# Agent pipeline

Fake handlers demonstrate graph execution, durable waits, and recovery without credentials, LLMs, git, or GitHub calls.

## Graph

```text
plan -> iterate-plan -> plan-signoff (Wait signoff)
          ^                | changes_requested
          +----------------+
                           | approved
                           v
implement -> review-fanout -> review[0,1,2] -> collect-feedback (Join All)
                                             | needs_fixes -> fix
                                             | clean         |
                                             v               v
                                           open-pr -> present (Wait ack)
                                                        | acknowledged
                                                        v
                                                       done
```

The reviewers are `reviewer-a`, `reviewer-b`, and `reviewer-c`. Findings sum to three, so the fake pipeline takes the `needs_fixes` edge.

## Subcommands

- `demo`: creates a temporary data directory by default, approves both waits, prints events and finally `RunCompleted`.
- `run --data DIR [--input JSON]`: starts a new run and drives it to a wait or terminal state.
- `resume --data DIR [--run ID] [--hold]`: resumes open runs; `--run` chooses which run to report, while workers drive all open pipeline runs. `--hold` keeps a parked process alive for kill tests.
- `signal --data DIR --run ID --name NAME --label LABEL`: resolves an open wait without background workers. `signoff` accepts `approved` / `changes_requested`; `ack` accepts `acknowledged`.
- `show --data DIR --run ID`: prints graph, status, sorted tasks, open signals, and events.

Plain `resume` discovers runs from the first 10,000 historical pipeline runs, then filters out terminal runs. In heavily reused directories, use `resume --run ID` to select a newer run explicitly.

Global flags work before or after the subcommand: `--workers` defaults to 3 (at least 1), `--lease-secs` to 2 (at least 1), and `--timeout-secs` to 600. Heartbeats run every 500 ms. `claim_limit = 1` makes one worker strictly sequential. `demo` and `run` accept `--input JSON`, defaulting to `null`.

```sh
cargo run -p agent-pipeline -- demo
cargo run -p agent-pipeline -- run --data /tmp/pipeline
cargo run -p agent-pipeline -- show --data /tmp/pipeline --run RUN_ID
cargo run -p agent-pipeline -- signal --data /tmp/pipeline --run RUN_ID --name signoff --label approved
cargo run -p agent-pipeline -- resume --data /tmp/pipeline
```

Use one process at a time per data directory. Stop a held process before sending a signal from another command. A completed run is reported by `resume --run ID`; plain `resume` prints `no open runs` when all runs have ended. Every started engine registers the graph and rejects a changed definition under the same id/version.

Exit codes: 0 success or parked, 1 engine/store/I/O error or failed/cancelled run, 2 usage, 3 graph mismatch, 4 wait timeout. Crash injection terminates with `SIGABRT` on Unix.

## Data directory

| File | Contents |
| --- | --- |
| `pipeline.db` | Workflow state and event log; SQLite may create `-wal` and `-shm` sidecars |
| `ledger.jsonl` | One durable `{step_key, outcome}` line per fake effect |
| `invocations.jsonl` | Every `{step_key, node_id, attempt}` dispatch, never deduplicated |

All three files are created with mode 0600 on Unix. Payloads are plaintext; use no secrets. The ledger line itself is the fake external effect, fsync'd before it counts. A repeat returns the saved `NodeOutcome` under a mutex; this small example reads the full ledger each time. Real external effects require remote idempotency or a transaction/outbox, since a mutex cannot bridge a process crash between an external call and local recording.

## Crash injection

`AGENT_PIPELINE_CRASH_AT` is a test-only switch. The example compiles `dmt`'s `sqlite` and `test-faults` features unconditionally and is never published. Workspace tests therefore enable fault code; the xtask per-package checks prove feature-off builds.

- `<node>` aborts inside that handler after its first durable ledger append.
- `<node>:<branch>` restricts the handler abort to a branch index, e.g. `review:1`.
- `<node>:after` uses the runtime's existing `BeforeApply` fault on attempt 1, e.g. `open-pr:after`.

Unknown nodes and malformed suffixes are usage errors. `demo`, `run`, and `resume` read the switch once; `show` and `signal` ignore it. A repeated in-handler step returns its saved outcome and does not abort again.

## Crash-resume proof

```sh
cargo test -p agent-pipeline --test crash_resume
```

1. Start and park at `signoff/0`.
2. Request changes and resume to `signoff/1`.
3. Hold while parked, send SIGKILL, and verify the wait survives.
4. Approve, then abort inside review branch 1 after recording its effect.
5. Restart, reclaim that branch, then abort after `open-pr` returns but before apply.
6. Restart and reclaim `open-pr`, then park at `ack/0`.
7. Acknowledge and verify completion.

Every child uses one worker, a 2 s lease, and a 30 s watchdog. The test proves 11 distinct effects, repeated invocation attempts for both crash points, 14 unique task completions matching the last claims, one join satisfaction, three park/resume transitions, gap-free events, and an empty final task/signal snapshot. Child working directories and all artifacts stay in temporary directories.

These deaths exercise parked state, an in-handler boundary, and the pre-apply boundary. Death inside a SQLite transaction is covered separately by the [SQLite atomicity tests](../../crates/dmt-store-sqlite/tests/atomicity.rs). See the [facade guide](../../crates/dmt/README.md) for the durability contract, WAL caveat, and host responsibilities.
