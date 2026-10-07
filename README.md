# dmt

dmt is an embeddable Rust library for long-lived, human-in-the-loop agent workflows. Workflows are data-defined graphs of nodes with an explicit run-lifecycle state machine. Runs persist to SQL so they can resume after a restart.

## Status

Milestone 1 is complete at the crate level: core graph and planner, store contract and conformance suite, SQLite backend, runtime engine, the `dmt` facade, and the runnable agent pipeline with a crash-resume proof are available.

## Crate map

| Package | Purpose | Status / task |
| --- | --- | --- |
| `dmt-core` | Graph, lifecycle, planner | Available: `dmt-lr2v` |
| `dmt-store` | Storage interface | Available: `dmt-gzao` |
| `dmt-store-conformance` | Shared store conformance checks | Available: `dmt-gzao` |
| `dmt-store-sqlite` | SQLite storage | Available: `dmt-0hq5` |
| `dmt-runtime` | Workflow execution | Available: `dmt-76cp` |
| `dmt` | Public facade | Available: `dmt-nvsm` |
| `examples/agent-pipeline` | Runnable workflow example | Available: `dmt-nvsm` |
| `xtask` | Repository quality gate | Available: `dmt-60e2` |

## Quick start

Run the available quality gate:

```sh
cargo xtask check
```

Run the fake agent workflow and its crash-resume proof without credentials:

```sh
cargo run -p agent-pipeline -- demo
cargo test -p agent-pipeline
```

See the [facade embedding guide](crates/dmt/README.md) and [example CLI guide](examples/agent-pipeline/README.md).

## Durability contract

The milestone-1 contract runs handlers at least once. Each node invocation carries a stable step key that handlers use as their idempotency key. State effects (events, task transitions, and new tasks) commit exactly once per outcome. SQLite WAL uses `synchronous = NORMAL`, which survives process death but can lose recent commits on power loss or OS crash; see the [SQLite durability options](crates/dmt-store-sqlite/README.md).

## SQLite limitation

Milestone 1 supports SQLite for a single process with any number of in-process workers. Multi-process SQLite claiming is not supported.

## Development

Read [AGENTS.md](AGENTS.md) for workspace conventions and the quality gate. Issues live in the Beans (`bn`) hub under `dmt-<hash>` ids.
