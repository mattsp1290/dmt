# dmt

dmt is an embeddable Rust library for long-lived, human-in-the-loop agent workflows. Workflows are data-defined graphs of nodes with an explicit run-lifecycle state machine. Runs persist to SQL so they can resume after a restart.

## Status

Milestone 1 is in progress. This repository currently contains the workspace scaffold and quality gate only. The crate map and quick start describe the milestone-1 target and identify the Beans tasks that deliver each part.

## Crate map

| Package | Purpose | Status / task |
| --- | --- | --- |
| `dmt-core` | Graph, lifecycle, planner | Planned: `dmt-lr2v` |
| `dmt-store` | Storage interface | Planned: `dmt-gzao` |
| `dmt-store-conformance` | Shared store conformance checks | Planned: `dmt-gzao` |
| `dmt-store-sqlite` | SQLite storage | Planned: `dmt-0hq5` |
| `dmt-runtime` | Workflow execution | Planned: `dmt-76cp` |
| `dmt` | Public facade | Planned: `dmt-nvsm` |
| `examples/agent-pipeline` | Runnable workflow example | Planned: `dmt-nvsm` |
| `xtask` | Repository quality gate | Available: `dmt-60e2` |

## Quick start

Run the available quality gate:

```sh
cargo xtask check
```

`cargo run -p agent-pipeline -- demo` is **not yet runnable**; it becomes available after `dmt-nvsm`.

## Durability contract

The milestone-1 contract runs handlers at least once. Each node invocation carries a stable step key that handlers use as their idempotency key. State effects (events, task transitions, and new tasks) commit exactly once per outcome.

## SQLite limitation

Milestone 1 supports SQLite for a single process with any number of in-process workers. Multi-process SQLite claiming is not supported.

## Development

Read [AGENTS.md](AGENTS.md) for workspace conventions and the quality gate. Issues live in the Beans (`bn`) hub under `dmt-<hash>` ids.
