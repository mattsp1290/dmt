# Agent instructions

## Workspace

This Cargo workspace uses Rust 2024. Run `cargo xtask check` before handing off changes. It runs `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `cargo test --workspace --all-features`, followed by feature-off `cargo clippy --package dmt-store-sqlite --all-targets -- -D warnings` and `cargo test --package dmt-store-sqlite`.

`cargo xtask check --offline` passes `--offline` to all workspace and feature-off Clippy and test commands. `cargo fmt` has no such flag and never uses the network.

Workspace lints forbid `unsafe_code` and warn on `clippy::all` and `clippy::pedantic`; the gate promotes warnings to errors. Do not add `#[allow]` to pass the gate without a comment explaining why.

## Beans issue tracker

Use `bn` to inspect and update work tracked under `dmt-<hash>` issue ids. `bn ready` lists unblocked tasks. Record the commit sha in the close reason. Do not close work owned by another branch or agent. Read the milestone plan with `bn plan show dmt-plan-2b9t`.

## Local agent artifacts

Keep `.agents/plans/`, `.agents/reviews/`, and `reviews/` local. They are ignored and must not be committed.

`.agents/next-milestone.json` is product metadata owned by the `select-next-milestone` skill. Edit it only through that skill's confirmation flow.

## Secrets and tests

Never commit credentials. Use environment variable names in documentation, never credential values. Tests must run without credentials. SQLite tests need no environment variables.
