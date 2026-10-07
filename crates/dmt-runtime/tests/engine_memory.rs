//! In-memory engine scenarios, grouped by the contract they verify.
mod common;

#[path = "memory/catalog_sweeps.rs"]
mod catalog_sweeps;
#[path = "memory/execution.rs"]
mod execution;
#[path = "memory/handlers.rs"]
mod handlers;
#[path = "memory/leases.rs"]
mod leases;
#[path = "memory/signals.rs"]
mod signals;
#[path = "memory/stopping.rs"]
mod stopping;
#[path = "memory/support.rs"]
mod support;
