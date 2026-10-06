//! Reusable transactional conformance tests for any dmt store backend.
use async_trait::async_trait;
use dmt_store::Store;
mod cases;
pub use cases::*;
pub mod harness;
/// Creates an independent, empty backend for each case. Keep backing resources alive
/// for the returned store's lifetime (for example, a temporary SQLite database).
#[async_trait]
pub trait StoreFactory: Send + Sync {
    type S: Store + 'static;
    async fn fresh(&self) -> Self::S;
}
macro_rules! cases {
    ($(($name:literal,$case:path)),+ $(,)?) => {
        pub const CASE_NAMES: [&str;24] = [$($name),+];
        /// Run every contract case in the documented order.
        /// # Panics
        /// Panics with the first failing case's name when a backend violates the contract.
        pub async fn run_all<F: StoreFactory>(factory: &F) { $($case(factory).await;)+ }
    };
}
cases!(
    ("apply_atomic", case_apply_atomic),
    ("version_conflict", case_version_conflict),
    ("lease_proof", case_lease_proof),
    ("run_terminal", case_run_terminal),
    ("insert_or_ignore", case_insert_or_ignore),
    ("join_guard", case_join_guard),
    ("claim_ready", case_claim_ready),
    ("heartbeat", case_heartbeat),
    ("event_sequence", case_event_sequence),
    ("signal_resolution", case_signal_resolution),
    ("register_graph", case_register_graph),
    ("create_run", case_create_run),
    ("micros_ordering", case_micros_ordering),
    ("load_run_view", case_load_run_view),
    ("list_runs", case_list_runs),
    ("wait_loop", case_wait_loop),
    (
        "crash_between_claim_and_apply",
        case_crash_between_claim_and_apply
    ),
    (
        "stale_dispatch_same_worker",
        case_stale_dispatch_same_worker
    ),
    ("no_claim_after_terminal", case_no_claim_after_terminal),
    ("cancel_skips_claimed", case_cancel_skips_claimed),
    ("reclaim_exhausts", case_reclaim_exhausts),
    (
        "exhausted_sweep_idempotent",
        case_exhausted_sweep_idempotent
    ),
    ("concurrent_claims", case_concurrent_claims),
    ("concurrent_join", case_concurrent_join),
);
#[cfg(test)]
mod tests {
    use super::CASE_NAMES;
    #[test]
    fn case_names_are_unique() {
        assert_eq!(CASE_NAMES.len(), 24);
        assert_eq!(
            CASE_NAMES
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            24
        );
    }
}
