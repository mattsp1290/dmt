mod common;
use common::SqliteFactory;
use dmt_store_conformance::{
    case_apply_atomic, case_cancel_skips_claimed, case_claim_ready, case_concurrent_claims,
    case_concurrent_join, case_crash_between_claim_and_apply, case_create_run, case_event_sequence,
    case_exhausted_sweep_idempotent, case_heartbeat, case_insert_or_ignore, case_join_guard,
    case_lease_proof, case_list_runs, case_load_run_view, case_micros_ordering,
    case_no_claim_after_terminal, case_reclaim_exhausts, case_register_graph, case_run_terminal,
    case_signal_resolution, case_stale_dispatch_same_worker, case_version_conflict, case_wait_loop,
    run_all,
};
#[tokio::test]
async fn apply_atomic() {
    case_apply_atomic(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn version_conflict() {
    case_version_conflict(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn lease_proof() {
    case_lease_proof(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn run_terminal() {
    case_run_terminal(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn insert_or_ignore() {
    case_insert_or_ignore(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn join_guard() {
    case_join_guard(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn claim_ready() {
    case_claim_ready(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn heartbeat() {
    case_heartbeat(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn event_sequence() {
    case_event_sequence(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn signal_resolution() {
    case_signal_resolution(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn register_graph() {
    case_register_graph(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn create_run() {
    case_create_run(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn micros_ordering() {
    case_micros_ordering(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn load_run_view() {
    case_load_run_view(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn list_runs() {
    case_list_runs(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn wait_loop() {
    case_wait_loop(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn crash_between_claim_and_apply() {
    case_crash_between_claim_and_apply(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn stale_dispatch_same_worker() {
    case_stale_dispatch_same_worker(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn no_claim_after_terminal() {
    case_no_claim_after_terminal(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn cancel_skips_claimed() {
    case_cancel_skips_claimed(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn reclaim_exhausts() {
    case_reclaim_exhausts(&SqliteFactory::new()).await;
}
#[tokio::test]
async fn exhausted_sweep_idempotent() {
    case_exhausted_sweep_idempotent(&SqliteFactory::new()).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims() {
    case_concurrent_claims(&SqliteFactory::new()).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_join() {
    case_concurrent_join(&SqliteFactory::new()).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_all_sqlite() {
    run_all(&SqliteFactory::new()).await;
}
