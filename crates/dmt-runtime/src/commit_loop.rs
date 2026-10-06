use crate::{EngineError, backoff::replan_delay, shared::Shared};
use dmt_core::{Commit, Graph, Micros, NodeOutcome, PlanError, RunSnapshot, TaskId, plan_outcome};
use dmt_store::{ClaimedTask, StoreError};
use tokio::time::sleep;

pub(crate) fn retryable(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::VersionConflict { .. }
            | StoreError::JoinDrift { .. }
            | StoreError::Busy
            | StoreError::Backend(_)
    )
}
pub(crate) fn transient(error: &StoreError) -> bool {
    matches!(error, StoreError::Busy | StoreError::Backend(_))
}
pub(crate) async fn delay(shared: &Shared, retry: u32) {
    if retry > 0 {
        sleep(replan_delay(shared.config.replan_backoff, retry - 1)).await;
    }
}
pub(crate) async fn resolve(
    shared: &Shared,
    id: &dmt_core::GraphId,
    version: u32,
) -> Result<std::sync::Arc<Graph>, EngineError> {
    let mut last = StoreError::Busy;
    for retry in 0..=shared.config.max_replan_attempts {
        delay(shared, retry).await;
        match shared
            .catalog
            .resolve(shared.store.as_ref(), id, version)
            .await
        {
            Ok(graph) => return Ok(graph),
            Err(EngineError::Store(e)) if transient(&e) => last = e,
            Err(e) => return Err(e),
        }
    }
    Err(EngineError::Contention {
        attempts: shared.config.max_replan_attempts.saturating_add(1),
        last,
    })
}
pub(crate) async fn outcome(
    shared: &Shared,
    graph: &Graph,
    claimed: &ClaimedTask,
    outcome: NodeOutcome,
) {
    for retry in 0..=shared.config.max_replan_attempts {
        delay(shared, retry).await;
        let snapshot = match shared.store.load_run(&claimed.task.run_id).await {
            Ok(Some(s)) => s,
            Err(e) if transient(&e) => continue,
            other => {
                tracing::error!(result = ?other.as_ref().map(Option::is_some), "run unavailable");
                return;
            }
        };
        let commit = match plan_with_downgrade(
            graph,
            &snapshot,
            &claimed.task.task_id,
            &outcome,
            shared.clock.now(),
        ) {
            Ok(c) => c,
            Err(
                PlanError::RunTerminal { .. }
                | PlanError::UnknownTask { .. }
                | PlanError::TaskNotRunning { .. },
            ) => {
                tracing::debug!("stale outcome dropped");
                return;
            }
            Err(error) => {
                tracing::error!(%error, "outcome cannot be planned");
                return;
            }
        };
        match shared
            .store
            .apply(commit, Some(claimed.proof.clone()))
            .await
        {
            Ok(_) => {
                shared.wake.notify_waiters();
                return;
            }
            Err(e) if retryable(&e) => tracing::debug!(error = %e, retry, "commit conflict"),
            Err(StoreError::LeaseLost { .. }) => {
                tracing::warn!("lease lost");
                return;
            }
            Err(StoreError::RunTerminal { .. }) => {
                tracing::debug!("run terminal");
                return;
            }
            Err(error) => {
                tracing::error!(%error, "commit rejected");
                return;
            }
        }
    }
    tracing::error!("replan attempts exhausted");
}
fn plan_with_downgrade(
    graph: &Graph,
    snapshot: &RunSnapshot,
    task: &TaskId,
    outcome: &NodeOutcome,
    now: Micros,
) -> Result<Commit, PlanError> {
    match plan_outcome(graph, snapshot, task, outcome.clone(), now) {
        Err(e @ (PlanError::KindMismatch { .. } | PlanError::EmptyFanOut { .. })) => plan_outcome(
            graph,
            snapshot,
            task,
            NodeOutcome::Fail {
                message: e.to_string(),
                retryable: false,
            },
            now,
        ),
        result => result,
    }
}
// Unowned operations are wired to signals, cancel, and sweeps in WP3.
#[allow(dead_code)]
pub(crate) struct Unowned {
    pub result: Result<(), EngineError>,
    pub ambiguous: Option<StoreError>,
}
// Kept in the core commit package so every unowned operation shares the retry bound.
#[allow(dead_code)]
pub(crate) async fn unowned<F>(shared: &Shared, run_id: &dmt_core::RunId, plan: F) -> Unowned
where
    F: Fn(&Graph, &RunSnapshot, Micros) -> Result<Commit, PlanError>,
{
    let mut ambiguous = None;
    let result = unowned_loop(shared, run_id, plan, &mut ambiguous).await;
    Unowned { result, ambiguous }
}
async fn unowned_loop<F>(
    shared: &Shared,
    run_id: &dmt_core::RunId,
    plan: F,
    ambiguous: &mut Option<StoreError>,
) -> Result<(), EngineError>
where
    F: Fn(&Graph, &RunSnapshot, Micros) -> Result<Commit, PlanError>,
{
    let mut last = StoreError::Busy;
    for retry in 0..=shared.config.max_replan_attempts {
        delay(shared, retry).await;
        let snapshot = match shared.store.load_run(run_id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Err(EngineError::RunNotFound {
                    run_id: run_id.clone(),
                });
            }
            Err(e) if transient(&e) => {
                last = e;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let graph = match shared
            .catalog
            .resolve(
                shared.store.as_ref(),
                &snapshot.graph_id,
                snapshot.graph_version,
            )
            .await
        {
            Ok(g) => g,
            Err(EngineError::Store(e)) if transient(&e) => {
                last = e;
                continue;
            }
            Err(e) => return Err(e),
        };
        let commit = plan(&graph, &snapshot, shared.clock.now())?;
        match shared.store.apply(commit, None).await {
            Ok(_) => {
                shared.wake.notify_waiters();
                return Ok(());
            }
            Err(e) if retryable(&e) => {
                if matches!(e, StoreError::Backend(_)) {
                    *ambiguous = Some(e.clone());
                }
                last = e;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Err(EngineError::Contention {
        attempts: shared.config.max_replan_attempts.saturating_add(1),
        last,
    })
}
