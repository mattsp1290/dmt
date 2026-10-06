use crate::{
    CancellationToken, NodeContext, NodeOutcomeResult, commit_loop,
    handler::node_input,
    shared::{InFlightGuard, Shared},
};
use dmt_core::{NodeKind, NodeOutcome};
use dmt_store::{ClaimedTask, HeartbeatResult, StoreError};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::Instrument;

pub(crate) async fn run(
    shared: Arc<Shared>,
    claimed: ClaimedTask,
    cancel: CancellationToken,
    guard: InFlightGuard,
) {
    let span = tracing::info_span!("dispatch", run_id = %claimed.task.run_id, node_id = %claimed.task.node_id, step_key = %claimed.task.step_key, attempt = claimed.task.attempt);
    async {
        let _guard = guard;
        let mut heartbeat = interval_at(Instant::now() + shared.config.heartbeat_every, shared.config.heartbeat_every);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let work = run_task(&shared, &claimed, &cancel);
        tokio::pin!(work);
        let mut lease_lost = false;
        loop {
            tokio::select! {
                () = &mut work => break,
                _ = heartbeat.tick(), if !lease_lost => {
                    match shared.store.heartbeat(&claimed.proof, shared.clock.now(), shared.config.lease_micros()).await {
                        Ok(HeartbeatResult::Extended { .. }) => {},
                        Ok(HeartbeatResult::Lost) | Err(StoreError::NotFound(_)) => {
                            tracing::warn!("lease lost"); cancel.cancel(); lease_lost = true;
                        }
                        Err(error) => tracing::warn!(%error, "heartbeat failed"),
                    }
                }
            }
        }
    }.instrument(span).await;
}
async fn run_task(shared: &Shared, claimed: &ClaimedTask, cancel: &CancellationToken) {
    let graph = match commit_loop::resolve(shared, &claimed.graph_id, claimed.graph_version).await {
        Ok(graph) => graph,
        Err(error) => {
            tracing::error!(%error, "dispatch graph unavailable");
            return;
        }
    };
    let Some(node) = graph.node(&claimed.task.node_id) else {
        tracing::error!("dispatch node unavailable");
        return;
    };
    if matches!(node.kind, NodeKind::Wait { .. } | NodeKind::End { .. }) {
        tracing::error!("node kind is never dispatched");
        return;
    }
    let outcome = if let Some(handler) = shared.handlers.get(&claimed.task.node_id) {
        match node_input(&node.kind, &claimed.task) {
            Ok(input) => {
                let ctx = NodeContext {
                    run_id: claimed.task.run_id.clone(),
                    graph_id: claimed.graph_id.clone(),
                    graph_version: claimed.graph_version,
                    node_id: claimed.task.node_id.clone(),
                    task_id: claimed.task.task_id.clone(),
                    step_key: claimed.task.step_key.clone(),
                    attempt: claimed.task.attempt,
                    cancel: cancel.clone(),
                };
                let handler = handler.clone();
                let mut task = shared.spawn(
                    async move { handler.run(ctx, input).await }
                        .instrument(tracing::Span::current()),
                );
                let timeout = node
                    .timeout_micros
                    .map(|us| Duration::from_micros(us.unsigned_abs()))
                    .or(shared.config.handler_timeout);
                let timeout_future = async {
                    if let Some(duration) = timeout {
                        tokio::time::sleep(duration).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                tokio::select! {
                    result = &mut task => match handler_result(result) { Some(outcome) => outcome, None => return },
                    () = timeout_future => {
                        cancel.cancel(); task.abort(); let _ = task.await;
                        NodeOutcome::Fail { message: format!("handler timed out after {timeout:?}"), retryable: true }
                    }
                }
            }
            Err(message) => NodeOutcome::Fail {
                message,
                retryable: false,
            },
        }
    } else {
        NodeOutcome::Fail {
            message: format!(
                "no handler registered for node {} of graph {}@{}",
                claimed.task.node_id, claimed.graph_id, claimed.graph_version
            ),
            retryable: false,
        }
    };
    commit_loop::outcome(shared, &graph, claimed, outcome).await;
}
fn handler_result(
    result: Result<Option<NodeOutcomeResult>, tokio::task::JoinError>,
) -> Option<NodeOutcome> {
    match result {
        Ok(Some(Ok(outcome))) => Some(outcome),
        Ok(Some(Err(error))) => Some(error.into()),
        Err(error) if error.is_panic() => {
            tracing::error!("handler panicked");
            Some(NodeOutcome::Fail {
                message: "handler panicked".into(),
                retryable: true,
            })
        }
        _ => None,
    }
}
