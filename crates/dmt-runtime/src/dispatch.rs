use crate::telemetry::{self, names};
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
    let span = tracing::info_span!("dispatch", run_id = %claimed.task.run_id, graph_id = %claimed.graph_id, graph_version = claimed.graph_version, node_id = %claimed.task.node_id, task_id = %claimed.task.task_id, step_key = %claimed.task.step_key, attempt = claimed.task.attempt, "operation.name" = "dmt.dispatch", "resource.name" = %format!("{}/{}", claimed.graph_id, claimed.task.node_id), outcome = tracing::field::Empty, result = tracing::field::Empty, "otel.status_code" = tracing::field::Empty);
    telemetry::run_scope(&span, &claimed.task.run_id);
    let start = std::time::Instant::now();
    async {
        let _guard = guard;
        let mut heartbeat = interval_at(
            Instant::now() + shared.config.heartbeat_every,
            shared.config.heartbeat_every,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let work = run_task(&shared, &claimed, &cancel);
        tokio::pin!(work);
        // Keep polling timeouts and outcomes while heartbeat I/O is pending.
        let beats = async {
            loop {
                heartbeat.tick().await;
                match shared
                    .store
                    .heartbeat(
                        &claimed.proof,
                        shared.clock.now(),
                        shared.config.lease_micros(),
                    )
                    .await
                {
                    Ok(HeartbeatResult::Extended { .. }) => {}
                    Ok(HeartbeatResult::Lost) | Err(StoreError::NotFound(_)) => {
                        metrics::counter!(names::LEASE_LOST, "graph_id" => claimed.graph_id.to_string(), "node_id" => claimed.task.node_id.to_string(), "source" => "heartbeat").increment(1);
                        tracing::warn!("lease lost");
                        cancel.cancel();
                        return;
                    }
                    Err(error) => tracing::warn!(error = %telemetry::truncate_message(&error.to_string()), "heartbeat failed"),
                }
            }
        };
        let result = tokio::select! {
            result = &mut work => result,
            () = beats => work.await,
        };
        let span = tracing::Span::current();
        span.record("result", result);
        if !matches!(result, "applied" | "dropped") { span.record("otel.status_code", "ERROR"); }
        metrics::histogram!(names::DISPATCH_DURATION, "graph_id" => claimed.graph_id.to_string(), "node_id" => claimed.task.node_id.to_string(), "result" => result).record(start.elapsed().as_secs_f64());
    }
    .instrument(span)
    .await;
}
async fn run_task(
    shared: &Shared,
    claimed: &ClaimedTask,
    cancel: &CancellationToken,
) -> &'static str {
    let graph = match commit_loop::resolve(shared, &claimed.graph_id, claimed.graph_version).await {
        Ok(graph) => graph,
        Err(error) => {
            tracing::error!(error = %telemetry::truncate_message(&error.to_string()), "dispatch graph unavailable");
            return "graph_unavailable";
        }
    };
    let Some(node) = graph.node(&claimed.task.node_id) else {
        tracing::error!("dispatch node unavailable");
        return "node_unavailable";
    };
    if matches!(node.kind, NodeKind::Wait { .. } | NodeKind::End { .. }) {
        tracing::error!("node kind is never dispatched");
        return "node_unavailable";
    }
    let (outcome, class) = if let Some(handler) = shared.handlers.get(&claimed.task.node_id) {
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
                let timeout = node
                    .timeout_micros
                    .map(|us| Duration::from_micros(us.unsigned_abs()))
                    .or(shared.config.handler_timeout);
                run_handler(shared, claimed, handler.clone(), ctx, input, timeout).await
            }
            Err(message) => (
                Some(NodeOutcome::Fail {
                    message,
                    retryable: false,
                }),
                "bad_input",
            ),
        }
    } else {
        (
            Some(NodeOutcome::Fail {
                message: format!(
                    "no handler registered for node {} of graph {}@{}",
                    claimed.task.node_id, claimed.graph_id, claimed.graph_version
                ),
                retryable: false,
            }),
            "no_handler",
        )
    };
    tracing::Span::current().record("outcome", class);
    metrics::counter!(names::DISPATCH_OUTCOMES, "graph_id" => claimed.graph_id.to_string(), "node_id" => claimed.task.node_id.to_string(), "outcome" => class).increment(1);
    match outcome {
        Some(outcome) => commit_loop::outcome(shared, &graph, claimed, outcome).await,
        None => "aborted",
    }
}
async fn run_handler(
    shared: &Shared,
    claimed: &ClaimedTask,
    handler: Arc<dyn crate::NodeHandler>,
    ctx: NodeContext,
    input: crate::NodeInput,
    timeout: Option<Duration>,
) -> (Option<NodeOutcome>, &'static str) {
    let span = tracing::info_span!("handler", run_id = %claimed.task.run_id, graph_id = %claimed.graph_id, node_id = %claimed.task.node_id, attempt = claimed.task.attempt, "operation.name" = "dmt.handler", "resource.name" = %claimed.task.node_id, outcome = tracing::field::Empty, "otel.status_code" = tracing::field::Empty, "error.message" = tracing::field::Empty);
    let start = std::time::Instant::now();
    let cancel = ctx.cancel.clone();
    let mut task =
        shared.spawn(async move { handler.run(ctx, input).await }.instrument(span.clone()));
    let timeout_future = async {
        if let Some(duration) = timeout {
            tokio::time::sleep(duration).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let (outcome, class) = async {
        tokio::select! {
            result = &mut task => handler_result(result),
            () = timeout_future => {
                tracing::warn!(timeout_ms = timeout.map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX)), "handler timed out");
                cancel.cancel(); task.abort(); let _ = task.await;
                (Some(NodeOutcome::Fail { message: format!("handler timed out after {timeout:?}"), retryable: true }), "timeout")
            }
        }
    }.instrument(span.clone()).await;
    span.record("outcome", class);
    if let Some(NodeOutcome::Fail { message, .. }) = &outcome {
        span.record("otel.status_code", "ERROR");
        span.record("error.message", telemetry::truncate_message(message));
    }
    metrics::histogram!(names::HANDLER_DURATION, "graph_id" => claimed.graph_id.to_string(), "node_id" => claimed.task.node_id.to_string(), "outcome" => class).record(start.elapsed().as_secs_f64());
    (outcome, class)
}

fn handler_result(
    result: Result<Option<NodeOutcomeResult>, tokio::task::JoinError>,
) -> (Option<NodeOutcome>, &'static str) {
    match result {
        Ok(Some(Ok(outcome))) => {
            let class = match &outcome {
                NodeOutcome::Done(_) => "done",
                NodeOutcome::FanOut(_) => "fan_out",
                NodeOutcome::Fail {
                    retryable: true, ..
                } => "fail_retryable",
                NodeOutcome::Fail { .. } => "fail_permanent",
            };
            (Some(outcome), class)
        }
        Ok(Some(Err(error))) => {
            let outcome = error.into();
            handler_result(Ok(Some(Ok(outcome))))
        }
        Err(error) if error.is_panic() => {
            tracing::error!("handler panicked");
            (
                Some(NodeOutcome::Fail {
                    message: "handler panicked".into(),
                    retryable: true,
                }),
                "panic",
            )
        }
        _ => (None, "aborted"),
    }
}
