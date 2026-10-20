use crate::telemetry::{self, names};
use crate::{EngineError, backoff::replan_delay, shared::Shared};
use dmt_core::{Commit, Graph, Micros, NodeOutcome, PlanError, RunSnapshot, TaskId, plan_outcome};
use dmt_store::{ClaimedTask, StoreError};
use tokio::time::sleep;
use tracing::Instrument;

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
) -> &'static str {
    let span = tracing::info_span!("commit", run_id = %claimed.task.run_id, kind = "outcome", "operation.name" = "dmt.commit", "resource.name" = "outcome", attempts = tracing::field::Empty, result = tracing::field::Empty);
    async {
        let start = std::time::Instant::now();
        let result = outcome_loop(shared, graph, claimed, outcome).await;
        tracing::Span::current().record("result", result);
        metrics::histogram!(names::COMMIT_DURATION, "kind" => "outcome", "result" => result)
            .record(start.elapsed().as_secs_f64());
        result
    }
    .instrument(span)
    .await
}
async fn outcome_loop(
    shared: &Shared,
    graph: &Graph,
    claimed: &ClaimedTask,
    outcome: NodeOutcome,
) -> &'static str {
    for retry in 0..=shared.config.max_replan_attempts {
        tracing::Span::current().record("attempts", retry.saturating_add(1));
        delay(shared, retry).await;
        let snapshot = match shared.store.load_run(&claimed.task.run_id).await {
            Ok(Some(s)) => s,
            Err(e) if transient(&e) => {
                attempt("outcome", store_attempt(&e));
                continue;
            }
            other => {
                tracing::error!(result = ?other.as_ref().map(Option::is_some), "run unavailable");
                attempt("outcome", "rejected");
                return "rejected";
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
                attempt("outcome", "dropped");
                return "dropped";
            }
            Err(error) => {
                tracing::error!(error = %telemetry::truncate_message(&error.to_string()), "outcome cannot be planned");
                attempt("outcome", "plan_error");
                return "rejected";
            }
        };
        #[cfg(feature = "test-faults")]
        crate::faults::before_apply(
            shared.config.fault.as_ref(),
            &claimed.task.node_id,
            claimed.proof.attempt,
        );
        let kinds = telemetry::event_kinds(&commit);
        match shared
            .store
            .apply(commit, Some(claimed.proof.clone()))
            .await
        {
            Ok(_) => {
                telemetry::count_events(&kinds);
                attempt("outcome", "applied");
                shared.wake.notify_waiters();
                return "applied";
            }
            Err(e) if retryable(&e) => {
                attempt("outcome", store_attempt(&e));
                tracing::debug!(error = %telemetry::truncate_message(&e.to_string()), retry, "commit conflict");
            }
            Err(StoreError::LeaseLost { .. }) => {
                tracing::warn!("lease lost");
                metrics::counter!(names::LEASE_LOST, "graph_id" => claimed.graph_id.to_string(), "node_id" => claimed.task.node_id.to_string(), "source" => "commit").increment(1);
                attempt("outcome", "lease_lost");
                return "lease_lost";
            }
            Err(StoreError::RunTerminal { .. }) => {
                tracing::debug!("run terminal");
                attempt("outcome", "terminal");
                return "terminal";
            }
            Err(error) => {
                tracing::error!(error = %telemetry::truncate_message(&error.to_string()), "commit rejected");
                attempt("outcome", "rejected");
                return "rejected";
            }
        }
    }
    tracing::error!("replan attempts exhausted");
    "exhausted"
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

pub(crate) struct Unowned {
    pub result: Result<(), EngineError>,
    pub ambiguous: Option<StoreError>,
}
pub(crate) enum Operation<'a> {
    Signal {
        name: &'a str,
        payload: &'a dmt_core::SignalPayload,
    },
    Cancel(&'a str),
    Timeout(&'a dmt_core::SignalId),
    Exhausted(&'a TaskId),
}
impl Operation<'_> {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Signal { .. } => "signal",
            Self::Cancel(_) => "cancel",
            Self::Timeout(_) => "timeout",
            Self::Exhausted(_) => "exhausted",
        }
    }
    async fn plan(
        &self,
        shared: &Shared,
        graph: &Graph,
        snapshot: &RunSnapshot,
        selected_signal: &mut Option<dmt_core::SignalId>,
    ) -> Result<Commit, EngineError> {
        Ok(match self {
            Self::Signal { name, payload } => {
                let signal = shared
                    .store
                    .find_open_signal(&snapshot.run_id, name)
                    .await?
                    .ok_or_else(|| EngineError::SignalNotFound {
                        run_id: snapshot.run_id.clone(),
                        name: (*name).into(),
                    })?;
                let selected = selected_signal.get_or_insert_with(|| signal.signal_id.clone());
                if selected != &signal.signal_id {
                    return Err(EngineError::SignalNotFound {
                        run_id: snapshot.run_id.clone(),
                        name: (*name).into(),
                    });
                }
                dmt_core::plan_signal(
                    graph,
                    snapshot,
                    selected,
                    (*payload).clone(),
                    shared.clock.now(),
                )?
            }
            Self::Cancel(reason) => {
                dmt_core::plan_cancel(graph, snapshot, (*reason).into(), shared.clock.now())?
            }
            Self::Timeout(signal) => {
                dmt_core::plan_timeout(graph, snapshot, signal, shared.clock.now())?
            }
            Self::Exhausted(task) => {
                dmt_core::plan_exhausted(graph, snapshot, task, shared.clock.now())?
            }
        })
    }
}
pub(crate) async fn unowned(
    shared: &Shared,
    run_id: &dmt_core::RunId,
    operation: Operation<'_>,
) -> Unowned {
    let mut ambiguous = None;
    let kind = operation.kind();
    let span = tracing::info_span!("commit", %run_id, kind, "operation.name" = "dmt.commit", "resource.name" = kind, attempts = tracing::field::Empty, result = tracing::field::Empty);
    let result = async {
        let start = std::time::Instant::now();
        let result = unowned_loop(shared, run_id, &operation, &mut ambiguous).await;
        let label = match &result {
            Ok(()) => "applied",
            Err(EngineError::RunNotFound { .. }) => "run_not_found",
            Err(
                EngineError::RunTerminal { .. }
                | EngineError::Store(StoreError::RunTerminal { .. })
                | EngineError::Plan(PlanError::RunTerminal { .. }),
            ) => "terminal",
            Err(EngineError::Contention { .. }) => "exhausted",
            Err(_) => "error",
        };
        tracing::Span::current().record("result", label);
        metrics::histogram!(names::COMMIT_DURATION, "kind" => kind, "result" => label)
            .record(start.elapsed().as_secs_f64());
        result
    }
    .instrument(span)
    .await;
    Unowned { result, ambiguous }
}
async fn unowned_loop(
    shared: &Shared,
    run_id: &dmt_core::RunId,
    operation: &Operation<'_>,
    ambiguous: &mut Option<StoreError>,
) -> Result<(), EngineError> {
    let kind = operation.kind();
    let mut last = StoreError::Busy;
    let mut selected_signal = None;
    for retry in 0..=shared.config.max_replan_attempts {
        tracing::Span::current().record("attempts", retry.saturating_add(1));
        delay(shared, retry).await;
        let snapshot = match shared.store.load_run(run_id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                attempt(kind, "dropped");
                return Err(EngineError::RunNotFound {
                    run_id: run_id.clone(),
                });
            }
            Err(e) if transient(&e) => {
                attempt(kind, store_attempt(&e));
                last = e;
                continue;
            }
            Err(e) => {
                attempt(kind, store_attempt(&e));
                return Err(e.into());
            }
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
                attempt(kind, store_attempt(&e));
                last = e;
                continue;
            }
            Err(e) => {
                attempt(kind, "plan_error");
                return Err(e);
            }
        };
        let commit = match operation
            .plan(shared, &graph, &snapshot, &mut selected_signal)
            .await
        {
            Ok(c) => c,
            Err(EngineError::Store(e)) if transient(&e) => {
                attempt(kind, store_attempt(&e));
                last = e;
                continue;
            }
            Err(e) => {
                attempt(kind, "plan_error");
                return Err(e);
            }
        };
        let kinds = telemetry::event_kinds(&commit);
        match shared.store.apply(commit, None).await {
            Ok(_) => {
                telemetry::count_events(&kinds);
                attempt(kind, "applied");
                shared.wake.notify_waiters();
                return Ok(());
            }
            Err(e) if retryable(&e) => {
                if matches!(e, StoreError::Backend(_)) {
                    *ambiguous = Some(e.clone());
                }
                attempt(kind, store_attempt(&e));
                last = e;
            }
            Err(e) => {
                attempt(kind, store_attempt(&e));
                return Err(e.into());
            }
        }
    }
    Err(EngineError::Contention {
        attempts: shared.config.max_replan_attempts.saturating_add(1),
        last,
    })
}

fn attempt(kind: &'static str, result: &'static str) {
    metrics::counter!(names::COMMIT_ATTEMPTS, "kind" => kind, "result" => result).increment(1);
}
fn store_attempt(error: &StoreError) -> &'static str {
    match error {
        StoreError::VersionConflict { .. } | StoreError::JoinDrift { .. } => "conflict",
        StoreError::Busy => "busy",
        StoreError::Backend(_) => "backend",
        StoreError::LeaseLost { .. } => "lease_lost",
        StoreError::RunTerminal { .. } => "terminal",
        _ => "rejected",
    }
}
