use crate::telemetry::{self, names};
use crate::{
    EngineError,
    commit_loop::{self, Operation},
    shared::Shared,
};
use dmt_core::{PlanError, RunId};
use dmt_store::StoreError;
use std::sync::Arc;
use tracing::Instrument;
const SWEEP_BATCH: usize = 64;
pub(crate) async fn run(shared: Arc<Shared>) {
    loop {
        tokio::select! { () = shared.stop.cancelled() => break, () = tokio::time::sleep(shared.config.sweep_interval) => {} }
        match shared
            .store
            .due_signals(shared.clock.now(), SWEEP_BATCH)
            .await
        {
            Ok(signals) => {
                for signal in signals {
                    if shared.stop.is_cancelled() {
                        return;
                    }
                    process(
                        &shared,
                        &signal.run_id,
                        Operation::Timeout(&signal.signal_id),
                    )
                    .await;
                }
            }
            Err(error) => {
                tracing::warn!(error = %telemetry::truncate_message(&error.to_string()), "due_signals failed");
            }
        }
        if shared.stop.is_cancelled() {
            break;
        }
        match shared.store.exhausted_tasks(SWEEP_BATCH).await {
            Ok(tasks) => {
                for task in tasks {
                    if shared.stop.is_cancelled() {
                        return;
                    }
                    process(&shared, &task.run_id, Operation::Exhausted(&task.task_id)).await;
                }
            }
            Err(error) => {
                tracing::warn!(error = %telemetry::truncate_message(&error.to_string()), "exhausted_tasks failed");
            }
        }
    }
}
async fn process(shared: &Shared, run_id: &RunId, operation: Operation<'_>) {
    let kind = operation.kind();
    let timeout = matches!(operation, Operation::Timeout(_));
    let span = tracing::info_span!("sweep", %run_id, kind, "operation.name" = "dmt.sweep", "resource.name" = kind, result = tracing::field::Empty);
    telemetry::run_scope(&span, run_id);
    async {
        let result = classify(
            commit_loop::unowned(shared, run_id, operation).await.result,
            timeout,
        );
        tracing::Span::current().record("result", result);
        metrics::counter!(names::SWEEPS, "kind" => kind, "result" => result).increment(1);
    }
    .instrument(span)
    .await;
}
fn classify(result: Result<(), EngineError>, timeout: bool) -> &'static str {
    let Err(error) = result else {
        return "applied";
    };
    let benign = match &error {
        EngineError::RunNotFound { .. }
        | EngineError::Store(StoreError::RunTerminal { .. })
        | EngineError::Plan(PlanError::RunTerminal { .. }) => true,
        EngineError::Plan(
            PlanError::UnknownSignal { .. }
            | PlanError::SignalResolved { .. }
            | PlanError::TaskNotAwaiting { .. },
        )
        | EngineError::Store(StoreError::AlreadyResolved { .. }) => timeout,
        EngineError::Plan(
            PlanError::UnknownTask { .. }
            | PlanError::AlreadyPlanned { .. }
            | PlanError::TaskNotExhausted { .. },
        ) => !timeout,
        _ => false,
    };
    if benign {
        tracing::debug!(error = %telemetry::truncate_message(&error.to_string()), "stale sweep dropped");
        "stale"
    } else if matches!(error, EngineError::Contention { .. }) {
        tracing::warn!(error = %telemetry::truncate_message(&error.to_string()), "sweep contention");
        "contention"
    } else {
        tracing::error!(error = %telemetry::truncate_message(&error.to_string()), "sweep failed");
        "failed"
    }
}
