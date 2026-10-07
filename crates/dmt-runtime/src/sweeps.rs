use crate::{
    EngineError,
    commit_loop::{self, Operation},
    shared::Shared,
};
use dmt_core::PlanError;
use dmt_store::StoreError;
use std::sync::Arc;
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
                    classify(
                        commit_loop::unowned(
                            &shared,
                            &signal.run_id,
                            Operation::Timeout(&signal.signal_id),
                        )
                        .await
                        .result,
                        true,
                    );
                }
            }
            Err(error) => tracing::warn!(%error, "due_signals failed"),
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
                    classify(
                        commit_loop::unowned(
                            &shared,
                            &task.run_id,
                            Operation::Exhausted(&task.task_id),
                        )
                        .await
                        .result,
                        false,
                    );
                }
            }
            Err(error) => tracing::warn!(%error, "exhausted_tasks failed"),
        }
    }
}
fn classify(result: Result<(), EngineError>, timeout: bool) {
    let Err(error) = result else {
        return;
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
        tracing::debug!(%error, "stale sweep dropped");
    } else if matches!(error, EngineError::Contention { .. }) {
        tracing::warn!(%error, "sweep contention");
    } else {
        tracing::error!(%error, "sweep failed");
    }
}
