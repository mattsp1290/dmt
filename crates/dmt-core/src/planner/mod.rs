mod edges;
mod machine;
mod outcomes;
mod parking;
mod schedule;
mod signals;
use crate::{
    Commit, CommitInvariant, Graph, JoinId, LifecycleError, LifecycleEvent, Micros, NodeId,
    NodeKind, NodeOutcome, RejectedTransition, RunId, RunSnapshot, RunStatus, SignalId,
    SignalPayload, TaskEvent, TaskId, TaskStatus, TaskUpdate,
};
use machine::Planning;
use serde_json::Value;
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("invalid run id: {run_id}")]
    InvalidRunId { run_id: RunId },
    #[error("graph hash mismatch: expected {expected}, actual {actual}")]
    GraphMismatch { expected: String, actual: String },
    #[error("run is terminal: {status}")]
    RunTerminal { status: RunStatus },
    #[error("corrupt run: {0}")]
    CorruptRun(#[from] LifecycleError),
    #[error("invalid lifecycle transition: {0:?}")]
    InvalidTransition(RejectedTransition),
    #[error("unknown task {task_id}")]
    UnknownTask { task_id: TaskId },
    #[error("unknown signal {signal_id}")]
    UnknownSignal { signal_id: SignalId },
    #[error("unknown node {node_id}")]
    UnknownNode { node_id: NodeId },
    #[error("unknown join {join_id}")]
    UnknownJoin { join_id: JoinId },
    #[error("task {task_id} is {status}, not running")]
    TaskNotRunning { task_id: TaskId, status: TaskStatus },
    #[error("task {task_id} is {status}, not exhausted")]
    TaskNotExhausted { task_id: TaskId, status: TaskStatus },
    #[error("task {task_id} is {status}, not awaiting")]
    TaskNotAwaiting { task_id: TaskId, status: TaskStatus },
    #[error("task {task_id} already planned")]
    AlreadyPlanned { task_id: TaskId },
    #[error("signal {signal_id} resolved")]
    SignalResolved { signal_id: SignalId },
    #[error("signal {signal_id} has no deadline")]
    NoDeadline { signal_id: SignalId },
    #[error("node {node_id} of kind {kind:?} cannot accept {outcome}")]
    KindMismatch {
        node_id: NodeId,
        kind: NodeKind,
        outcome: &'static str,
    },
    #[error("empty fan-out at {node_id}")]
    EmptyFanOut { node_id: NodeId },
    #[error("invalid commit: {0}")]
    Invalid(#[from] CommitInvariant),
}
/// Start a new run with deterministic task ids and host-supplied time.
/// # Errors
/// Rejects invalid run ids or internal commit inconsistencies.
pub fn plan_start(
    graph: &Graph,
    run_id: RunId,
    input: Value,
    now: Micros,
) -> Result<Commit, PlanError> {
    if !run_id.is_valid() {
        return Err(PlanError::InvalidRunId { run_id });
    }
    let mut ctx = Planning::start(graph, run_id, input.clone(), now)?;
    ctx.schedule_node(graph.start(), input)?;
    ctx.finish()
}
/// Plan a claimed handler's result.
/// # Errors
/// Rejects stale, corrupt, terminal, or mismatched inputs.
pub fn plan_outcome(
    graph: &Graph,
    snapshot: &RunSnapshot,
    task_id: &TaskId,
    outcome: NodeOutcome,
    now: Micros,
) -> Result<Commit, PlanError> {
    let mut ctx = Planning::load(graph, snapshot, now)?;
    let task = snapshot
        .task(task_id)
        .ok_or_else(|| PlanError::UnknownTask {
            task_id: task_id.clone(),
        })?;
    if task.status != TaskStatus::Running {
        return Err(PlanError::TaskNotRunning {
            task_id: task_id.clone(),
            status: task.status,
        });
    }
    ctx.outcome(task, outcome)?;
    ctx.finish()
}
/// Resolve an open signal, resuming a parked run first.
/// # Errors
/// Rejects stale, corrupt, terminal, or mismatched inputs.
pub fn plan_signal(
    graph: &Graph,
    snapshot: &RunSnapshot,
    signal_id: &SignalId,
    payload: SignalPayload,
    now: Micros,
) -> Result<Commit, PlanError> {
    let mut ctx = Planning::load(graph, snapshot, now)?;
    ctx.resolve_signal(signal_id, payload, false)?;
    ctx.finish()
}
/// Resolve a signal selected by the store's due-signals query.
/// # Errors
/// Rejects signals without deadlines and stale, corrupt, or terminal inputs.
pub fn plan_timeout(
    graph: &Graph,
    snapshot: &RunSnapshot,
    signal_id: &SignalId,
    now: Micros,
) -> Result<Commit, PlanError> {
    let mut ctx = Planning::load(graph, snapshot, now)?;
    ctx.resolve_signal(
        signal_id,
        SignalPayload {
            label: "timeout".into(),
            payload: Value::Null,
        },
        true,
    )?;
    ctx.finish()
}
/// Route a task exhausted by store-owned lease reclamation.
/// # Errors
/// Rejects tasks already planned, unknown tasks, and stale or terminal inputs.
pub fn plan_exhausted(
    graph: &Graph,
    snapshot: &RunSnapshot,
    task_id: &TaskId,
    now: Micros,
) -> Result<Commit, PlanError> {
    let mut ctx = Planning::load(graph, snapshot, now)?;
    let task = snapshot
        .task(task_id)
        .ok_or_else(|| PlanError::UnknownTask {
            task_id: task_id.clone(),
        })?;
    if task.status != TaskStatus::Exhausted {
        return Err(PlanError::TaskNotExhausted {
            task_id: task_id.clone(),
            status: task.status,
        });
    }
    if task.planned_at.is_some() {
        return Err(PlanError::AlreadyPlanned {
            task_id: task_id.clone(),
        });
    }
    ctx.commit.task_updates.push(TaskUpdate {
        task_id: task.task_id.clone(),
        status: task.status,
        attempt: task.attempt,
        run_at: task.run_at,
        outcome: task.outcome.clone(),
        planned_at: Some(now),
    });
    ctx.after_exhaustion(task, "lease reclaims exceeded".into())?;
    ctx.finish()
}
/// Cancel a run and its ready or awaiting tasks; running tasks finish in the store.
/// # Errors
/// Rejects stale, corrupt, terminal, or mismatched inputs.
pub fn plan_cancel(
    graph: &Graph,
    snapshot: &RunSnapshot,
    reason: String,
    now: Micros,
) -> Result<Commit, PlanError> {
    let mut ctx = Planning::load(graph, snapshot, now)?;
    ctx.feed(LifecycleEvent::Cancel { reason })?;
    for task in &snapshot.tasks {
        if matches!(task.status, TaskStatus::Ready | TaskStatus::Awaiting) {
            ctx.commit.task_updates.push(TaskUpdate {
                task_id: task.task_id.clone(),
                status: task
                    .status
                    .next(TaskEvent::Cancel)
                    .map_err(|e| PlanError::Invalid(CommitInvariant(e.to_string())))?,
                attempt: task.attempt,
                run_at: task.run_at,
                outcome: task.outcome.clone(),
                planned_at: task.planned_at,
            });
        }
    }
    for signal in &snapshot.signals {
        if signal.resolved_at.is_none() {
            ctx.commit.resolved_signals.push(crate::SignalResolution {
                signal_id: signal.signal_id.clone(),
                label: "cancelled".into(),
                payload: Value::Null,
            });
        }
    }
    ctx.finish()
}
