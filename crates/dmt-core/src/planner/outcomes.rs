use super::{
    PlanError,
    edges::{select_edge, select_failed_edge},
    machine::Planning,
};
use crate::{
    BranchResult, ExhaustReason, LifecycleEvent, NodeKind, NodeOutcome, Outcome, RunEvent,
    TaskEvent, TaskRecord, TaskStatus, TaskUpdate,
};
impl Planning<'_> {
    pub(super) fn complete_task(
        &mut self,
        task: &TaskRecord,
        stored: NodeOutcome,
        outcome: Outcome,
    ) {
        self.commit.task_updates.push(TaskUpdate {
            task_id: task.task_id.clone(),
            status: TaskStatus::Completed,
            attempt: task.attempt,
            run_at: task.run_at,
            outcome: Some(stored),
            planned_at: task.planned_at,
        });
        self.commit.events.push(RunEvent::TaskCompleted {
            task_id: task.task_id.clone(),
            attempt: task.attempt,
            outcome,
        });
    }
    pub(super) fn follow(&mut self, task: &TaskRecord, outcome: Outcome) -> Result<(), PlanError> {
        if let Some(edge) = select_edge(self.graph, &task.node_id, &outcome.label) {
            self.schedule_node(&edge.to, outcome.payload)
        } else {
            self.feed(LifecycleEvent::Fail {
                message: format!("no edge from {} for label {}", task.node_id, outcome.label),
            })
        }
    }
    pub(super) fn outcome(
        &mut self,
        task: &TaskRecord,
        outcome: NodeOutcome,
    ) -> Result<(), PlanError> {
        let kind = &self
            .graph
            .node(&task.node_id)
            .ok_or_else(|| PlanError::UnknownNode {
                node_id: task.node_id.clone(),
            })?
            .kind;
        match (kind, outcome) {
            (NodeKind::Task | NodeKind::Join { .. }, NodeOutcome::Done(outcome)) => {
                self.complete_task(task, NodeOutcome::Done(outcome.clone()), outcome.clone());
                self.follow(task, outcome)
            }
            (NodeKind::Branch, NodeOutcome::Done(outcome)) => {
                let index = task
                    .branch
                    .as_ref()
                    .ok_or_else(|| {
                        PlanError::Invalid(crate::CommitInvariant(
                            "branch missing reference".into(),
                        ))
                    })?
                    .index;
                self.complete_task(task, NodeOutcome::Done(outcome.clone()), outcome.clone());
                self.contribute(task, BranchResult::Done { index, outcome })
            }
            (NodeKind::FanOut { branch, join }, NodeOutcome::FanOut(values)) => {
                self.fan_out(task, branch, join, values)
            }
            (
                NodeKind::Task | NodeKind::Branch | NodeKind::Join { .. } | NodeKind::FanOut { .. },
                NodeOutcome::Fail { message, retryable },
            ) => self.failure(task, message, retryable),
            (kind, outcome) => Err(PlanError::KindMismatch {
                node_id: task.node_id.clone(),
                kind: kind.clone(),
                outcome: match outcome {
                    NodeOutcome::Done(_) => "Done",
                    NodeOutcome::FanOut(_) => "FanOut",
                    NodeOutcome::Fail { .. } => "Fail",
                },
            }),
        }
    }
    fn failure(
        &mut self,
        task: &TaskRecord,
        message: String,
        retryable: bool,
    ) -> Result<(), PlanError> {
        self.commit.events.push(RunEvent::TaskFailed {
            task_id: task.task_id.clone(),
            attempt: task.attempt,
            message: message.clone(),
            retryable,
        });
        let retry = retryable && task.attempt < task.max_attempts;
        let event = if retry {
            TaskEvent::FailRetry
        } else {
            TaskEvent::FailFinal
        };
        let status = task.status.next(event).expect("running failure transition");
        let attempt = task.attempt + u32::from(retry);
        let run_at = if retry {
            self.commit.now.saturating_add(
                self.graph
                    .node(&task.node_id)
                    .ok_or_else(|| PlanError::UnknownNode {
                        node_id: task.node_id.clone(),
                    })?
                    .retry
                    .backoff(task.attempt),
            )
        } else {
            task.run_at
        };
        self.commit.task_updates.push(TaskUpdate {
            task_id: task.task_id.clone(),
            status,
            attempt,
            run_at,
            outcome: Some(NodeOutcome::Fail {
                message: message.clone(),
                retryable,
            }),
            planned_at: if retry { None } else { Some(self.commit.now) },
        });
        if retry {
            self.commit.events.push(RunEvent::TaskRetryScheduled {
                task_id: task.task_id.clone(),
                attempt,
                run_at,
            });
            Ok(())
        } else {
            self.commit.events.push(RunEvent::TaskExhausted {
                task_id: task.task_id.clone(),
                reason: ExhaustReason::HandlerFailures,
            });
            self.after_exhaustion(task, message)
        }
    }
    pub(super) fn after_exhaustion(
        &mut self,
        task: &TaskRecord,
        message: String,
    ) -> Result<(), PlanError> {
        let kind = &self
            .graph
            .node(&task.node_id)
            .ok_or_else(|| PlanError::UnknownNode {
                node_id: task.node_id.clone(),
            })?
            .kind;
        match kind {
            NodeKind::Branch => {
                let index = task
                    .branch
                    .as_ref()
                    .ok_or_else(|| {
                        PlanError::Invalid(crate::CommitInvariant(
                            "branch missing reference".into(),
                        ))
                    })?
                    .index;
                self.contribute(task, BranchResult::Failed { index, message })
            }
            NodeKind::Task | NodeKind::Join { .. } => {
                if let Some(edge) = select_failed_edge(self.graph, &task.node_id) {
                    self.schedule_node(&edge.to, serde_json::json!({"message":message}))
                } else {
                    self.feed(LifecycleEvent::Fail { message })
                }
            }
            NodeKind::FanOut { .. } => self.feed(LifecycleEvent::Fail { message }),
            _ => Err(PlanError::KindMismatch {
                node_id: task.node_id.clone(),
                kind: kind.clone(),
                outcome: "exhausted",
            }),
        }
    }
}
