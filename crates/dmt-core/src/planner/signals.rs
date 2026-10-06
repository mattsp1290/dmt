use super::{PlanError, machine::Planning};
use crate::{
    LifecycleEvent, NodeOutcome, Outcome, RunEvent, RunStatus, SignalId, SignalPayload,
    SignalResolution, TaskStatus,
};
impl Planning<'_> {
    pub(super) fn resolve_signal(
        &mut self,
        id: &SignalId,
        payload: SignalPayload,
        timeout: bool,
    ) -> Result<(), PlanError> {
        let signal =
            self.snapshot
                .and_then(|s| s.signal(id))
                .ok_or_else(|| PlanError::UnknownSignal {
                    signal_id: id.clone(),
                })?;
        if signal.resolved_at.is_some() {
            return Err(PlanError::SignalResolved {
                signal_id: id.clone(),
            });
        }
        if timeout && signal.deadline_at.is_none() {
            return Err(PlanError::NoDeadline {
                signal_id: id.clone(),
            });
        }
        let task = self
            .snapshot
            .and_then(|s| s.task(&signal.task_id))
            .ok_or_else(|| PlanError::UnknownTask {
                task_id: signal.task_id.clone(),
            })?;
        if task.status != TaskStatus::Awaiting {
            return Err(PlanError::TaskNotAwaiting {
                task_id: task.task_id.clone(),
                status: task.status,
            });
        }
        if self.status() == RunStatus::Parked {
            self.feed(LifecycleEvent::Resume)?;
        }
        self.commit.events.push(if timeout {
            RunEvent::SignalTimedOut {
                signal_id: id.clone(),
                signal_key: signal.key.clone(),
            }
        } else {
            RunEvent::SignalReceived {
                signal_id: id.clone(),
                signal_key: signal.key.clone(),
                label: payload.label.clone(),
            }
        });
        self.commit.resolved_signals.push(SignalResolution {
            signal_id: id.clone(),
            label: payload.label.clone(),
            payload: payload.payload.clone(),
        });
        let outcome = Outcome::with_payload(payload.label, payload.payload);
        self.complete_task(task, NodeOutcome::Done(outcome.clone()), outcome.clone());
        self.follow(task, outcome)
    }
}
