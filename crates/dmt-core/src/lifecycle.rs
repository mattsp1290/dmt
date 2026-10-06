use crate::RunEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use statig::blocking::{IntoStateMachineExt, Outcome, UninitializedStateMachine};
use statig::state_machine;

/// Persisted run status, matching the machine's leaf states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Created,
    Active,
    Parked,
    Completed,
    Failed,
    Cancelled,
}
impl RunStatus {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}
impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Created => "created",
            Self::Active => "active",
            Self::Parked => "parked",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        })
    }
}
/// Inputs accepted by the lifecycle machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    Start {
        graph_id: String,
        graph_version: u32,
        definition_hash: String,
        input: Value,
    },
    Park,
    Resume,
    Complete {
        output: Value,
    },
    Fail {
        message: String,
    },
    Cancel {
        reason: String,
    },
}
impl LifecycleEvent {
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Start { .. } => "Start",
            Self::Park => "Park",
            Self::Resume => "Resume",
            Self::Complete { .. } => "Complete",
            Self::Fail { .. } => "Fail",
            Self::Cancel { .. } => "Cancel",
        }
    }
}
/// Effects collected while handling events.
#[derive(Debug, Default)]
pub struct Effects {
    pub emitted: Vec<RunEvent>,
    pub rejected: Option<RejectedTransition>,
}
/// An event rejected without changing the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedTransition {
    pub status: RunStatus,
    pub event: &'static str,
}
/// Stateless lifecycle storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunLifecycle;
#[state_machine(
    initial = "State::created()",
    state(derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)),
    superstate(derive(Debug))
)]
impl RunLifecycle {
    #[state]
    fn created(context: &mut Effects, event: &LifecycleEvent) -> Outcome<State> {
        if let LifecycleEvent::Start {
            graph_id,
            graph_version,
            definition_hash,
            input,
        } = event
        {
            context.emitted.push(RunEvent::RunStarted {
                graph_id: graph_id.clone(),
                graph_version: *graph_version,
                definition_hash: definition_hash.clone(),
                input: input.clone(),
            });
            Outcome::Transition(State::active())
        } else {
            reject(context, event, RunStatus::Created)
        }
    }
    #[state(superstate = "running")]
    fn active(context: &mut Effects, event: &LifecycleEvent) -> Outcome<State> {
        if matches!(event, LifecycleEvent::Park) {
            context.emitted.push(RunEvent::RunParked);
            Outcome::Transition(State::parked())
        } else if matches!(
            event,
            LifecycleEvent::Complete { .. }
                | LifecycleEvent::Fail { .. }
                | LifecycleEvent::Cancel { .. }
        ) {
            Outcome::Super
        } else {
            reject(context, event, RunStatus::Active)
        }
    }
    #[state(superstate = "running")]
    fn parked(context: &mut Effects, event: &LifecycleEvent) -> Outcome<State> {
        if matches!(event, LifecycleEvent::Resume) {
            context.emitted.push(RunEvent::RunResumed);
            Outcome::Transition(State::active())
        } else if matches!(
            event,
            LifecycleEvent::Complete { .. }
                | LifecycleEvent::Fail { .. }
                | LifecycleEvent::Cancel { .. }
        ) {
            Outcome::Super
        } else {
            reject(context, event, RunStatus::Parked)
        }
    }
    #[superstate]
    fn running(context: &mut Effects, event: &LifecycleEvent) -> Outcome<State> {
        match event {
            LifecycleEvent::Complete { output } => {
                context.emitted.push(RunEvent::RunCompleted {
                    output: output.clone(),
                });
                Outcome::Transition(State::completed())
            }
            LifecycleEvent::Fail { message } => {
                context.emitted.push(RunEvent::RunFailed {
                    message: message.clone(),
                });
                Outcome::Transition(State::failed())
            }
            LifecycleEvent::Cancel { reason } => {
                context.emitted.push(RunEvent::RunCancelled {
                    reason: reason.clone(),
                });
                Outcome::Transition(State::cancelled())
            }
            _ => Outcome::Handled,
        }
    }
    #[state]
    fn completed(context: &mut Effects, event: &LifecycleEvent) -> Outcome<State> {
        reject(context, event, RunStatus::Completed)
    }
    #[state]
    fn failed(context: &mut Effects, event: &LifecycleEvent) -> Outcome<State> {
        reject(context, event, RunStatus::Failed)
    }
    #[state]
    fn cancelled(context: &mut Effects, event: &LifecycleEvent) -> Outcome<State> {
        reject(context, event, RunStatus::Cancelled)
    }
}
fn reject(context: &mut Effects, event: &LifecycleEvent, status: RunStatus) -> Outcome<State> {
    context.rejected = Some(RejectedTransition {
        status,
        event: event.name(),
    });
    Outcome::Handled
}
impl From<&State> for RunStatus {
    fn from(state: &State) -> Self {
        match state {
            State::Created {} => Self::Created,
            State::Active {} => Self::Active,
            State::Parked {} => Self::Parked,
            State::Completed {} => Self::Completed,
            State::Failed {} => Self::Failed,
            State::Cancelled {} => Self::Cancelled,
        }
    }
}
/// Initialized machine serialized into run records.
pub type LifecycleMachine = statig::blocking::InitializedStateMachine<RunLifecycle>;
#[must_use]
pub fn new_machine(effects: &mut Effects) -> LifecycleMachine {
    RunLifecycle
        .uninitialized_state_machine()
        .init_with_context(effects)
}
/// Restore and verify a persisted machine.
/// # Errors
/// Returns an error for invalid JSON or a status mismatch.
pub fn restore(
    status: RunStatus,
    machine_json: &Value,
    effects: &mut Effects,
) -> Result<LifecycleMachine, LifecycleError> {
    let uninit: UninitializedStateMachine<RunLifecycle> =
        serde_json::from_value(machine_json.clone())
            .map_err(|e| LifecycleError::Deserialize(e.to_string()))?;
    let machine = uninit.init_with_context(effects);
    let actual = RunStatus::from(machine.state());
    if actual != status {
        return Err(LifecycleError::CorruptRun {
            stored: status,
            machine: actual,
        });
    }
    Ok(machine)
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleError {
    #[error("stored run status {stored} disagrees with machine status {machine}")]
    CorruptRun {
        stored: RunStatus,
        machine: RunStatus,
    },
    #[error("invalid lifecycle JSON: {0}")]
    Deserialize(String),
}
