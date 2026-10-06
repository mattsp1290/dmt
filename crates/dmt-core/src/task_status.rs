use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Ready,
    Running,
    Awaiting,
    Completed,
    Exhausted,
    Cancelled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskEvent {
    Claim,
    Reclaim,
    LeaseExhausted,
    RunTerminal,
    Done,
    FailRetry,
    FailFinal,
    Signal,
    Timeout,
    Cancel,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionOwner {
    Store,
    Planner,
}
impl TaskEvent {
    #[must_use]
    pub fn owner(self) -> TransitionOwner {
        match self {
            Self::Claim | Self::Reclaim | Self::LeaseExhausted | Self::RunTerminal => {
                TransitionOwner::Store
            }
            _ => TransitionOwner::Planner,
        }
    }
}
impl TaskStatus {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Exhausted | Self::Cancelled)
    }
    /// Apply a legal task transition.
    /// # Errors
    /// Returns an error for every pair absent from the transition table.
    pub fn next(self, event: TaskEvent) -> Result<Self, InvalidTaskTransition> {
        match (self, event) {
            (Self::Ready, TaskEvent::Claim) | (Self::Running, TaskEvent::Reclaim) => {
                Ok(Self::Running)
            }
            (Self::Ready | Self::Running, TaskEvent::RunTerminal)
            | (Self::Ready | Self::Awaiting, TaskEvent::Cancel) => Ok(Self::Cancelled),
            (Self::Running, TaskEvent::LeaseExhausted | TaskEvent::FailFinal) => {
                Ok(Self::Exhausted)
            }
            (Self::Running, TaskEvent::Done)
            | (Self::Awaiting, TaskEvent::Signal | TaskEvent::Timeout) => Ok(Self::Completed),
            (Self::Running, TaskEvent::FailRetry) => Ok(Self::Ready),
            _ => Err(InvalidTaskTransition { from: self, event }),
        }
    }
}
impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Awaiting => "awaiting",
            Self::Completed => "completed",
            Self::Exhausted => "exhausted",
            Self::Cancelled => "cancelled",
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid task transition from {from} via {event:?}")]
pub struct InvalidTaskTransition {
    pub from: TaskStatus,
    pub event: TaskEvent,
}
