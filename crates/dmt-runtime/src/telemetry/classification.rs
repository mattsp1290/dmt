use crate::EngineError;
use dmt_core::{NodeOutcome, PlanError};
use dmt_store::StoreError;

#[derive(Clone, Copy)]
pub(crate) enum HandlerClass {
    Done,
    FanOut,
    FailRetryable,
    FailPermanent,
    Panic,
    Timeout,
    BadInput,
    NoHandler,
    Aborted,
}
impl HandlerClass {
    pub fn label(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::FanOut => "fan_out",
            Self::FailRetryable => "fail_retryable",
            Self::FailPermanent => "fail_permanent",
            Self::Panic => "panic",
            Self::Timeout => "timeout",
            Self::BadInput => "bad_input",
            Self::NoHandler => "no_handler",
            Self::Aborted => "aborted",
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) enum CommitResult {
    Applied,
    Dropped,
    LeaseLost,
    Terminal,
    Rejected,
    Exhausted,
    RunNotFound,
    Error,
}
impl CommitResult {
    pub fn label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Dropped => "dropped",
            Self::LeaseLost => "lease_lost",
            Self::Terminal => "terminal",
            Self::Rejected => "rejected",
            Self::Exhausted => "exhausted",
            Self::RunNotFound => "run_not_found",
            Self::Error => "error",
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) enum DispatchResult {
    Applied,
    Dropped,
    LeaseLost,
    Terminal,
    Rejected,
    Exhausted,
    GraphUnavailable,
    NodeUnavailable,
    Aborted,
}
impl DispatchResult {
    pub fn label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Dropped => "dropped",
            Self::LeaseLost => "lease_lost",
            Self::Terminal => "terminal",
            Self::Rejected => "rejected",
            Self::Exhausted => "exhausted",
            Self::GraphUnavailable => "graph_unavailable",
            Self::NodeUnavailable => "node_unavailable",
            Self::Aborted => "aborted",
        }
    }
}

impl HandlerClass {
    pub fn for_outcome(outcome: &NodeOutcome) -> Self {
        match outcome {
            NodeOutcome::Done(_) => Self::Done,
            NodeOutcome::FanOut(_) => Self::FanOut,
            NodeOutcome::Fail {
                retryable: true, ..
            } => Self::FailRetryable,
            NodeOutcome::Fail { .. } => Self::FailPermanent,
        }
    }
}
impl CommitResult {
    pub fn for_unowned(result: &Result<(), EngineError>) -> Self {
        match result {
            Ok(()) => Self::Applied,
            Err(EngineError::RunNotFound { .. }) => Self::RunNotFound,
            Err(
                EngineError::RunTerminal { .. }
                | EngineError::Store(StoreError::RunTerminal { .. })
                | EngineError::Plan(PlanError::RunTerminal { .. }),
            ) => Self::Terminal,
            Err(EngineError::Contention { .. }) => Self::Exhausted,
            Err(_) => Self::Error,
        }
    }
}
impl From<CommitResult> for DispatchResult {
    fn from(result: CommitResult) -> Self {
        match result {
            CommitResult::Applied => Self::Applied,
            CommitResult::Dropped => Self::Dropped,
            CommitResult::LeaseLost => Self::LeaseLost,
            CommitResult::Terminal => Self::Terminal,
            CommitResult::Rejected | CommitResult::RunNotFound | CommitResult::Error => {
                Self::Rejected
            }
            CommitResult::Exhausted => Self::Exhausted,
        }
    }
}
impl DispatchResult {
    pub fn is_error(self) -> bool {
        !matches!(self, Self::Applied | Self::Dropped)
    }
}
