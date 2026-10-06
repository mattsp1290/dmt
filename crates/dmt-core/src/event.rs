use crate::{
    BranchResult, GraphId, JoinId, Micros, NodeId, Outcome, SignalId, SignalKey, StepKey, TaskId,
    WorkerId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
/// Append-only events; the store assigns sequence numbers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum RunEvent {
    RunStarted {
        graph_id: GraphId,
        graph_version: u32,
        definition_hash: String,
        input: Value,
    },
    TaskScheduled {
        task_id: TaskId,
        node_id: NodeId,
        step_key: StepKey,
        attempt: u32,
        run_at: Micros,
        max_attempts: u32,
    },
    TaskClaimed {
        task_id: TaskId,
        worker_id: WorkerId,
        attempt: u32,
        lease_until: Micros,
    },
    TaskCompleted {
        task_id: TaskId,
        attempt: u32,
        outcome: Outcome,
    },
    TaskFailed {
        task_id: TaskId,
        attempt: u32,
        message: String,
        retryable: bool,
    },
    TaskRetryScheduled {
        task_id: TaskId,
        attempt: u32,
        run_at: Micros,
    },
    TaskExhausted {
        task_id: TaskId,
        reason: ExhaustReason,
    },
    FanOutScheduled {
        task_id: TaskId,
        join_id: JoinId,
        join_step_key: StepKey,
        branch_count: u32,
    },
    BranchContributed {
        join_id: JoinId,
        branch_index: u32,
        result: BranchResult,
    },
    JoinSatisfied {
        join_id: JoinId,
        received: u32,
        failed: u32,
        quorum_met: bool,
    },
    WaitOpened {
        task_id: TaskId,
        signal_id: SignalId,
        signal_key: SignalKey,
        deadline_at: Option<Micros>,
    },
    SignalReceived {
        signal_id: SignalId,
        signal_key: SignalKey,
        label: String,
    },
    SignalTimedOut {
        signal_id: SignalId,
        signal_key: SignalKey,
    },
    RunParked,
    RunResumed,
    RunCompleted {
        output: Value,
    },
    RunFailed {
        message: String,
    },
    RunCancelled {
        reason: String,
    },
}
impl RunEvent {
    pub const KINDS: [&'static str; 18] = [
        "RunStarted",
        "TaskScheduled",
        "TaskClaimed",
        "TaskCompleted",
        "TaskFailed",
        "TaskRetryScheduled",
        "TaskExhausted",
        "FanOutScheduled",
        "BranchContributed",
        "JoinSatisfied",
        "WaitOpened",
        "SignalReceived",
        "SignalTimedOut",
        "RunParked",
        "RunResumed",
        "RunCompleted",
        "RunFailed",
        "RunCancelled",
    ];
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::RunStarted { .. } => "RunStarted",
            Self::TaskScheduled { .. } => "TaskScheduled",
            Self::TaskClaimed { .. } => "TaskClaimed",
            Self::TaskCompleted { .. } => "TaskCompleted",
            Self::TaskFailed { .. } => "TaskFailed",
            Self::TaskRetryScheduled { .. } => "TaskRetryScheduled",
            Self::TaskExhausted { .. } => "TaskExhausted",
            Self::FanOutScheduled { .. } => "FanOutScheduled",
            Self::BranchContributed { .. } => "BranchContributed",
            Self::JoinSatisfied { .. } => "JoinSatisfied",
            Self::WaitOpened { .. } => "WaitOpened",
            Self::SignalReceived { .. } => "SignalReceived",
            Self::SignalTimedOut { .. } => "SignalTimedOut",
            Self::RunParked => "RunParked",
            Self::RunResumed => "RunResumed",
            Self::RunCompleted { .. } => "RunCompleted",
            Self::RunFailed { .. } => "RunFailed",
            Self::RunCancelled { .. } => "RunCancelled",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExhaustReason {
    HandlerFailures,
    LeaseReclaimsExceeded,
}
#[cfg(test)]
mod tests {
    use super::*;
    fn samples() -> Vec<RunEvent> {
        vec![
            RunEvent::RunStarted {
                graph_id: "graph".into(),
                graph_version: 1,
                definition_hash: "hash".into(),
                input: Value::Null,
            },
            RunEvent::TaskScheduled {
                task_id: "run/task/0".into(),
                node_id: "task".into(),
                step_key: StepKey::task(&"run".into(), &"task".into(), 0),
                attempt: 1,
                run_at: Micros(0),
                max_attempts: 3,
            },
            RunEvent::TaskClaimed {
                task_id: "run/task/0".into(),
                worker_id: "worker".into(),
                attempt: 1,
                lease_until: Micros(10),
            },
            RunEvent::TaskCompleted {
                task_id: "run/task/0".into(),
                attempt: 1,
                outcome: Outcome::done("ok"),
            },
            RunEvent::TaskFailed {
                task_id: "run/task/0".into(),
                attempt: 1,
                message: "error".into(),
                retryable: true,
            },
            RunEvent::TaskRetryScheduled {
                task_id: "run/task/0".into(),
                attempt: 1,
                run_at: Micros(0),
            },
            RunEvent::TaskExhausted {
                task_id: "run/task/0".into(),
                reason: ExhaustReason::HandlerFailures,
            },
            RunEvent::FanOutScheduled {
                task_id: "run/task/0".into(),
                join_id: "run/join/0".into(),
                join_step_key: StepKey::task(&"run".into(), &"join".into(), 0),
                branch_count: 2,
            },
            RunEvent::BranchContributed {
                join_id: "run/join/0".into(),
                branch_index: 0,
                result: BranchResult::Done {
                    index: 0,
                    outcome: Outcome::done("ok"),
                },
            },
            RunEvent::JoinSatisfied {
                join_id: "run/join/0".into(),
                received: 2,
                failed: 0,
                quorum_met: true,
            },
            RunEvent::WaitOpened {
                task_id: "run/task/0".into(),
                signal_id: "run/gate/0".into(),
                signal_key: SignalKey::new("gate", 0),
                deadline_at: Some(Micros(10)),
            },
            RunEvent::SignalReceived {
                signal_id: "run/gate/0".into(),
                signal_key: SignalKey::new("gate", 0),
                label: "ok".into(),
            },
            RunEvent::SignalTimedOut {
                signal_id: "run/gate/0".into(),
                signal_key: SignalKey::new("gate", 0),
            },
            RunEvent::RunParked,
            RunEvent::RunResumed,
            RunEvent::RunCompleted {
                output: Value::Null,
            },
            RunEvent::RunFailed {
                message: "error".into(),
            },
            RunEvent::RunCancelled {
                reason: "stop".into(),
            },
        ]
    }
    #[test]
    fn every_event_round_trips() {
        let events = samples();
        assert_eq!(events.len(), RunEvent::KINDS.len());
        for (event, kind) in events.iter().zip(RunEvent::KINDS) {
            let json = serde_json::to_value(event).unwrap();
            assert_eq!(event.kind(), kind);
            assert_eq!(json["kind"], kind);
            assert_eq!(serde_json::from_value::<RunEvent>(json).unwrap(), *event);
        }
    }
}
