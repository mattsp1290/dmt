use crate::{BranchRef, JoinRecord, RunEvent};
use crate::{
    BranchResult, GraphId, JoinId, Micros, NodeId, NodeOutcome, RunId, RunStatus, SignalId,
    SignalKey, StepKey, TaskId, TaskStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    pub run_id: RunId,
    pub expected_run_version: u64,
    pub now: Micros,
    pub new_run: Option<NewRun>,
    pub events: Vec<RunEvent>,
    pub run_state: Option<RunStateUpdate>,
    pub task_updates: Vec<TaskUpdate>,
    pub new_tasks: Vec<NewTask>,
    pub new_join: Option<JoinRecord>,
    pub join_contribution: Option<JoinContribution>,
    pub new_signal: Option<NewSignal>,
    pub resolved_signals: Vec<SignalResolution>,
    pub node_occurrences: BTreeMap<NodeId, u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRun {
    pub graph_id: GraphId,
    pub graph_version: u32,
    pub definition_hash: String,
    pub input: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStateUpdate {
    pub status: RunStatus,
    pub machine_json: Value,
    pub output: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUpdate {
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub attempt: u32,
    pub run_at: Micros,
    pub outcome: Option<NodeOutcome>,
    pub planned_at: Option<Micros>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTask {
    pub task_id: TaskId,
    pub node_id: NodeId,
    pub step_key: StepKey,
    pub status: TaskStatus,
    pub attempt: u32,
    pub max_attempts: u32,
    pub run_at: Micros,
    pub input: Value,
    pub branch: Option<BranchRef>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinContribution {
    pub join_id: JoinId,
    pub result: BranchResult,
    pub expected_received: u32,
    pub expected_failed: u32,
    pub satisfied: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSignal {
    pub signal_id: SignalId,
    pub task_id: TaskId,
    pub key: SignalKey,
    pub name: String,
    pub deadline_at: Option<Micros>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalResolution {
    pub signal_id: SignalId,
    pub label: String,
    pub payload: Value,
}

/// A malformed commit produced by a programming error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("commit invariant violated: {0}")]
pub struct CommitInvariant(pub String);
impl Commit {
    /// Verify cross-record and event invariants before persistence.
    /// # Errors
    /// Returns an error for a missing, inconsistent, or duplicate record.
    pub fn check(&self) -> Result<(), CommitInvariant> {
        use std::collections::BTreeSet;
        let require = |condition, message: &str| {
            if condition {
                Ok(())
            } else {
                Err(CommitInvariant(message.into()))
            }
        };
        require(self.run_id.is_valid(), "invalid run id")?;
        require(
            self.new_run.is_some() == (self.expected_run_version == 0),
            "start/version mismatch",
        )?;
        let lifecycle = self.events.iter().any(|e| {
            matches!(
                e,
                RunEvent::RunStarted { .. }
                    | RunEvent::RunParked
                    | RunEvent::RunResumed
                    | RunEvent::RunCompleted { .. }
                    | RunEvent::RunFailed { .. }
                    | RunEvent::RunCancelled { .. }
            )
        });
        require(
            self.run_state.is_some() == lifecycle,
            "lifecycle/state mismatch",
        )?;
        let mut scheduled = Vec::new();
        for event in &self.events {
            match event {
                RunEvent::TaskScheduled {
                    task_id,
                    node_id,
                    step_key,
                    attempt,
                    run_at,
                    max_attempts,
                } => {
                    require(
                        self.new_tasks.iter().any(|t| {
                            &t.task_id == task_id
                                && &t.node_id == node_id
                                && &t.step_key == step_key
                                && t.attempt == *attempt
                                && t.run_at == *run_at
                                && t.max_attempts == *max_attempts
                                && t.status == TaskStatus::Ready
                        }),
                        "scheduled event missing matching task",
                    )?;
                    scheduled.push(task_id);
                }
                RunEvent::WaitOpened {
                    task_id,
                    signal_id,
                    signal_key,
                    deadline_at,
                } => {
                    require(
                        self.new_tasks
                            .iter()
                            .any(|t| &t.task_id == task_id && t.status == TaskStatus::Awaiting),
                        "wait event missing task",
                    )?;
                    require(
                        self.new_signal.as_ref().is_some_and(|s| {
                            &s.task_id == task_id
                                && &s.signal_id == signal_id
                                && &s.key == signal_key
                                && s.deadline_at == *deadline_at
                        }),
                        "wait event missing signal",
                    )?;
                    scheduled.push(task_id);
                }
                _ => {}
            }
        }
        let mut ids = BTreeSet::new();
        let mut keys = BTreeSet::new();
        for task in &self.new_tasks {
            require(
                ids.insert(&task.task_id) && keys.insert(&task.step_key),
                "duplicate task id or step key",
            )?;
            require(
                task.task_id.as_str() == task.step_key.as_str(),
                "task id differs from step key",
            )?;
            require(
                scheduled.iter().filter(|id| **id == &task.task_id).count() == 1,
                "task missing unique scheduling event",
            )?;
        }
        self.check_relations()
    }
    fn check_relations(&self) -> Result<(), CommitInvariant> {
        let require = |condition, message: &str| {
            if condition {
                Ok(())
            } else {
                Err(CommitInvariant(message.into()))
            }
        };
        let fanouts: Vec<_> = self
            .events
            .iter()
            .filter_map(|e| {
                if let RunEvent::FanOutScheduled {
                    join_id,
                    join_step_key,
                    branch_count,
                    ..
                } = e
                {
                    Some((join_id, join_step_key, branch_count))
                } else {
                    None
                }
            })
            .collect();
        require(
            fanouts.len() == usize::from(self.new_join.is_some()),
            "fan-out/join mismatch",
        )?;
        if let Some(join) = &self.new_join {
            require(
                fanouts[0] == (&join.join_id, &join.step_key, &join.expected),
                "fan-out/join fields mismatch",
            )?;
        }
        if let Some(contribution) = &self.join_contribution {
            require(self.events.iter().any(|e| matches!(e, RunEvent::BranchContributed {join_id, branch_index, result} if join_id == &contribution.join_id && *branch_index == contribution.result.index() && result == &contribution.result)), "contribution missing event")?;
        }
        require(
            self.events
                .iter()
                .filter(|e| matches!(e, RunEvent::WaitOpened { .. }))
                .count()
                == usize::from(self.new_signal.is_some()),
            "wait/signal mismatch",
        )?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EndStatus, GraphBuilder, plan_start};
    fn start() -> Commit {
        let graph = GraphBuilder::new("test", 1)
            .start("a")
            .task("a")
            .end("end", EndStatus::Completed)
            .edge("a", "end")
            .build()
            .unwrap();
        plan_start(&graph, "run".into(), Value::Null, Micros(0)).unwrap()
    }
    #[test]
    fn invalid_run_id() {
        let mut c = start();
        c.run_id = "bad/run".into();
        assert!(c.check().is_err());
    }
    #[test]
    fn start_version_mismatch() {
        let mut c = start();
        c.expected_run_version = 1;
        assert!(c.check().is_err());
    }
    #[test]
    fn lifecycle_state_mismatch() {
        let mut c = start();
        c.run_state = None;
        assert!(c.check().is_err());
    }
    #[test]
    fn missing_scheduled_task() {
        let mut c = start();
        c.new_tasks.clear();
        assert!(c.check().is_err());
    }
    #[test]
    fn missing_scheduling_event() {
        let mut c = start();
        c.events
            .retain(|e| !matches!(e, RunEvent::TaskScheduled { .. }));
        assert!(c.check().is_err());
    }
    #[test]
    fn duplicate_key() {
        let mut c = start();
        c.new_tasks.push(c.new_tasks[0].clone());
        assert!(c.check().is_err());
    }
    #[test]
    fn wrong_task_id() {
        let mut c = start();
        c.new_tasks[0].task_id = "other".into();
        assert!(c.check().is_err());
    }
    #[test]
    fn missing_join() {
        let mut c = start();
        c.events.push(RunEvent::FanOutScheduled {
            task_id: "fo".into(),
            join_id: "join".into(),
            join_step_key: StepKey::task(&c.run_id, &"join".into(), 0),
            branch_count: 1,
        });
        assert!(c.check().is_err());
    }
    #[test]
    fn missing_contribution_event() {
        let mut c = start();
        c.join_contribution = Some(JoinContribution {
            join_id: "join".into(),
            result: BranchResult::Failed {
                index: 0,
                message: "error".into(),
            },
            expected_received: 0,
            expected_failed: 1,
            satisfied: true,
        });
        assert!(c.check().is_err());
    }
    #[test]
    fn missing_wait_event() {
        let mut c = start();
        c.new_signal = Some(NewSignal {
            signal_id: "signal".into(),
            task_id: "task".into(),
            key: SignalKey::new("gate", 0),
            name: "gate".into(),
            deadline_at: None,
        });
        assert!(c.check().is_err());
    }
}
