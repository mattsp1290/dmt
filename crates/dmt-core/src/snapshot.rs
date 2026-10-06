use crate::{
    BranchResult, GraphId, JoinId, JoinPolicy, Micros, NodeId, NodeOutcome, RunId, RunStatus,
    SignalId, SignalKey, StepKey, TaskId, TaskStatus, WorkerId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub run_id: RunId,
    pub graph_id: GraphId,
    pub graph_version: u32,
    pub definition_hash: String,
    pub status: RunStatus,
    pub version: u64,
    pub machine_json: Value,
    pub input: Value,
    pub output: Option<Value>,
    pub node_occurrences: BTreeMap<NodeId, u32>,
    pub tasks: Vec<TaskRecord>,
    pub joins: Vec<JoinRecord>,
    pub signals: Vec<SignalRecord>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub node_id: NodeId,
    pub step_key: StepKey,
    pub status: TaskStatus,
    pub attempt: u32,
    pub max_attempts: u32,
    pub run_at: Micros,
    pub lease_owner: Option<WorkerId>,
    pub lease_until: Option<Micros>,
    pub input: Value,
    pub outcome: Option<NodeOutcome>,
    pub branch: Option<BranchRef>,
    pub planned_at: Option<Micros>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRef {
    pub join_id: JoinId,
    pub index: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRecord {
    pub join_id: JoinId,
    pub run_id: RunId,
    pub node_id: NodeId,
    pub step_key: StepKey,
    pub policy: JoinPolicy,
    pub expected: u32,
    pub received: u32,
    pub failed: u32,
    pub results: Vec<BranchResult>,
    pub satisfied_at: Option<Micros>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalRecord {
    pub signal_id: SignalId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub key: SignalKey,
    pub name: String,
    pub deadline_at: Option<Micros>,
    pub resolved_at: Option<Micros>,
}
impl RunSnapshot {
    #[must_use]
    pub fn task(&self, id: &TaskId) -> Option<&TaskRecord> {
        self.tasks.iter().find(|t| &t.task_id == id)
    }
    #[must_use]
    pub fn join(&self, id: &JoinId) -> Option<&JoinRecord> {
        self.joins.iter().find(|j| &j.join_id == id)
    }
    #[must_use]
    pub fn signal(&self, id: &SignalId) -> Option<&SignalRecord> {
        self.signals.iter().find(|s| &s.signal_id == id)
    }
    #[must_use]
    pub fn occurrence(&self, id: &NodeId) -> u32 {
        self.node_occurrences.get(id).copied().unwrap_or(0)
    }
}
