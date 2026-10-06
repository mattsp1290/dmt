use dmt_core::{
    GraphId, JoinId, Micros, RunEvent, RunId, RunStatus, StepKey, TaskId, TaskRecord, WorkerId,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseProof {
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRequest {
    pub worker_id: WorkerId,
    pub now: Micros,
    pub lease_micros: i64,
    pub limit: usize,
    pub graphs: Vec<GraphId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedTask {
    pub task: TaskRecord,
    pub graph_id: GraphId,
    pub graph_version: u32,
    pub proof: LeaseProof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatResult {
    Extended { lease_until: Micros },
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApplyResult {
    pub run_version: u64,
    pub inserted_tasks: Vec<TaskId>,
    pub ignored_tasks: Vec<StepKey>,
    pub join_satisfied: Option<JoinId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub seq: u64,
    pub recorded_at: Micros,
    pub event: RunEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFilter {
    pub status: Option<RunStatus>,
    pub graph_id: Option<GraphId>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub run_id: RunId,
    pub graph_id: GraphId,
    pub graph_version: u32,
    pub status: RunStatus,
    pub version: u64,
    pub created_at: Micros,
    pub updated_at: Micros,
}
impl RunFilter {
    #[must_use]
    pub fn all(limit: usize) -> Self {
        Self {
            status: None,
            graph_id: None,
            limit,
        }
    }
}
