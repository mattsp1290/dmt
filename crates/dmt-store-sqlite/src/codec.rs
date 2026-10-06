use dmt_core::{
    BranchRef, BranchResult, JoinPolicy, JoinRecord, NodeId, NodeOutcome, RunEvent, RunSnapshot,
    RunStatus, SignalRecord, TaskRecord, TaskStatus,
};
use dmt_store::{EventRecord, RunSummary, StoreError};
use serde_json::Value;
use sqlx::{FromRow, types::Json};
use std::collections::BTreeMap;

pub(crate) fn corrupt(message: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("corrupt row: {message}"))
}
pub(crate) fn u32_from(value: i64) -> Result<u32, StoreError> {
    u32::try_from(value).map_err(corrupt)
}
pub(crate) fn u64_from(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(corrupt)
}
pub(crate) fn i64_from(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|error| StoreError::Backend(format!("encode: {error}")))
}
pub(crate) fn limit(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
pub(crate) fn task_status(value: &str) -> Result<TaskStatus, StoreError> {
    match value {
        "ready" => Ok(TaskStatus::Ready),
        "running" => Ok(TaskStatus::Running),
        "awaiting" => Ok(TaskStatus::Awaiting),
        "completed" => Ok(TaskStatus::Completed),
        "exhausted" => Ok(TaskStatus::Exhausted),
        "cancelled" => Ok(TaskStatus::Cancelled),
        _ => Err(corrupt(format!("unknown task status {value:?}"))),
    }
}
pub(crate) fn run_status(value: &str) -> Result<RunStatus, StoreError> {
    match value {
        "created" => Ok(RunStatus::Created),
        "active" => Ok(RunStatus::Active),
        "parked" => Ok(RunStatus::Parked),
        "completed" => Ok(RunStatus::Completed),
        "failed" => Ok(RunStatus::Failed),
        "cancelled" => Ok(RunStatus::Cancelled),
        _ => Err(corrupt(format!("unknown run status {value:?}"))),
    }
}
#[derive(FromRow)]
pub(crate) struct Head {
    pub status: String,
    pub version: i64,
    pub next_seq: i64,
    pub graph_id: String,
}
#[derive(FromRow)]
pub(crate) struct TaskRow {
    pub id: String,
    pub run_id: String,
    pub node_id: String,
    pub step_key: String,
    pub status: String,
    pub attempt: i64,
    pub max_attempts: i64,
    pub run_at: i64,
    pub lease_owner: Option<String>,
    pub lease_until: Option<i64>,
    pub input_json: Json<Value>,
    pub outcome_json: Option<Json<NodeOutcome>>,
    pub join_id: Option<String>,
    pub branch_index: Option<i64>,
    pub planned_at: Option<i64>,
}
impl TaskRow {
    pub(crate) fn decode(self) -> Result<TaskRecord, StoreError> {
        let branch = match (self.join_id, self.branch_index) {
            (None, None) => None,
            (Some(id), Some(index)) => Some(BranchRef {
                join_id: id.into(),
                index: u32_from(index)?,
            }),
            _ => return Err(corrupt("incomplete task branch")),
        };
        Ok(TaskRecord {
            task_id: self.id.into(),
            run_id: self.run_id.into(),
            node_id: self.node_id.into(),
            step_key: serde_json::from_value(Value::String(self.step_key)).map_err(corrupt)?,
            status: task_status(&self.status)?,
            attempt: u32_from(self.attempt)?,
            max_attempts: u32_from(self.max_attempts)?,
            run_at: dmt_core::Micros(self.run_at),
            lease_owner: self.lease_owner.map(Into::into),
            lease_until: self.lease_until.map(dmt_core::Micros),
            input: self.input_json.0,
            outcome: self.outcome_json.map(|json| json.0),
            branch,
            planned_at: self.planned_at.map(dmt_core::Micros),
        })
    }
}
#[derive(FromRow)]
pub(crate) struct Candidate {
    #[sqlx(flatten)]
    pub task: TaskRow,
    pub run_status: String,
    pub graph_version: i64,
    pub graph_id: String,
}
#[derive(FromRow)]
pub(crate) struct JoinRow {
    pub id: String,
    pub run_id: String,
    pub node_id: String,
    pub step_key: String,
    pub policy_json: Json<JoinPolicy>,
    pub expected: i64,
    pub received: i64,
    pub failed: i64,
    pub results_json: Json<Vec<BranchResult>>,
    pub satisfied_at: Option<i64>,
}
impl JoinRow {
    pub(crate) fn decode(self) -> Result<JoinRecord, StoreError> {
        Ok(JoinRecord {
            join_id: self.id.into(),
            run_id: self.run_id.into(),
            node_id: self.node_id.into(),
            step_key: serde_json::from_value(Value::String(self.step_key)).map_err(corrupt)?,
            policy: self.policy_json.0,
            expected: u32_from(self.expected)?,
            received: u32_from(self.received)?,
            failed: u32_from(self.failed)?,
            results: self.results_json.0,
            satisfied_at: self.satisfied_at.map(dmt_core::Micros),
        })
    }
}
#[derive(FromRow)]
pub(crate) struct SignalRow {
    pub id: String,
    pub run_id: String,
    pub task_id: String,
    pub key: String,
    pub name: String,
    pub deadline_at: Option<i64>,
    pub resolved_at: Option<i64>,
}
impl SignalRow {
    pub(crate) fn decode(self) -> Result<SignalRecord, StoreError> {
        Ok(SignalRecord {
            signal_id: self.id.into(),
            run_id: self.run_id.into(),
            task_id: self.task_id.into(),
            key: serde_json::from_value(Value::String(self.key)).map_err(corrupt)?,
            name: self.name,
            deadline_at: self.deadline_at.map(dmt_core::Micros),
            resolved_at: self.resolved_at.map(dmt_core::Micros),
        })
    }
}
#[derive(FromRow)]
pub(crate) struct RunRow {
    pub id: String,
    pub graph_id: String,
    pub graph_version: i64,
    pub definition_hash: String,
    pub status: String,
    pub version: i64,
    pub machine_json: Json<Value>,
    pub occurrences_json: Json<BTreeMap<NodeId, u32>>,
    pub input_json: Json<Value>,
    pub output_json: Option<Json<Value>>,
}
impl RunRow {
    pub(crate) fn decode(self) -> Result<RunSnapshot, StoreError> {
        Ok(RunSnapshot {
            run_id: self.id.into(),
            graph_id: self.graph_id.into(),
            graph_version: u32_from(self.graph_version)?,
            definition_hash: self.definition_hash,
            status: run_status(&self.status)?,
            version: u64_from(self.version)?,
            machine_json: self.machine_json.0,
            input: self.input_json.0,
            output: self.output_json.map(|json| json.0),
            node_occurrences: self.occurrences_json.0,
            tasks: Vec::new(),
            joins: Vec::new(),
            signals: Vec::new(),
        })
    }
}
#[derive(FromRow)]
pub(crate) struct SummaryRow {
    pub id: String,
    pub graph_id: String,
    pub graph_version: i64,
    pub status: String,
    pub version: i64,
    pub created_at: i64,
    pub updated_at: i64,
}
impl SummaryRow {
    pub(crate) fn decode(self) -> Result<RunSummary, StoreError> {
        Ok(RunSummary {
            run_id: self.id.into(),
            graph_id: self.graph_id.into(),
            graph_version: u32_from(self.graph_version)?,
            status: run_status(&self.status)?,
            version: u64_from(self.version)?,
            created_at: dmt_core::Micros(self.created_at),
            updated_at: dmt_core::Micros(self.updated_at),
        })
    }
}
#[derive(FromRow)]
pub(crate) struct EventRow {
    pub seq: i64,
    pub recorded_at: i64,
    pub payload_json: Json<RunEvent>,
}
impl EventRow {
    pub(crate) fn decode(self) -> Result<EventRecord, StoreError> {
        Ok(EventRecord {
            seq: u64_from(self.seq)?,
            recorded_at: dmt_core::Micros(self.recorded_at),
            event: self.payload_json.0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn status_literals_match_display() {
        for (text, status) in [
            ("ready", TaskStatus::Ready),
            ("running", TaskStatus::Running),
            ("awaiting", TaskStatus::Awaiting),
            ("completed", TaskStatus::Completed),
            ("exhausted", TaskStatus::Exhausted),
            ("cancelled", TaskStatus::Cancelled),
        ] {
            assert_eq!(status.to_string(), text);
            assert_eq!(task_status(text).unwrap(), status);
        }
        for (text, status) in [
            ("created", RunStatus::Created),
            ("active", RunStatus::Active),
            ("parked", RunStatus::Parked),
            ("completed", RunStatus::Completed),
            ("failed", RunStatus::Failed),
            ("cancelled", RunStatus::Cancelled),
        ] {
            assert_eq!(status.to_string(), text);
            assert_eq!(run_status(text).unwrap(), status);
        }
        assert!(task_status("bogus").is_err());
        assert!(run_status("bogus").is_err());
    }
    #[test]
    fn integer_bounds() {
        assert!(i64_from(u64::MAX).is_err());
        assert!(u32_from(-1).is_err());
        assert!(u64_from(-1).is_err());
        assert_eq!(
            i64_from(u64::try_from(i64::MAX).unwrap()).unwrap(),
            i64::MAX
        );
        assert_eq!(limit(usize::MAX), i64::MAX);
    }
}
