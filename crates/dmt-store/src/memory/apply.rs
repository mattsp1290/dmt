use super::{Inner, RunRow, append_event, clear_lease, lease_matches};
use crate::{ApplyResult, LeaseProof, StoreError};
use dmt_core::{
    BranchResult, Commit, RunSnapshot, RunStatus, SignalRecord, TaskRecord, TaskStatus,
};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn create(inner: &mut Inner, commit: Commit) -> Result<dmt_core::RunId, StoreError> {
    let new = commit
        .new_run
        .as_ref()
        .filter(|_| commit.expected_run_version == 0)
        .ok_or_else(|| {
            StoreError::InvalidCommit("create requires new_run and version zero".into())
        })?;
    let graph = inner
        .graphs
        .get(&(new.graph_id.clone(), new.graph_version))
        .ok_or_else(|| {
            StoreError::NotFound(format!("graph {}@{}", new.graph_id, new.graph_version))
        })?;
    if graph.definition_hash() != new.definition_hash {
        return Err(StoreError::GraphMismatch {
            graph_id: new.graph_id.clone(),
            version: new.graph_version,
        });
    }
    if let Some(row) = inner.runs.get(&commit.run_id) {
        return Err(StoreError::VersionConflict {
            expected: 0,
            actual: row.snapshot.version,
        });
    }
    check_records(inner, &commit)?;
    let run_id = commit.run_id.clone();
    inner.runs.insert(
        run_id.clone(),
        RunRow {
            snapshot: RunSnapshot {
                run_id: run_id.clone(),
                graph_id: new.graph_id.clone(),
                graph_version: new.graph_version,
                definition_hash: new.definition_hash.clone(),
                status: RunStatus::Created,
                version: 0,
                machine_json: Value::Null,
                input: new.input.clone(),
                output: None,
                node_occurrences: std::collections::BTreeMap::default(),
                tasks: Vec::new(),
                joins: Vec::new(),
                signals: Vec::new(),
            },
            events: Vec::new(),
            created_at: commit.now,
            updated_at: commit.now,
        },
    );
    mutate(inner, commit);
    Ok(run_id)
}
pub(super) fn apply(
    inner: &mut Inner,
    commit: Commit,
    by: Option<&LeaseProof>,
) -> Result<ApplyResult, StoreError> {
    let run = inner
        .runs
        .get(&commit.run_id)
        .ok_or_else(|| StoreError::NotFound(format!("run {}", commit.run_id)))?;
    if run.snapshot.status.is_terminal() {
        return Err(StoreError::RunTerminal {
            run_id: commit.run_id,
        });
    }
    if run.snapshot.version != commit.expected_run_version {
        return Err(StoreError::VersionConflict {
            expected: commit.expected_run_version,
            actual: run.snapshot.version,
        });
    }
    if commit.new_run.is_some() {
        return Err(StoreError::InvalidCommit(
            "apply cannot create a run".into(),
        ));
    }
    if let Some(proof) = by {
        let task = inner
            .tasks
            .get(&proof.task_id)
            .ok_or_else(|| StoreError::NotFound(format!("task {}", proof.task_id)))?;
        if task.run_id != commit.run_id || !lease_matches(task, proof) {
            return Err(StoreError::LeaseLost {
                task_id: proof.task_id.clone(),
            });
        }
    }
    // Check overflow before any writes, including event allocation.
    if commit.expected_run_version == u64::MAX {
        return Err(StoreError::InvalidCommit("run version overflow".into()));
    }
    check_records(inner, &commit)?;
    Ok(mutate(inner, commit))
}
fn check_records(inner: &Inner, commit: &Commit) -> Result<(), StoreError> {
    for update in &commit.task_updates {
        if inner
            .tasks
            .get(&update.task_id)
            .is_none_or(|t| t.run_id != commit.run_id)
        {
            return Err(StoreError::NotFound(format!("task {}", update.task_id)));
        }
    }
    if let Some(c) = &commit.join_contribution {
        let join = inner
            .joins
            .get(&c.join_id)
            .filter(|j| j.run_id == commit.run_id)
            .ok_or_else(|| StoreError::NotFound(format!("join {}", c.join_id)))?;
        let received = join
            .received
            .checked_add(u32::from(matches!(c.result, BranchResult::Done { .. })));
        let failed = join
            .failed
            .checked_add(u32::from(matches!(c.result, BranchResult::Failed { .. })));
        if join.satisfied_at.is_some()
            || received != Some(c.expected_received)
            || failed != Some(c.expected_failed)
        {
            return Err(StoreError::JoinDrift {
                join_id: c.join_id.clone(),
            });
        }
    }
    let mut resolved = BTreeSet::new();
    for resolution in &commit.resolved_signals {
        let signal = inner
            .signals
            .get(&resolution.signal_id)
            .filter(|s| s.run_id == commit.run_id)
            .ok_or_else(|| StoreError::NotFound(format!("signal {}", resolution.signal_id)))?;
        if signal.resolved_at.is_some() || !resolved.insert(&resolution.signal_id) {
            return Err(StoreError::AlreadyResolved {
                signal_id: resolution.signal_id.clone(),
            });
        }
    }
    if let Some(signal) = &commit.new_signal
        && (inner.signals.contains_key(&signal.signal_id)
            || inner
                .signals
                .values()
                .any(|s| s.run_id == commit.run_id && s.key == signal.key))
    {
        return Err(StoreError::InvalidCommit(
            "duplicate signal id or key".into(),
        ));
    }
    if let Some(join) = &commit.new_join
        && (join.run_id != commit.run_id || inner.joins.contains_key(&join.join_id))
    {
        return Err(StoreError::InvalidCommit(
            "duplicate join id or wrong run".into(),
        ));
    }
    for task in &commit.new_tasks {
        if task.task_id.as_str() != task.step_key.as_str()
            || inner
                .tasks
                .get(&task.task_id)
                .is_some_and(|t| t.step_key != task.step_key)
        {
            return Err(StoreError::InvalidCommit(
                "task id differs from step key".into(),
            ));
        }
    }
    Ok(())
}
fn mutate(inner: &mut Inner, commit: Commit) -> ApplyResult {
    let mut result = ApplyResult {
        run_version: commit.expected_run_version + 1,
        ..ApplyResult::default()
    };
    for update in commit.task_updates {
        let task = inner
            .tasks
            .get_mut(&update.task_id)
            .expect("validated task");
        if update.status == TaskStatus::Cancelled
            && !matches!(task.status, TaskStatus::Ready | TaskStatus::Awaiting)
        {
            continue;
        }
        task.status = update.status;
        task.attempt = update.attempt;
        task.run_at = update.run_at;
        task.outcome = update.outcome;
        task.planned_at = update.planned_at;
        if task.status != TaskStatus::Running {
            clear_lease(task);
        }
    }
    // Validated task ids equal their step keys, so the task map also owns step uniqueness.
    for task in commit.new_tasks {
        if inner.tasks.contains_key(&task.task_id) {
            result.ignored_tasks.push(task.step_key);
            continue;
        }
        result.inserted_tasks.push(task.task_id.clone());
        inner.tasks.insert(
            task.task_id.clone(),
            TaskRecord {
                task_id: task.task_id,
                run_id: commit.run_id.clone(),
                node_id: task.node_id,
                step_key: task.step_key,
                status: task.status,
                attempt: task.attempt,
                max_attempts: task.max_attempts,
                run_at: task.run_at,
                lease_owner: None,
                lease_until: None,
                input: task.input,
                outcome: None,
                branch: task.branch,
                planned_at: None,
            },
        );
    }
    if let Some(join) = commit.new_join {
        inner.joins.insert(join.join_id.clone(), join);
    }
    if let Some(c) = commit.join_contribution {
        let join = inner.joins.get_mut(&c.join_id).expect("validated join");
        join.received = c.expected_received;
        join.failed = c.expected_failed;
        join.results.push(c.result);
        if c.satisfied {
            join.satisfied_at = Some(commit.now);
            result.join_satisfied = Some(c.join_id);
        }
    }
    if let Some(s) = commit.new_signal {
        inner.signals.insert(
            s.signal_id.clone(),
            SignalRecord {
                signal_id: s.signal_id,
                run_id: commit.run_id.clone(),
                task_id: s.task_id,
                key: s.key,
                name: s.name,
                deadline_at: s.deadline_at,
                resolved_at: None,
            },
        );
    }
    for s in commit.resolved_signals {
        inner
            .signals
            .get_mut(&s.signal_id)
            .expect("validated signal")
            .resolved_at = Some(commit.now);
    }
    let row = inner.runs.get_mut(&commit.run_id).expect("validated run");
    if let Some(state) = commit.run_state {
        row.snapshot.status = state.status;
        row.snapshot.machine_json = state.machine_json;
        row.snapshot.output = state.output;
    }
    row.snapshot.node_occurrences = commit.node_occurrences;
    for event in commit.events {
        append_event(row, event, commit.now);
    }
    row.snapshot.version = result.run_version;
    row.updated_at = commit.now;
    result
}
