use crate::{
    codec::{JoinRow, u32_from},
    sql,
    tx::WriteTx,
};
use dmt_core::{BranchResult, Commit};
use dmt_store::{LeaseProof, StoreError};
use std::collections::BTreeSet;

pub(crate) async fn lease(
    tx: &mut WriteTx,
    commit: &Commit,
    proof: &LeaseProof,
) -> Result<(), StoreError> {
    let row: Option<(String, String, Option<String>, i64)> = tx
        .fetch_optional(sqlx::query_as(sql::SELECT_TASK_LEASE).bind(proof.task_id.as_str()))
        .await?;
    let (run_id, status, owner, attempt) =
        row.ok_or_else(|| StoreError::NotFound(format!("task {}", proof.task_id)))?;
    if run_id != commit.run_id.as_str()
        || status != "running"
        || owner.as_deref() != Some(proof.worker_id.as_str())
        || u32_from(attempt)? != proof.attempt
    {
        return Err(StoreError::LeaseLost {
            task_id: proof.task_id.clone(),
        });
    }
    Ok(())
}

pub(crate) async fn records(
    tx: &mut WriteTx,
    commit: &Commit,
) -> Result<Option<Vec<BranchResult>>, StoreError> {
    for update in &commit.task_updates {
        let row: Option<(String,)> = tx
            .fetch_optional(sqlx::query_as(sql::SELECT_TASK_RUN).bind(update.task_id.as_str()))
            .await?;
        if row.is_none_or(|(run,)| run != commit.run_id.as_str()) {
            return Err(StoreError::NotFound(format!("task {}", update.task_id)));
        }
    }
    let results = join(tx, commit).await?;
    let mut resolved = BTreeSet::new();
    for resolution in &commit.resolved_signals {
        let row: Option<(String, Option<i64>)> = tx
            .fetch_optional(
                sqlx::query_as(sql::SELECT_SIGNAL_STATE).bind(resolution.signal_id.as_str()),
            )
            .await?;
        let (_, resolved_at) = row
            .filter(|(run, _)| run == commit.run_id.as_str())
            .ok_or_else(|| StoreError::NotFound(format!("signal {}", resolution.signal_id)))?;
        if resolved_at.is_some() || !resolved.insert(&resolution.signal_id) {
            return Err(StoreError::AlreadyResolved {
                signal_id: resolution.signal_id.clone(),
            });
        }
    }
    if let Some(signal) = &commit.new_signal {
        let row: Option<(i64,)> = tx
            .fetch_optional(
                sqlx::query_as(sql::SIGNAL_CONFLICT)
                    .bind(signal.signal_id.as_str())
                    .bind(commit.run_id.as_str())
                    .bind(signal.key.as_str()),
            )
            .await?;
        if row.is_some() {
            return Err(StoreError::InvalidCommit(
                "duplicate signal id or key".into(),
            ));
        }
    }
    if let Some(join) = &commit.new_join {
        if join.run_id != commit.run_id {
            return Err(StoreError::InvalidCommit("wrong join run".into()));
        }
        let row: Option<(i64,)> = tx
            .fetch_optional(sqlx::query_as(sql::JOIN_EXISTS).bind(join.join_id.as_str()))
            .await?;
        if row.is_some() {
            return Err(StoreError::InvalidCommit("duplicate join id".into()));
        }
    }
    for task in &commit.new_tasks {
        if task.task_id.as_str() != task.step_key.as_str() {
            return Err(StoreError::InvalidCommit(
                "task id differs from step key".into(),
            ));
        }
    }
    Ok(results)
}

async fn join(tx: &mut WriteTx, commit: &Commit) -> Result<Option<Vec<BranchResult>>, StoreError> {
    let Some(c) = &commit.join_contribution else {
        return Ok(None);
    };
    let row: Option<JoinRow> = tx
        .fetch_optional(sqlx::query_as(sql::SELECT_JOIN_GUARD).bind(c.join_id.as_str()))
        .await?;
    let row = row
        .filter(|row| row.run_id == commit.run_id.as_str())
        .ok_or_else(|| StoreError::NotFound(format!("join {}", c.join_id)))?
        .decode()?;
    if row.satisfied_at.is_some()
        || row
            .received
            .checked_add(u32::from(matches!(c.result, BranchResult::Done { .. })))
            != Some(c.expected_received)
        || row
            .failed
            .checked_add(u32::from(matches!(c.result, BranchResult::Failed { .. })))
            != Some(c.expected_failed)
    {
        return Err(StoreError::JoinDrift {
            join_id: c.join_id.clone(),
        });
    }
    Ok(Some(row.results))
}
