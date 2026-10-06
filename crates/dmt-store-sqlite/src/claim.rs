use crate::{
    SqliteStore,
    codec::{Candidate, run_status, u32_from},
    error::map_sqlx,
    sql,
    tx::WriteTx,
    writes::append_event,
};
use dmt_core::{ExhaustReason, RunEvent, TaskEvent, TaskRecord, TaskStatus};
use dmt_store::{ClaimRequest, ClaimedTask, LeaseProof, StoreError};
use sqlx::types::Json;

const CLAIM_PAGE_SIZE: usize = 64;

pub(crate) async fn claim(
    store: &SqliteStore,
    req: &ClaimRequest,
) -> Result<Vec<ClaimedTask>, StoreError> {
    if req.limit == 0 || req.graphs.is_empty() {
        return Ok(Vec::new());
    }
    let probe: Option<i64> = sqlx::query_scalar(sql::CLAIM_PROBE)
        .bind(Json(&req.graphs))
        .bind(req.now.0)
        .fetch_optional(&store.reader)
        .await
        .map_err(map_sqlx)?;
    if probe.is_none() {
        return Ok(Vec::new());
    }
    let mut tx = WriteTx::begin(store).await?;
    let mut claimed = Vec::new();
    let mut cursor: Option<(i64, String)> = None;
    loop {
        let page: Vec<Candidate> = tx
            .fetch_all(
                sqlx::query_as(sql::CLAIM_PAGE)
                    .bind(Json(&req.graphs))
                    .bind(req.now.0)
                    .bind(i64::from(cursor.is_none()))
                    .bind(cursor.as_ref().map_or(0, |(time, _)| *time))
                    .bind(cursor.as_ref().map_or("", |(_, id)| id.as_str()))
                    .bind(i64::try_from(CLAIM_PAGE_SIZE).map_err(crate::codec::corrupt)?),
            )
            .await?;
        let count = page.len();
        for candidate in page {
            if claimed.len() == req.limit {
                break;
            }
            cursor = Some((candidate.task.run_at, candidate.task.id.clone()));
            if let Some(task) = visit(&mut tx, req, candidate).await? {
                claimed.push(task);
            }
        }
        if claimed.len() == req.limit || count < CLAIM_PAGE_SIZE {
            break;
        }
    }
    tx.commit().await?;
    Ok(claimed)
}

async fn visit(
    tx: &mut WriteTx,
    req: &ClaimRequest,
    candidate: Candidate,
) -> Result<Option<ClaimedTask>, StoreError> {
    let mut task = candidate.task.decode()?;
    if run_status(&candidate.run_status)?.is_terminal() {
        clear(tx, &task, TaskEvent::RunTerminal, req.now).await?;
        return Ok(None);
    }
    if task.status == TaskStatus::Running && task.attempt >= task.max_attempts {
        clear(tx, &task, TaskEvent::LeaseExhausted, req.now).await?;
        event(
            tx,
            &task,
            &RunEvent::TaskExhausted {
                task_id: task.task_id.clone(),
                reason: ExhaustReason::LeaseReclaimsExceeded,
            },
            req.now,
        )
        .await?;
        return Ok(None);
    }
    if task.status == TaskStatus::Running {
        task.status = transition(task.status, TaskEvent::Reclaim)?;
        task.attempt = task
            .attempt
            .checked_add(1)
            .ok_or_else(|| crate::codec::corrupt("task attempt overflow"))?;
    } else {
        task.status = transition(task.status, TaskEvent::Claim)?;
    }
    let lease_until = req.now.saturating_add(req.lease_micros);
    task.lease_owner = Some(req.worker_id.clone());
    task.lease_until = Some(lease_until);
    tx.execute(
        sqlx::query(sql::TASK_CLAIM)
            .bind(task.task_id.as_str())
            .bind(task.status.to_string())
            .bind(i64::from(task.attempt))
            .bind(req.worker_id.as_str())
            .bind(lease_until.0)
            .bind(req.now.0),
    )
    .await?;
    event(
        tx,
        &task,
        &RunEvent::TaskClaimed {
            task_id: task.task_id.clone(),
            worker_id: req.worker_id.clone(),
            attempt: task.attempt,
            lease_until,
        },
        req.now,
    )
    .await?;
    Ok(Some(ClaimedTask {
        proof: LeaseProof {
            task_id: task.task_id.clone(),
            worker_id: req.worker_id.clone(),
            attempt: task.attempt,
        },
        task,
        graph_id: candidate.graph_id.into(),
        graph_version: u32_from(candidate.graph_version)?,
    }))
}
fn transition(status: TaskStatus, event: TaskEvent) -> Result<TaskStatus, StoreError> {
    status.next(event).map_err(crate::codec::corrupt)
}
async fn clear(
    tx: &mut WriteTx,
    task: &TaskRecord,
    event: TaskEvent,
    now: dmt_core::Micros,
) -> Result<(), StoreError> {
    tx.execute(
        sqlx::query(sql::TASK_CLEAR_LEASE)
            .bind(task.task_id.as_str())
            .bind(transition(task.status, event)?.to_string())
            .bind(now.0),
    )
    .await?;
    Ok(())
}
async fn event(
    tx: &mut WriteTx,
    task: &TaskRecord,
    event: &RunEvent,
    now: dmt_core::Micros,
) -> Result<(), StoreError> {
    let row: Option<(i64,)> = tx
        .fetch_optional(sqlx::query_as(sql::ALLOC_SEQ).bind(task.run_id.as_str()))
        .await?;
    let (seq,) = row.ok_or_else(|| crate::codec::corrupt("missing claim run"))?;
    append_event(tx, &task.run_id, seq, event, now).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;
    #[tokio::test]
    async fn claim_statements_use_an_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        SqliteStore::migrate(&path).await.unwrap();
        let store = SqliteStore::open(path, crate::SqliteOptions::default())
            .await
            .unwrap();
        for statement in [sql::EXPLAIN_PROBE, sql::EXPLAIN_PAGE] {
            let mut query = sqlx::query(statement).bind("[\"linear\"]").bind(0_i64);
            if statement == sql::EXPLAIN_PAGE {
                query = query.bind(1_i64).bind(0_i64).bind("").bind(64_i64);
            }
            let rows = query.fetch_all(&store.reader).await.unwrap();
            let details: Vec<String> = rows.iter().map(|row| row.get("detail")).collect();
            assert!(
                details
                    .iter()
                    .any(|detail| detail.contains("SEARCH t USING")),
                "{details:?}"
            );
            assert!(
                details
                    .iter()
                    .all(|detail| !detail.starts_with("SCAN") || detail.contains("json_each")),
                "{details:?}"
            );
        }
    }
}
