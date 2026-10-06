use crate::{
    SqliteStore,
    codec::{Head, i64_from},
    sql,
    tx::WriteTx,
};
use dmt_core::{BranchResult, Commit, Graph, JoinId, Micros, RunEvent, RunId};
use dmt_store::{ApplyResult, HeartbeatResult, LeaseProof, StoreError};
use sqlx::types::Json;

pub(crate) async fn mutate(
    tx: &mut WriteTx,
    commit: &Commit,
    head: &Head,
    results: Option<Vec<BranchResult>>,
) -> Result<ApplyResult, StoreError> {
    let version = head
        .version
        .checked_add(1)
        .ok_or_else(|| StoreError::InvalidCommit("run version overflow".into()))?;
    let mut result = ApplyResult {
        run_version: u64::try_from(version).map_err(crate::codec::corrupt)?,
        ..ApplyResult::default()
    };
    tasks(tx, commit, head, &mut result).await?;
    result.join_satisfied = joins(tx, commit, results).await?;
    signals(tx, commit).await?;
    let count = i64::try_from(commit.events.len()).map_err(crate::codec::corrupt)?;
    let next_seq = head
        .next_seq
        .checked_add(count)
        .ok_or_else(|| StoreError::Backend("event sequence overflow".into()))?;
    for (offset, event) in commit.events.iter().enumerate() {
        let offset = i64::try_from(offset).map_err(crate::codec::corrupt)?;
        append_event(
            tx,
            &commit.run_id,
            head.next_seq + offset,
            event,
            commit.now,
        )
        .await?;
    }
    let statement = if commit.run_state.is_some() {
        sql::UPDATE_RUN_WITH_STATE
    } else {
        sql::UPDATE_RUN
    };
    let mut query = sqlx::query(statement)
        .bind(commit.run_id.as_str())
        .bind(version)
        .bind(next_seq)
        .bind(Json(&commit.node_occurrences))
        .bind(commit.now.0)
        .bind(i64_from(commit.expected_run_version)?);
    if let Some(state) = &commit.run_state {
        query = query
            .bind(state.status.to_string())
            .bind(Json(&state.machine_json))
            .bind(state.output.as_ref().map(Json));
    }
    if tx.execute(query).await? != 1 {
        return Err(StoreError::Backend("run changed under write lock".into()));
    }
    Ok(result)
}

async fn tasks(
    tx: &mut WriteTx,
    commit: &Commit,
    head: &Head,
    result: &mut ApplyResult,
) -> Result<(), StoreError> {
    for update in &commit.task_updates {
        tx.execute(
            sqlx::query(sql::UPDATE_TASK)
                .bind(update.task_id.as_str())
                .bind(update.status.to_string())
                .bind(i64::from(update.attempt))
                .bind(update.run_at.0)
                .bind(update.outcome.as_ref().map(Json))
                .bind(update.planned_at.map(|time| time.0))
                .bind(commit.now.0),
        )
        .await?;
    }
    for task in &commit.new_tasks {
        let inserted = tx
            .execute(
                sqlx::query(sql::INSERT_TASK)
                    .bind(task.task_id.as_str())
                    .bind(commit.run_id.as_str())
                    .bind(&head.graph_id)
                    .bind(task.node_id.as_str())
                    .bind(task.step_key.as_str())
                    .bind(task.status.to_string())
                    .bind(i64::from(task.attempt))
                    .bind(i64::from(task.max_attempts))
                    .bind(task.run_at.0)
                    .bind(Json(&task.input))
                    .bind(task.branch.as_ref().map(|branch| branch.join_id.as_str()))
                    .bind(task.branch.as_ref().map(|branch| i64::from(branch.index)))
                    .bind(commit.now.0),
            )
            .await?;
        if inserted == 1 {
            result.inserted_tasks.push(task.task_id.clone());
        } else {
            result.ignored_tasks.push(task.step_key.clone());
        }
    }
    Ok(())
}

async fn joins(
    tx: &mut WriteTx,
    commit: &Commit,
    results: Option<Vec<BranchResult>>,
) -> Result<Option<JoinId>, StoreError> {
    if let Some(join) = &commit.new_join {
        tx.execute(
            sqlx::query(sql::INSERT_JOIN)
                .bind(join.join_id.as_str())
                .bind(join.run_id.as_str())
                .bind(join.node_id.as_str())
                .bind(join.step_key.as_str())
                .bind(Json(&join.policy))
                .bind(i64::from(join.expected))
                .bind(i64::from(join.received))
                .bind(i64::from(join.failed))
                .bind(Json(&join.results))
                .bind(join.satisfied_at.map(|time| time.0)),
        )
        .await?;
    }
    if let Some(c) = &commit.join_contribution {
        let mut results =
            results.ok_or_else(|| StoreError::Backend("missing checked join results".into()))?;
        results.push(c.result.clone());
        let affected = tx
            .execute(
                sqlx::query(sql::UPDATE_JOIN_CONTRIBUTION)
                    .bind(c.join_id.as_str())
                    .bind(i64::from(c.expected_received))
                    .bind(i64::from(c.expected_failed))
                    .bind(Json(results))
                    .bind(c.satisfied.then_some(commit.now).map(|time| time.0)),
            )
            .await?;
        if affected == 0 {
            return Err(StoreError::JoinDrift {
                join_id: c.join_id.clone(),
            });
        }
        if c.satisfied {
            return Ok(Some(c.join_id.clone()));
        }
    }
    Ok(None)
}

async fn signals(tx: &mut WriteTx, commit: &Commit) -> Result<(), StoreError> {
    if let Some(signal) = &commit.new_signal {
        tx.execute(
            sqlx::query(sql::INSERT_SIGNAL)
                .bind(signal.signal_id.as_str())
                .bind(commit.run_id.as_str())
                .bind(signal.task_id.as_str())
                .bind(signal.key.as_str())
                .bind(&signal.name)
                .bind(signal.deadline_at.map(|time| time.0)),
        )
        .await?;
    }
    for resolution in &commit.resolved_signals {
        let affected = tx
            .execute(
                sqlx::query(sql::RESOLVE_SIGNAL)
                    .bind(resolution.signal_id.as_str())
                    .bind(commit.now.0)
                    .bind(&resolution.label)
                    .bind(Json(&resolution.payload)),
            )
            .await?;
        if affected == 0 {
            return Err(StoreError::AlreadyResolved {
                signal_id: resolution.signal_id.clone(),
            });
        }
    }
    Ok(())
}

pub(crate) async fn append_event(
    tx: &mut WriteTx,
    run_id: &RunId,
    seq: i64,
    event: &RunEvent,
    now: Micros,
) -> Result<(), StoreError> {
    tx.execute(
        sqlx::query(sql::INSERT_EVENT)
            .bind(run_id.as_str())
            .bind(seq)
            .bind(event.kind())
            .bind(Json(event))
            .bind(now.0),
    )
    .await?;
    Ok(())
}

pub(crate) async fn register(store: &SqliteStore, graph: &Graph) -> Result<(), StoreError> {
    let mut tx = WriteTx::begin(store).await?;
    let hash: Option<(String,)> = tx
        .fetch_optional(
            sqlx::query_as(sql::SELECT_GRAPH_HASH)
                .bind(graph.id().as_str())
                .bind(i64::from(graph.version())),
        )
        .await?;
    if let Some((hash,)) = hash {
        return if hash == graph.definition_hash() {
            Ok(())
        } else {
            Err(StoreError::GraphMismatch {
                graph_id: graph.id().clone(),
                version: graph.version(),
            })
        };
    }
    tx.execute(
        sqlx::query(sql::INSERT_GRAPH)
            .bind(graph.id().as_str())
            .bind(i64::from(graph.version()))
            .bind(graph.definition_hash())
            .bind(Json(graph)),
    )
    .await?;
    tx.commit().await
}

pub(crate) async fn heartbeat(
    store: &SqliteStore,
    proof: &LeaseProof,
    now: dmt_core::Micros,
    lease_micros: i64,
) -> Result<HeartbeatResult, StoreError> {
    let mut tx = WriteTx::begin(store).await?;
    let lease_until = now.saturating_add(lease_micros);
    if tx
        .execute(
            sqlx::query(sql::HEARTBEAT)
                .bind(proof.task_id.as_str())
                .bind(lease_until.0)
                .bind(now.0)
                .bind(proof.worker_id.as_str())
                .bind(i64::from(proof.attempt)),
        )
        .await?
        == 1
    {
        tx.commit().await?;
        return Ok(HeartbeatResult::Extended { lease_until });
    }
    let exists: Option<(i64,)> = tx
        .fetch_optional(sqlx::query_as(sql::TASK_EXISTS).bind(proof.task_id.as_str()))
        .await?;
    if exists.is_none() {
        return Err(StoreError::NotFound(format!("task {}", proof.task_id)));
    }
    Ok(HeartbeatResult::Lost)
}
