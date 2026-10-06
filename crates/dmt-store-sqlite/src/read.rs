use crate::{
    SqliteStore,
    codec::{JoinRow, RunRow, SignalRow, TaskRow},
    error::map_sqlx,
    sql,
    tx::WriteTx,
};
use dmt_core::{Graph, RunId, RunSnapshot};
use dmt_store::{HeartbeatResult, LeaseProof, StoreError};
use sqlx::types::Json;

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

pub(crate) async fn load_run(
    store: &SqliteStore,
    run_id: &RunId,
) -> Result<Option<RunSnapshot>, StoreError> {
    let mut tx = store.reader.begin().await.map_err(map_sqlx)?;
    let row: Option<RunRow> = sqlx::query_as(sql::SELECT_RUN)
        .bind(run_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut snapshot = row.decode()?;
    let tasks: Vec<TaskRow> = sqlx::query_as(sql::SELECT_RUN_TASKS)
        .bind(run_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx)?;
    snapshot.tasks = tasks
        .into_iter()
        .map(TaskRow::decode)
        .collect::<Result<_, _>>()?;
    let joins: Vec<JoinRow> = sqlx::query_as(sql::SELECT_RUN_JOINS)
        .bind(run_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx)?;
    snapshot.joins = joins
        .into_iter()
        .map(JoinRow::decode)
        .collect::<Result<_, _>>()?;
    let signals: Vec<SignalRow> = sqlx::query_as(sql::SELECT_OPEN_SIGNALS)
        .bind(run_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx)?;
    snapshot.signals = signals
        .into_iter()
        .map(SignalRow::decode)
        .collect::<Result<_, _>>()?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(Some(snapshot))
}
