use crate::{
    SqliteStore,
    codec::{JoinRow, RunRow, SignalRow, TaskRow},
    error::map_sqlx,
    sql,
};
use dmt_core::{RunId, RunSnapshot};
use dmt_store::StoreError;

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
