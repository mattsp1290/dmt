use crate::{
    SqliteStore, checks,
    codec::{Head, run_status, u64_from},
    sql,
    tx::WriteTx,
    writes,
};
use dmt_core::{Commit, RunId};
use dmt_store::{ApplyResult, LeaseProof, StoreError};
use sqlx::types::Json;

pub(crate) async fn create(store: &SqliteStore, commit: &Commit) -> Result<RunId, StoreError> {
    let new = commit
        .new_run
        .as_ref()
        .filter(|_| commit.expected_run_version == 0)
        .ok_or_else(|| {
            StoreError::InvalidCommit("create requires new_run and version zero".into())
        })?;
    let mut tx = WriteTx::begin(store).await?;
    let hash: Option<(String,)> = tx
        .fetch_optional(
            sqlx::query_as(sql::SELECT_GRAPH_HASH)
                .bind(new.graph_id.as_str())
                .bind(i64::from(new.graph_version)),
        )
        .await?;
    let (hash,) = hash.ok_or_else(|| {
        StoreError::NotFound(format!("graph {}@{}", new.graph_id, new.graph_version))
    })?;
    if hash != new.definition_hash {
        return Err(StoreError::GraphMismatch {
            graph_id: new.graph_id.clone(),
            version: new.graph_version,
        });
    }
    let existing: Option<Head> = tx
        .fetch_optional(sqlx::query_as(sql::SELECT_RUN_HEAD).bind(commit.run_id.as_str()))
        .await?;
    if let Some(head) = existing {
        return Err(StoreError::VersionConflict {
            expected: 0,
            actual: u64_from(head.version)?,
        });
    }
    let results = checks::records(&mut tx, commit).await?;
    tx.execute(
        sqlx::query(sql::INSERT_RUN)
            .bind(commit.run_id.as_str())
            .bind(new.graph_id.as_str())
            .bind(i64::from(new.graph_version))
            .bind(&new.definition_hash)
            .bind(Json(&new.input))
            .bind(commit.now.0),
    )
    .await?;
    let head = Head {
        status: "created".into(),
        version: 0,
        next_seq: 1,
        graph_id: new.graph_id.to_string(),
    };
    writes::mutate(&mut tx, commit, &head, results).await?;
    tx.commit().await?;
    Ok(commit.run_id.clone())
}

pub(crate) async fn apply(
    store: &SqliteStore,
    commit: &Commit,
    by: Option<&LeaseProof>,
) -> Result<ApplyResult, StoreError> {
    let mut tx = WriteTx::begin(store).await?;
    let head: Option<Head> = tx
        .fetch_optional(sqlx::query_as(sql::SELECT_RUN_HEAD).bind(commit.run_id.as_str()))
        .await?;
    let head = head.ok_or_else(|| StoreError::NotFound(format!("run {}", commit.run_id)))?;
    if run_status(&head.status)?.is_terminal() {
        return Err(StoreError::RunTerminal {
            run_id: commit.run_id.clone(),
        });
    }
    let version = u64_from(head.version)?;
    if version != commit.expected_run_version {
        return Err(StoreError::VersionConflict {
            expected: commit.expected_run_version,
            actual: version,
        });
    }
    if commit.new_run.is_some() {
        return Err(StoreError::InvalidCommit(
            "apply cannot create a run".into(),
        ));
    }
    if let Some(proof) = by {
        checks::lease(&mut tx, commit, proof).await?;
    }
    if head.version == i64::MAX {
        return Err(StoreError::InvalidCommit("run version overflow".into()));
    }
    let results = checks::records(&mut tx, commit).await?;
    let result = writes::mutate(&mut tx, commit, &head, results).await?;
    tx.commit().await?;
    Ok(result)
}
