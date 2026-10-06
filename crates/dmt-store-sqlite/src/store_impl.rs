use crate::{
    SqliteStore, apply, claim,
    codec::{self, EventRow, SignalRow, SummaryRow, TaskRow},
    error::map_sqlx,
    read, sql,
};
use async_trait::async_trait;
use dmt_core::{
    Commit, Graph, GraphId, Micros, RunId, RunSnapshot, SignalRecord, TaskId, TaskRecord,
};
use dmt_store::{
    ApplyResult, ClaimRequest, ClaimedTask, EventRecord, HeartbeatResult, LeaseProof, RunFilter,
    RunSummary, Store, StoreError,
};

#[async_trait]
impl Store for SqliteStore {
    async fn register_graph(&self, graph: &Graph) -> Result<(), StoreError> {
        read::register(self, graph).await
    }
    async fn load_graph(&self, id: &GraphId, version: u32) -> Result<Option<Graph>, StoreError> {
        let json: Option<String> = sqlx::query_scalar(sql::SELECT_GRAPH_JSON)
            .bind(id.as_str())
            .bind(i64::from(version))
            .fetch_optional(&self.reader)
            .await
            .map_err(map_sqlx)?;
        json.map(|json| Graph::from_json(&json).map_err(codec::corrupt))
            .transpose()
    }
    async fn create_run(&self, commit: Commit) -> Result<RunId, StoreError> {
        apply::create(self, &commit).await
    }
    async fn apply(
        &self,
        commit: Commit,
        by: Option<LeaseProof>,
    ) -> Result<ApplyResult, StoreError> {
        apply::apply(self, &commit, by.as_ref()).await
    }
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, StoreError> {
        read::load_run(self, run_id).await
    }
    async fn load_task(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        let row: Option<TaskRow> = sqlx::query_as(sql::SELECT_TASK)
            .bind(task_id.as_str())
            .fetch_optional(&self.reader)
            .await
            .map_err(map_sqlx)?;
        row.map(TaskRow::decode).transpose()
    }
    async fn claim_ready(&self, req: ClaimRequest) -> Result<Vec<ClaimedTask>, StoreError> {
        claim::claim(self, &req).await
    }
    async fn heartbeat(
        &self,
        proof: &LeaseProof,
        now: Micros,
        lease_micros: i64,
    ) -> Result<HeartbeatResult, StoreError> {
        read::heartbeat(self, proof, now, lease_micros).await
    }
    async fn find_open_signal(
        &self,
        run_id: &RunId,
        name: &str,
    ) -> Result<Option<SignalRecord>, StoreError> {
        let row: Option<SignalRow> = sqlx::query_as(sql::FIND_OPEN_SIGNAL)
            .bind(run_id.as_str())
            .bind(name)
            .fetch_optional(&self.reader)
            .await
            .map_err(map_sqlx)?;
        row.map(SignalRow::decode).transpose()
    }
    async fn due_signals(
        &self,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<SignalRecord>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows: Vec<SignalRow> = sqlx::query_as(sql::DUE_SIGNALS)
            .bind(now.0)
            .bind(codec::limit(limit))
            .fetch_all(&self.reader)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter().map(SignalRow::decode).collect()
    }
    async fn exhausted_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows: Vec<TaskRow> = sqlx::query_as(sql::EXHAUSTED_TASKS)
            .bind(codec::limit(limit))
            .fetch_all(&self.reader)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter().map(TaskRow::decode).collect()
    }
    async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        let Ok(after_seq) = i64::try_from(after_seq) else {
            return Ok(Vec::new());
        };
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows: Vec<EventRow> = sqlx::query_as(sql::SELECT_EVENTS)
            .bind(run_id.as_str())
            .bind(after_seq)
            .bind(codec::limit(limit))
            .fetch_all(&self.reader)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter().map(EventRow::decode).collect()
    }
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        if filter.limit == 0 {
            return Ok(Vec::new());
        }
        let rows: Vec<SummaryRow> = sqlx::query_as(sql::LIST_RUNS)
            .bind(filter.status.map(|status| status.to_string()))
            .bind(filter.graph_id.as_ref().map(GraphId::as_str))
            .bind(codec::limit(filter.limit))
            .fetch_all(&self.reader)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter().map(SummaryRow::decode).collect()
    }
}
