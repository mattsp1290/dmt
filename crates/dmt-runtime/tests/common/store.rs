use async_trait::async_trait;
use dmt_core::*;
use dmt_store::*;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Barrier;

#[derive(Clone, Debug)]
pub enum Fault {
    Delay(Duration),
    Instead(StoreError),
    After,
    Lost,
}
#[derive(Clone, Debug)]
pub struct Applied {
    pub proof: Option<LeaseProof>,
    pub result: Result<ApplyResult, StoreError>,
}
struct ReadBarrier {
    remaining: usize,
    barrier: Arc<Barrier>,
}
pub struct CountingStore {
    pub inner: Arc<dyn Store>,
    pub applies: Mutex<Vec<Applied>>,
    pub limits: Mutex<Vec<usize>>,
    barrier: Mutex<Option<ReadBarrier>>,
    scripts: Mutex<BTreeMap<&'static str, VecDeque<Fault>>>,
    calls: Mutex<BTreeMap<&'static str, usize>>,
}
impl CountingStore {
    pub fn new(inner: Arc<dyn Store>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            applies: Mutex::default(),
            limits: Mutex::default(),
            barrier: Mutex::default(),
            scripts: Mutex::default(),
            calls: Mutex::default(),
        })
    }
    pub fn hold_next_load_runs(&self, n: usize) {
        *self.barrier.lock().unwrap() = Some(ReadBarrier {
            remaining: n,
            barrier: Arc::new(Barrier::new(n)),
        });
    }
    pub fn script(&self, method: &'static str, faults: Vec<Fault>) {
        self.scripts.lock().unwrap().insert(method, faults.into());
    }
    pub fn calls(&self, method: &'static str) -> usize {
        self.calls.lock().unwrap().get(method).copied().unwrap_or(0)
    }
    fn fault(&self, method: &'static str) -> Option<Fault> {
        *self.calls.lock().unwrap().entry(method).or_default() += 1;
        self.scripts
            .lock()
            .unwrap()
            .get_mut(method)
            .and_then(VecDeque::pop_front)
    }
    pub fn records(&self) -> Vec<Applied> {
        self.applies.lock().unwrap().clone()
    }
}

async fn respond<T>(
    fault: Option<Fault>,
    work: impl Future<Output = Result<T, StoreError>>,
) -> Result<T, StoreError> {
    match fault {
        Some(Fault::Instead(error)) => Err(error),
        Some(Fault::After) => {
            work.await?;
            Err(StoreError::Backend("committed but response lost".into()))
        }
        Some(Fault::Delay(duration)) => {
            tokio::time::sleep(duration).await;
            work.await
        }
        Some(Fault::Lost) => panic!("Lost is only valid for heartbeat"),
        None => work.await,
    }
}
impl CountingStore {
    async fn call<T>(
        &self,
        method: &'static str,
        work: impl Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        respond(self.fault(method), work).await
    }
    async fn hold_snapshot(&self) {
        let barrier = {
            let mut slot = self.barrier.lock().unwrap();
            slot.as_mut().and_then(|b| {
                if b.remaining == 0 {
                    None
                } else {
                    b.remaining -= 1;
                    Some(b.barrier.clone())
                }
            })
        };
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
    }
}
#[async_trait]
impl Store for CountingStore {
    async fn register_graph(&self, graph: &Graph) -> Result<(), StoreError> {
        self.call("register_graph", self.inner.register_graph(graph))
            .await
    }
    async fn load_graph(&self, id: &GraphId, version: u32) -> Result<Option<Graph>, StoreError> {
        self.call("load_graph", self.inner.load_graph(id, version))
            .await
    }
    async fn create_run(&self, commit: Commit) -> Result<RunId, StoreError> {
        self.call("create_run", self.inner.create_run(commit)).await
    }
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, StoreError> {
        let fault = self.fault("load_run");
        let work = async {
            let result = self.inner.load_run(run_id).await;
            self.hold_snapshot().await;
            result
        };
        respond(fault, work).await
    }
    async fn load_task(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        self.call("load_task", self.inner.load_task(task_id)).await
    }
    async fn claim_ready(&self, req: ClaimRequest) -> Result<Vec<ClaimedTask>, StoreError> {
        self.limits.lock().unwrap().push(req.limit);
        self.call("claim_ready", self.inner.claim_ready(req)).await
    }
    async fn heartbeat(
        &self,
        proof: &LeaseProof,
        now: Micros,
        lease_micros: i64,
    ) -> Result<HeartbeatResult, StoreError> {
        let fault = self.fault("heartbeat");
        if matches!(fault, Some(Fault::Lost)) {
            return Ok(HeartbeatResult::Lost);
        }
        respond(fault, self.inner.heartbeat(proof, now, lease_micros)).await
    }
    async fn apply(
        &self,
        commit: Commit,
        by: Option<LeaseProof>,
    ) -> Result<ApplyResult, StoreError> {
        let proof = by.clone();
        let result = self.call("apply", self.inner.apply(commit, by)).await;
        self.applies.lock().unwrap().push(Applied {
            proof,
            result: result.clone(),
        });
        result
    }
    async fn find_open_signal(
        &self,
        run_id: &RunId,
        name: &str,
    ) -> Result<Option<SignalRecord>, StoreError> {
        self.call(
            "find_open_signal",
            self.inner.find_open_signal(run_id, name),
        )
        .await
    }
    async fn due_signals(
        &self,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<SignalRecord>, StoreError> {
        self.call("due_signals", self.inner.due_signals(now, limit))
            .await
    }
    async fn exhausted_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError> {
        self.call("exhausted_tasks", self.inner.exhausted_tasks(limit))
            .await
    }
    async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.call("events", self.inner.events(run_id, after_seq, limit))
            .await
    }
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        self.call("list_runs", self.inner.list_runs(filter)).await
    }
}
