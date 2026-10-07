// Each integration binary uses a different subset of shared helpers.
#![allow(dead_code)]
use async_trait::async_trait;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Barrier, Semaphore};
pub const T0: Micros = Micros(1_000_000_000);
pub fn fast_config() -> EngineConfig {
    EngineConfig {
        poll_interval: Duration::from_millis(10),
        sweep_interval: Duration::from_millis(20),
        heartbeat_every: Duration::from_secs(3600),
        lease: Duration::from_secs(10),
        ..EngineConfig::default()
    }
}
// Handler closures share a fallible result signature even when this outcome succeeds.
#[allow(clippy::unnecessary_wraps)]
pub fn done() -> Result<NodeOutcome, HandlerError> {
    Ok(NodeOutcome::Done(Outcome::done("ok")))
}
type HandlerFuture = Pin<Box<dyn Future<Output = Result<NodeOutcome, HandlerError>> + Send>>;
struct FnHandler<F>(F);
#[async_trait]
impl<F> NodeHandler for FnHandler<F>
where
    F: Fn(NodeContext, NodeInput) -> HandlerFuture + Send + Sync,
{
    async fn run(&self, ctx: NodeContext, input: NodeInput) -> Result<NodeOutcome, HandlerError> {
        (self.0)(ctx, input).await
    }
}
pub fn handler<F, Fut>(f: F) -> Arc<dyn NodeHandler>
where
    F: Fn(NodeContext, NodeInput) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<NodeOutcome, HandlerError>> + Send + 'static,
{
    Arc::new(FnHandler(move |ctx, input| {
        Box::pin(f(ctx, input)) as HandlerFuture
    }))
}
#[derive(Clone, Default)]
pub struct Recorder(pub Arc<Mutex<Vec<NodeContext>>>);
impl Recorder {
    pub fn record(&self, ctx: NodeContext) {
        self.0.lock().unwrap().push(ctx);
    }
    pub fn entries(&self) -> Vec<NodeContext> {
        self.0.lock().unwrap().clone()
    }
    pub fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}
#[derive(Clone)]
pub struct Gate(Arc<Semaphore>);
impl Gate {
    pub fn new() -> Self {
        Self(Arc::new(Semaphore::new(0)))
    }
    pub async fn wait(&self) {
        self.0.acquire().await.unwrap().forget();
    }
    pub fn open(&self) {
        self.0.add_permits(1000);
    }
}
pub async fn eventually<F, Fut>(description: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(30), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out: {description}"));
}
pub async fn start(
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    graph: Graph,
    config: EngineConfig,
    handlers: &[(&str, Arc<dyn NodeHandler>)],
) -> EngineHandle {
    let mut builder = Engine::builder()
        .store(store)
        .clock(clock)
        .config(config)
        .graph(graph);
    for (id, h) in handlers {
        builder = builder.handler(*id, h.clone());
    }
    builder.build().unwrap().start().await.unwrap()
}
pub async fn completed(handle: &EngineHandle, run: &RunId) {
    assert_eq!(
        handle
            .wait_quiescent(run, Duration::from_secs(30))
            .await
            .unwrap(),
        Quiescent::Terminal(RunStatus::Completed)
    );
}
pub async fn events(handle: &EngineHandle, run: &RunId) -> Vec<RunEvent> {
    handle
        .events(run, 0, 1000)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.event)
        .collect()
}
pub async fn kind_count(handle: &EngineHandle, run: &RunId, kind: &str) -> usize {
    events(handle, run)
        .await
        .iter()
        .filter(|e| e.kind() == kind)
        .count()
}
pub fn task_id(run: &RunId, node: &str) -> TaskId {
    StepKey::task(run, &node.into(), 0).as_str().into()
}

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

#[async_trait]
impl Store for CountingStore {
    async fn register_graph(&self, graph: &Graph) -> Result<(), StoreError> {
        let fault = self.fault("register_graph");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.register_graph(graph).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn load_graph(&self, id: &GraphId, version: u32) -> Result<Option<Graph>, StoreError> {
        let fault = self.fault("load_graph");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.load_graph(id, version).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn create_run(&self, commit: Commit) -> Result<RunId, StoreError> {
        let fault = self.fault("create_run");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.create_run(commit).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, StoreError> {
        let fault = self.fault("load_run");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.load_run(run_id).await
        };
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
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn load_task(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        let fault = self.fault("load_task");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.load_task(task_id).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn claim_ready(&self, req: ClaimRequest) -> Result<Vec<ClaimedTask>, StoreError> {
        self.limits.lock().unwrap().push(req.limit);
        let fault = self.fault("claim_ready");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.claim_ready(req).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn heartbeat(
        &self,
        proof: &LeaseProof,
        now: Micros,
        lease_micros: i64,
    ) -> Result<HeartbeatResult, StoreError> {
        let fault = self.fault("heartbeat");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        if matches!(fault, Some(Fault::Lost)) {
            return Ok(HeartbeatResult::Lost);
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.heartbeat(proof, now, lease_micros).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn apply(
        &self,
        commit: Commit,
        by: Option<LeaseProof>,
    ) -> Result<ApplyResult, StoreError> {
        let proof = by.clone();
        let fault = self.fault("apply");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.apply(commit, by).await
        };
        let result = if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        };
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
        let fault = self.fault("find_open_signal");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.find_open_signal(run_id, name).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn due_signals(
        &self,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<SignalRecord>, StoreError> {
        let fault = self.fault("due_signals");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.due_signals(now, limit).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn exhausted_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError> {
        let fault = self.fault("exhausted_tasks");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.exhausted_tasks(limit).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        let fault = self.fault("events");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.events(run_id, after_seq, limit).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        let fault = self.fault("list_runs");
        if let Some(Fault::Delay(duration)) = &fault {
            tokio::time::sleep(*duration).await;
        }
        let result = if let Some(Fault::Instead(ref error)) = fault {
            Err(error.clone())
        } else {
            self.inner.list_runs(filter).await
        };
        if matches!(fault, Some(Fault::After)) && result.is_ok() {
            Err(StoreError::Backend("committed but response lost".into()))
        } else {
            result
        }
    }
}
pub async fn run_contention() -> (Arc<CountingStore>, EngineHandle, RunId) {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let clock = Arc::new(ManualClock::new(T0));
    let branches = Recorder::default();
    let gate = Gate::new();
    let record = branches.clone();
    let wait = gate.clone();
    let h = start(
        store.clone(),
        clock,
        fixtures::fan_out(JoinPolicy::All),
        EngineConfig {
            workers: 1,
            claim_limit: 8,
            ..fast_config()
        },
        &[
            ("a", handler(|_, _| async { done() })),
            (
                "fo",
                handler(|_, _| async {
                    Ok(NodeOutcome::FanOut((0..8).map(|i| json!(i)).collect()))
                }),
            ),
            (
                "br",
                handler(move |ctx, _| {
                    let record = record.clone();
                    let wait = wait.clone();
                    async move {
                        record.record(ctx);
                        wait.wait().await;
                        done()
                    }
                }),
            ),
            ("jn", handler(|_, _| async { done() })),
        ],
    )
    .await;
    let run = h.start_run(&"fan-out".into(), Value::Null).await.unwrap();
    eventually("eight branches", || async { branches.count() == 8 }).await;
    store.hold_next_load_runs(8);
    gate.open();
    eventually("contention completes", || async {
        store.inner.load_run(&run).await.unwrap().unwrap().status == RunStatus::Completed
    })
    .await;
    completed(&h, &run).await;
    (store, h, run)
}
