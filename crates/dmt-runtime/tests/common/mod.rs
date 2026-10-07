// Each integration binary uses a different subset of shared helpers.
#![allow(dead_code)]
use async_trait::async_trait;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;
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

pub mod store;
pub use store::CountingStore;

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
