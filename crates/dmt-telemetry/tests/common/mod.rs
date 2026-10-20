// Integration binaries use different subsets of the fixtures.
#![allow(dead_code)]
use async_trait::async_trait;
use dmt_core::{Graph, NodeOutcome, Outcome};
use dmt_runtime::{
    Engine, EngineConfig, EngineHandle, HandlerError, NodeContext, NodeHandler, NodeInput,
};
use dmt_telemetry::{LogSink, TelemetryConfig};
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

pub fn config() -> TelemetryConfig {
    TelemetryConfig {
        metrics: false,
        logs: LogSink::Off,
        ..TelemetryConfig::from_vars(|_| None).unwrap()
    }
}
struct Handler(Option<Arc<tokio::sync::Barrier>>, bool);
#[async_trait]
impl NodeHandler for Handler {
    async fn run(&self, _: NodeContext, _: NodeInput) -> Result<NodeOutcome, HandlerError> {
        if self.1 {
            return Ok(NodeOutcome::FanOut(vec![1.into(), 2.into(), 3.into()]));
        }
        if let Some(barrier) = &self.0 {
            tokio::time::timeout(Duration::from_secs(10), barrier.wait())
                .await
                .map_err(|_| HandlerError::Permanent("branch barrier timed out".into()))?;
        }
        Ok(NodeOutcome::Done(Outcome::done("ok")))
    }
}
pub async fn engine(graph: Graph) -> EngineHandle {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut builder = Engine::builder()
        .store(Arc::new(dmt_store::MemoryStore::new()))
        .config(EngineConfig {
            workers: 1,
            claim_limit: 8,
            poll_interval: Duration::from_millis(5),
            ..EngineConfig::default()
        })
        .graph(graph.clone());
    for (id, node) in graph.nodes() {
        if matches!(
            node.kind,
            dmt_core::NodeKind::Wait { .. } | dmt_core::NodeKind::End { .. }
        ) {
            continue;
        }
        builder = builder.handler(
            id.clone(),
            Arc::new(Handler(
                (id.as_str() == "br").then(|| barrier.clone()),
                id.as_str() == "fo",
            )),
        );
    }
    builder.build().unwrap().start().await.unwrap()
}
#[derive(Default)]
pub struct SharedBuf(pub Mutex<Vec<u8>>);
impl io::Write for &SharedBuf {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
