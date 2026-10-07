use crate::common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

pub(super) fn payload(label: &str) -> SignalPayload {
    SignalPayload {
        label: label.into(),
        payload: Value::Null,
    }
}

pub(super) async fn loop_engine(
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    config: EngineConfig,
) -> EngineHandle {
    let done = handler(|_, _| async { done() });
    start(
        store,
        clock,
        fixtures::loop_via_wait(),
        config,
        &[("plan", done.clone()), ("implement", done)],
    )
    .await
}

pub(super) async fn parked(h: &EngineHandle, run: &RunId) {
    assert_eq!(
        h.wait_quiescent(run, Duration::from_secs(5)).await.unwrap(),
        Quiescent::Parked
    );
}

pub(super) fn wait_start_graph(deadline: Option<i64>) -> Graph {
    let builder = GraphBuilder::new("x", 1)
        .start("w")
        .wait("w", "go", deadline)
        .task("t")
        .end("done", EndStatus::Completed)
        .edge_on("w", "t", "ok")
        .edge("t", "done");
    if deadline.is_some() {
        builder.edge_on("w", "t", "timeout").build().unwrap()
    } else {
        builder.build().unwrap()
    }
}
