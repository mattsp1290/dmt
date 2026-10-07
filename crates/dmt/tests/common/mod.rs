use dmt::*;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
struct Work;
#[dmt::async_trait]
impl NodeHandler for Work {
    async fn run(&self, _: NodeContext, _: NodeInput) -> Result<NodeOutcome, HandlerError> {
        Ok(NodeOutcome::Done(Outcome::done("ok")))
    }
}
pub async fn quick_start(store: Arc<dyn Store>) {
    let graph = GraphBuilder::new("quick-start", 1)
        .start("work")
        .task("work")
        .wait("approval", "approve", None)
        .end("done", EndStatus::Completed)
        .edge("work", "approval")
        .edge_on("approval", "done", "approved")
        .build()
        .unwrap();
    let handle = Engine::builder()
        .store(store)
        .graph(graph)
        .handler("work", Arc::new(Work))
        .config(EngineConfig {
            poll_interval: Duration::from_millis(10),
            ..EngineConfig::default()
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let run = handle
        .start_run(&"quick-start".into(), Value::Null)
        .await
        .unwrap();
    let timeout = Duration::from_secs(5);
    assert_eq!(
        handle.wait_quiescent(&run, timeout).await.unwrap(),
        Quiescent::Parked
    );
    handle
        .signal(
            &run,
            "approve",
            SignalPayload {
                label: "approved".into(),
                payload: Value::Null,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        handle.wait_quiescent(&run, timeout).await.unwrap(),
        Quiescent::Terminal(RunStatus::Completed)
    );
    let events = handle.events(&run, 0, 1000).await.unwrap();
    assert_eq!(events.last().unwrap().event.kind(), "RunCompleted");
    assert_eq!(
        events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    handle.shutdown(timeout).await.unwrap();
}
