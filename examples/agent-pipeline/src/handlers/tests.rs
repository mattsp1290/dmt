use super::*;
use dmt::{
    CancellationToken, Engine, EngineConfig, MemoryStore, Quiescent, RunStatus, SignalPayload,
};
use std::time::Duration;
fn context(node: &str) -> NodeContext {
    NodeContext {
        run_id: "r".into(),
        graph_id: graph::GRAPH_ID.into(),
        graph_version: 1,
        node_id: node.into(),
        task_id: format!("r/{node}/0").into(),
        step_key: dmt::StepKey::task(&"r".into(), &node.into(), 0),
        attempt: 2,
        cancel: CancellationToken::new(),
    }
}
#[test]
fn repeat_step_returns_recorded_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let effects = Effects {
        ledger: Ledger::open(dir.path()),
        crash: None,
    };
    let ctx = context(graph::PLAN);
    let recorded = done("recorded", json!({"first": true}));
    effects
        .ledger
        .perform_once(ctx.step_key.as_str(), || recorded.clone())
        .unwrap();
    assert_eq!(
        step(&effects, &ctx, &NodeInput::Task(Value::Null), plan).unwrap(),
        recorded
    );
    assert_eq!(crate::ledger::read_entries(dir.path()).unwrap().len(), 1);
    assert_eq!(
        crate::ledger::read_invocations(dir.path()).unwrap().len(),
        1
    );
}
#[test]
fn fan_out_repeat_returns_fan_out() {
    let dir = tempfile::tempdir().unwrap();
    let effects = Effects {
        ledger: Ledger::open(dir.path()),
        crash: None,
    };
    let ctx = context(graph::REVIEW_FANOUT);
    let input = NodeInput::Task(Value::Null);
    let recorded = fan_out(&ctx, &input);
    effects
        .ledger
        .perform_once(ctx.step_key.as_str(), || recorded.clone())
        .unwrap();
    assert_eq!(step(&effects, &ctx, &input, fan_out).unwrap(), recorded);
    assert!(matches!(recorded, NodeOutcome::FanOut(_)));
    assert_eq!(crate::ledger::read_entries(dir.path()).unwrap().len(), 1);
}
#[tokio::test]
async fn pipeline_completes_in_process() {
    let dir = tempfile::tempdir().unwrap();
    let effects = Arc::new(Effects {
        ledger: Ledger::open(dir.path()),
        crash: None,
    });
    let builder = Engine::builder()
        .store(Arc::new(MemoryStore::new()))
        .graph(graph::pipeline())
        .config(EngineConfig {
            workers: 1,
            claim_limit: 1,
            poll_interval: Duration::from_millis(10),
            ..EngineConfig::default()
        });
    let handle = register(builder, &effects)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let run = handle
        .start_run(&graph::GRAPH_ID.into(), Value::Null)
        .await
        .unwrap();
    for (name, label) in [
        (graph::SIGNOFF, graph::CHANGES_REQUESTED),
        (graph::SIGNOFF, graph::APPROVED),
        (graph::ACK, graph::ACKNOWLEDGED),
    ] {
        assert_eq!(
            handle
                .wait_quiescent(&run, Duration::from_secs(5))
                .await
                .unwrap(),
            Quiescent::Parked
        );
        handle
            .signal(
                &run,
                name,
                SignalPayload {
                    label: label.into(),
                    payload: Value::Null,
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(
        handle
            .wait_quiescent(&run, Duration::from_secs(5))
            .await
            .unwrap(),
        Quiescent::Terminal(RunStatus::Completed)
    );
    assert_pipeline_ledger(dir.path(), &run);
    let events = handle.events(&run, 0, 1000).await.unwrap();
    for (kind, count) in [
        ("TaskCompleted", 14),
        ("RunParked", 3),
        ("RunResumed", 3),
        ("SignalReceived", 3),
        ("JoinSatisfied", 1),
    ] {
        assert_eq!(
            events.iter().filter(|e| e.event.kind() == kind).count(),
            count
        );
    }
    let completed: std::collections::BTreeSet<_> = events
        .iter()
        .filter_map(|e| match &e.event {
            dmt::RunEvent::TaskCompleted { task_id, .. } => Some(task_id),
            _ => None,
        })
        .collect();
    assert_eq!(completed.len(), 14);
    assert_eq!(
        events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    assert_eq!(events.last().unwrap().event.kind(), "RunCompleted");
    handle.shutdown(Duration::from_secs(5)).await.unwrap();
}

fn assert_pipeline_ledger(path: &std::path::Path, run: &dmt::RunId) {
    let entries = crate::ledger::read_entries(path).unwrap();
    let suffixes = [
        "plan/0",
        "iterate-plan/0",
        "iterate-plan/1",
        "implement/0",
        "review-fanout/0",
        "review/0/0",
        "review/0/1",
        "review/0/2",
        "collect-feedback/0",
        "fix/0",
        "open-pr/0",
    ];
    assert_eq!(
        entries
            .iter()
            .map(|e| e.step_key.clone())
            .collect::<Vec<_>>(),
        suffixes.map(|s| format!("{run}/{s}"))
    );
}
