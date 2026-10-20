mod common;
use common::*;
use dmt_core::*;
use dmt_store::*;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
#[derive(Default)]
struct Capture {
    spans: Mutex<Vec<BTreeMap<String, String>>>,
    events: Mutex<Vec<String>>,
    next: AtomicU64,
    stack: Mutex<Vec<Id>>,
    metadata: Mutex<Vec<&'static Metadata<'static>>>,
}
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut fields = Fields(BTreeMap::new());
        attrs.record(&mut fields);
        fields
            .0
            .insert("span_name".into(), attrs.metadata().name().into());
        self.spans.lock().unwrap().push(fields.0);
        self.metadata.lock().unwrap().push(attrs.metadata());
        Id::from_u64(self.next.fetch_add(1, Ordering::Relaxed) + 1)
    }
    fn record(&self, id: &Id, record: &Record<'_>) {
        let mut fields = Fields(BTreeMap::new());
        record.record(&mut fields);
        self.spans.lock().unwrap()[usize::try_from(id.into_u64() - 1).unwrap()].extend(fields.0);
    }
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.events.lock().unwrap().extend(fields.0.into_values());
    }
    fn enter(&self, id: &Id) {
        self.stack.lock().unwrap().push(id.clone());
    }
    fn exit(&self, _: &Id) {
        self.stack.lock().unwrap().pop();
    }
    fn current_span(&self) -> tracing_core::span::Current {
        let stack = self.stack.lock().unwrap();
        if let Some(id) = stack.last() {
            // Span metadata must remain static; the capture only needs the id for record().
            tracing_core::span::Current::new(
                id.clone(),
                self.metadata.lock().unwrap()[usize::try_from(id.into_u64() - 1).unwrap()],
            )
        } else {
            tracing_core::span::Current::none()
        }
    }
}
#[tokio::test(start_paused = true)]
async fn dispatch_spans_and_conflict_events_are_emitted() {
    let capture = Arc::new(Capture::default());
    let _guard = tracing::subscriber::set_default(capture.clone());
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::linear(),
        fast_config(),
        &[
            ("a", handler(|_, _| async { done() })),
            ("b", handler(|_, _| async { done() })),
        ],
    )
    .await;
    let run = h.start_run(&"linear".into(), Value::Null).await.unwrap();
    completed(&h, &run).await;
    assert_linear_spans(&capture, &run);
    h.abort().await;
    let (_, h, _) = run_contention().await;
    assert!(
        capture
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e == "commit conflict")
    );
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn signal_cancel_and_sweep_spans_record_results() {
    let capture = Arc::new(Capture::default());
    let _guard = tracing::subscriber::set_default(capture.clone());
    let clock = Arc::new(ManualClock::new(T0));
    let h = start(
        Arc::new(MemoryStore::new()),
        clock.clone(),
        fixtures::wait_with_deadline(1_000_000),
        fast_config(),
        &[("a", handler(|_, _| async { done() }))],
    )
    .await;
    let run = h.start_run(&"wait".into(), Value::Null).await.unwrap();
    h.wait_quiescent(&run, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        h.signal(
            &run,
            "unknown",
            SignalPayload {
                label: "ok".into(),
                payload: Value::Null
            }
        )
        .await
        .is_err()
    );
    let _ = clock.advance(1_000_000);
    eventually("deadline", || async {
        h.run(&run).await.unwrap().unwrap().status.is_terminal()
    })
    .await;
    assert!(h.cancel(&run, "test").await.is_err());
    h.abort().await;
    let spans = capture.spans.lock().unwrap();
    for (name, result) in [
        ("signal", "not_found"),
        ("cancel", "terminal"),
        ("sweep", "applied"),
    ] {
        let span = spans.iter().find(|s| s["span_name"] == name).unwrap();
        assert_eq!(span["run_id"], run.as_str());
        assert_eq!(span["operation.name"], format!("\"dmt.{name}\""));
        assert_eq!(span["result"], format!("\"{result}\""));
        let resource = if name == "sweep" { "timeout" } else { name };
        assert_eq!(span["resource.name"], format!("\"{resource}\""));
    }
    let signal = spans.iter().find(|s| s["span_name"] == "signal").unwrap();
    assert_eq!(signal["resource.name"], "\"signal\"");
    assert_eq!(signal["name"], "\"unknown\"");
    let sweep = spans.iter().find(|s| s["span_name"] == "sweep").unwrap();
    assert_eq!(sweep["kind"], "\"timeout\"");
}

#[tokio::test(start_paused = true)]
async fn handler_failure_timeout_and_panic_are_classified() {
    let capture = Arc::new(Capture::default());
    let _guard = tracing::subscriber::set_default(capture.clone());
    for class in ["fail_permanent", "timeout", "panic"] {
        let h = start(
            Arc::new(MemoryStore::new()),
            Arc::new(ManualClock::new(T0)),
            fixtures::retry_chain(1),
            dmt_runtime::EngineConfig {
                handler_timeout: Some(std::time::Duration::from_millis(10)),
                ..fast_config()
            },
            &[(
                "a",
                handler(move |_, _| async move {
                    match class {
                        "timeout" => std::future::pending::<()>().await,
                        "panic" => panic!("test panic"),
                        _ => (),
                    }
                    Ok(NodeOutcome::Fail {
                        message: "é".repeat(1000),
                        retryable: false,
                    })
                }),
            )],
        )
        .await;
        let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
        h.wait_quiescent(&run, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        h.abort().await;
        let spans = capture.spans.lock().unwrap();
        let span = spans
            .iter()
            .find(|s| s["span_name"] == "handler" && s["run_id"] == run.as_str())
            .unwrap();
        assert_eq!(span["outcome"], format!("\"{class}\""));
        assert_eq!(span["otel.status_code"], "\"ERROR\"");
        assert_eq!(span["graph_id"], "retry");
        // Debug formatting adds a pair of quotes to string fields.
        assert!(span["error.message"].len() <= 258);
    }
    assert!(
        capture
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|m| m == "handler timed out")
    );
}

#[tokio::test(start_paused = true)]
async fn payload_sentinels_never_reach_telemetry() {
    let capture = Arc::new(Capture::default());
    let _guard = tracing::subscriber::set_default(capture.clone());
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::wait_with_deadline(1_000_000),
        fast_config(),
        &[(
            "a",
            handler(|_, _| async {
                Ok(NodeOutcome::Done(Outcome {
                    label: "ok".into(),
                    payload: serde_json::json!({"out": "SENTINEL-OUTPUT"}),
                }))
            }),
        )],
    )
    .await;
    let run = h
        .start_run(
            &"wait".into(),
            serde_json::json!({"secret": "SENTINEL-INPUT"}),
        )
        .await
        .unwrap();
    h.wait_quiescent(&run, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    h.signal(
        &run,
        "gate",
        SignalPayload {
            label: "ok".into(),
            payload: serde_json::json!({"p": "SENTINEL-SIGNAL"}),
        },
    )
    .await
    .unwrap();
    h.abort().await;
    assert!(
        !format!(
            "{:?}{:?}",
            capture.spans.lock().unwrap(),
            capture.events.lock().unwrap()
        )
        .contains("SENTINEL-")
    );
}

#[tokio::test(start_paused = true)]
async fn store_instrumentation_can_be_disabled() {
    let capture = Arc::new(Capture::default());
    let _guard = tracing::subscriber::set_default(capture.clone());
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::linear(),
        dmt_runtime::EngineConfig {
            instrument_store: false,
            ..fast_config()
        },
        &[
            ("a", handler(|_, _| async { done() })),
            ("b", handler(|_, _| async { done() })),
        ],
    )
    .await;
    let run = h.start_run(&"linear".into(), Value::Null).await.unwrap();
    completed(&h, &run).await;
    h.abort().await;
    assert!(
        capture
            .spans
            .lock()
            .unwrap()
            .iter()
            .all(|s| s["span_name"] != "store")
    );
}

fn assert_linear_spans(capture: &Capture, run: &RunId) {
    let spans = capture.spans.lock().unwrap();
    assert!(spans.iter().any(|s| s["span_name"] == "store"
        && s.get("method").is_some_and(|m| m == "\"apply\"")
        && s["backend"] == "\"memory\""));
    let dispatches: Vec<_> = spans
        .iter()
        .filter(|s| s["span_name"] == "dispatch")
        .collect();
    assert_eq!(dispatches.len(), 2);
    for (span, node) in dispatches.iter().zip(["a", "b"]) {
        assert_eq!(span["span_name"], "dispatch");
        assert_eq!(span["run_id"], run.as_str());
        assert_eq!(span["node_id"], node);
        assert_eq!(
            span["step_key"],
            StepKey::task(run, &node.into(), 0).as_str()
        );
        assert_eq!(span["attempt"], "1");
        assert_eq!(span["graph_id"], "linear");
        assert_eq!(span["graph_version"], "1");
        assert_eq!(span["task_id"], span["step_key"]);
        assert_eq!(span["operation.name"], "\"dmt.dispatch\"");
        assert_eq!(span["resource.name"], format!("linear/{node}"));
        assert_eq!(span["outcome"], "\"done\"");
        assert_eq!(span["result"], "\"applied\"");
    }
    let starts: Vec<_> = spans
        .iter()
        .filter(|s| s["span_name"] == "start_run")
        .collect();
    assert_eq!(starts.len(), 1);
    let start = starts[0];
    assert_eq!(start["run_id"], run.as_str());
    assert_eq!(start["graph_id"], "linear");
    assert_eq!(start["graph_version"], "1");
    assert_eq!(start["operation.name"], "\"dmt.start_run\"");
    assert_eq!(start["resource.name"], "linear");
    assert_eq!(start["result"], "\"ok\"");
    let store = spans
        .iter()
        .find(|s| s["span_name"] == "store" && s["method"] == "\"apply\"")
        .unwrap();
    assert_eq!(store["backend"], "\"memory\"");
    assert_eq!(store["operation.name"], "\"dmt.store\"");
    assert_eq!(store["resource.name"], "\"apply\"");
    assert_eq!(store["span.type"], "\"db\"");
    assert_eq!(store["db.system"], "\"memory\"");
    assert_eq!(store["result"], "\"ok\"");
    for name in ["handler", "commit"] {
        let selected: Vec<_> = spans.iter().filter(|s| s["span_name"] == name).collect();
        assert_eq!(selected.len(), 2);
        for span in selected {
            assert_eq!(span["run_id"], run.as_str());
            if name == "handler" {
                assert_eq!(span["outcome"], "\"done\"");
                assert_eq!(span["graph_id"], "linear");
                assert_eq!(span["attempt"], "1");
                assert_eq!(span["operation.name"], "\"dmt.handler\"");
                assert_eq!(span["resource.name"].trim_matches('"'), span["node_id"]);
            } else {
                assert_eq!(span["kind"], "\"outcome\"");
                assert_eq!(span["operation.name"], "\"dmt.commit\"");
                assert_eq!(span["resource.name"], "\"outcome\"");
                assert_eq!(span["attempts"], "1");
                assert_eq!(span["result"], "\"applied\"");
            }
        }
    }
}
