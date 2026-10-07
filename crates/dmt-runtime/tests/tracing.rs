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
            .insert("name".into(), attrs.metadata().name().into());
        self.spans.lock().unwrap().push(fields.0);
        Id::from_u64(self.next.fetch_add(1, Ordering::Relaxed) + 1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        if let Some(message) = fields.0.get("message") {
            self.events.lock().unwrap().push(message.clone());
        }
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
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
    {
        let spans = capture.spans.lock().unwrap();
        assert_eq!(spans.len(), 2);
        for (span, node) in spans.iter().zip(["a", "b"]) {
            assert_eq!(span["name"], "dispatch");
            assert_eq!(span["run_id"], run.as_str());
            assert_eq!(span["node_id"], node);
            assert_eq!(
                span["step_key"],
                StepKey::task(&run, &node.into(), 0).as_str()
            );
            assert_eq!(span["attempt"], "1");
        }
    }
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
