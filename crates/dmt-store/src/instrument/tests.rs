use super::*;
use crate::MemoryStore;
use metrics_util::debugging::DebugValue;
use std::{collections::BTreeMap, sync::Mutex};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    span::{Attributes, Record},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<BTreeMap<u64, BTreeMap<String, String>>>>);
struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &tracing::span::Id, _: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        fields.insert("name".into(), attrs.metadata().name().into());
        self.0.lock().unwrap().insert(id.into_u64(), fields);
    }
    fn on_record(&self, id: &tracing::span::Id, record: &Record<'_>, _: Context<'_, S>) {
        record.record(&mut Fields(
            self.0.lock().unwrap().get_mut(&id.into_u64()).unwrap(),
        ));
    }
}
#[test]
fn instrumented_store_records_spans_and_metrics() {
    let capture = Capture::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    metrics::with_local_recorder(&recorder, || {
        rt.block_on(async {
            let inner = Arc::new(MemoryStore::new());
            let graph = dmt_core::GraphBuilder::new("test", 1)
                .start("a")
                .task("a")
                .end("done", dmt_core::EndStatus::Completed)
                .edge("a", "done")
                .build()
                .unwrap();
            inner.register_graph(&graph).await.unwrap();
            let mut commit =
                dmt_core::plan_start(&graph, "test".into(), serde_json::Value::Null, Micros(0))
                    .unwrap();
            inner.create_run(commit.clone()).await.unwrap();
            commit.expected_run_version = 0;
            let store = InstrumentedStore::new(inner);
            store.register_graph(&graph).await.unwrap();
            assert!(store.load_run(&"unknown".into()).await.unwrap().is_none());
            assert!(matches!(
                store.apply(commit, None).await,
                Err(StoreError::VersionConflict { .. })
            ));
        });
    });
    let entries = snapshot.snapshot().into_vec();
    for (method, result) in [
        ("register_graph", "ok"),
        ("load_run", "ok"),
        ("apply", "version_conflict"),
    ] {
        for name in [names::CALLS, names::DURATION] {
            let value = entries
                .iter()
                .find(|(key, _, _, _)| {
                    key.key().name() == name
                        && [
                            ("backend", "memory"),
                            ("method", method),
                            ("result", result),
                        ]
                        .iter()
                        .all(|(k, v)| key.key().labels().any(|l| l.key() == *k && l.value() == *v))
                })
                .unwrap();
            match &value.3 {
                DebugValue::Counter(n) => assert_eq!(*n, 1),
                DebugValue::Histogram(v) => assert_eq!(v.len(), 1),
                DebugValue::Gauge(_) => panic!("unexpected gauge"),
            }
        }
        assert!(
            capture
                .0
                .lock()
                .unwrap()
                .values()
                .any(|s| s["name"] == "store"
                    && s["backend"] == "\"memory\""
                    && s["method"] == format!("\"{method}\"")
                    && s["result"] == format!("\"{result}\"")
                    && s["operation.name"] == "\"dmt.store\"")
        );
    }
}
#[test]
fn backend_names() {
    let store = Arc::new(MemoryStore::new());
    assert_eq!(store.backend_name(), "memory");
    assert_eq!(InstrumentedStore::new(store).backend_name(), "memory");
}

#[test]
fn result_label_covers_every_variant() {
    let errors = [
        StoreError::VersionConflict {
            expected: 1,
            actual: 2,
        },
        StoreError::JoinDrift {
            join_id: "join".into(),
        },
        StoreError::LeaseLost {
            task_id: "task".into(),
        },
        StoreError::RunTerminal {
            run_id: "run".into(),
        },
        StoreError::NotFound("task".into()),
        StoreError::AlreadyResolved {
            signal_id: "signal".into(),
        },
        StoreError::GraphMismatch {
            graph_id: "graph".into(),
            version: 1,
        },
        StoreError::InvalidCommit("shape".into()),
        StoreError::Busy,
        StoreError::Backend("io".into()),
    ];
    let expected = [
        "version_conflict",
        "join_drift",
        "lease_lost",
        "run_terminal",
        "not_found",
        "already_resolved",
        "graph_mismatch",
        "invalid_commit",
        "busy",
        "backend",
    ];
    for (error, label) in errors.into_iter().zip(expected) {
        assert_eq!(result_label::<()>(&Err(error)), label);
    }
    assert_eq!(result_label(&Ok(())), "ok");
}
