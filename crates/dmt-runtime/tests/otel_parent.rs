mod common;
use common::*;
use dmt_core::fixtures;
use dmt_runtime::telemetry;
use dmt_store::{ManualClock, MemoryStore};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use serde_json::Value;
use std::sync::Arc;
use tracing_subscriber::prelude::*;

#[tokio::test(start_paused = true)]
async fn run_scoped_spans_share_the_run_trace_id() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
    let _guard = tracing::subscriber::set_default(subscriber);
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
    h.abort().await;
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    for (name, count) in [
        ("start_run", 1),
        ("dispatch", 2),
        ("handler", 2),
        ("commit", 2),
    ] {
        let selected: Vec<_> = spans.iter().filter(|s| s.name == name).collect();
        assert_eq!(selected.len(), count, "{name}");
        for span in selected {
            assert_eq!(
                span.span_context.trace_id().to_bytes(),
                telemetry::run_trace_id(&run)
            );
            if matches!(name, "start_run" | "dispatch") {
                assert_eq!(
                    span.parent_span_id.to_bytes(),
                    telemetry::run_root_span_id(&run)
                );
            } else if name == "handler" {
                assert!(spans.iter().any(
                    |p| p.name == "dispatch" && p.span_context.span_id() == span.parent_span_id
                ));
            }
        }
    }
}
