mod common;
use dmt_core::fixtures;
use dmt_runtime::telemetry;
use dmt_telemetry::{Telemetry, TelemetryConfig};
use opentelemetry_sdk::trace::InMemorySpanExporter;
use std::time::Duration;
use tracing_subscriber::prelude::*;
#[tokio::test(flavor = "current_thread")]
async fn run_scoped_spans_share_the_run_trace_id() {
    for filter in ["info", "debug"] {
        let exporter = InMemorySpanExporter::default();
        let (layers, wiring) = Telemetry::builder(TelemetryConfig {
            filter: filter.into(),
            ..common::config()
        })
        .trace_exporter(exporter.clone())
        .build()
        .unwrap();
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layers));
        let h = common::engine(fixtures::linear()).await;
        let run = h
            .start_run(&"linear".into(), serde_json::Value::Null)
            .await
            .unwrap();
        h.wait_quiescent(&run, Duration::from_secs(5))
            .await
            .unwrap();
        h.shutdown(Duration::from_secs(1)).await.unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        drop(guard);
        wiring.shutdown(Duration::from_secs(1)).unwrap();
        for name in ["start_run", "dispatch", "handler", "commit"] {
            let selected: Vec<_> = spans.iter().filter(|s| s.name == name).collect();
            assert_eq!(selected.len(), if name == "start_run" { 1 } else { 2 });
            for span in selected {
                assert_eq!(
                    span.span_context.trace_id().to_bytes(),
                    telemetry::run_trace_id(&run)
                );
                assert!(
                    span.attributes
                        .iter()
                        .any(|a| a.key.as_str() == "run_id" && a.value.as_str() == run.as_str())
                );
                if matches!(name, "start_run" | "dispatch") {
                    assert_eq!(
                        span.parent_span_id.to_bytes(),
                        telemetry::run_root_span_id(&run)
                    );
                }
            }
        }
        assert_eq!(spans.iter().any(|s| s.name == "store"), filter == "debug");
    }
}
