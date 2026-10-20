mod common;
use dmt_telemetry::{LogSink, Telemetry, TelemetryConfig};
use opentelemetry_sdk::trace::InMemorySpanExporter;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tracing_subscriber::prelude::*;

#[test]
fn json_log_lines_carry_ids_of_the_exported_span() {
    let exporter = InMemorySpanExporter::default();
    let writer = Arc::new(common::SharedBuf::default());
    let (layers, telemetry) = Telemetry::builder(TelemetryConfig {
        logs: LogSink::Stderr,
        service: "test-service".into(),
        env: Some("test".into()),
        version: Some("v1".into()),
        ..common::config()
    })
    .trace_exporter(exporter.clone())
    .log_writer(writer.clone())
    .build()
    .unwrap();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layers));
    {
        let span =
            tracing::info_span!("dispatch", run_id = "r1", "operation.name" = "dmt.dispatch");
        let _entered = span.enter();
        tracing::warn!(error = "boom", "lease lost");
    }
    tracing::info!("outside");
    let spans = exporter.get_finished_spans().unwrap();
    drop(guard);
    telemetry.shutdown(Duration::from_secs(1)).unwrap();
    assert_eq!(spans.len(), 1);
    let span = &spans[0];
    let bytes = writer.0.lock().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    let lines: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    let line = &lines[0];
    assert_eq!(line["status"], "warn");
    assert_eq!(line["message"], "lease lost");
    assert_eq!(line["service"], "test-service");
    assert_eq!(line["env"], "test");
    assert_eq!(line["version"], "v1");
    assert_eq!(line["dmt"]["run_id"], "r1");
    assert_eq!(line["span"], "dispatch");
    assert_eq!(line["fields"]["error"], "boom");
    assert_eq!(line["trace_id"], span.span_context.trace_id().to_string());
    assert_eq!(line["span_id"], span.span_context.span_id().to_string());
    let trace = span.span_context.trace_id().to_bytes();
    let mut low = [0; 8];
    low.copy_from_slice(&trace[8..]);
    assert_eq!(line["dd.trace_id"], u64::from_be_bytes(low).to_string());
    assert_eq!(
        line["dd.span_id"],
        u64::from_be_bytes(span.span_context.span_id().to_bytes()).to_string()
    );
    for key in ["trace_id", "span_id", "dd.trace_id", "dd.span_id"] {
        assert!(lines[1].get(key).is_none());
    }
}
#[test]
fn log_file_is_created_with_readable_mode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.log");
    let (layers, telemetry) = Telemetry::builder(TelemetryConfig {
        traces: false,
        logs: LogSink::File(path.clone()),
        ..common::config()
    })
    .build()
    .unwrap();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layers));
    tracing::info!("file log");
    drop(guard);
    telemetry.shutdown(Duration::from_secs(1)).unwrap();
    let line: Value = serde_json::from_str(std::fs::read_to_string(&path).unwrap().trim()).unwrap();
    assert_eq!(line["message"], "file log");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }
}
#[tokio::test(flavor = "current_thread")]
async fn nested_spans_do_not_deadlock_the_formatter() {
    let writer = Arc::new(common::SharedBuf::default());
    let (layers, telemetry) = Telemetry::builder(TelemetryConfig {
        logs: LogSink::Stderr,
        ..common::config()
    })
    .trace_exporter(InMemorySpanExporter::default())
    .log_writer(writer.clone())
    .build()
    .unwrap();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layers));
    // Run the synchronous formatter on a thread so the timeout can detect a lock deadlock.
    let subscriber = tracing::dispatcher::get_default(Clone::clone);
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&subscriber, || {
                let a = tracing::info_span!("a", run_id = "r", shared = "outer");
                let _a = a.enter();
                let b = tracing::info_span!("b", shared = "inner");
                let _b = b.enter();
                let c = tracing::info_span!("c");
                let _c = c.enter();
                b.record("shared", "updated");
                tracing::info!("nested");
            });
        }),
    )
    .await
    .unwrap()
    .unwrap();
    drop(guard);
    telemetry.shutdown(Duration::from_secs(1)).unwrap();
    let line: Value = serde_json::from_slice(&writer.0.lock().unwrap()).unwrap();
    assert_eq!(line["dmt"]["run_id"], "r");
    assert_eq!(line["dmt"]["shared"], "updated");
    assert_eq!(line["span"], "c");
}
