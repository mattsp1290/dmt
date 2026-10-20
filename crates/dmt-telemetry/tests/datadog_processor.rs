#[path = "common/agent.rs"]
mod agent;
mod common;
use dmt_core::{JoinPolicy, fixtures};
use dmt_runtime::{Quiescent, telemetry};
use dmt_telemetry::Telemetry;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SimpleSpanProcessor};
use rmpv::Value;
use std::{collections::BTreeSet, time::Duration};
use tracing_subscriber::prelude::*;

fn field<'a>(value: &'a Value, key: &str) -> &'a Value {
    &value
        .as_map()
        .unwrap()
        .iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .unwrap_or_else(|| panic!("missing {key} in {value:?}"))
        .1
}
fn low(bytes: [u8; 16]) -> u64 {
    let mut low = [0; 8];
    low.copy_from_slice(&bytes[8..]);
    u64::from_be_bytes(low)
}
#[tokio::test(flavor = "current_thread")]
async fn fan_out_run_survives_the_datadog_span_processor() {
    let agent = agent::Agent::start();
    let exporter = InMemorySpanExporter::default();
    let config = dmt_telemetry::TelemetryConfig {
        trace_agent_url: Some(agent.url.clone()),
        ..common::config()
    };
    let (layers, telemetry) = Telemetry::builder(config)
        .extra_span_processor(SimpleSpanProcessor::new(exporter.clone()))
        .build()
        .unwrap();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layers));
    let h = common::engine(fixtures::fan_out(JoinPolicy::All)).await;
    let run = h
        .start_run(&"fan-out".into(), serde_json::Value::Null)
        .await
        .unwrap();
    assert_eq!(
        h.wait_quiescent(&run, Duration::from_secs(15))
            .await
            .unwrap(),
        Quiescent::Terminal(dmt_core::RunStatus::Completed)
    );
    h.shutdown(Duration::from_secs(1)).await.unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    drop(guard);
    telemetry.shutdown(Duration::from_secs(3)).unwrap();
    let branches: Vec<_> = spans
        .iter()
        .filter(|s| {
            s.name == "dispatch"
                && s.attributes
                    .iter()
                    .any(|a| a.key.as_str() == "node_id" && a.value.as_str() == "br")
        })
        .collect();
    assert_eq!(branches.len(), 3);
    assert!(
        branches.iter().map(|s| s.start_time).max().unwrap()
            < branches.iter().map(|s| s.end_time).min().unwrap(),
        "branches must overlap"
    );
    let expected: BTreeSet<_> = spans
        .iter()
        .map(|s| {
            (
                low(s.span_context.trace_id().to_bytes()),
                u64::from_be_bytes(s.span_context.span_id().to_bytes()),
            )
        })
        .collect();
    assert_exports(&agent, &expected, &run);
}
fn assert_exports(agent: &agent::Agent, expected: &BTreeSet<(u64, u64)>, run: &dmt_core::RunId) {
    let requests = agent.requests.lock().unwrap();
    eprintln!(
        "Agent paths: {:?}",
        requests.iter().map(|r| &r.path).collect::<Vec<_>>()
    );
    let mut decoded = Vec::new();
    let mut chunks = 0;
    for request in requests.iter().filter(|r| r.path == "/v0.4/traces") {
        assert!(request.count.is_some());
        let payload =
            rmpv::decode::read_value(&mut request.body.as_slice()).expect("valid v0.4 msgpack");
        for trace in payload.as_array().unwrap() {
            chunks += 1;
            decoded.extend(trace.as_array().unwrap().iter().cloned());
        }
    }
    eprintln!("Datadog trace chunks: {chunks}");
    let actual: BTreeSet<_> = decoded
        .iter()
        .map(|s| {
            (
                field(s, "trace_id").as_u64().unwrap(),
                field(s, "span_id").as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(decoded.len(), actual.len(), "duplicate span exports");
    assert_eq!(&actual, expected, "no spans lost by Datadog processor");
    assert!(!actual.is_empty());
    let ids: BTreeSet<_> = actual.iter().map(|(_, id)| *id).collect();
    let trace = telemetry::run_trace_id(run);
    let mut high = [0; 8];
    high.copy_from_slice(&trace[..8]);
    for span in &decoded {
        assert_eq!(field(span, "trace_id").as_u64().unwrap(), low(trace));
        let parent = field(span, "parent_id").as_u64().unwrap();
        assert!(
            parent == u64::from_be_bytes(telemetry::run_root_span_id(run)) || ids.contains(&parent)
        );
        let meta = field(span, "meta");
        if let Some((_, value)) = meta
            .as_map()
            .unwrap()
            .iter()
            .find(|(key, _)| key.as_str() == Some("_dd.p.tid"))
        {
            assert_eq!(
                value.as_str().unwrap(),
                format!("{:016x}", u64::from_be_bytes(high))
            );
        }
        assert!(
            field(field(span, "metrics"), "_sampling_priority_v1")
                .as_f64()
                .unwrap()
                >= 1.0
        );
    }
}
