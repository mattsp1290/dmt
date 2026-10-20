mod common;
use dmt_telemetry::{Telemetry, TelemetryConfig};
use std::{
    net::TcpListener,
    time::{Duration, Instant},
};
use tracing_subscriber::prelude::*;
fn shutdown(url: String) {
    let (layers, telemetry) = Telemetry::builder(TelemetryConfig {
        trace_agent_url: Some(url),
        trace_shutdown_timeout: Duration::from_secs(1),
        ..common::config()
    })
    .build()
    .unwrap();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layers));
    {
        let span = tracing::info_span!("dispatch", run_id = "r");
        let _entered = span.enter();
        tracing::info!("offline");
    }
    drop(guard);
    let start = Instant::now();
    let _ = telemetry.shutdown(Duration::from_secs(1));
    assert!(start.elapsed() < Duration::from_secs(2));
}
#[test]
fn shutdown_is_bounded_when_the_agent_refuses() {
    if common::isolated_datadog_test("shutdown_is_bounded_when_the_agent_refuses") {
        return;
    }
    shutdown("http://127.0.0.1:1".into());
}
#[test]
fn shutdown_is_bounded_when_the_agent_black_holes() {
    if common::isolated_datadog_test("shutdown_is_bounded_when_the_agent_black_holes") {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    shutdown(format!("http://{}", listener.local_addr().unwrap()));
}
