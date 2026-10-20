use dmt_telemetry::{LogSink, TelemetryConfig, TelemetryError};
use std::time::Duration;
fn parse(vars: &[(&str, &str)]) -> Result<TelemetryConfig, TelemetryError> {
    TelemetryConfig::from_vars(|key| {
        vars.iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| (*v).into())
    })
}
#[test]
fn defaults_and_standard_tags() {
    let config = parse(&[]).unwrap();
    assert_eq!(config.service, "dmt");
    assert_eq!(config.env, None);
    assert_eq!(config.version, None);
    assert!(config.traces && config.metrics);
    assert_eq!(config.logs, LogSink::Stderr);
    assert_eq!(config.filter, "info");
    assert_eq!(config.dogstatsd_addr, "127.0.0.1:8125");
    assert_eq!(config.metrics_flush_interval, Duration::from_secs(1));
    assert_eq!(config.trace_shutdown_timeout, Duration::from_secs(2));
    assert!(
        !config.datadog_internal_logs
            && !config.datadog_remote_config
            && !config.datadog_instrumentation_telemetry
    );
    assert_eq!(config.log_file_mode, 0o640);
    let config = parse(&[
        ("DD_SERVICE", "service"),
        ("DD_ENV", "test"),
        ("DD_VERSION", "v1"),
        ("DD_AGENT_HOST", "localhost"),
        ("DD_DOGSTATSD_PORT", "9999"),
        ("DD_TRACE_AGENT_URL", "http://localhost:9998"),
    ])
    .unwrap();
    assert_eq!(config.service, "service");
    assert_eq!(config.env.as_deref(), Some("test"));
    assert_eq!(config.version.as_deref(), Some("v1"));
    assert_eq!(config.dogstatsd_addr, "localhost:9999");
    assert_eq!(
        config.trace_agent_url.as_deref(),
        Some("http://localhost:9998")
    );
}
#[test]
fn sinks_modes_booleans_and_durations() {
    let config = parse(&[
        ("DMT_TELEMETRY_LOGS", "file:/tmp/x"),
        ("DMT_TELEMETRY_METRICS_FLUSH_MS", "10"),
        ("DMT_TELEMETRY_LOG_FILE_MODE", "600"),
        ("DD_LOG_LEVEL", "debug"),
    ])
    .unwrap();
    assert_eq!(config.logs, LogSink::File("/tmp/x".into()));
    assert_eq!(config.metrics_flush_interval, Duration::from_millis(100));
    assert_eq!(config.log_file_mode, 0o600);
    assert!(!config.datadog_internal_logs);
    assert!(
        parse(&[("DMT_TELEMETRY_DATADOG_LOGS", "1")])
            .unwrap()
            .datadog_internal_logs
    );
    for value in ["true", "TRUE", "1", "false", "FALSE", "0"] {
        assert!(parse(&[("DMT_TELEMETRY_TRACES", value)]).is_ok());
    }
    for (key, value) in [
        ("DMT_TELEMETRY_METRICS", "bad"),
        ("DMT_TELEMETRY_LOGS", "file:"),
        ("DMT_TELEMETRY_LOGS", "bad"),
        ("DMT_TELEMETRY_LOG_FILE_MODE", "999"),
        ("DMT_TELEMETRY_LOG_FILE_MODE", "1777"),
        ("DMT_TELEMETRY_TRACE_SHUTDOWN_MS", "bad"),
    ] {
        assert!(matches!(
            parse(&[(key, value)]),
            Err(TelemetryError::Config(_))
        ));
    }
}
