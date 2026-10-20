use crate::TelemetryConfig;
use datadog_opentelemetry::{DatadogTracingBuilder, configuration::Config, log::LevelFilter};

pub(crate) fn datadog_builder(config: &TelemetryConfig) -> DatadogTracingBuilder {
    let mut builder = Config::builder();
    builder.set_service(config.service.clone());
    if let Some(env) = &config.env {
        builder.set_env(env.clone());
    }
    if let Some(version) = &config.version {
        builder.set_version(version.clone());
    }
    if let Some(url) = &config.trace_agent_url {
        builder.set_trace_agent_url(url.clone());
    }
    if !config.datadog_internal_logs {
        builder.set_log_level_filter(LevelFilter::Off);
    }
    builder.set_telemetry_enabled(config.datadog_instrumentation_telemetry);
    builder.set_remote_config_enabled(config.datadog_remote_config);
    builder.set_trace_writer_force_flush_timeout(config.trace_shutdown_timeout);
    datadog_opentelemetry::tracing().with_config(builder.build())
}
