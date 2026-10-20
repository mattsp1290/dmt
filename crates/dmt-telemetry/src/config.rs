use std::{path::PathBuf, time::Duration};

/// Destination for JSON log lines. Shipping is the Agent's responsibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogSink {
    /// Disable logs.
    Off,
    /// Write to standard error.
    Stderr,
    /// Append to a file.
    File(PathBuf),
}
/// Datadog transport and formatting settings.
// Independent signal and Datadog opt-ins map directly to environment booleans;
// they are not mutually exclusive lifecycle states.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Unified service name.
    pub service: String,
    /// Deployment environment.
    pub env: Option<String>,
    /// Deployed version.
    pub version: Option<String>,
    /// Enable Datadog tracing.
    pub traces: bool,
    /// Enable `DogStatsD` metrics.
    pub metrics: bool,
    /// JSON log destination.
    pub logs: LogSink,
    /// `EnvFilter` directives, defaulting to info.
    pub filter: String,
    /// UDP address of the `DogStatsD` receiver.
    pub dogstatsd_addr: String,
    /// Optional trace Agent URL override.
    pub trace_agent_url: Option<String>,
    /// Datadog writer force-flush bound.
    pub trace_shutdown_timeout: Duration,
    /// Metric aggregation cadence, at least 100 ms.
    pub metrics_flush_interval: Duration,
    /// Opt in to Datadog's internal console logs (may write stdout).
    pub datadog_internal_logs: bool,
    /// Unix mode for newly created log files, default 0o640.
    pub log_file_mode: u32,
    /// Opt in to Datadog instrumentation telemetry.
    pub datadog_instrumentation_telemetry: bool,
    /// Opt in to Datadog remote configuration.
    pub datadog_remote_config: bool,
}
impl TelemetryConfig {
    /// Read standard Datadog and DMT environment settings.
    /// # Errors
    /// Returns invalid boolean, duration, log sink, or file-mode settings.
    pub fn from_env() -> Result<Self, TelemetryError> {
        Self::from_vars(|key| std::env::var(key).ok())
    }
    /// Parse settings without mutating the process environment.
    /// # Errors
    /// Returns invalid boolean, duration, log sink, or file-mode settings.
    pub fn from_vars(get: impl Fn(&str) -> Option<String>) -> Result<Self, TelemetryError> {
        let boolean = |key: &str, default| match get(key) {
            None => Ok(default),
            Some(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "1" => Ok(true),
                "false" | "0" => Ok(false),
                _ => Err(TelemetryError::Config(format!("invalid boolean for {key}"))),
            },
        };
        let millis = |key: &str, default| -> Result<Duration, TelemetryError> {
            get(key)
                .map_or(Ok(default), |s| {
                    s.parse().map_err(|_| {
                        TelemetryError::Config(format!("invalid milliseconds for {key}"))
                    })
                })
                .map(Duration::from_millis)
        };
        let logs = match get("DMT_TELEMETRY_LOGS").as_deref().unwrap_or("stderr") {
            "off" => LogSink::Off,
            "stderr" => LogSink::Stderr,
            value if value.starts_with("file:") && value.len() > 5 => {
                LogSink::File(value[5..].into())
            }
            _ => return Err(TelemetryError::Config("invalid DMT_TELEMETRY_LOGS".into())),
        };
        let mode = get("DMT_TELEMETRY_LOG_FILE_MODE").unwrap_or_else(|| "640".into());
        let log_file_mode = u32::from_str_radix(&mode, 8)
            .ok()
            .filter(|m| *m <= 0o777)
            .ok_or_else(|| TelemetryError::Config("invalid DMT_TELEMETRY_LOG_FILE_MODE".into()))?;
        Ok(Self {
            service: get("DD_SERVICE").unwrap_or_else(|| "dmt".into()),
            env: get("DD_ENV"),
            version: get("DD_VERSION"),
            traces: boolean("DMT_TELEMETRY_TRACES", true)?,
            metrics: boolean("DMT_TELEMETRY_METRICS", true)?,
            logs,
            filter: get("DMT_TELEMETRY_FILTER").unwrap_or_else(|| "info".into()),
            dogstatsd_addr: format!(
                "{}:{}",
                get("DD_AGENT_HOST").unwrap_or_else(|| "127.0.0.1".into()),
                get("DD_DOGSTATSD_PORT").unwrap_or_else(|| "8125".into())
            ),
            trace_agent_url: get("DD_TRACE_AGENT_URL"),
            trace_shutdown_timeout: millis("DMT_TELEMETRY_TRACE_SHUTDOWN_MS", 2000)?,
            metrics_flush_interval: millis("DMT_TELEMETRY_METRICS_FLUSH_MS", 1000)?
                .max(Duration::from_millis(100)),
            datadog_internal_logs: boolean("DMT_TELEMETRY_DATADOG_LOGS", false)?,
            log_file_mode,
            datadog_instrumentation_telemetry: boolean(
                "DD_INSTRUMENTATION_TELEMETRY_ENABLED",
                false,
            )?,
            datadog_remote_config: boolean("DD_REMOTE_CONFIGURATION_ENABLED", false)?,
        })
    }
}
/// Configuration, installation, transport, or shutdown failure.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    /// Invalid configuration.
    #[error("{0}")]
    Config(String),
    /// Invalid filter directive.
    #[error(transparent)]
    Filter(#[from] tracing_subscriber::filter::ParseError),
    /// `DogStatsD` initialization failed.
    #[error("DogStatsD: {0}")]
    Dogstatsd(String),
    /// Log file could not be opened.
    #[error(transparent)]
    LogFile(#[from] std::io::Error),
    /// A process global was already installed.
    #[error("{0} already installed")]
    AlreadyInstalled(&'static str),
    /// Exporter shutdown failed.
    #[error("shutdown: {0}")]
    Shutdown(String),
}
