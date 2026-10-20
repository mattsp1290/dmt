use crate::{TelemetryConfig, TelemetryError};
use metrics_exporter_dogstatsd::{BuildError, DogStatsDBuilder};
use std::time::Duration;

pub(crate) fn install_recorder(config: &TelemetryConfig) -> Result<Duration, TelemetryError> {
    let mut labels = vec![metrics::Label::new("service", config.service.clone())];
    if let Some(env) = &config.env {
        labels.push(metrics::Label::new("env", env.clone()));
    }
    if let Some(version) = &config.version {
        labels.push(metrics::Label::new("version", version.clone()));
    }
    DogStatsDBuilder::default()
        .with_remote_address(&config.dogstatsd_addr)
        .map_err(|e| TelemetryError::Dogstatsd(e.to_string()))?
        .with_flush_interval(config.metrics_flush_interval)
        .with_global_labels(labels)
        .with_telemetry(false)
        .install()
        .map_err(|e| match e {
            BuildError::FailedToInstall => TelemetryError::AlreadyInstalled("metrics recorder"),
            other => TelemetryError::Dogstatsd(other.to_string()),
        })?;
    Ok(config.metrics_flush_interval)
}
