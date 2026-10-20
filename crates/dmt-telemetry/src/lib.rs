//! Optional Datadog wiring for dmt hosts. Library instrumentation remains vendor neutral.
mod config;
mod logs;
mod metrics;
mod traces;
pub use config::{LogSink, TelemetryConfig, TelemetryError};
pub use logs::DatadogJson;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::{
    Resource,
    trace::{SdkTracerProvider, SpanExporter, SpanProcessor},
};
use std::time::Duration;
use tracing_subscriber::{
    Layer, Registry,
    fmt::{MakeWriter, writer::BoxMakeWriter},
    prelude::*,
};

/// A boxed layer for a fresh registry; attach these before other host layers.
pub type TelemetryLayers = Vec<Box<dyn Layer<Registry> + Send + Sync>>;
type ProviderFactory = Box<dyn FnOnce(Resource) -> SdkTracerProvider>;
type DatadogExtension = Box<
    dyn FnOnce(
        datadog_opentelemetry::DatadogTracingBuilder,
    ) -> datadog_opentelemetry::DatadogTracingBuilder,
>;

/// Install a subscriber and optional recorder process-wide.
/// # Errors
/// Returns invalid configuration, log/transport setup errors, or already installed globals.
pub fn init(config: TelemetryConfig) -> Result<Telemetry, TelemetryError> {
    Telemetry::builder(config).install()
}
/// Shutdown ownership for providers built by this crate.
pub struct Telemetry {
    tracer: Option<SdkTracerProvider>,
    metrics_wait: Option<Duration>,
}
impl Telemetry {
    /// Configure custom exporters, processors, writers, or subscriber composition.
    #[must_use]
    pub fn builder(config: TelemetryConfig) -> Builder {
        Builder {
            config,
            provider_factory: None,
            datadog_extension: None,
            writer: None,
        }
    }
    /// Drain traces, wait for metric forwarding, then release the provider.
    /// Blocking: call after dropping the runtime, or inside `spawn_blocking`.
    /// Metrics require two flush intervals plus 50 ms; `timeout` bounds trace shutdown only.
    /// # Errors
    /// Returns the trace shutdown error after still waiting for metric forwarding.
    pub fn shutdown(self, timeout: Duration) -> Result<(), TelemetryError> {
        let result = self.tracer.map_or(Ok(()), |provider| {
            provider
                .shutdown_with_timeout(timeout)
                .map_err(|e| TelemetryError::Shutdown(e.to_string()))
        });
        if let Some(interval) = self.metrics_wait {
            std::thread::sleep(
                interval
                    .saturating_mul(2)
                    .saturating_add(Duration::from_millis(50)),
            );
        }
        result
    }
}
/// Compose telemetry without installing process globals until `install`.
pub struct Builder {
    config: TelemetryConfig,
    provider_factory: Option<ProviderFactory>,
    datadog_extension: Option<DatadogExtension>,
    writer: Option<BoxMakeWriter>,
}
impl Builder {
    /// Replace Datadog with a simple SDK exporter (useful for tests).
    #[must_use]
    pub fn trace_exporter(self, exporter: impl SpanExporter + 'static) -> Self {
        Self {
            provider_factory: Some(Box::new(move |resource| {
                SdkTracerProvider::builder()
                    .with_simple_exporter(exporter)
                    .with_resource(resource)
                    .build()
            })),
            ..self
        }
    }
    /// Observe SDK spans alongside the real Datadog processor.
    #[must_use]
    pub fn extra_span_processor(self, processor: impl SpanProcessor + 'static) -> Self {
        Self {
            datadog_extension: Some(Box::new(move |builder| {
                builder.with_span_processor(processor)
            })),
            ..self
        }
    }
    /// Override the log sink writer. `LogSink::Off` still disables logs.
    #[must_use]
    pub fn log_writer(self, writer: impl for<'a> MakeWriter<'a> + Send + Sync + 'static) -> Self {
        Self {
            writer: Some(BoxMakeWriter::new(writer)),
            ..self
        }
    }
    /// Build layers for a fresh registry, installing no subscriber or recorder globals.
    /// # Errors
    /// Returns an invalid filter or unavailable log file.
    pub fn build(self) -> Result<(TelemetryLayers, Telemetry), TelemetryError> {
        let filter = tracing_subscriber::EnvFilter::try_new(&self.config.filter)?;
        // Open the writer before starting exporter threads so file failures have no side effects.
        let logs = logs::layer(&self.config, self.writer)?;
        let mut layers: TelemetryLayers = Vec::new();
        let tracer = if self.config.traces {
            let provider = if let Some(factory) = self.provider_factory {
                factory(
                    Resource::builder()
                        .with_service_name(self.config.service.clone())
                        .build(),
                )
            } else {
                let mut builder = traces::datadog_builder(&self.config);
                if let Some(extend) = self.datadog_extension {
                    builder = extend(builder);
                }
                builder.init_local().0
            };
            layers.push(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("dmt"))
                    .with_filter(filter.clone())
                    .boxed(),
            );
            Some(provider)
        } else {
            None
        };
        if let Some(layer) = logs {
            layers.push(layer.with_filter(filter).boxed());
        }
        Ok((
            layers,
            Telemetry {
                tracer,
                metrics_wait: None,
            },
        ))
    }
    /// Install just the `DogStatsD` recorder, for hosts with their own subscriber.
    /// # Errors
    /// Returns invalid transport settings or an already installed recorder.
    pub fn install_metrics(config: &TelemetryConfig) -> Result<Option<Duration>, TelemetryError> {
        if !config.metrics {
            return Ok(None);
        }
        let interval = metrics::install_recorder(config)?;
        dmt_runtime::telemetry::describe();
        dmt_store::instrument::describe();
        Ok(Some(interval))
    }
    /// Install process-wide subscriber, then optional metrics recorder.
    /// # Errors
    /// Returns setup errors or already installed globals. Globals cannot be uninstalled;
    /// if recorder installation fails, the subscriber remains installed.
    pub fn install(self) -> Result<Telemetry, TelemetryError> {
        let config = self.config.clone();
        let (layers, mut telemetry) = self.build()?;
        tracing_subscriber::registry()
            .with(layers)
            .try_init()
            .map_err(|_| TelemetryError::AlreadyInstalled("tracing subscriber"))?;
        telemetry.metrics_wait = Self::install_metrics(&config)?;
        Ok(telemetry)
    }
}
