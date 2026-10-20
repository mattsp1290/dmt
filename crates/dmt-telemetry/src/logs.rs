use crate::{LogSink, TelemetryConfig, TelemetryError};
use opentelemetry::trace::TraceContextExt;
use serde_json::{Map, Value};
use std::{
    fmt,
    io::{self, LineWriter},
    sync::Mutex,
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    Registry,
    fmt::{
        self as subscriber_fmt, FmtContext, FormatEvent, FormatFields, FormattedFields,
        format::{JsonFields, Writer},
        writer::BoxMakeWriter,
    },
    registry::LookupSpan,
};

/// Datadog JSON lines with both OpenTelemetry and decimal Datadog correlation ids.
pub struct DatadogJson {
    service: String,
    env: Option<String>,
    version: Option<String>,
}
#[derive(Default)]
struct Fields(Map<String, Value>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().into(), Value::String(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl<S, N> FormatEvent<S, N> for DatadogJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // The OTel layer activates this context on enter. Subscriber callbacks suppress
        // tracing's default dispatcher, so Span::current().context() is empty here.
        // Read the active OTel context before taking any span extensions locks.
        let context = opentelemetry::Context::current();
        let otel_span = context.span();
        let sc = otel_span.span_context();
        let mut line = Map::new();
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| fmt::Error)?;
        line.insert("timestamp".into(), timestamp.into());
        line.insert(
            "status".into(),
            event
                .metadata()
                .level()
                .as_str()
                .to_ascii_lowercase()
                .into(),
        );
        let mut fields = Fields::default();
        event.record(&mut fields);
        line.insert(
            "message".into(),
            fields
                .0
                .remove("message")
                .unwrap_or_else(|| event.metadata().name().into()),
        );
        line.insert("target".into(), event.metadata().target().into());
        line.insert("service".into(), self.service.clone().into());
        if let Some(env) = &self.env {
            line.insert("env".into(), env.clone().into());
        }
        if let Some(version) = &self.version {
            line.insert("version".into(), version.clone().into());
        }
        if sc.is_valid() && sc.is_sampled() {
            let trace = sc.trace_id().to_bytes();
            let span = u64::from_be_bytes(sc.span_id().to_bytes());
            let mut low = [0; 8];
            low.copy_from_slice(&trace[8..]);
            line.insert(
                "trace_id".into(),
                format!("{:032x}", u128::from_be_bytes(trace)).into(),
            );
            line.insert("span_id".into(), format!("{span:016x}").into());
            line.insert(
                "dd.trace_id".into(),
                u64::from_be_bytes(low).to_string().into(),
            );
            line.insert("dd.span_id".into(), span.to_string().into());
        }
        if let Some(current) = ctx.lookup_current() {
            let mut merged = Map::new();
            for span in current.scope().from_root() {
                let extensions = span.extensions();
                if let Some(fields) = extensions.get::<FormattedFields<JsonFields>>() {
                    match serde_json::from_str::<Map<String, Value>>(fields.fields.as_str()) {
                        Ok(fields) => merged.extend(fields),
                        Err(_) => {
                            merged.insert("span_fields_raw".into(), fields.fields.clone().into());
                        }
                    }
                }
            }
            line.insert("dmt".into(), merged.into());
            line.insert("span".into(), current.name().into());
        }
        if !fields.0.is_empty() {
            line.insert("fields".into(), fields.0.into());
        }
        writeln!(writer, "{}", Value::Object(line))
    }
}
type LogLayer = subscriber_fmt::Layer<Registry, JsonFields, DatadogJson, BoxMakeWriter>;
pub(crate) fn layer(
    config: &TelemetryConfig,
    writer: Option<BoxMakeWriter>,
) -> Result<Option<LogLayer>, TelemetryError> {
    if config.logs == LogSink::Off {
        return Ok(None);
    }
    let writer = if let Some(writer) = writer {
        writer
    } else {
        match &config.logs {
            LogSink::Off => return Ok(None),
            LogSink::Stderr => BoxMakeWriter::new(io::stderr),
            LogSink::File(path) => {
                let mut options = std::fs::OpenOptions::new();
                options.create(true).append(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(config.log_file_mode);
                }
                BoxMakeWriter::new(Mutex::new(LineWriter::new(options.open(path)?)))
            }
        }
    };
    Ok(Some(
        subscriber_fmt::layer()
            .fmt_fields(JsonFields::new())
            .event_format(DatadogJson {
                service: config.service.clone(),
                env: config.env.clone(),
                version: config.version.clone(),
            })
            .with_writer(writer),
    ))
}
