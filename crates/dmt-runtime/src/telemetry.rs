//! Runtime metric names and deterministic run trace contexts.
use crate::EngineError;
use dmt_core::{Commit, RunId};

/// Stable metric names. Labels must contain only bounded graph/node identifiers.
pub mod names {
    /// Runtime runs started total metric.
    pub const RUNS_STARTED: &str = "dmt.runtime.runs_started_total";
    /// Runtime tasks claimed total metric.
    pub const TASKS_CLAIMED: &str = "dmt.runtime.tasks_claimed_total";
    /// Runtime in flight metric.
    pub const IN_FLIGHT: &str = "dmt.runtime.in_flight";
    /// Runtime dispatch.duration seconds metric.
    pub const DISPATCH_DURATION: &str = "dmt.runtime.dispatch.duration_seconds";
    /// Runtime dispatch.outcomes total metric.
    pub const DISPATCH_OUTCOMES: &str = "dmt.runtime.dispatch.outcomes_total";
    /// Runtime handler.duration seconds metric.
    pub const HANDLER_DURATION: &str = "dmt.runtime.handler.duration_seconds";
    /// Runtime lease lost total metric.
    pub const LEASE_LOST: &str = "dmt.runtime.lease_lost_total";
    /// Runtime commit.attempts total metric.
    pub const COMMIT_ATTEMPTS: &str = "dmt.runtime.commit.attempts_total";
    /// Runtime commit.duration seconds metric.
    pub const COMMIT_DURATION: &str = "dmt.runtime.commit.duration_seconds";
    /// Runtime events total metric.
    pub const EVENTS: &str = "dmt.runtime.events_total";
    /// Runtime signals total metric.
    pub const SIGNALS: &str = "dmt.runtime.signals_total";
    /// Runtime cancels total metric.
    pub const CANCELS: &str = "dmt.runtime.cancels_total";
    /// Runtime sweeps total metric.
    pub const SWEEPS: &str = "dmt.runtime.sweeps_total";
}
/// Register metric descriptions with the current recorder. Safe to repeat.
pub fn describe() {
    metrics::describe_counter!(names::RUNS_STARTED, "Runtime runs_started_total");
    metrics::describe_counter!(names::TASKS_CLAIMED, "Runtime tasks_claimed_total");
    metrics::describe_gauge!(names::IN_FLIGHT, "Runtime in_flight");
    metrics::describe_histogram!(
        names::DISPATCH_DURATION,
        metrics::Unit::Seconds,
        "Runtime dispatch.duration_seconds"
    );
    metrics::describe_counter!(names::DISPATCH_OUTCOMES, "Runtime dispatch.outcomes_total");
    metrics::describe_histogram!(
        names::HANDLER_DURATION,
        metrics::Unit::Seconds,
        "Runtime handler.duration_seconds"
    );
    metrics::describe_counter!(names::LEASE_LOST, "Runtime lease_lost_total");
    metrics::describe_counter!(names::COMMIT_ATTEMPTS, "Runtime commit.attempts_total");
    metrics::describe_histogram!(
        names::COMMIT_DURATION,
        metrics::Unit::Seconds,
        "Runtime commit.duration_seconds"
    );
    metrics::describe_counter!(names::EVENTS, "Runtime events_total");
    metrics::describe_counter!(names::SIGNALS, "Runtime signals_total");
    metrics::describe_counter!(names::CANCELS, "Runtime cancels_total");
    metrics::describe_counter!(names::SWEEPS, "Runtime sweeps_total");
}
pub(crate) fn truncate_message(text: &str) -> &str {
    let mut end = text.len().min(256);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
pub(crate) fn host_result<T>(result: &Result<T, EngineError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(EngineError::RunNotFound { .. } | EngineError::SignalNotFound { .. }) => "not_found",
        Err(EngineError::RunTerminal { .. }) => "terminal",
        Err(EngineError::Indeterminate { .. }) => "indeterminate",
        Err(EngineError::Contention { .. }) => "contention",
        Err(_) => "error",
    }
}
pub(crate) fn event_kinds(commit: &Commit) -> Vec<&'static str> {
    commit.events.iter().map(dmt_core::RunEvent::kind).collect()
}
pub(crate) fn count_events(kinds: &[&'static str]) {
    for &kind in kinds {
        metrics::counter!(names::EVENTS, "kind" => kind).increment(1);
    }
}
pub(crate) fn run_scope(span: &tracing::Span, run_id: &RunId) {
    #[cfg(feature = "otel")]
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let _ = span.set_parent(run_parent_context(run_id));
    }
    #[cfg(not(feature = "otel"))]
    let _ = (span, run_id);
}
/// Deterministic trace id; Datadog requires a nonzero low 64-bit word.
#[cfg(feature = "otel")]
#[must_use]
pub fn run_trace_id(run_id: &RunId) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let mut bytes = uuid::Uuid::parse_str(run_id.as_str()).map_or_else(
        |_| {
            let digest = Sha256::digest(run_id.as_str());
            let mut bytes = [0; 16];
            bytes.copy_from_slice(&digest[..16]);
            bytes
        },
        |id| *id.as_bytes(),
    );
    if bytes[8..] == [0; 8] {
        bytes[15] = 1;
    }
    bytes
}
/// Deterministic synthetic root id. This root itself is never exported.
#[cfg(feature = "otel")]
#[must_use]
pub fn run_root_span_id(run_id: &RunId) -> [u8; 8] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"dmt-run-root\0");
    digest.update(run_id.as_str());
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&digest.finalize()[..8]);
    if bytes == [0; 8] {
        bytes[7] = 1;
    }
    bytes
}
/// Sampled local parent shared by all run-scoped spans, including concurrent tasks.
#[cfg(feature = "otel")]
#[must_use]
pub fn run_parent_context(run_id: &RunId) -> opentelemetry::Context {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from_bytes(run_trace_id(run_id)),
        SpanId::from_bytes(run_root_span_id(run_id)),
        TraceFlags::SAMPLED,
        false,
        TraceState::default(),
    ))
}
