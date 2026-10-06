//! Validated test graphs for downstream crates; no stability promise.
use crate::{EndStatus, Graph, GraphBuilder, JoinPolicy, RetryPolicy};
/// # Panics
/// Panics if supplied policy or deadline violates graph validation.
#[must_use]
pub fn linear() -> Graph {
    GraphBuilder::new("linear", 1)
        .start("a")
        .task("a")
        .task("b")
        .end("done", EndStatus::Completed)
        .edge("a", "b")
        .edge("b", "done")
        .build()
        .unwrap()
}
/// # Panics
/// Panics if supplied policy or deadline violates graph validation.
#[must_use]
pub fn loop_via_wait() -> Graph {
    GraphBuilder::new("loop", 1)
        .start("plan")
        .task("plan")
        .wait("signoff", "signoff", None)
        .task("implement")
        .end("done", EndStatus::Completed)
        .edge("plan", "signoff")
        .edge_on("signoff", "plan", "changes_requested")
        .edge_on("signoff", "implement", "approved")
        .edge("implement", "done")
        .build()
        .unwrap()
}
/// # Panics
/// Panics if supplied policy or deadline violates graph validation.
#[must_use]
pub fn fan_out(policy: JoinPolicy) -> Graph {
    GraphBuilder::new("fan-out", 1)
        .start("a")
        .task("a")
        .fan_out("fo", "br", "jn")
        .branch("br")
        .join("jn", policy)
        .end("done", EndStatus::Completed)
        .edge("a", "fo")
        .edge("jn", "done")
        .build()
        .unwrap()
}
/// # Panics
/// Panics if supplied policy or deadline violates graph validation.
#[must_use]
pub fn fan_out_then_wait(policy: JoinPolicy, deadline: Option<i64>) -> Graph {
    let b = GraphBuilder::new("fan-out-wait", 1)
        .start("a")
        .task("a")
        .fan_out("fo", "br", "jn")
        .branch("br")
        .join("jn", policy)
        .wait("w", "gate", deadline)
        .task("t")
        .end("done", EndStatus::Completed)
        .edge("a", "fo")
        .edge("jn", "w")
        .edge_on("w", "t", "ok")
        .edge("t", "done");
    if deadline.is_some() {
        b.edge_on("w", "t", "timeout").build().unwrap()
    } else {
        b.build().unwrap()
    }
}
/// # Panics
/// Panics if supplied policy or deadline violates graph validation.
#[must_use]
pub fn wait_with_deadline(d: i64) -> Graph {
    GraphBuilder::new("wait", 1)
        .start("a")
        .task("a")
        .wait("w", "gate", Some(d))
        .end("done", EndStatus::Completed)
        .end("failed", EndStatus::Failed)
        .edge("a", "w")
        .edge_on("w", "done", "ok")
        .edge_on("w", "failed", "timeout")
        .build()
        .unwrap()
}

/// # Panics
/// Panics unless `max_attempts` is in 1..=1000.
#[must_use]
pub fn retry_chain(max_attempts: u32) -> Graph {
    GraphBuilder::new("retry", 1)
        .start("a")
        .task_with(
            "a",
            RetryPolicy {
                max_attempts,
                ..RetryPolicy::default()
            },
            None,
        )
        .end("done", EndStatus::Completed)
        .end("failed", EndStatus::Failed)
        .edge("a", "done")
        .edge_on("a", "failed", "failed")
        .build()
        .unwrap()
}
/// The milestone's agent pipeline, including approval and acknowledgement waits.
/// # Panics
/// Panics if the static fixture becomes invalid.
#[must_use]
pub fn pipeline() -> Graph {
    GraphBuilder::new("agent-pipeline", 1)
        .start("plan")
        .task("plan")
        .task("iterate-plan")
        .wait("plan-signoff", "signoff", None)
        .task("implement")
        .fan_out("review-fanout", "review", "collect-feedback")
        .branch("review")
        .join("collect-feedback", JoinPolicy::All)
        .task("fix")
        .task("open-pr")
        .wait("present", "ack", None)
        .end("done", EndStatus::Completed)
        .edge("plan", "iterate-plan")
        .edge("iterate-plan", "plan-signoff")
        .edge_on("plan-signoff", "implement", "approved")
        .edge_on("plan-signoff", "iterate-plan", "changes_requested")
        .edge("implement", "review-fanout")
        .edge_on("collect-feedback", "fix", "needs_fixes")
        .edge_on("collect-feedback", "open-pr", "clean")
        .edge("fix", "open-pr")
        .edge("open-pr", "present")
        .edge_on("present", "done", "acknowledged")
        .build()
        .unwrap()
}
/// All fixture families with representative parameters.
#[must_use]
pub fn all() -> Vec<Graph> {
    vec![
        linear(),
        loop_via_wait(),
        fan_out(JoinPolicy::All),
        fan_out_then_wait(JoinPolicy::Quorum(1), None),
        wait_with_deadline(10_000_000),
        retry_chain(4),
        pipeline(),
    ]
}
