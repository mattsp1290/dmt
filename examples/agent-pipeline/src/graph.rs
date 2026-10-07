use dmt::{EndStatus, Graph, GraphBuilder, JoinPolicy};
pub const GRAPH_ID: &str = "agent-pipeline";
pub const GRAPH_VERSION: u32 = 1;
pub const PLAN: &str = "plan";
pub const ITERATE_PLAN: &str = "iterate-plan";
pub const PLAN_SIGNOFF: &str = "plan-signoff";
pub const IMPLEMENT: &str = "implement";
pub const REVIEW_FANOUT: &str = "review-fanout";
pub const REVIEW: &str = "review";
pub const COLLECT_FEEDBACK: &str = "collect-feedback";
pub const FIX: &str = "fix";
pub const OPEN_PR: &str = "open-pr";
pub const PRESENT: &str = "present";
pub const DONE: &str = "done";
pub const SIGNOFF: &str = "signoff";
pub const ACK: &str = "ack";
pub const APPROVED: &str = "approved";
pub const CHANGES_REQUESTED: &str = "changes_requested";
pub const ACKNOWLEDGED: &str = "acknowledged";
pub const NEEDS_FIXES: &str = "needs_fixes";
pub const CLEAN: &str = "clean";
/// The pipeline graph as validated data.
/// # Panics
/// Panics if the static graph becomes invalid.
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
        .expect("the pipeline graph is valid")
}
#[must_use]
pub fn executable_nodes() -> [&'static str; 8] {
    [
        PLAN,
        ITERATE_PLAN,
        IMPLEMENT,
        REVIEW_FANOUT,
        REVIEW,
        COLLECT_FEEDBACK,
        FIX,
        OPEN_PR,
    ]
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_core_fixture() {
        assert_eq!(
            pipeline().definition_hash(),
            dmt_core::fixtures::pipeline().definition_hash()
        );
    }
    #[test]
    fn executable_nodes_cover_every_handler_node() {
        let graph = pipeline();
        let actual: std::collections::BTreeSet<_> = graph
            .nodes()
            .filter(|(_, n)| {
                !matches!(
                    n.kind,
                    dmt::NodeKind::Wait { .. } | dmt::NodeKind::End { .. }
                )
            })
            .map(|(id, _)| id.as_str())
            .collect();
        assert_eq!(actual, executable_nodes().into_iter().collect());
    }
}
