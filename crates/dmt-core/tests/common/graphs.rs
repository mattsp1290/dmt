use dmt_core::{EndStatus, Graph, GraphBuilder, JoinPolicy};
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
