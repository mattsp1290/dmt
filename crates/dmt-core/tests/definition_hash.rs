use dmt_core::{EndStatus, Graph, GraphBuilder};
use serde_json::json;
fn graph() -> Graph {
    GraphBuilder::new("hash", 1)
        .start("a")
        .task("a")
        .end("end", EndStatus::Completed)
        .edge("a", "end")
        .edge_on("a", "end", "ok")
        .build()
        .unwrap()
}
#[test]
fn hash_is_lowercase_sha256_and_edge_order_independent() {
    let graph = graph();
    let hash = graph.definition_hash();
    assert_eq!(hash.len(), 64);
    assert!(
        hash.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    let mut value = serde_json::to_value(&graph).unwrap();
    value["edges"].as_array_mut().unwrap().reverse();
    assert_eq!(
        Graph::from_json(&value.to_string())
            .unwrap()
            .definition_hash(),
        hash
    );
}
#[test]
fn definition_fields_change_hash() {
    let graph = graph();
    let original = serde_json::to_value(&graph).unwrap();
    for (path, value) in [
        ("/edges/1/guard/Label", json!("other")),
        ("/nodes/end/kind/status", json!("failed")),
        ("/nodes/a/retry/max_attempts", json!(4)),
        ("/version", json!(2)),
        ("/id", json!("other")),
    ] {
        let mut changed = original.clone();
        *changed.pointer_mut(path).unwrap() = value;
        assert_ne!(
            Graph::from_json(&changed.to_string())
                .unwrap()
                .definition_hash(),
            graph.definition_hash(),
            "{path}"
        );
    }
}
#[test]
fn pipeline_hash_is_pinned() {
    // Changing the derive shape of Graph, NodeKind, Edge, Guard, JoinPolicy,
    // RetryPolicy, or EndStatus requires updating this literal in the same commit.
    assert_eq!(
        dmt_core::fixtures::pipeline().definition_hash(),
        "532315045ec69a49a08fefd3682d3e18e405d9316b94eb4e979271527054763e"
    );
}
