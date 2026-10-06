use dmt_core::{EndStatus, Graph, GraphBuilder, GraphError, JoinPolicy, RetryPolicy};
use serde_json::{Value, json};
fn linear() -> Graph {
    GraphBuilder::new("test", 1)
        .start("a")
        .task("a")
        .end("end", EndStatus::Completed)
        .edge("a", "end")
        .build()
        .unwrap()
}
fn raw() -> Value {
    serde_json::to_value(linear()).unwrap()
}
fn errors(value: &Value) -> Vec<GraphError> {
    match Graph::from_json(&value.to_string()) {
        Err(GraphError::Invalid(errors)) => errors,
        other => panic!("expected invalid graph, got {other:?}"),
    }
}
macro_rules! invalid {
    ($name:ident, $edit:expr, $pattern:pat) => {
        #[test]
        fn $name() {
            let mut value = raw();
            ($edit)(&mut value);
            assert!(
                errors(&value).iter().any(|error| matches!(error, $pattern)),
                "{:?}",
                errors(&value)
            );
        }
    };
}
invalid!(
    empty_and_missing_start,
    |v: &mut Value| {
        v["nodes"] = json!({});
    },
    GraphError::EmptyGraph
);
invalid!(
    invalid_start_kind,
    |v: &mut Value| {
        v["start"] = json!("end");
    },
    GraphError::StartKind { .. }
);
invalid!(
    edge_endpoint_missing,
    |v: &mut Value| {
        v["edges"][0]["to"] = json!("missing");
    },
    GraphError::EdgeEndpointMissing { .. }
);
invalid!(
    fan_out_target_kind,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] = json!({"kind":"fan_out", "branch":"end", "join":"end"});
    },
    GraphError::FanOutTargetKind { .. }
);
invalid!(
    branch_and_join_ownership,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] = json!({"kind":"branch"});
    },
    GraphError::BranchOwnership { .. }
);
invalid!(
    implicit_successor_edge,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] = json!({"kind":"branch"});
    },
    GraphError::ImplicitSuccessorEdge { .. }
);
invalid!(
    no_outgoing_edge,
    |v: &mut Value| {
        v["edges"] = json!([]);
    },
    GraphError::NoOutgoingEdge { .. }
);
invalid!(
    multiple_default_guards,
    |v: &mut Value| {
        v["edges"]
            .as_array_mut()
            .unwrap()
            .push(json!({"from":"a", "to":"end", "guard":"Default"}));
    },
    GraphError::MultipleDefaultGuards { .. }
);
invalid!(
    duplicate_label_guard,
    |v: &mut Value| {
        v["edges"] = json!([{"from":"a", "to":"end", "guard":{"Label":"x"}}, {"from":"a", "to":"end", "guard":{"Label":"x"}}]);
    },
    GraphError::DuplicateGuard { .. }
);
invalid!(
    end_has_edges,
    |v: &mut Value| {
        v["edges"]
            .as_array_mut()
            .unwrap()
            .push(json!({"from":"end", "to":"a", "guard":"Default"}));
    },
    GraphError::EndHasEdges { .. }
);
invalid!(
    wait_deadline_requires_timeout,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] = json!({"kind":"wait", "signal":"approve", "deadline_micros":1});
    },
    GraphError::WaitDeadlineWithoutTimeoutEdge { .. }
);
invalid!(
    duplicate_signal_name,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] =
            json!({"kind":"wait", "signal":"approve", "deadline_micros":null});
        v["nodes"]["b"] = v["nodes"]["a"].clone();
    },
    GraphError::DuplicateSignalName { .. }
);
invalid!(
    unreachable_node,
    |v: &mut Value| {
        v["nodes"]["unreachable"] = v["nodes"]["end"].clone();
    },
    GraphError::Unreachable { .. }
);
invalid!(
    invalid_identifier,
    |v: &mut Value| {
        v["edges"][0]["guard"] = json!({"Label":"bad/label"});
    },
    GraphError::InvalidIdentifier { .. }
);
invalid!(
    invalid_retry_policy,
    |v: &mut Value| {
        v["nodes"]["a"]["retry"]["max_attempts"] = json!(0);
    },
    GraphError::InvalidRetryPolicy { .. }
);
invalid!(
    reserved_label,
    |v: &mut Value| {
        v["edges"][0]["guard"] = json!({"Label":"cancelled"});
    },
    GraphError::ReservedLabel { .. }
);
invalid!(
    invalid_quorum,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] = json!({"kind":"join", "policy":{"Quorum":0}});
    },
    GraphError::InvalidQuorum { .. }
);
invalid!(
    invalid_timeout,
    |v: &mut Value| {
        v["nodes"]["a"]["timeout_micros"] = json!(0);
    },
    GraphError::InvalidTimeout { .. }
);
invalid!(
    invalid_deadline,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] = json!({"kind":"wait", "signal":"approve", "deadline_micros":0});
    },
    GraphError::InvalidDeadline { .. }
);
invalid!(
    join_ownership,
    |v: &mut Value| {
        v["nodes"]["a"]["kind"] = json!({"kind":"join", "policy":"All"});
    },
    GraphError::JoinOwnership { .. }
);
#[test]
fn builder_rejects_duplicate_node() {
    assert!(
        matches!(GraphBuilder::new("test", 1).start("a").task("a").task("a").end("end", EndStatus::Completed).edge("a", "end").build(), Err(GraphError::Invalid(errors)) if errors.iter().any(|e| matches!(e, GraphError::DuplicateNode {..})))
    );
}
#[test]
fn from_json_round_trips_builder_graph() {
    let graph = linear();
    assert_eq!(Graph::from_json(&graph.to_json()).unwrap(), graph);
}
#[test]
fn deserialize_rejects_invalid_graph() {
    let mut value = raw();
    value["start"] = json!("missing");
    assert!(serde_json::from_value::<Graph>(value).is_err());
}
#[test]
fn pipeline_shape_is_valid() {
    let graph = GraphBuilder::new("pipeline", 1)
        .start("plan")
        .task("plan")
        .fan_out("map", "worker", "reduce")
        .branch("worker")
        .join("reduce", JoinPolicy::All)
        .wait("approve", "approval", Some(60_000_000))
        .end("done", EndStatus::Completed)
        .end("failed", EndStatus::Failed)
        .edge("plan", "map")
        .edge("reduce", "approve")
        .edge("approve", "done")
        .edge_on("approve", "failed", "timeout")
        .build()
        .unwrap();
    assert_eq!(graph.validate(), [] as [GraphError; 0]);
}
#[test]
fn retry_policy_bounds() {
    for retry in [
        RetryPolicy {
            max_attempts: 1001,
            ..RetryPolicy::default()
        },
        RetryPolicy {
            initial_backoff_micros: -1,
            ..RetryPolicy::default()
        },
        RetryPolicy {
            max_backoff_micros: 0,
            ..RetryPolicy::default()
        },
        RetryPolicy {
            multiplier_permille: 999,
            ..RetryPolicy::default()
        },
    ] {
        assert!(
            GraphBuilder::new("test", 1)
                .start("a")
                .task_with("a", retry, None)
                .end("end", EndStatus::Completed)
                .edge("a", "end")
                .build()
                .is_err()
        );
    }
}
