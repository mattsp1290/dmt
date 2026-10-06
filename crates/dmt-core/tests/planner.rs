mod common;
#[path = "common/graphs.rs"]
mod graphs;
use common::Sim;
use dmt_core::{
    BranchResult, EndStatus, Graph, GraphBuilder, JoinInput, JoinPolicy, Micros, NodeOutcome,
    Outcome, PlanError, RetryPolicy, RunEvent, RunStatus, SignalPayload, TaskId, TaskStatus,
    plan_cancel, plan_exhausted, plan_outcome, plan_signal, plan_start, plan_timeout,
};
use serde_json::{Value, json};
fn start(graph: Graph) -> Sim {
    Sim::start(graph, Value::Null, Micros(0))
}
fn done(sim: &mut Sim, node: &str, label: &str) -> dmt_core::Commit {
    let id = sim
        .snapshot
        .tasks
        .iter()
        .find(|t| {
            t.node_id.as_str() == node
                && matches!(t.status, TaskStatus::Ready | TaskStatus::Running)
        })
        .unwrap()
        .task_id
        .clone();
    if sim.snapshot.task(&id).unwrap().status == TaskStatus::Ready {
        sim.claim(&id);
    }
    sim.outcome(
        &id,
        NodeOutcome::Done(Outcome::with_payload(label, json!(42))),
        Micros(0),
    )
}
fn setup_fan(graph: Graph, count: u32) -> Sim {
    let mut sim = start(graph);
    done(&mut sim, "a", "ok");
    let id = sim.task_by_node("fo", 0).task_id.clone();
    sim.claim(&id);
    sim.outcome(
        &id,
        NodeOutcome::FanOut((0..count).map(|n| json!(n)).collect()),
        Micros(0),
    );
    sim
}
fn branches(sim: &Sim) -> Vec<TaskId> {
    sim.all_tasks
        .values()
        .filter(|t| t.node_id.as_str() == "br")
        .map(|t| t.task_id.clone())
        .collect()
}
fn branch_done(sim: &mut Sim, id: &TaskId) -> dmt_core::Commit {
    if sim.snapshot.task(id).unwrap().status == TaskStatus::Ready {
        sim.claim(id);
    }
    sim.outcome(id, NodeOutcome::Done(Outcome::done("ok")), Micros(0))
}
fn kinds(c: &dmt_core::Commit) -> Vec<&'static str> {
    c.events.iter().map(RunEvent::kind).collect()
}
fn fail(sim: &mut Sim, id: &TaskId, retryable: bool) -> dmt_core::Commit {
    if sim.snapshot.task(id).unwrap().status == TaskStatus::Ready {
        sim.claim(id);
    }
    sim.outcome(
        id,
        NodeOutcome::Fail {
            message: "broken".into(),
            retryable,
        },
        Micros(0),
    )
}
fn signal(sim: &mut Sim, name: &str, label: &str) -> dmt_core::Commit {
    sim.signal(
        name,
        SignalPayload {
            label: label.into(),
            payload: json!(7),
        },
        Micros(0),
    )
}
fn no_failed(max: u32) -> Graph {
    GraphBuilder::new("retry", 1)
        .start("a")
        .task_with(
            "a",
            RetryPolicy {
                max_attempts: max,
                ..RetryPolicy::default()
            },
            None,
        )
        .end("done", EndStatus::Completed)
        .edge("a", "done")
        .build()
        .unwrap()
}
fn recovery() -> Graph {
    GraphBuilder::new("recovery", 1)
        .start("a")
        .task_with(
            "a",
            RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            },
            None,
        )
        .task("recover")
        .end("done", EndStatus::Completed)
        .edge("a", "done")
        .edge_on("a", "recover", "failed")
        .edge("recover", "done")
        .build()
        .unwrap()
}
fn straggler(deadline: Option<i64>) -> (Sim, Vec<TaskId>) {
    let mut sim = setup_fan(
        graphs::fan_out_then_wait(JoinPolicy::Quorum(2), deadline),
        3,
    );
    let ids = branches(&sim);
    for id in &ids {
        sim.claim(id);
    }
    branch_done(&mut sim, &ids[0]);
    branch_done(&mut sim, &ids[1]);
    done(&mut sim, "jn", "ok");
    (sim, ids)
}
#[test]
fn linear_completes() {
    let mut sim = start(graphs::linear());
    assert_eq!(sim.snapshot.status, RunStatus::Active);
    done(&mut sim, "a", "ok");
    done(&mut sim, "b", "ok");
    assert_eq!(sim.snapshot.status, RunStatus::Completed);
    assert_eq!(sim.snapshot.output, Some(json!(42)));
    assert_eq!(
        sim.events.iter().map(RunEvent::kind).collect::<Vec<_>>(),
        vec![
            "RunStarted",
            "TaskScheduled",
            "TaskCompleted",
            "TaskScheduled",
            "TaskCompleted",
            "RunCompleted"
        ]
    );
    assert_eq!(sim.task_by_node("a", 0).step_key.as_str(), "run/a/0");
    assert_eq!(sim.task_by_node("b", 0).step_key.as_str(), "run/b/0");
    assert_eq!(
        sim.commits
            .iter()
            .map(|c| c.expected_run_version)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}
#[test]
fn start_on_wait_parks_immediately() {
    let sim = start(
        GraphBuilder::new("wait", 1)
            .start("w")
            .wait("w", "signoff", None)
            .end("done", EndStatus::Completed)
            .edge("w", "done")
            .build()
            .unwrap(),
    );
    assert_eq!(
        kinds(&sim.commits[0]),
        vec!["RunStarted", "WaitOpened", "RunParked"]
    );
    assert_eq!(sim.snapshot.status, RunStatus::Parked);
    assert_eq!(sim.open_signal("signoff").key.as_str(), "signoff/0");
}
#[test]
fn loop_through_wait_three_times() {
    let mut sim = start(graphs::loop_via_wait());
    for i in 0..3 {
        done(&mut sim, "plan", "ok");
        assert_eq!(
            sim.open_signal("signoff").key.as_str(),
            format!("signoff/{i}")
        );
        assert_eq!(
            sim.task_by_node("plan", i).step_key.as_str(),
            format!("run/plan/{i}")
        );
        signal(
            &mut sim,
            "signoff",
            if i < 2 {
                "changes_requested"
            } else {
                "approved"
            },
        );
    }
    done(&mut sim, "implement", "ok");
    assert_eq!(sim.snapshot.status, RunStatus::Completed);
    for kind in ["RunParked", "RunResumed"] {
        assert_eq!(sim.events.iter().filter(|e| e.kind() == kind).count(), 3);
    }
}
#[test]
fn fan_out_all_three() {
    let mut sim = setup_fan(graphs::fan_out(JoinPolicy::All), 3);
    let ids = branches(&sim);
    assert_eq!(
        ids.iter().map(TaskId::as_str).collect::<Vec<_>>(),
        vec!["run/br/0/0", "run/br/0/1", "run/br/0/2"]
    );
    for (i, id) in ids.iter().enumerate() {
        let c = branch_done(&mut sim, id);
        assert_eq!(c.new_tasks.len(), usize::from(i == 2));
    }
    let task = sim.task_by_node("jn", 0);
    assert_eq!(task.step_key.as_str(), "run/jn/0");
    let input: JoinInput = serde_json::from_value(task.input.clone()).unwrap();
    assert_eq!(
        input
            .results
            .iter()
            .map(BranchResult::index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(input.quorum_met);
    assert!(!sim.events.iter().any(|e| matches!(e, RunEvent::RunParked)));
}
#[test]
fn quorum_two_of_three() {
    let mut sim = setup_fan(graphs::fan_out(JoinPolicy::Quorum(2)), 3);
    let ids = branches(&sim);
    branch_done(&mut sim, &ids[0]);
    let c = branch_done(&mut sim, &ids[1]);
    assert!(c.events.iter().any(|e| matches!(
        e,
        RunEvent::JoinSatisfied {
            quorum_met: true,
            ..
        }
    )));
    let join = sim.snapshot.joins.clone();
    let c = branch_done(&mut sim, &ids[2]);
    assert!(c.join_contribution.is_none());
    assert_eq!(c.new_tasks, []);
    assert!(
        c.events
            .iter()
            .any(|e| matches!(e, RunEvent::BranchContributed { .. }))
    );
    assert_eq!(sim.snapshot.joins, join);
    assert_eq!(sim.snapshot.occurrence(&"jn".into()), 1);
}
#[test]
fn join_satisfied_while_branch_running() {
    let (mut sim, ids) = straggler(None);
    assert_eq!(sim.snapshot.status, RunStatus::Active);
    assert!(
        !sim.commits
            .last()
            .unwrap()
            .events
            .iter()
            .any(|e| matches!(e, RunEvent::RunParked))
    );
    let c = branch_done(&mut sim, &ids[2]);
    assert_eq!(
        kinds(&c),
        vec!["TaskCompleted", "BranchContributed", "RunParked"]
    );
}
#[test]
fn signal_while_straggler_running() {
    let graph = GraphBuilder::new("straggler", 1)
        .start("fo")
        .fan_out("fo", "br", "jn")
        .branch("br")
        .join("jn", JoinPolicy::Quorum(2))
        .wait("w", "gate", None)
        .end("done", EndStatus::Completed)
        .edge("jn", "w")
        .edge_on("w", "done", "ok")
        .build()
        .unwrap();
    let mut sim = start(graph);
    let fo = sim.task_by_node("fo", 0).task_id.clone();
    sim.claim(&fo);
    sim.outcome(
        &fo,
        NodeOutcome::FanOut(vec![json!(1), json!(2), json!(3)]),
        Micros(0),
    );
    let ids = branches(&sim);
    for id in &ids {
        sim.claim(id);
    }
    branch_done(&mut sim, &ids[0]);
    branch_done(&mut sim, &ids[1]);
    done(&mut sim, "jn", "ok");
    let c = signal(&mut sim, "gate", "ok");
    assert_eq!(
        kinds(&c),
        vec!["SignalReceived", "TaskCompleted", "RunCompleted"]
    );
    assert!(matches!(
        plan_outcome(
            &sim.graph,
            &sim.snapshot,
            &ids[2],
            NodeOutcome::Done(Outcome::done("ok")),
            Micros(0)
        ),
        Err(PlanError::RunTerminal { .. })
    ));
}
#[test]
fn timeout_while_straggler_running() {
    let (mut sim, _) = straggler(Some(10));
    let c = sim.timeout("gate", Micros(10));
    assert_eq!(
        kinds(&c),
        vec!["SignalTimedOut", "TaskCompleted", "TaskScheduled"]
    );
    assert_eq!(sim.snapshot.status, RunStatus::Active);
}
#[test]
fn straggler_finishes_after_wait_resolved() {
    let (mut sim, ids) = straggler(None);
    signal(&mut sim, "gate", "ok");
    let c = branch_done(&mut sim, &ids[2]);
    assert_eq!(kinds(&c), vec!["TaskCompleted", "BranchContributed"]);
    assert!(c.run_state.is_none());
    done(&mut sim, "t", "ok");
    assert_eq!(sim.snapshot.status, RunStatus::Completed);
}
#[test]
fn same_snapshot_plans_identically() {
    let mut sim = setup_fan(graphs::fan_out(JoinPolicy::All), 3);
    let id = branches(&sim)[0].clone();
    sim.claim(&id);
    let outcome = NodeOutcome::Done(Outcome::done("ok"));
    assert_eq!(
        plan_outcome(&sim.graph, &sim.snapshot, &id, outcome.clone(), Micros(0)),
        plan_outcome(&sim.graph, &sim.snapshot, &id, outcome, Micros(0))
    );
}
#[test]
fn branch_exhausted_under_all() {
    let mut sim = setup_fan(graphs::fan_out(JoinPolicy::All), 2);
    let ids = branches(&sim);
    branch_done(&mut sim, &ids[0]);
    let c = fail(&mut sim, &ids[1], false);
    assert_eq!(
        kinds(&c),
        vec![
            "TaskFailed",
            "TaskExhausted",
            "BranchContributed",
            "JoinSatisfied",
            "TaskScheduled"
        ]
    );
    let input: JoinInput = serde_json::from_value(sim.task_by_node("jn", 0).input.clone()).unwrap();
    assert!(!input.quorum_met);
    assert!(matches!(
        input.results[1],
        BranchResult::Failed { index: 1, .. }
    ));
}
#[test]
fn branch_exhausted_under_quorum() {
    let mut sim = setup_fan(graphs::fan_out(JoinPolicy::Quorum(2)), 2);
    let ids = branches(&sim);
    fail(&mut sim, &ids[0], false);
    let c = branch_done(&mut sim, &ids[1]);
    assert!(c.events.iter().any(|e| matches!(
        e,
        RunEvent::JoinSatisfied {
            received: 1,
            failed: 1,
            quorum_met: false,
            ..
        }
    )));
}
#[test]
fn wait_resolved_by_signal() {
    let mut sim = start(graphs::wait_with_deadline(10_000_000));
    let c = done(&mut sim, "a", "ok");
    assert_eq!(c.new_signal.unwrap().deadline_at, Some(Micros(10_000_000)));
    assert_eq!(sim.snapshot.status, RunStatus::Parked);
    let c = signal(&mut sim, "gate", "ok");
    assert_eq!(
        kinds(&c),
        vec![
            "RunResumed",
            "SignalReceived",
            "TaskCompleted",
            "RunCompleted"
        ]
    );
    assert_eq!(c.resolved_signals[0].label, "ok");
}
#[test]
fn wait_timeout_follows_timeout_edge() {
    let mut sim = start(graphs::wait_with_deadline(10));
    done(&mut sim, "a", "ok");
    let c = sim.timeout("gate", Micros(10));
    assert_eq!(
        kinds(&c),
        vec!["RunResumed", "SignalTimedOut", "TaskCompleted", "RunFailed"]
    );
}
#[test]
fn retry_backoff_one_two_four() {
    let mut sim = start(no_failed(4));
    let id = sim.task_by_node("a", 0).task_id.clone();
    for (i, delay) in [1_000_000, 2_000_000, 4_000_000].into_iter().enumerate() {
        let c = fail(&mut sim, &id, true);
        assert_eq!(c.task_updates[0].run_at, Micros(delay));
        assert_eq!(c.task_updates[0].attempt, u32::try_from(i).unwrap() + 2);
    }
    let c = fail(&mut sim, &id, true);
    assert_eq!(kinds(&c), vec!["TaskFailed", "TaskExhausted", "RunFailed"]);
}
#[test]
fn exhausted_with_failed_edge() {
    let mut sim = start(recovery());
    let id = sim.task_by_node("a", 0).task_id.clone();
    let c = fail(&mut sim, &id, false);
    assert_eq!(c.new_tasks[0].node_id.as_str(), "recover");
    assert_eq!(c.new_tasks[0].input, json!({"message":"broken"}));
    assert_eq!(sim.snapshot.status, RunStatus::Active);
}
#[test]
fn exhausted_without_failed_edge() {
    let mut sim = start(no_failed(1));
    let id = sim.task_by_node("a", 0).task_id.clone();
    let c = fail(&mut sim, &id, false);
    assert_eq!(kinds(&c), vec!["TaskFailed", "TaskExhausted", "RunFailed"]);
    assert_eq!(sim.snapshot.status, RunStatus::Failed);
}
#[test]
fn plan_exhausted_from_store_reclaim() {
    let mut sim = start(recovery());
    let id = sim.task_by_node("a", 0).task_id.clone();
    sim.claim(&id);
    sim.reclaim(&id);
    let c = sim.exhausted(&id, Micros(10));
    assert_eq!(kinds(&c), vec!["TaskScheduled"]);
    assert_eq!(c.task_updates[0].planned_at, Some(Micros(10)));
    assert_eq!(sim.snapshot.status, RunStatus::Active);
    assert!(matches!(
        plan_exhausted(&sim.graph, &sim.snapshot, &id, Micros(10)),
        Err(PlanError::AlreadyPlanned { .. })
    ));
}
#[test]
fn exhausted_ignores_default_edge() {
    let mut sim = start(no_failed(1));
    let id = sim.task_by_node("a", 0).task_id.clone();
    let c = fail(&mut sim, &id, false);
    assert_eq!(c.new_tasks, []);
    assert_eq!(sim.snapshot.status, RunStatus::Failed);
}
#[test]
fn no_edge_fails_run() {
    let graph = GraphBuilder::new("no-edge", 1)
        .start("a")
        .task("a")
        .end("done", EndStatus::Completed)
        .edge_on("a", "done", "ok")
        .build()
        .unwrap();
    let mut sim = start(graph);
    let c = done(&mut sim, "a", "nope");
    assert_eq!(kinds(&c), vec!["TaskCompleted", "RunFailed"]);
    assert!(matches!(&c.events[1],RunEvent::RunFailed {message} if message.contains("nope")));
}
#[test]
fn cancel_from_parked() {
    let mut sim = start(graphs::loop_via_wait());
    done(&mut sim, "plan", "ok");
    let id = sim.open_signal("signoff").signal_id.clone();
    let c = sim.cancel("stop", Micros(0));
    assert_eq!(kinds(&c), vec!["RunCancelled"]);
    assert_eq!(c.task_updates[0].status, TaskStatus::Cancelled);
    assert_eq!(c.resolved_signals[0].label, "cancelled");
    assert!(matches!(
        plan_signal(
            &sim.graph,
            &sim.snapshot,
            &id,
            SignalPayload {
                label: "approved".into(),
                payload: Value::Null
            },
            Micros(0)
        ),
        Err(PlanError::RunTerminal { .. })
    ));
}
#[test]
fn cancel_leaves_running_tasks() {
    let mut sim = setup_fan(graphs::fan_out(JoinPolicy::All), 2);
    let ids = branches(&sim);
    sim.claim(&ids[0]);
    let c = sim.cancel("stop", Micros(0));
    assert!(c.task_updates.iter().all(|u| u.task_id != ids[0]));
    assert_eq!(c.task_updates[0].task_id, ids[1]);
    assert_eq!(c.task_updates[0].status, TaskStatus::Cancelled);
}
#[test]
fn terminal_rejects_every_planner_call() {
    let mut sim = start(graphs::linear());
    done(&mut sim, "a", "ok");
    done(&mut sim, "b", "ok");
    let task = "missing".into();
    let sig = "missing".into();
    let results = [
        plan_outcome(
            &sim.graph,
            &sim.snapshot,
            &task,
            NodeOutcome::Done(Outcome::done("ok")),
            Micros(0),
        ),
        plan_signal(
            &sim.graph,
            &sim.snapshot,
            &sig,
            SignalPayload {
                label: "ok".into(),
                payload: Value::Null,
            },
            Micros(0),
        ),
        plan_timeout(&sim.graph, &sim.snapshot, &sig, Micros(0)),
        plan_exhausted(&sim.graph, &sim.snapshot, &task, Micros(0)),
        plan_cancel(&sim.graph, &sim.snapshot, "stop".into(), Micros(0)),
    ];
    assert!(results.iter().all(|r| matches!(
        r,
        Err(PlanError::RunTerminal {
            status: RunStatus::Completed
        })
    )));
}
#[test]
fn end_reached_with_straggler() {
    let mut sim = setup_fan(graphs::fan_out(JoinPolicy::Quorum(1)), 2);
    let ids = branches(&sim);
    for id in &ids {
        sim.claim(id);
    }
    branch_done(&mut sim, &ids[0]);
    done(&mut sim, "jn", "ok");
    assert_eq!(sim.snapshot.status, RunStatus::Completed);
    assert_eq!(sim.all_tasks[&ids[1]].status, TaskStatus::Running);
}
#[test]
fn kind_mismatch_and_empty_fanout() {
    let mut sim = start(graphs::fan_out(JoinPolicy::All));
    let a = sim.task_by_node("a", 0).task_id.clone();
    sim.claim(&a);
    assert!(matches!(
        plan_outcome(
            &sim.graph,
            &sim.snapshot,
            &a,
            NodeOutcome::FanOut(vec![Value::Null]),
            Micros(0)
        ),
        Err(PlanError::KindMismatch { .. })
    ));
    sim.outcome(&a, NodeOutcome::Done(Outcome::done("ok")), Micros(0));
    let fo = sim.task_by_node("fo", 0).task_id.clone();
    sim.claim(&fo);
    assert!(matches!(
        plan_outcome(
            &sim.graph,
            &sim.snapshot,
            &fo,
            NodeOutcome::Done(Outcome::done("ok")),
            Micros(0)
        ),
        Err(PlanError::KindMismatch { .. })
    ));
    assert!(matches!(
        plan_outcome(
            &sim.graph,
            &sim.snapshot,
            &fo,
            NodeOutcome::FanOut(vec![]),
            Micros(0)
        ),
        Err(PlanError::EmptyFanOut { .. })
    ));
}
#[test]
fn invalid_run_id_rejected() {
    for id in ["a/b", ""] {
        assert!(matches!(
            plan_start(&graphs::linear(), id.into(), Value::Null, Micros(0)),
            Err(PlanError::InvalidRunId { .. })
        ));
    }
}
#[test]
fn fan_out_exhausted_fails_run() {
    let mut sim = start(graphs::fan_out(JoinPolicy::All));
    done(&mut sim, "a", "ok");
    let id = sim.task_by_node("fo", 0).task_id.clone();
    let c = fail(&mut sim, &id, false);
    assert_eq!(kinds(&c), vec!["TaskFailed", "TaskExhausted", "RunFailed"]);
}
#[test]
fn graph_mismatch_rejected() {
    let mut sim = start(graphs::linear());
    sim.snapshot.definition_hash = "wrong".into();
    assert!(matches!(
        plan_cancel(&sim.graph, &sim.snapshot, "stop".into(), Micros(0)),
        Err(PlanError::GraphMismatch { .. })
    ));
}
#[test]
fn status_column_equals_machine_state() {
    let mut sim = start(graphs::loop_via_wait());
    done(&mut sim, "plan", "ok");
    signal(&mut sim, "signoff", "approved");
    done(&mut sim, "implement", "ok");
    for c in &sim.commits {
        if let Some(state) = &c.run_state {
            let mut effects = dmt_core::Effects::default();
            let machine =
                dmt_core::restore(state.status, &state.machine_json, &mut effects).unwrap();
            assert_eq!(RunStatus::from(machine.state()), state.status);
            assert_eq!(effects.emitted, []);
        }
    }
}
