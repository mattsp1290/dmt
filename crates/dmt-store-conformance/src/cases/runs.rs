use crate::{
    StoreFactory,
    harness::{
        LEASE, T0, assert_contiguous, branches, claim_all, claim_one, complete, done, events,
        exhaust, exhaust_then_continue, kinds, ok, plan_done, run, signal_id, snapshot, start_run,
        task_id,
    },
};
use dmt_core::{
    EndStatus, GraphBuilder, JoinPolicy, NewRun, RunStatus, SignalPayload, TaskStatus, fixtures,
    plan_exhausted, plan_signal, plan_start, plan_timeout,
};
use dmt_store::{RunFilter, Store, StoreError};
use serde_json::{Value, json};

fn short_graph(version: u32) -> dmt_core::Graph {
    GraphBuilder::new("linear", version)
        .start("a")
        .task("a")
        .end("done", EndStatus::Completed)
        .edge("a", "done")
        .build()
        .unwrap()
}
/// Prove transactional rule 11.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_register_graph<F: StoreFactory>(factory: &F) {
    const CASE: &str = "register_graph";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    for _ in 0..2 {
        ok(CASE, store.register_graph(&graph).await);
    }
    assert_eq!(
        ok(CASE, store.load_graph(graph.id(), 1).await),
        Some(graph.clone()),
        "{CASE}: graph roundtrip"
    );
    assert_eq!(
        store.register_graph(&short_graph(1)).await,
        Err(StoreError::GraphMismatch {
            graph_id: graph.id().clone(),
            version: 1
        }),
        "{CASE}: hash guard"
    );
    assert_eq!(
        ok(CASE, store.load_graph(graph.id(), 1).await),
        Some(graph.clone()),
        "{CASE}: graph overwritten"
    );
    assert!(
        ok(CASE, store.load_graph(graph.id(), 2).await).is_none(),
        "{CASE}: unknown version"
    );
    assert!(
        ok(CASE, store.load_graph(&"missing".into(), 1).await).is_none(),
        "{CASE}: unknown graph"
    );
}
/// Prove transactional rule 12.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_create_run<F: StoreFactory>(factory: &F) {
    const CASE: &str = "create_run";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = run("run-1");
    let start = ok(CASE, plan_start(&graph, run.clone(), Value::Null, T0));
    assert!(
        matches!(
            store.create_run(start.clone()).await,
            Err(StoreError::NotFound(_))
        ),
        "{CASE}: unregistered graph"
    );
    assert!(
        ok(CASE, store.load_run(&run).await).is_none(),
        "{CASE}: failed create wrote run"
    );
    assert!(
        ok(CASE, store.list_runs(RunFilter::all(10)).await).is_empty(),
        "{CASE}: failed create summary"
    );
    ok(CASE, store.register_graph(&graph).await);
    assert_eq!(
        ok(CASE, store.create_run(start.clone()).await),
        run,
        "{CASE}: returned run"
    );
    let s = snapshot(CASE, &store, &run).await;
    assert_eq!(
        (s.version, s.status),
        (1, RunStatus::Active),
        "{CASE}: initial run state"
    );
    assert_eq!(
        (&s.graph_id, s.graph_version, s.definition_hash.as_str()),
        (graph.id(), 1, graph.definition_hash().as_str()),
        "{CASE}: graph key/hash"
    );
    assert_eq!(s.tasks.len(), 1, "{CASE}: initial tasks");
    let a = &s.tasks[0];
    assert_eq!(
        (&a.task_id, a.status, a.attempt),
        (&task_id(&run, "a", 0), TaskStatus::Ready, 1),
        "{CASE}: initial task"
    );
    assert!(
        a.lease_owner.is_none() && a.lease_until.is_none(),
        "{CASE}: initial lease"
    );
    assert_eq!(
        s.node_occurrences,
        [("a".into(), 1)].into(),
        "{CASE}: occurrences"
    );
    assert!(
        s.joins.is_empty() && s.signals.is_empty(),
        "{CASE}: initial relations"
    );
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        kinds(&ev),
        ["RunStarted", "TaskScheduled"],
        "{CASE}: initial events"
    );
    assert_contiguous(CASE, &ev);
    assert_eq!(
        store.create_run(start).await,
        Err(StoreError::VersionConflict {
            expected: 0,
            actual: 1
        }),
        "{CASE}: duplicate run"
    );
    reject_start_hash(CASE, &store).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    let outcome = plan_done(CASE, &store, &graph, &a.task.task_id, "ok", T0).await;
    assert!(
        matches!(
            store.create_run(outcome.clone()).await,
            Err(StoreError::InvalidCommit(_))
        ),
        "{CASE}: wrong create shape"
    );
    let mut bad = outcome.clone();
    bad.new_run = Some(NewRun {
        graph_id: graph.id().clone(),
        graph_version: 1,
        definition_hash: graph.definition_hash(),
        input: Value::Null,
    });
    assert!(
        matches!(
            store.apply(bad, Some(a.proof.clone())).await,
            Err(StoreError::InvalidCommit(_))
        ),
        "{CASE}: apply start shape"
    );
    ok(CASE, store.apply(outcome, Some(a.proof)).await);
}
/// Prove transactional rule 9.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_event_sequence<F: StoreFactory>(factory: &F) {
    const CASE: &str = "event_sequence";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    for (node, time) in [("a", 10), ("b", 20)] {
        let a = claim_one(
            CASE,
            &store,
            "w1",
            T0.saturating_add(time),
            &graph,
            &task_id(&run, node, 0),
        )
        .await;
        ok(
            CASE,
            complete(
                CASE,
                &store,
                &graph,
                &a,
                done(),
                T0.saturating_add(time + 1),
            )
            .await,
        );
    }
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        kinds(&ev),
        [
            "RunStarted",
            "TaskScheduled",
            "TaskClaimed",
            "TaskCompleted",
            "TaskScheduled",
            "TaskClaimed",
            "TaskCompleted",
            "RunCompleted"
        ],
        "{CASE}: event order"
    );
    assert_contiguous(CASE, &ev);
    for (index, time) in [(0, 0), (2, 10), (3, 11), (5, 20), (6, 21)] {
        assert_eq!(
            ev[index].recorded_at,
            T0.saturating_add(time),
            "{CASE}: timestamp at {index}"
        );
    }
    assert_eq!(
        ok(CASE, store.events(&run, 3, 2).await)
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        [4, 5],
        "{CASE}: pagination"
    );
    assert!(
        ok(CASE, store.events(&run, 8, 10).await).is_empty(),
        "{CASE}: end cursor"
    );
    assert!(
        ok(CASE, store.events(&"unknown".into(), 0, 10).await).is_empty(),
        "{CASE}: unknown run events"
    );
}
/// Prove transactional rule 14.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_micros_ordering<F: StoreFactory>(factory: &F) {
    const CASE: &str = "micros_ordering";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    for (name, time) in [("run-3", 0), ("run-2", 1), ("run-1", 2)] {
        start_run(CASE, &store, &graph, name, T0.saturating_add(time)).await;
    }
    let got = claim_all(CASE, &store, "w1", T0.saturating_add(1), &graph).await;
    assert_eq!(
        got.iter()
            .map(|c| c.task.task_id.as_str())
            .collect::<Vec<_>>(),
        ["run-3/a/0", "run-2/a/0"],
        "{CASE}: microsecond claim order"
    );
    let got = claim_all(CASE, &store, "w1", T0.saturating_add(2), &graph).await;
    assert_eq!(
        got.iter()
            .map(|c| c.task.task_id.as_str())
            .collect::<Vec<_>>(),
        ["run-1/a/0"],
        "{CASE}: later ready task"
    );
    let wait = fixtures::wait_with_deadline(1);
    let run = start_run(CASE, &store, &wait, "run-4", T0).await;
    let a = claim_one(
        CASE,
        &store,
        "w1",
        T0.saturating_add(100),
        &wait,
        &task_id(&run, "a", 0),
    )
    .await;
    ok(
        CASE,
        complete(CASE, &store, &wait, &a, done(), T0.saturating_add(100)).await,
    );
    let s = snapshot(CASE, &store, &run).await;
    assert_eq!(
        s.signals[0].deadline_at,
        Some(T0.saturating_add(101)),
        "{CASE}: deadline precision"
    );
    assert!(
        ok(CASE, store.due_signals(T0.saturating_add(100), 10).await).is_empty(),
        "{CASE}: early deadline"
    );
    assert_eq!(
        ok(CASE, store.due_signals(T0.saturating_add(101), 10).await),
        s.signals,
        "{CASE}: inclusive deadline"
    );
    let timeout = ok(
        CASE,
        plan_timeout(&wait, &s, &s.signals[0].signal_id, T0.saturating_add(101)),
    );
    ok(CASE, store.apply(timeout, None).await);
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Failed,
        "{CASE}: timeout route"
    );
    assert!(
        ok(CASE, store.due_signals(T0.saturating_add(101), 10).await).is_empty(),
        "{CASE}: resolved deadline returned"
    );
}
/// Prove load run view.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_load_run_view<F: StoreFactory>(factory: &F) {
    const CASE: &str = "load_run_view";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out_then_wait(JoinPolicy::All, None);
    let run = branches(CASE, &store, &graph, "run-1", 2).await;
    let claimed = claim_all(CASE, &store, "w1", T0, &graph).await;
    for a in claimed {
        ok(CASE, complete(CASE, &store, &graph, &a, done(), T0).await);
    }
    let jn = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "jn", 0)).await;
    ok(CASE, complete(CASE, &store, &graph, &jn, done(), T0).await);
    let s = snapshot(CASE, &store, &run).await;
    assert_eq!(s.status, RunStatus::Parked, "{CASE}: parked");
    assert!(
        s.tasks
            .iter()
            .any(|t| t.task_id == task_id(&run, "w", 0) && t.status == TaskStatus::Awaiting),
        "{CASE}: awaiting row hidden"
    );
    assert_eq!(s.joins.len(), 1, "{CASE}: satisfied join hidden");
    assert!(
        s.joins[0].satisfied_at.is_some(),
        "{CASE}: satisfaction hidden"
    );
    assert_eq!(s.signals.len(), 1, "{CASE}: open signal hidden");
    assert_eq!(
        s.signals[0].signal_id,
        signal_id(&run, "gate", 0),
        "{CASE}: signal id"
    );
    assert!(
        s.signals[0].resolved_at.is_none(),
        "{CASE}: open signal state"
    );
    let resolve = ok(
        CASE,
        plan_signal(
            &graph,
            &s,
            &s.signals[0].signal_id,
            SignalPayload {
                label: "ok".into(),
                payload: json!(true),
            },
            T0,
        ),
    );
    ok(CASE, store.apply(resolve, None).await);
    let s = snapshot(CASE, &store, &run).await;
    assert!(s.signals.is_empty(), "{CASE}: resolved row included");
    assert!(
        s.tasks
            .iter()
            .any(|t| t.task_id == task_id(&run, "t", 0) && t.status == TaskStatus::Ready),
        "{CASE}: ready row hidden"
    );
    let graph = exhaust_then_continue();
    let (run, a) = exhaust(CASE, &store, &graph, "run-2").await;
    let s = snapshot(CASE, &store, &run).await;
    assert!(
        s.tasks.iter().any(|t| t.task_id == a.task.task_id
            && t.status == TaskStatus::Exhausted
            && t.planned_at.is_none()),
        "{CASE}: unplanned exhaustion hidden"
    );
    let c = ok(
        CASE,
        plan_exhausted(
            &graph,
            &s,
            &a.task.task_id,
            T0.saturating_add(2 * LEASE + 3),
        ),
    );
    ok(CASE, store.apply(c, None).await);
    assert!(
        snapshot(CASE, &store, &run)
            .await
            .tasks
            .iter()
            .any(|t| t.task_id == a.task.task_id
                && t.status == TaskStatus::Exhausted
                && t.planned_at.is_some()),
        "{CASE}: planned exhaustion hidden"
    );
}
/// Prove list runs.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_list_runs<F: StoreFactory>(factory: &F) {
    const CASE: &str = "list_runs";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let retry = fixtures::retry_chain(3);
    let run1 = start_run(CASE, &store, &graph, "run-1", T0).await;
    start_run(CASE, &store, &retry, "run-2", T0.saturating_add(1)).await;
    start_run(CASE, &store, &graph, "run-3", T0.saturating_add(2)).await;
    for node in ["a", "b"] {
        let a = claim_one(
            CASE,
            &store,
            "w1",
            T0.saturating_add(1),
            &graph,
            &task_id(&run1, node, 0),
        )
        .await;
        ok(
            CASE,
            complete(CASE, &store, &graph, &a, done(), T0.saturating_add(1)).await,
        );
    }
    let rows = ok(CASE, store.list_runs(RunFilter::all(10)).await);
    assert_eq!(
        rows.iter().map(|r| r.run_id.as_str()).collect::<Vec<_>>(),
        ["run-1", "run-2", "run-3"],
        "{CASE}: ordering"
    );
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(
            r.created_at,
            T0.saturating_add(i64::try_from(i).unwrap()),
            "{CASE}: created time"
        );
        assert_eq!(r.graph_version, 1, "{CASE}: graph version");
        assert_eq!(
            r.version,
            snapshot(CASE, &store, &r.run_id).await.version,
            "{CASE}: version"
        );
    }
    assert_eq!(
        rows[0].updated_at,
        T0.saturating_add(1),
        "{CASE}: updated time"
    );
    assert_eq!(
        ok(
            CASE,
            store
                .list_runs(RunFilter {
                    status: Some(RunStatus::Completed),
                    ..RunFilter::all(10)
                })
                .await
        ),
        vec![rows[0].clone()],
        "{CASE}: status filter"
    );
    assert_eq!(
        ok(
            CASE,
            store
                .list_runs(RunFilter {
                    graph_id: Some(graph.id().clone()),
                    ..RunFilter::all(10)
                })
                .await
        ),
        vec![rows[0].clone(), rows[2].clone()],
        "{CASE}: graph filter"
    );
    assert_eq!(
        ok(CASE, store.list_runs(RunFilter::all(2)).await),
        rows[..2],
        "{CASE}: limit"
    );
    assert!(
        ok(CASE, store.list_runs(RunFilter::all(0)).await).is_empty(),
        "{CASE}: zero limit"
    );
}

async fn reject_start_hash<S: Store>(case: &str, store: &S) {
    let graph2 = short_graph(2);
    ok(case, store.register_graph(&graph2).await);
    let mut bad = ok(case, plan_start(&graph2, "run-2".into(), Value::Null, T0));
    bad.new_run.as_mut().unwrap().definition_hash = "bogus".into();
    assert_eq!(
        store.create_run(bad).await,
        Err(StoreError::GraphMismatch {
            graph_id: graph2.id().clone(),
            version: 2
        }),
        "{case}: start hash guard"
    );
    assert!(
        ok(case, store.load_run(&"run-2".into()).await).is_none(),
        "{case}: mismatched create wrote"
    );
}
