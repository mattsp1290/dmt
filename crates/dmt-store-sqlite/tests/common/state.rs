use dmt_core::{
    Graph, JoinPolicy, Micros, NodeOutcome, Outcome, RunEvent, RunId, RunSnapshot, RunStatus,
    SignalPayload, SignalRecord, TaskId, TaskRecord, fixtures, plan_signal,
};
use dmt_store::{ClaimedTask, EventRecord, RunFilter, RunSummary, Store};
use dmt_store_conformance::harness::{self as h, LEASE, T0};
use serde_json::json;

pub struct MixedState {
    pub graphs: Vec<Graph>,
    pub runs: Vec<RunId>,
    pub tasks: Vec<TaskId>,
    pub leased: ClaimedTask,
}
#[derive(Debug, PartialEq)]
pub struct Observed {
    graphs: Vec<Option<Graph>>,
    runs: Vec<Option<RunSnapshot>>,
    tasks: Vec<Option<TaskRecord>>,
    events: Vec<Vec<EventRecord>>,
    summaries: Vec<RunSummary>,
    due: Vec<SignalRecord>,
    exhausted: Vec<TaskRecord>,
}

pub fn done() -> NodeOutcome {
    NodeOutcome::Done(Outcome::with_payload("ok", json!(1)))
}

pub async fn finish_node(
    store: &impl Store,
    graph: &Graph,
    run: &RunId,
    node: &str,
    outcome: NodeOutcome,
) {
    let claimed = h::claim_one("state", store, "w1", T0, graph, &h::task_id(run, node, 0)).await;
    h::complete("state", store, graph, &claimed, outcome, T0)
        .await
        .unwrap();
}

pub async fn parked_run(store: &impl Store, graph: &Graph, name: &str) -> RunId {
    let run = h::start_run("state", store, graph, name, T0).await;
    finish_node(store, graph, &run, "a", done()).await;
    finish_node(
        store,
        graph,
        &run,
        "fo",
        NodeOutcome::FanOut(vec![json!(0), json!(1), json!(2)]),
    )
    .await;
    for index in 0..3 {
        let id = h::branch_id(&run, "br", 0, index);
        let claimed = h::claim_one("state", store, "w1", T0, graph, &id).await;
        h::complete("state", store, graph, &claimed, done(), T0)
            .await
            .unwrap();
    }
    finish_node(store, graph, &run, "jn", done()).await;
    run
}

pub async fn mixed_state(store: &impl Store) -> MixedState {
    let graphs = vec![
        fixtures::fan_out_then_wait(JoinPolicy::All, Some(60_000_000)),
        fixtures::retry_chain(1),
        fixtures::linear(),
    ];
    let wait = parked_run(store, &graphs[0], "run-wait").await;
    let exhaust = h::start_run("state", store, &graphs[1], "run-exhaust", T0).await;
    h::claim_one(
        "state",
        store,
        "w1",
        T0,
        &graphs[1],
        &h::task_id(&exhaust, "a", 0),
    )
    .await;
    assert_eq!(
        h::claim_all(
            "state",
            store,
            "w1",
            T0.saturating_add(LEASE + 1),
            &graphs[1]
        )
        .await,
        []
    );
    let lease = h::start_run("state", store, &graphs[2], "run-lease", T0).await;
    let leased = h::claim_one(
        "state",
        store,
        "w1",
        T0.saturating_add(2 * LEASE),
        &graphs[2],
        &h::task_id(&lease, "a", 0),
    )
    .await;
    let runs = vec![wait, exhaust, lease];
    let mut tasks = Vec::new();
    for run in &runs {
        for record in h::events("state", store, run).await {
            if let RunEvent::TaskScheduled { task_id, .. } = record.event {
                tasks.push(task_id);
            }
        }
    }
    tasks.sort();
    tasks.dedup();
    MixedState {
        graphs,
        runs,
        tasks,
        leased,
    }
}

pub async fn observe(store: &impl Store, state: &MixedState) -> Observed {
    let mut graphs = Vec::new();
    for graph in &state.graphs {
        graphs.push(store.load_graph(graph.id(), graph.version()).await.unwrap());
    }
    let mut runs = Vec::new();
    let mut events = Vec::new();
    for run in &state.runs {
        runs.push(store.load_run(run).await.unwrap());
        events.push(store.events(run, 0, 10_000).await.unwrap());
    }
    let mut tasks = Vec::new();
    for id in &state.tasks {
        tasks.push(store.load_task(id).await.unwrap());
    }
    Observed {
        graphs,
        runs,
        tasks,
        events,
        summaries: store.list_runs(RunFilter::all(100)).await.unwrap(),
        due: store.due_signals(Micros(i64::MAX), 100).await.unwrap(),
        exhausted: store.exhausted_tasks(100).await.unwrap(),
    }
}

pub async fn continue_runs(store: &impl Store, state: &MixedState) {
    let now = T0.saturating_add(3 * LEASE);
    assert!(matches!(
        store
            .heartbeat(&state.leased.proof, now, LEASE)
            .await
            .unwrap(),
        dmt_store::HeartbeatResult::Extended { .. }
    ));
    h::complete(
        "restart",
        store,
        &state.graphs[2],
        &state.leased,
        done(),
        now,
    )
    .await
    .unwrap();
    let run = &state.runs[0];
    let signal = store.find_open_signal(run, "gate").await.unwrap().unwrap();
    let snapshot = h::snapshot("restart", store, run).await;
    let commit = plan_signal(
        &state.graphs[0],
        &snapshot,
        &signal.signal_id,
        SignalPayload {
            label: "ok".into(),
            payload: json!(1),
        },
        now,
    )
    .unwrap();
    store.apply(commit, None).await.unwrap();
    let claimed = h::claim_one(
        "restart",
        store,
        "w1",
        now,
        &state.graphs[0],
        &h::task_id(run, "t", 0),
    )
    .await;
    h::complete("restart", store, &state.graphs[0], &claimed, done(), now)
        .await
        .unwrap();
    assert_eq!(
        h::snapshot("restart", store, run).await.status,
        RunStatus::Completed
    );
    let exhausted = store.exhausted_tasks(10).await.unwrap();
    assert_eq!(exhausted.len(), 1);
    assert_eq!(exhausted[0].run_id, state.runs[1]);
    for run in &state.runs {
        h::assert_contiguous("restart", &h::events("restart", store, run).await);
    }
}
