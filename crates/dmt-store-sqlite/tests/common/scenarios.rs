use super::state::{done, finish_node};
use dmt_core::{
    Commit, Graph, JoinPolicy, NodeOutcome, RunId, SignalPayload, fixtures, plan_cancel,
    plan_outcome, plan_signal, plan_start,
};
use dmt_store::{ApplyResult, ClaimRequest, ClaimedTask, LeaseProof, Store, StoreError};
use dmt_store_conformance::harness::{self as h, LEASE, T0};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy)]
pub enum Scenario {
    FanOut,
    Join,
    Wait,
    Signal,
    Create,
    Claim,
}
#[derive(Debug, Clone)]
pub enum Operation {
    Apply(Commit, Option<LeaseProof>),
    Create(Commit),
    Claim(ClaimRequest),
}
#[derive(Debug, PartialEq)]
pub enum ResultValue {
    Applied(ApplyResult),
    Created(RunId),
    Claimed(Vec<ClaimedTask>),
}
impl Operation {
    pub async fn run(&self, store: &impl Store) -> Result<ResultValue, StoreError> {
        match self {
            Self::Apply(commit, proof) => store
                .apply(commit.clone(), proof.clone())
                .await
                .map(ResultValue::Applied),
            Self::Create(commit) => store
                .create_run(commit.clone())
                .await
                .map(ResultValue::Created),
            Self::Claim(request) => store
                .claim_ready(request.clone())
                .await
                .map(ResultValue::Claimed),
        }
    }
    pub fn lower_bound(&self) -> usize {
        match self {
            Self::Apply(commit, _) | Self::Create(commit) => {
                2 + commit.events.len() + commit.new_tasks.len() + commit.task_updates.len()
            }
            Self::Claim(_) => 14,
        }
    }
}

async fn planned(
    store: &impl Store,
    graph: &Graph,
    id: &dmt_core::TaskId,
    outcome: NodeOutcome,
) -> Operation {
    let claimed = h::claim_one("atomicity", store, "w1", T0, graph, id).await;
    let snapshot = h::snapshot("atomicity", store, &claimed.task.run_id).await;
    Operation::Apply(
        plan_outcome(graph, &snapshot, id, outcome, T0).unwrap(),
        Some(claimed.proof),
    )
}

pub async fn prepare(store: &impl Store, scenario: Scenario) -> Operation {
    if matches!(scenario, Scenario::Claim) {
        return claim(store).await;
    }
    let graph = fixtures::fan_out_then_wait(JoinPolicy::All, None);
    if matches!(scenario, Scenario::Create) {
        store.register_graph(&graph).await.unwrap();
        return Operation::Create(plan_start(&graph, h::run("run-1"), Value::Null, T0).unwrap());
    }
    let run = h::start_run("atomicity", store, &graph, "run-1", T0).await;
    finish_node(store, &graph, &run, "a", done()).await;
    let fanout = planned(
        store,
        &graph,
        &h::task_id(&run, "fo", 0),
        NodeOutcome::FanOut(vec![json!(0), json!(1), json!(2)]),
    )
    .await;
    if matches!(scenario, Scenario::FanOut) {
        return fanout;
    }
    fanout.run(store).await.unwrap();
    for index in 0..2 {
        planned(store, &graph, &h::branch_id(&run, "br", 0, index), done())
            .await
            .run(store)
            .await
            .unwrap();
    }
    let join = planned(store, &graph, &h::branch_id(&run, "br", 0, 2), done()).await;
    if matches!(scenario, Scenario::Join) {
        return join;
    }
    join.run(store).await.unwrap();
    let wait = planned(store, &graph, &h::task_id(&run, "jn", 0), done()).await;
    if matches!(scenario, Scenario::Wait) {
        return wait;
    }
    wait.run(store).await.unwrap();
    let snapshot = h::snapshot("atomicity", store, &run).await;
    let signal = store.find_open_signal(&run, "gate").await.unwrap().unwrap();
    Operation::Apply(
        plan_signal(
            &graph,
            &snapshot,
            &signal.signal_id,
            SignalPayload {
                label: "ok".into(),
                payload: json!(1),
            },
            T0,
        )
        .unwrap(),
        None,
    )
}

async fn claim(store: &impl Store) -> Operation {
    let fanout = fixtures::fan_out(JoinPolicy::All);
    let branches = h::start_run("claim", store, &fanout, "run-branches", T0).await;
    finish_node(store, &fanout, &branches, "a", done()).await;
    finish_node(
        store,
        &fanout,
        &branches,
        "fo",
        NodeOutcome::FanOut(vec![json!(0), json!(1), json!(2)]),
    )
    .await;
    let retry = fixtures::retry_chain(1);
    let exhaust = h::start_run("claim", store, &retry, "run-exhaust", T0).await;
    h::claim_one(
        "claim",
        store,
        "w1",
        T0,
        &retry,
        &h::task_id(&exhaust, "a", 0),
    )
    .await;
    let linear = fixtures::linear();
    let cancelled = h::start_run("claim", store, &linear, "run-terminal", T0).await;
    h::claim_one(
        "claim",
        store,
        "w1",
        T0,
        &linear,
        &h::task_id(&cancelled, "a", 0),
    )
    .await;
    let snapshot = h::snapshot("claim", store, &cancelled).await;
    store
        .apply(
            plan_cancel(&linear, &snapshot, "cancel".into(), T0).unwrap(),
            None,
        )
        .await
        .unwrap();
    Operation::Claim(h::claim_request(
        "w2",
        T0.saturating_add(LEASE + 1),
        &[&fanout, &retry, &linear],
        8,
    ))
}
