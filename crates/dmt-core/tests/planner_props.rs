mod common;
use common::Sim;
use dmt_core::{
    EndStatus, Graph, GraphBuilder, JoinPolicy, Micros, NodeOutcome, Outcome, RunEvent, RunStatus,
    SignalPayload, TaskId, TaskStatus, fixtures, plan_cancel, plan_exhausted, plan_outcome,
    plan_signal, plan_timeout,
};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
const NOW: Micros = Micros(10_000_000);
fn outcome(sim: &mut Sim, id: &TaskId, outcome: NodeOutcome) {
    if sim.snapshot.task(id).unwrap().status == TaskStatus::Ready {
        sim.claim(id);
    }
    let first = plan_outcome(&sim.graph, &sim.snapshot, id, outcome.clone(), NOW).unwrap();
    assert_eq!(
        first,
        plan_outcome(&sim.graph, &sim.snapshot, id, outcome, NOW).unwrap()
    );
    sim.apply(first).unwrap();
    oracle(sim);
}
fn signal(sim: &mut Sim, label: &str, timeout: bool) {
    let id = sim.snapshot.signals[0].signal_id.clone();
    let plan = || {
        if timeout {
            plan_timeout(&sim.graph, &sim.snapshot, &id, NOW)
        } else {
            plan_signal(
                &sim.graph,
                &sim.snapshot,
                &id,
                SignalPayload {
                    label: label.into(),
                    payload: Value::Null,
                },
                NOW,
            )
        }
    };
    let first = plan().unwrap();
    assert_eq!(first, plan().unwrap());
    sim.apply(first).unwrap();
    oracle(sim);
}
fn maybe_cancel(sim: &mut Sim, step: u32, cancel_at: Option<u32>) -> bool {
    if Some(step) != cancel_at || sim.snapshot.status.is_terminal() {
        return false;
    }
    let first = plan_cancel(&sim.graph, &sim.snapshot, "cancel".into(), NOW).unwrap();
    assert_eq!(
        first,
        plan_cancel(&sim.graph, &sim.snapshot, "cancel".into(), NOW).unwrap()
    );
    sim.apply(first).unwrap();
    true
}
fn reclaim(sim: &mut Sim, id: &TaskId, count: u32, allow_exhaust: bool) -> u32 {
    if sim.snapshot.task(id).unwrap().status == TaskStatus::Ready {
        sim.claim(id);
    }
    let mut reclaimed = 0;
    for _ in 0..count {
        let task = sim.snapshot.task(id).unwrap();
        if task.status == TaskStatus::Exhausted
            || (!allow_exhaust && task.attempt == task.max_attempts)
        {
            break;
        }
        let before = task.attempt;
        sim.reclaim(id);
        reclaimed += 1;
        let after = sim.snapshot.task(id).unwrap();
        if after.status == TaskStatus::Exhausted {
            let first = plan_exhausted(&sim.graph, &sim.snapshot, id, NOW).unwrap();
            assert_eq!(
                first,
                plan_exhausted(&sim.graph, &sim.snapshot, id, NOW).unwrap()
            );
            sim.apply(first).unwrap();
            break;
        }
        assert_eq!(after.attempt, before + 1);
    }
    reclaimed
}
fn oracle(sim: &Sim) {
    if sim.snapshot.status.is_terminal() {
        return;
    }
    let pending = sim.snapshot.tasks.iter().any(|t| {
        matches!(t.status, TaskStatus::Ready | TaskStatus::Running)
            || (t.status == TaskStatus::Exhausted && t.planned_at.is_none())
    });
    let awaiting = sim
        .snapshot
        .tasks
        .iter()
        .any(|t| t.status == TaskStatus::Awaiting);
    assert_eq!(
        sim.snapshot.status == RunStatus::Parked,
        !pending && awaiting
    );
}
fn invariants(sim: &Sim, cancelled: bool, expected: RunStatus) {
    assert_eq!(
        sim.snapshot.status,
        if cancelled {
            RunStatus::Cancelled
        } else {
            expected
        }
    );
    let mut keys = BTreeSet::new();
    let mut signals = BTreeSet::new();
    for (index, c) in sim.commits.iter().enumerate() {
        c.check().unwrap();
        assert_eq!(c.expected_run_version, u64::try_from(index).unwrap());
        for task in &c.new_tasks {
            assert!(keys.insert(&task.step_key));
        }
        if let Some(signal) = &c.new_signal {
            assert!(signals.insert(&signal.key));
        }
        if let Some(state) = &c.run_state {
            let mut effects = dmt_core::Effects::default();
            let machine =
                dmt_core::restore(state.status, &state.machine_json, &mut effects).unwrap();
            assert_eq!(RunStatus::from(machine.state()), state.status);
            assert_eq!(effects.emitted, []);
        }
    }
    assert_eq!(
        sim.snapshot.version,
        u64::try_from(sim.commits.len()).unwrap()
    );
    let mut completed = BTreeMap::new();
    let mut satisfied = BTreeMap::new();
    for event in &sim.events {
        match event {
            RunEvent::TaskCompleted { task_id, .. } => {
                *completed.entry(task_id).or_insert(0) += 1;
            }
            RunEvent::JoinSatisfied { join_id, .. } => {
                *satisfied.entry(join_id).or_insert(0) += 1;
            }
            _ => {}
        }
    }
    for task in sim.all_tasks.values() {
        assert_eq!(
            completed.get(&task.task_id).copied().unwrap_or(0),
            u32::from(task.status == TaskStatus::Completed)
        );
    }
    for join in sim.all_joins.values() {
        assert_eq!(
            satisfied.get(&join.join_id).copied().unwrap_or(0),
            u32::from(join.satisfied_at.is_some())
        );
    }
    let parked = sim
        .events
        .iter()
        .filter(|e| matches!(e, RunEvent::RunParked))
        .count();
    let resumed = sim
        .events
        .iter()
        .filter(|e| matches!(e, RunEvent::RunResumed))
        .count();
    let cancelled_parked = sim.commits.last().is_some_and(|c| {
        c.events
            .iter()
            .any(|e| matches!(e, RunEvent::RunCancelled { .. }))
    }) && sim
        .commits
        .iter()
        .rev()
        .skip(1)
        .find_map(|c| c.run_state.as_ref())
        .is_some_and(|s| s.status == RunStatus::Parked);
    assert_eq!(parked, resumed + usize::from(cancelled_parked));
}
fn done_node(sim: &mut Sim, node: &str, label: &str, reclaims: u32) {
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
    reclaim(sim, &id, reclaims, false);
    outcome(sim, &id, NodeOutcome::Done(Outcome::done(label)));
}
fn linear(nodes: u32) -> Graph {
    let mut builder = GraphBuilder::new("linear-property", 1).start("n0");
    for i in 0..nodes {
        builder = builder.task(format!("n{i}"));
        builder = builder.edge(
            format!("n{i}"),
            if i + 1 == nodes {
                "done".into()
            } else {
                format!("n{}", i + 1)
            },
        );
    }
    builder.end("done", EndStatus::Completed).build().unwrap()
}
proptest! {
    #![proptest_config(ProptestConfig {cases:256,..ProptestConfig::default()})]
    #[test]
    fn linear_shapes(nodes in 1u32..=6,cancel in prop::option::of(0u32..30),reclaims in prop::collection::vec(0u32..3,6)) {
        let mut sim=Sim::start(linear(nodes),Value::Null,NOW);oracle(&sim);let mut cancelled=false;
        for step in 0..nodes {if maybe_cancel(&mut sim,step,cancel) {cancelled=true;break;}done_node(&mut sim,&format!("n{step}"),"ok",reclaims[usize::try_from(step).unwrap()]);}
        invariants(&sim,cancelled,RunStatus::Completed);
    }
    #[test]
    fn loop_shapes(rejections in 0u32..=4,cancel in prop::option::of(0u32..50),reclaims in prop::collection::vec(0u32..3,6)) {
        let mut sim=Sim::start(fixtures::loop_via_wait(),Value::Null,NOW);let mut cancelled=false;let mut step=0;
        for i in 0..=rejections {
            if maybe_cancel(&mut sim,step,cancel) {cancelled=true;break;}step+=1;
            done_node(&mut sim,"plan","ok",reclaims[usize::try_from(i).unwrap()]);
            if maybe_cancel(&mut sim,step,cancel) {cancelled=true;break;}step+=1;
            signal(&mut sim,if i<rejections {"changes_requested"} else {"approved"},false);
        }
        if !cancelled {if maybe_cancel(&mut sim,step,cancel) {cancelled=true;} else {done_node(&mut sim,"implement","ok",reclaims[5]);}}
        prop_assert!(step<=8*(rejections+1)+10);invariants(&sim,cancelled,RunStatus::Completed);
    }
    #[test]
    fn fan_out_shapes(branches in 1u32..=6,quorum in 0u32..=6,failures in prop::collection::vec(any::<bool>(),6),cancel in prop::option::of(0u32..60),reclaims in prop::collection::vec(0u32..3,10),wait in prop::option::of((any::<bool>(),any::<bool>())),rotation in 0usize..6) {
        let policy=if quorum==0 {JoinPolicy::All} else {JoinPolicy::Quorum(quorum.min(branches))};
        let graph=if let Some((deadline,_))=wait {fixtures::fan_out_then_wait(policy,deadline.then_some(10))} else {fixtures::fan_out(policy)};
        let mut sim=Sim::start(graph,Value::Null,NOW);let mut step=0;let mut cancelled=false;
        let mut ids=Vec::new();let mut contributed=BTreeSet::new();
        while !sim.snapshot.status.is_terminal() {
            prop_assert!(step<6*branches+20);
            if maybe_cancel(&mut sim,step,cancel) {cancelled=true;break;}step+=1;
            if !sim.snapshot.signals.is_empty() {signal(&mut sim,"ok",wait.is_some_and(|(deadline,timeout)|deadline&&timeout));continue;}
            if sim.snapshot.tasks.iter().any(|t|t.node_id.as_str()=="a"&&t.status==TaskStatus::Ready) {done_node(&mut sim,"a","ok",reclaims[0]);continue;}
            if sim.snapshot.tasks.iter().any(|t|t.node_id.as_str()=="fo"&&t.status==TaskStatus::Ready) {
                let id=sim.task_by_node("fo",0).task_id.clone();reclaim(&mut sim,&id,reclaims[1],false);outcome(&mut sim,&id,NodeOutcome::FanOut((0..branches).map(|n|json!(n)).collect()));
                ids=sim.snapshot.tasks.iter().filter(|t|t.node_id.as_str()=="br").map(|t|t.task_id.clone()).collect();let len=ids.len();ids.rotate_left(rotation%len);for id in &ids {sim.claim(id);}continue;
            }
            // Prefer the join to leave running stragglers while opening and resolving a wait.
            if sim.snapshot.tasks.iter().any(|t|t.node_id.as_str()=="jn"&&t.status==TaskStatus::Ready) {done_node(&mut sim,"jn","ok",reclaims[8]);continue;}
            if let Some(id)=ids.iter().find(|id|!contributed.contains(*id)).cloned() {
                let index=sim.snapshot.task(&id).unwrap().branch.as_ref().unwrap().index;contributed.insert(id.clone());reclaim(&mut sim,&id,reclaims[usize::try_from(index).unwrap()+2],false);
                outcome(&mut sim,&id,if failures[usize::try_from(index).unwrap()] {NodeOutcome::Fail {message:"branch failure".into(),retryable:false}} else {NodeOutcome::Done(Outcome::done("ok"))});continue;
            }
            done_node(&mut sim,"t","ok",reclaims[9]);
        }
        invariants(&sim,cancelled,RunStatus::Completed);
    }
    #[test]
    fn retry_shapes(max in 1u32..=5,failures in 0u32..=5,reclaims in 0u32..=5,cancel in prop::option::of(0u32..40)) {
        let mut sim=Sim::start(fixtures::retry_chain(max),Value::Null,NOW);let id=sim.task_by_node("a",0).task_id.clone();let mut cancelled=false;let mut step=0;let mut reclaim_count=0;let mut failed_count=0;
        while !sim.snapshot.status.is_terminal() {
            prop_assert!(step<4*max+10);
            if maybe_cancel(&mut sim,step,cancel) {cancelled=true;break;}step+=1;
            if reclaim_count<reclaims {reclaim_count+=reclaim(&mut sim,&id,1,true);if sim.snapshot.status.is_terminal() {break;}continue;}
            if failed_count<failures {failed_count+=1;let attempt=sim.snapshot.task(&id).unwrap().attempt;outcome(&mut sim,&id,NodeOutcome::Fail {message:"retry".into(),retryable:true});if !sim.snapshot.status.is_terminal() {prop_assert_eq!(sim.snapshot.task(&id).unwrap().run_at,NOW.saturating_add(sim.graph.node(&"a".into()).unwrap().retry.backoff(attempt)));}} else {outcome(&mut sim,&id,NodeOutcome::Done(Outcome::done("ok")));}
        }
        let expected=if failures+reclaims<max {RunStatus::Completed} else {RunStatus::Failed};invariants(&sim,cancelled,expected);
    }
}
