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

#[derive(Clone)]
enum Script {
    Linear,
    Loop {
        rejections: u32,
    },
    FanOut {
        branches: u32,
        failures: Vec<bool>,
        timeout: bool,
        rotation: usize,
    },
    Retry {
        failures: u32,
    },
}
struct Driver {
    sim: Sim,
    script: Script,
    now: Micros,
    step: u32,
    budget: u32,
    reclaim_steps: BTreeSet<u32>,
    reclaims: u32,
    cancel_at: Option<u32>,
    cancelled: bool,
}
impl Driver {
    fn new(
        graph: Graph,
        script: Script,
        budget: u32,
        reclaim_steps: Vec<u32>,
        cancel_at: Option<u32>,
    ) -> Self {
        Self {
            sim: Sim::start(graph, Value::Null, NOW),
            script,
            now: NOW,
            step: 0,
            budget,
            reclaim_steps: reclaim_steps.into_iter().collect(),
            reclaims: 0,
            cancel_at,
            cancelled: false,
        }
    }
    fn drive(&mut self) {
        oracle(&self.sim);
        while !self.sim.snapshot.status.is_terminal() {
            assert!(self.step < self.budget, "shape exceeded step budget");
            let step = self.step;
            self.step += 1;
            if Some(step) == self.cancel_at {
                self.cancel();
                break;
            }
            if !self.sim.snapshot.signals.is_empty() {
                self.resolve();
                continue;
            }
            let id = self.next_task();
            self.now = self.now.max(self.sim.snapshot.task(&id).unwrap().run_at);
            if self.sim.snapshot.task(&id).unwrap().status == TaskStatus::Ready {
                self.sim.claim(&id);
            }
            let task = self.sim.snapshot.task(&id).unwrap();
            let can_reclaim =
                matches!(self.script, Script::Retry { .. }) || task.attempt < task.max_attempts;
            if self.reclaim_steps.contains(&step) && can_reclaim {
                self.reclaim(&id);
                continue;
            }
            let outcome = self.scripted_outcome(&id);
            self.outcome(&id, outcome);
        }
    }
    fn next_task(&self) -> TaskId {
        let mut tasks: Vec<_> = self
            .sim
            .snapshot
            .tasks
            .iter()
            .filter(|t| matches!(t.status, TaskStatus::Ready | TaskStatus::Running))
            .collect();
        let rotation = if let Script::FanOut { rotation, .. } = self.script {
            rotation
        } else {
            0
        };
        tasks.sort_by_key(|t| {
            let rank = match t.node_id.as_str() {
                "a" => 0,
                "fo" => 1,
                "jn" => 2,
                "br" => 3,
                _ => 4,
            };
            let index = t
                .branch
                .as_ref()
                .map_or(0, |b| usize::try_from(b.index).unwrap());
            let branch_rank = if let Script::FanOut { branches, .. } = self.script {
                (index + rotation) % usize::try_from(branches).unwrap()
            } else {
                index
            };
            (rank, branch_rank, t.run_at, &t.step_key)
        });
        tasks[0].task_id.clone()
    }
    fn cancel(&mut self) {
        let first = plan_cancel(
            &self.sim.graph,
            &self.sim.snapshot,
            "cancel".into(),
            self.now,
        )
        .unwrap();
        assert_eq!(
            first,
            plan_cancel(
                &self.sim.graph,
                &self.sim.snapshot,
                "cancel".into(),
                self.now
            )
            .unwrap()
        );
        self.sim.apply(first).unwrap();
        self.cancelled = true;
    }
    fn resolve(&mut self) {
        let id = self.sim.snapshot.signals[0].signal_id.clone();
        let (label, timeout) = match &mut self.script {
            Script::Loop { rejections } if *rejections > 0 => {
                *rejections -= 1;
                ("changes_requested", false)
            }
            Script::Loop { .. } => ("approved", false),
            Script::FanOut { timeout, .. } => ("ok", *timeout),
            _ => unreachable!("this script has no waits"),
        };
        let plan = || {
            if timeout {
                plan_timeout(&self.sim.graph, &self.sim.snapshot, &id, self.now)
            } else {
                plan_signal(
                    &self.sim.graph,
                    &self.sim.snapshot,
                    &id,
                    SignalPayload {
                        label: label.into(),
                        payload: Value::Null,
                    },
                    self.now,
                )
            }
        };
        let first = plan().unwrap();
        assert_eq!(first, plan().unwrap());
        self.sim.apply(first).unwrap();
        oracle(&self.sim);
    }
    fn reclaim(&mut self, id: &TaskId) {
        let before = self.sim.snapshot.task(id).unwrap().attempt;
        self.sim.reclaim(id);
        self.reclaims += 1;
        let after = self.sim.snapshot.task(id).unwrap();
        if after.status == TaskStatus::Exhausted {
            let first = plan_exhausted(&self.sim.graph, &self.sim.snapshot, id, self.now).unwrap();
            assert_eq!(
                first,
                plan_exhausted(&self.sim.graph, &self.sim.snapshot, id, self.now).unwrap()
            );
            self.sim.apply(first).unwrap();
        } else {
            assert_eq!(after.attempt, before + 1);
        }
        oracle(&self.sim);
    }
    fn scripted_outcome(&mut self, id: &TaskId) -> NodeOutcome {
        let task = self.sim.snapshot.task(id).unwrap();
        match &mut self.script {
            Script::FanOut { branches, .. } if task.node_id.as_str() == "fo" => {
                NodeOutcome::FanOut((0..*branches).map(|n| json!(n)).collect())
            }
            Script::FanOut { failures, .. }
                if task.node_id.as_str() == "br"
                    && failures[usize::try_from(task.branch.as_ref().unwrap().index).unwrap()] =>
            {
                NodeOutcome::Fail {
                    message: "branch failure".into(),
                    retryable: false,
                }
            }
            Script::Retry { failures } if *failures > 0 => {
                *failures -= 1;
                NodeOutcome::Fail {
                    message: "retry".into(),
                    retryable: true,
                }
            }
            _ => NodeOutcome::Done(Outcome::done("ok")),
        }
    }
    fn outcome(&mut self, id: &TaskId, outcome: NodeOutcome) {
        let task = self.sim.snapshot.task(id).unwrap();
        let previous_run_at = task.run_at;
        let attempt = task.attempt;
        let first = plan_outcome(
            &self.sim.graph,
            &self.sim.snapshot,
            id,
            outcome.clone(),
            self.now,
        )
        .unwrap();
        assert_eq!(
            first,
            plan_outcome(&self.sim.graph, &self.sim.snapshot, id, outcome, self.now).unwrap()
        );
        if matches!(self.script, Script::Retry { .. })
            && first.task_updates[0].status == TaskStatus::Ready
        {
            // These generated policies use default 2x scaling, independently calculated here.
            let expected_delay = 1_000_000_i64 * (1_i64 << (attempt - 1));
            let run_at = first.task_updates[0].run_at;
            assert_eq!(run_at.as_i64() - previous_run_at.as_i64(), expected_delay);
            assert_eq!(run_at, self.now.saturating_add(expected_delay));
        }
        self.sim.apply(first).unwrap();
        oracle(&self.sim);
    }
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
    fn linear_shapes(nodes in 1u32..=6,cancel in prop::option::of(0u32..30),reclaims in prop::collection::vec(0u32..30,0..30)) {
        let mut driver=Driver::new(linear(nodes),Script::Linear,3*nodes+10,reclaims,cancel);
        driver.drive();invariants(&driver.sim,driver.cancelled,RunStatus::Completed);
    }
    #[test]
    fn loop_shapes(rejections in 0u32..=4,cancel in prop::option::of(0u32..50),reclaims in prop::collection::vec(0u32..50,0..50)) {
        let mut driver=Driver::new(fixtures::loop_via_wait(),Script::Loop {rejections},8*(rejections+1)+10,reclaims,cancel);
        driver.drive();invariants(&driver.sim,driver.cancelled,RunStatus::Completed);
    }
    #[test]
    fn fan_out_shapes(branches in 1u32..=6,quorum in 0u32..=6,failures in prop::collection::vec(any::<bool>(),6),cancel in prop::option::of(0u32..60),reclaims in prop::collection::vec(0u32..60,0..60),wait in prop::option::of((any::<bool>(),any::<bool>())),rotation in 0usize..6) {
        let policy=if quorum==0 {JoinPolicy::All} else {JoinPolicy::Quorum(quorum.min(branches))};
        let graph=if let Some((deadline,_))=wait {fixtures::fan_out_then_wait(policy,deadline.then_some(10))} else {fixtures::fan_out(policy)};
        let script=Script::FanOut {branches,failures,timeout:wait.is_some_and(|(d,t)|d&&t),rotation};
        let mut driver=Driver::new(graph,script,6*branches+20,reclaims,cancel);
        driver.drive();invariants(&driver.sim,driver.cancelled,RunStatus::Completed);
    }
    #[test]
    fn retry_shapes(max in 1u32..=5,failures in 0u32..=5,reclaims in prop::collection::vec(0u32..30,0..30),cancel in prop::option::of(0u32..40)) {
        let mut driver=Driver::new(fixtures::retry_chain(max),Script::Retry {failures},4*max+10,reclaims,cancel);
        driver.drive();let expected=if failures+driver.reclaims<max {RunStatus::Completed} else {RunStatus::Failed};
        invariants(&driver.sim,driver.cancelled,expected);
    }
}
#[test]
fn cancel_between_reclaim_and_outcome() {
    let mut driver = Driver::new(linear(1), Script::Linear, 13, vec![0], Some(1));
    driver.drive();
    let task = driver.sim.task_by_node("n0", 0);
    assert_eq!(task.attempt, 2);
    assert_eq!(task.status, TaskStatus::Running);
    assert!(
        !driver
            .sim
            .events
            .iter()
            .any(|e| matches!(e, RunEvent::TaskCompleted { .. }))
    );
    invariants(&driver.sim, true, RunStatus::Completed);
}
#[test]
fn failure_retry_reclaim_failure_advances_time() {
    let mut driver = Driver::new(
        fixtures::retry_chain(5),
        Script::Retry { failures: 2 },
        30,
        vec![1],
        None,
    );
    driver.drive();
    let retries: Vec<_> = driver
        .sim
        .events
        .iter()
        .filter_map(|e| {
            if let RunEvent::TaskRetryScheduled {
                attempt, run_at, ..
            } = e
            {
                Some((*attempt, *run_at))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        retries,
        vec![
            (2, NOW.saturating_add(1_000_000)),
            (4, NOW.saturating_add(5_000_000))
        ]
    );
    assert_eq!(driver.reclaims, 1);
    invariants(&driver.sim, false, RunStatus::Completed);
}
