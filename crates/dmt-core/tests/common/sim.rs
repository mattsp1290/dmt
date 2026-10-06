use dmt_core::{
    BranchResult, Commit, Effects, ExhaustReason, Graph, JoinId, JoinRecord, Micros, NodeOutcome,
    RunEvent, RunId, RunSnapshot, RunStatus, SignalId, SignalPayload, SignalRecord, TaskEvent,
    TaskId, TaskRecord, TaskStatus, plan_cancel, plan_exhausted, plan_outcome, plan_signal,
    plan_start, plan_timeout, restore,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimError {
    NotFound,
    VersionConflict,
    RunTerminal,
    DuplicateStepKey,
    JoinDrift,
    AlreadyResolved,
}
#[derive(Clone)]
pub struct Sim {
    pub graph: Graph,
    pub snapshot: RunSnapshot,
    pub events: Vec<RunEvent>,
    pub commits: Vec<Commit>,
    pub all_tasks: BTreeMap<TaskId, TaskRecord>,
    pub all_joins: BTreeMap<JoinId, JoinRecord>,
    pub all_signals: BTreeMap<SignalId, SignalRecord>,
}
impl Sim {
    pub fn start(graph: Graph, input: Value, now: Micros) -> Self {
        let commit = plan_start(&graph, RunId::from("run"), input.clone(), now).unwrap();
        let snapshot = RunSnapshot {
            run_id: commit.run_id.clone(),
            graph_id: graph.id().clone(),
            graph_version: graph.version(),
            definition_hash: graph.definition_hash(),
            status: RunStatus::Created,
            version: 0,
            machine_json: Value::Null,
            input,
            output: None,
            node_occurrences: BTreeMap::new(),
            tasks: Vec::new(),
            joins: Vec::new(),
            signals: Vec::new(),
        };
        let mut sim = Self {
            graph,
            snapshot,
            events: Vec::new(),
            commits: Vec::new(),
            all_tasks: BTreeMap::new(),
            all_joins: BTreeMap::new(),
            all_signals: BTreeMap::new(),
        };
        sim.apply(commit).unwrap();
        sim
    }
    fn validate(&self, commit: &Commit) -> Result<(), SimError> {
        if commit.expected_run_version != self.snapshot.version {
            return Err(SimError::VersionConflict);
        }
        if self.snapshot.status.is_terminal() {
            return Err(SimError::RunTerminal);
        }
        if commit
            .task_updates
            .iter()
            .any(|u| !self.all_tasks.contains_key(&u.task_id))
            || commit
                .resolved_signals
                .iter()
                .any(|s| !self.all_signals.contains_key(&s.signal_id))
        {
            return Err(SimError::NotFound);
        }
        let mut keys: BTreeSet<_> = self.all_tasks.values().map(|t| &t.step_key).collect();
        if commit.new_tasks.iter().any(|t| !keys.insert(&t.step_key)) {
            return Err(SimError::DuplicateStepKey);
        }
        if let Some(c) = &commit.join_contribution {
            let join = self.all_joins.get(&c.join_id).ok_or(SimError::NotFound)?;
            if join.satisfied_at.is_some()
                || c.expected_received
                    != join.received + u32::from(matches!(c.result, BranchResult::Done { .. }))
                || c.expected_failed
                    != join.failed + u32::from(matches!(c.result, BranchResult::Failed { .. }))
            {
                return Err(SimError::JoinDrift);
            }
        }
        if commit
            .resolved_signals
            .iter()
            .any(|s| self.all_signals[&s.signal_id].resolved_at.is_some())
        {
            return Err(SimError::AlreadyResolved);
        }
        Ok(())
    }
    pub fn apply(&mut self, commit: Commit) -> Result<(), SimError> {
        self.validate(&commit)?;
        commit.check().unwrap();
        if let Some(state) = &commit.run_state {
            let mut effects = Effects::default();
            let machine = restore(state.status, &state.machine_json, &mut effects).unwrap();
            assert_eq!(RunStatus::from(machine.state()), state.status);
            assert_eq!(effects.emitted, [] as [RunEvent; 0]);
            self.snapshot.status = state.status;
            self.snapshot.machine_json = state.machine_json.clone();
            self.snapshot.output.clone_from(&state.output);
        }
        for u in &commit.task_updates {
            let task = self.all_tasks.get_mut(&u.task_id).unwrap();
            if u.status == TaskStatus::Cancelled
                && !matches!(task.status, TaskStatus::Ready | TaskStatus::Awaiting)
            {
                continue;
            }
            task.status = u.status;
            task.attempt = u.attempt;
            task.run_at = u.run_at;
            task.outcome.clone_from(&u.outcome);
            task.planned_at = u.planned_at;
            if u.status != TaskStatus::Running {
                task.lease_owner = None;
                task.lease_until = None;
            }
        }
        for t in &commit.new_tasks {
            self.all_tasks.insert(
                t.task_id.clone(),
                TaskRecord {
                    task_id: t.task_id.clone(),
                    run_id: commit.run_id.clone(),
                    node_id: t.node_id.clone(),
                    step_key: t.step_key.clone(),
                    status: t.status,
                    attempt: t.attempt,
                    max_attempts: t.max_attempts,
                    run_at: t.run_at,
                    lease_owner: None,
                    lease_until: None,
                    input: t.input.clone(),
                    outcome: None,
                    branch: t.branch.clone(),
                    planned_at: None,
                },
            );
        }
        if let Some(j) = &commit.new_join {
            self.all_joins.insert(j.join_id.clone(), j.clone());
        }
        if let Some(c) = &commit.join_contribution {
            let j = self.all_joins.get_mut(&c.join_id).unwrap();
            j.received = c.expected_received;
            j.failed = c.expected_failed;
            j.results.push(c.result.clone());
            if c.satisfied {
                j.satisfied_at = Some(commit.now);
            }
        }
        if let Some(s) = &commit.new_signal {
            self.all_signals.insert(
                s.signal_id.clone(),
                SignalRecord {
                    signal_id: s.signal_id.clone(),
                    run_id: commit.run_id.clone(),
                    task_id: s.task_id.clone(),
                    key: s.key.clone(),
                    name: s.name.clone(),
                    deadline_at: s.deadline_at,
                    resolved_at: None,
                },
            );
        }
        for s in &commit.resolved_signals {
            self.all_signals.get_mut(&s.signal_id).unwrap().resolved_at = Some(commit.now);
        }
        self.snapshot.node_occurrences = commit.node_occurrences.clone();
        self.snapshot.version += 1;
        self.events.extend(commit.events.clone());
        self.commits.push(commit);
        self.load_run_view();
        Ok(())
    }
    fn load_run_view(&mut self) {
        self.snapshot.tasks = self
            .all_tasks
            .values()
            .filter(|t| {
                matches!(
                    t.status,
                    TaskStatus::Ready
                        | TaskStatus::Running
                        | TaskStatus::Awaiting
                        | TaskStatus::Exhausted
                )
            })
            .cloned()
            .collect();
        self.snapshot.joins = self.all_joins.values().cloned().collect();
        self.snapshot.signals = self
            .all_signals
            .values()
            .filter(|s| s.resolved_at.is_none())
            .cloned()
            .collect();
    }
    pub fn claim(&mut self, id: &TaskId) {
        let t = self.all_tasks.get_mut(id).unwrap();
        t.status = t.status.next(TaskEvent::Claim).unwrap();
        t.lease_owner = Some("worker".into());
        t.lease_until = Some(Micros(i64::MAX));
        self.load_run_view();
    }
    pub fn reclaim(&mut self, id: &TaskId) {
        let t = self.all_tasks.get_mut(id).unwrap();
        if t.attempt < t.max_attempts {
            t.status = t.status.next(TaskEvent::Reclaim).unwrap();
            t.attempt += 1;
        } else {
            t.status = t.status.next(TaskEvent::LeaseExhausted).unwrap();
            self.events.push(RunEvent::TaskExhausted {
                task_id: id.clone(),
                reason: ExhaustReason::LeaseReclaimsExceeded,
            });
        }
        self.load_run_view();
    }
    pub fn ready_tasks(&self) -> Vec<TaskId> {
        let mut tasks: Vec<_> = self
            .all_tasks
            .values()
            .filter(|t| t.status == TaskStatus::Ready)
            .collect();
        tasks.sort_by_key(|t| (t.run_at, &t.step_key));
        tasks.into_iter().map(|t| t.task_id.clone()).collect()
    }
    pub fn task_by_node(&self, node: &str, occurrence: u32) -> &TaskRecord {
        let key = dmt_core::StepKey::task(&self.snapshot.run_id, &node.into(), occurrence);
        self.all_tasks.values().find(|t| t.step_key == key).unwrap()
    }
    pub fn open_signal(&self, name: &str) -> &SignalRecord {
        self.snapshot
            .signals
            .iter()
            .find(|s| s.name == name)
            .unwrap()
    }
    pub fn outcome(&mut self, id: &TaskId, outcome: NodeOutcome, now: Micros) -> Commit {
        let c = plan_outcome(&self.graph, &self.snapshot, id, outcome, now).unwrap();
        self.apply(c.clone()).unwrap();
        c
    }
    pub fn signal(&mut self, name: &str, payload: SignalPayload, now: Micros) -> Commit {
        let c = plan_signal(
            &self.graph,
            &self.snapshot,
            &self.open_signal(name).signal_id,
            payload,
            now,
        )
        .unwrap();
        self.apply(c.clone()).unwrap();
        c
    }
    pub fn timeout(&mut self, name: &str, now: Micros) -> Commit {
        let c = plan_timeout(
            &self.graph,
            &self.snapshot,
            &self.open_signal(name).signal_id,
            now,
        )
        .unwrap();
        self.apply(c.clone()).unwrap();
        c
    }
    pub fn exhausted(&mut self, id: &TaskId, now: Micros) -> Commit {
        let c = plan_exhausted(&self.graph, &self.snapshot, id, now).unwrap();
        self.apply(c.clone()).unwrap();
        c
    }
    pub fn cancel(&mut self, reason: &str, now: Micros) -> Commit {
        let c = plan_cancel(&self.graph, &self.snapshot, reason.into(), now).unwrap();
        self.apply(c.clone()).unwrap();
        c
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use dmt_core::{EndStatus, GraphBuilder, Outcome, SignalKey, SignalResolution, TaskUpdate};
    fn sim() -> Sim {
        Sim::start(
            GraphBuilder::new("test", 1)
                .start("a")
                .task("a")
                .task("b")
                .end("end", EndStatus::Completed)
                .edge("a", "b")
                .edge("b", "end")
                .build()
                .unwrap(),
            Value::Null,
            Micros(0),
        )
    }
    fn planned(sim: &mut Sim) -> Commit {
        let id = sim.ready_tasks()[0].clone();
        sim.claim(&id);
        plan_outcome(
            &sim.graph,
            &sim.snapshot,
            &id,
            NodeOutcome::Done(Outcome::done("ok")),
            Micros(0),
        )
        .unwrap()
    }
    #[test]
    fn version_conflict_is_atomic() {
        let mut sim = sim();
        let mut c = planned(&mut sim);
        c.expected_run_version = 99;
        let before = sim.snapshot.clone();
        assert_eq!(sim.apply(c), Err(SimError::VersionConflict));
        assert_eq!(sim.snapshot, before);
    }
    #[test]
    fn unknown_task_is_atomic() {
        let mut sim = sim();
        let mut c = planned(&mut sim);
        c.task_updates.push(TaskUpdate {
            task_id: "unknown".into(),
            status: TaskStatus::Completed,
            attempt: 1,
            run_at: Micros(0),
            outcome: None,
            planned_at: None,
        });
        let before = sim.snapshot.clone();
        assert_eq!(sim.apply(c), Err(SimError::NotFound));
        assert_eq!(sim.snapshot, before);
    }
    #[test]
    fn duplicate_step_is_rejected() {
        let mut sim = sim();
        let mut c = planned(&mut sim);
        c.new_tasks[0].step_key = sim.all_tasks.values().next().unwrap().step_key.clone();
        assert_eq!(sim.apply(c), Err(SimError::DuplicateStepKey));
    }
    #[test]
    fn terminal_rejected() {
        let mut sim = sim();
        let c = planned(&mut sim);
        sim.snapshot.status = RunStatus::Completed;
        assert_eq!(sim.apply(c), Err(SimError::RunTerminal));
    }
    #[test]
    fn join_drift_rejected() {
        let mut sim = sim();
        let mut c = planned(&mut sim);
        let key = dmt_core::StepKey::task(&sim.snapshot.run_id, &"join".into(), 0);
        sim.all_joins.insert(
            "join".into(),
            JoinRecord {
                join_id: "join".into(),
                run_id: sim.snapshot.run_id.clone(),
                node_id: "join".into(),
                step_key: key,
                policy: dmt_core::JoinPolicy::All,
                expected: 2,
                received: 0,
                failed: 0,
                results: Vec::new(),
                satisfied_at: None,
            },
        );
        c.join_contribution = Some(dmt_core::JoinContribution {
            join_id: "join".into(),
            result: BranchResult::Failed {
                index: 0,
                message: "error".into(),
            },
            expected_received: 0,
            expected_failed: 2,
            satisfied: false,
        });
        assert_eq!(sim.apply(c), Err(SimError::JoinDrift));
    }
    #[test]
    fn already_resolved_rejected() {
        let mut sim = sim();
        let mut c = planned(&mut sim);
        sim.all_signals.insert(
            "signal".into(),
            SignalRecord {
                signal_id: "signal".into(),
                run_id: sim.snapshot.run_id.clone(),
                task_id: "task".into(),
                key: SignalKey::new("gate", 0),
                name: "gate".into(),
                deadline_at: None,
                resolved_at: Some(Micros(0)),
            },
        );
        c.resolved_signals.push(SignalResolution {
            signal_id: "signal".into(),
            label: "ok".into(),
            payload: Value::Null,
        });
        assert_eq!(sim.apply(c), Err(SimError::AlreadyResolved));
    }
    #[test]
    fn updates_clear_lease() {
        let mut sim = sim();
        let c = planned(&mut sim);
        let id = c.task_updates[0].task_id.clone();
        sim.apply(c).unwrap();
        assert!(sim.all_tasks[&id].lease_owner.is_none());
        assert!(sim.all_tasks[&id].lease_until.is_none());
    }
    #[test]
    fn cancel_skips_newly_claimed_task() {
        let mut sim = sim();
        let c = plan_cancel(&sim.graph, &sim.snapshot, "stop".into(), Micros(0)).unwrap();
        let id = sim.ready_tasks()[0].clone();
        sim.claim(&id);
        sim.apply(c).unwrap();
        assert_eq!(sim.all_tasks[&id].status, TaskStatus::Running);
    }
}
