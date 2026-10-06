use super::PlanError;
use crate::{
    Commit, Effects, Graph, LifecycleEvent, LifecycleMachine, Micros, NewRun, RunId, RunSnapshot,
    RunStateUpdate, RunStatus, new_machine, restore,
};
use serde_json::Value;
use std::collections::BTreeMap;
pub(super) struct Planning<'a> {
    pub graph: &'a Graph,
    pub snapshot: Option<&'a RunSnapshot>,
    pub commit: Commit,
    pub machine: LifecycleMachine,
    changed: bool,
    output: Option<Value>,
}
impl<'a> Planning<'a> {
    fn empty(
        graph: &'a Graph,
        snapshot: Option<&'a RunSnapshot>,
        run_id: RunId,
        now: Micros,
        machine: LifecycleMachine,
    ) -> Self {
        Self {
            graph,
            snapshot,
            machine,
            changed: false,
            output: snapshot.and_then(|s| s.output.clone()),
            commit: Commit {
                run_id,
                now,
                expected_run_version: snapshot.map_or(0, |s| s.version),
                new_run: None,
                events: Vec::new(),
                run_state: None,
                task_updates: Vec::new(),
                new_tasks: Vec::new(),
                new_join: None,
                join_contribution: None,
                new_signal: None,
                resolved_signals: Vec::new(),
                node_occurrences: snapshot
                    .map_or_else(BTreeMap::new, |s| s.node_occurrences.clone()),
            },
        }
    }
    pub fn start(
        graph: &'a Graph,
        run_id: RunId,
        input: Value,
        now: Micros,
    ) -> Result<Self, PlanError> {
        let mut ctx = Self::empty(
            graph,
            None,
            run_id,
            now,
            new_machine(&mut Effects::default()),
        );
        let hash = graph.definition_hash();
        ctx.commit.new_run = Some(NewRun {
            graph_id: graph.id().clone(),
            graph_version: graph.version(),
            definition_hash: hash.clone(),
            input: input.clone(),
        });
        ctx.feed(LifecycleEvent::Start {
            graph_id: graph.id().clone(),
            graph_version: graph.version(),
            definition_hash: hash,
            input,
        })?;
        Ok(ctx)
    }
    pub fn load(
        graph: &'a Graph,
        snapshot: &'a RunSnapshot,
        now: Micros,
    ) -> Result<Self, PlanError> {
        let actual = graph.definition_hash();
        if actual != snapshot.definition_hash {
            return Err(PlanError::GraphMismatch {
                expected: snapshot.definition_hash.clone(),
                actual,
            });
        }
        if snapshot.status.is_terminal() {
            return Err(PlanError::RunTerminal {
                status: snapshot.status,
            });
        }
        let machine = restore(
            snapshot.status,
            &snapshot.machine_json,
            &mut Effects::default(),
        )?;
        Ok(Self::empty(
            graph,
            Some(snapshot),
            snapshot.run_id.clone(),
            now,
            machine,
        ))
    }
    pub fn status(&self) -> RunStatus {
        RunStatus::from(self.machine.state())
    }
    pub fn feed(&mut self, event: LifecycleEvent) -> Result<(), PlanError> {
        let mut effects = Effects::default();
        self.machine.handle_with_context(&event, &mut effects);
        if let Some(rejected) = effects.rejected {
            return Err(PlanError::InvalidTransition(rejected));
        }
        if let LifecycleEvent::Complete { output } = event {
            self.output = Some(output);
        }
        self.changed = true;
        self.commit.events.extend(effects.emitted);
        Ok(())
    }
    pub fn finish(mut self) -> Result<Commit, PlanError> {
        self.parking()?;
        if self.changed {
            self.commit.run_state = Some(RunStateUpdate {
                status: self.status(),
                machine_json: serde_json::to_value(&self.machine).expect("lifecycle serializes"),
                output: self.output,
            });
        }
        self.commit.check()?;
        Ok(self.commit)
    }
}
