use super::{PlanError, machine::Planning};
use crate::{
    BranchRef, BranchResult, EndStatus, JoinContribution, JoinId, JoinInput, JoinPolicy,
    JoinRecord, LifecycleEvent, NewSignal, NewTask, NodeId, NodeKind, Outcome, RunEvent, SignalId,
    SignalKey, StepKey, TaskId, TaskRecord, TaskStatus,
};
use serde_json::Value;
impl Planning<'_> {
    fn occurrence(&mut self, node: &NodeId) -> u32 {
        let occurrence = self
            .commit
            .node_occurrences
            .entry(node.clone())
            .or_default();
        let next = *occurrence;
        *occurrence += 1;
        next
    }
    pub(super) fn add_task(
        &mut self,
        node: &NodeId,
        key: StepKey,
        input: Value,
        branch: Option<BranchRef>,
    ) -> Result<(), PlanError> {
        let max_attempts = self
            .graph
            .node(node)
            .ok_or_else(|| PlanError::UnknownNode {
                node_id: node.clone(),
            })?
            .retry
            .max_attempts;
        let task_id = TaskId::from(key.as_str());
        self.commit.events.push(RunEvent::TaskScheduled {
            task_id: task_id.clone(),
            node_id: node.clone(),
            step_key: key.clone(),
            attempt: 1,
            run_at: self.commit.now,
            max_attempts,
        });
        self.commit.new_tasks.push(NewTask {
            task_id,
            node_id: node.clone(),
            step_key: key,
            status: TaskStatus::Ready,
            attempt: 1,
            max_attempts,
            run_at: self.commit.now,
            input,
            branch,
        });
        Ok(())
    }
    pub(super) fn schedule_node(&mut self, node: &NodeId, input: Value) -> Result<(), PlanError> {
        let kind = self
            .graph
            .node(node)
            .ok_or_else(|| PlanError::UnknownNode {
                node_id: node.clone(),
            })?
            .kind
            .clone();
        match kind {
            NodeKind::Task | NodeKind::FanOut { .. } => {
                let occurrence = self.occurrence(node);
                let key = StepKey::task(&self.commit.run_id, node, occurrence);
                self.add_task(node, key, input, None)
            }
            NodeKind::Wait {
                signal,
                deadline_micros,
            } => {
                let occurrence = self.occurrence(node);
                let step_key = StepKey::task(&self.commit.run_id, node, occurrence);
                let task_id = TaskId::from(step_key.as_str());
                let key = SignalKey::new(&signal, occurrence);
                let signal_id = SignalId::for_run(&self.commit.run_id, &key);
                let deadline_at = deadline_micros.map(|d| self.commit.now.saturating_add(d));
                self.commit.events.push(RunEvent::WaitOpened {
                    task_id: task_id.clone(),
                    signal_id: signal_id.clone(),
                    signal_key: key.clone(),
                    deadline_at,
                });
                self.commit.new_tasks.push(NewTask {
                    task_id: task_id.clone(),
                    node_id: node.clone(),
                    step_key,
                    status: TaskStatus::Awaiting,
                    attempt: 1,
                    max_attempts: 1,
                    run_at: self.commit.now,
                    input,
                    branch: None,
                });
                self.commit.new_signal = Some(NewSignal {
                    signal_id,
                    task_id,
                    key,
                    name: signal,
                    deadline_at,
                });
                Ok(())
            }
            NodeKind::End {
                status: EndStatus::Completed,
            } => self.feed(LifecycleEvent::Complete { output: input }),
            NodeKind::End {
                status: EndStatus::Failed,
            } => self.feed(LifecycleEvent::Fail {
                message: format!("reached end node {node}"),
            }),
            _ => Err(PlanError::KindMismatch {
                node_id: node.clone(),
                kind,
                outcome: "schedule",
            }),
        }
    }
    pub(super) fn fan_out(
        &mut self,
        task: &TaskRecord,
        branch: &NodeId,
        join: &NodeId,
        values: Vec<Value>,
    ) -> Result<(), PlanError> {
        if values.is_empty() {
            return Err(PlanError::EmptyFanOut {
                node_id: task.node_id.clone(),
            });
        }
        let policy = match self
            .graph
            .node(join)
            .ok_or_else(|| PlanError::UnknownNode {
                node_id: join.clone(),
            })?
            .kind
        {
            NodeKind::Join { policy } => policy,
            ref kind => {
                return Err(PlanError::KindMismatch {
                    node_id: join.clone(),
                    kind: kind.clone(),
                    outcome: "fan_out",
                });
            }
        };
        let expected = u32::try_from(values.len()).map_err(|_| {
            PlanError::Invalid(crate::CommitInvariant(
                "fan-out exceeds u32 branches".into(),
            ))
        })?;
        let b_occ = self.occurrence(branch);
        let j_occ = self.occurrence(join);
        let key = StepKey::task(&self.commit.run_id, join, j_occ);
        let join_id = JoinId::from(key.as_str());
        self.complete_task(
            task,
            crate::NodeOutcome::FanOut(values.clone()),
            Outcome::with_payload("fan_out", Value::Array(values.clone())),
        );
        self.commit.events.push(RunEvent::FanOutScheduled {
            task_id: task.task_id.clone(),
            join_id: join_id.clone(),
            join_step_key: key.clone(),
            branch_count: expected,
        });
        self.commit.new_join = Some(JoinRecord {
            join_id: join_id.clone(),
            run_id: self.commit.run_id.clone(),
            node_id: join.clone(),
            step_key: key,
            policy,
            expected,
            received: 0,
            failed: 0,
            results: Vec::new(),
            satisfied_at: None,
        });
        for (index, input) in (0..expected).zip(values) {
            let key = StepKey::branch(&self.commit.run_id, branch, b_occ, index);
            self.add_task(
                branch,
                key,
                input,
                Some(BranchRef {
                    join_id: join_id.clone(),
                    index,
                }),
            )?;
        }
        Ok(())
    }
    pub(super) fn contribute(
        &mut self,
        task: &TaskRecord,
        result: BranchResult,
    ) -> Result<(), PlanError> {
        let reference = task.branch.as_ref().ok_or_else(|| {
            PlanError::Invalid(crate::CommitInvariant(
                "branch missing join reference".into(),
            ))
        })?;
        let join = self
            .snapshot
            .and_then(|s| s.join(&reference.join_id))
            .ok_or_else(|| PlanError::UnknownJoin {
                join_id: reference.join_id.clone(),
            })?;
        self.commit.events.push(RunEvent::BranchContributed {
            join_id: join.join_id.clone(),
            branch_index: result.index(),
            result: result.clone(),
        });
        if join.satisfied_at.is_some() {
            return Ok(());
        }
        let received = join.received + u32::from(matches!(result, BranchResult::Done { .. }));
        let failed = join.failed + u32::from(matches!(result, BranchResult::Failed { .. }));
        let required = match join.policy {
            JoinPolicy::All => join.expected,
            JoinPolicy::Quorum(n) => n,
        };
        let quorum_met = received >= required;
        let satisfied = received + failed == join.expected
            || (matches!(join.policy, JoinPolicy::Quorum(_)) && quorum_met);
        self.commit.join_contribution = Some(JoinContribution {
            join_id: join.join_id.clone(),
            result: result.clone(),
            expected_received: received,
            expected_failed: failed,
            satisfied,
        });
        if satisfied {
            self.commit.events.push(RunEvent::JoinSatisfied {
                join_id: join.join_id.clone(),
                received,
                failed,
                quorum_met,
            });
            let mut results = join.results.clone();
            results.push(result);
            results.sort_by_key(BranchResult::index);
            let input = serde_json::to_value(JoinInput {
                results,
                expected: join.expected,
                quorum_met,
            })
            .expect("join input serializes");
            self.add_task(&join.node_id, join.step_key.clone(), input, None)?;
        }
        Ok(())
    }
}
