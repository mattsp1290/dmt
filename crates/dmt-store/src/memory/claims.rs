use super::{Inner, append_event, clear_lease};
use crate::{ClaimRequest, ClaimedTask, LeaseProof};
use dmt_core::{ExhaustReason, RunEvent, TaskEvent, TaskStatus};

pub(super) fn claim(inner: &mut Inner, req: &ClaimRequest) -> Vec<ClaimedTask> {
    if req.limit == 0 || req.graphs.is_empty() {
        return Vec::new();
    }
    let mut candidates: Vec<_> = inner
        .tasks
        .values()
        .filter(|t| {
            req.graphs
                .contains(&inner.runs[&t.run_id].snapshot.graph_id)
                && ((t.status == TaskStatus::Ready && t.run_at <= req.now)
                    || (t.status == TaskStatus::Running
                        && t.lease_until.is_some_and(|l| l < req.now)))
        })
        .map(|t| (t.run_at, t.task_id.clone()))
        .collect();
    candidates.sort();
    let mut claimed = Vec::new();
    for (_, id) in candidates {
        if claimed.len() == req.limit {
            break;
        }
        let task = inner.tasks.get_mut(&id).expect("candidate exists");
        let row = inner.runs.get_mut(&task.run_id).expect("task run exists");
        // The candidate predicate admits only Ready and Running, making these transitions legal.
        if row.snapshot.status.is_terminal() {
            task.status = task
                .status
                .next(TaskEvent::RunTerminal)
                .expect("candidate transition");
            clear_lease(task);
            continue;
        }
        if task.status == TaskStatus::Running {
            if task.attempt >= task.max_attempts {
                task.status = task
                    .status
                    .next(TaskEvent::LeaseExhausted)
                    .expect("running transition");
                clear_lease(task);
                append_event(
                    row,
                    RunEvent::TaskExhausted {
                        task_id: id,
                        reason: ExhaustReason::LeaseReclaimsExceeded,
                    },
                    req.now,
                );
                continue;
            }
            task.status = task
                .status
                .next(TaskEvent::Reclaim)
                .expect("running transition");
            task.attempt += 1;
        } else {
            task.status = task
                .status
                .next(TaskEvent::Claim)
                .expect("ready transition");
        }
        let lease_until = req.now.saturating_add(req.lease_micros);
        task.lease_owner = Some(req.worker_id.clone());
        task.lease_until = Some(lease_until);
        append_event(
            row,
            RunEvent::TaskClaimed {
                task_id: id.clone(),
                worker_id: req.worker_id.clone(),
                attempt: task.attempt,
                lease_until,
            },
            req.now,
        );
        claimed.push(ClaimedTask {
            task: task.clone(),
            graph_id: row.snapshot.graph_id.clone(),
            graph_version: row.snapshot.graph_version,
            proof: LeaseProof {
                task_id: id,
                worker_id: req.worker_id.clone(),
                attempt: task.attempt,
            },
        });
    }
    claimed
}
