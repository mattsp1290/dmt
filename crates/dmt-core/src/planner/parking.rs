use super::{PlanError, machine::Planning};
use crate::{LifecycleEvent, RunStatus, TaskStatus};
impl Planning<'_> {
    pub(super) fn parking(&mut self) -> Result<(), PlanError> {
        if self.status().is_terminal() {
            return Ok(());
        }
        let mut pending = false;
        let mut awaiting = false;
        for task in self.snapshot.into_iter().flat_map(|s| &s.tasks) {
            let update = self
                .commit
                .task_updates
                .iter()
                .find(|u| u.task_id == task.task_id);
            let (status, planned) =
                update.map_or((task.status, task.planned_at), |u| (u.status, u.planned_at));
            pending |= matches!(status, TaskStatus::Ready | TaskStatus::Running)
                || (status == TaskStatus::Exhausted && planned.is_none());
            awaiting |= status == TaskStatus::Awaiting;
        }
        for task in &self.commit.new_tasks {
            pending |= task.status == TaskStatus::Ready;
            awaiting |= task.status == TaskStatus::Awaiting;
        }
        match (pending, awaiting, self.status()) {
            (false, true, RunStatus::Active) => self.feed(LifecycleEvent::Park),
            (true, _, RunStatus::Parked) => self.feed(LifecycleEvent::Resume),
            (false, false, RunStatus::Active) => self.feed(LifecycleEvent::Fail {
                message: "no pending work and no open wait".into(),
            }),
            _ => Ok(()),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EndStatus, GraphBuilder, Micros, NewTask, StepKey};
    fn graph() -> crate::Graph {
        GraphBuilder::new("test", 1)
            .start("a")
            .task("a")
            .end("end", EndStatus::Completed)
            .edge("a", "end")
            .build()
            .unwrap()
    }
    #[test]
    fn no_work_fails() {
        let graph = graph();
        let mut ctx =
            Planning::start(&graph, "run".into(), serde_json::Value::Null, Micros(0)).unwrap();
        ctx.parking().unwrap();
        assert_eq!(ctx.status(), RunStatus::Failed);
    }
    #[test]
    fn awaiting_parks() {
        let graph = graph();
        let mut ctx =
            Planning::start(&graph, "run".into(), serde_json::Value::Null, Micros(0)).unwrap();
        ctx.commit.new_tasks.push(task(TaskStatus::Awaiting));
        ctx.parking().unwrap();
        assert_eq!(ctx.status(), RunStatus::Parked);
    }
    #[test]
    fn pending_resumes() {
        let graph = graph();
        let mut ctx =
            Planning::start(&graph, "run".into(), serde_json::Value::Null, Micros(0)).unwrap();
        ctx.feed(LifecycleEvent::Park).unwrap();
        ctx.commit.new_tasks.push(task(TaskStatus::Ready));
        ctx.parking().unwrap();
        assert_eq!(ctx.status(), RunStatus::Active);
    }
    fn task(status: TaskStatus) -> NewTask {
        NewTask {
            task_id: "run/a/0".into(),
            node_id: "a".into(),
            step_key: StepKey::task(&"run".into(), &"a".into(), 0),
            status,
            attempt: 1,
            max_attempts: 1,
            run_at: Micros(0),
            input: serde_json::Value::Null,
            branch: None,
        }
    }
}
