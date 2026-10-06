use dmt_core::{
    Micros, NodeOutcome, Outcome, RunStatus, TaskStatus, fixtures, plan_outcome, plan_start,
};
use dmt_store::{ClaimRequest, MemoryStore, RunFilter, Store};
use serde_json::Value;

#[tokio::test]
async fn linear_run() {
    let store = MemoryStore::new();
    let graph = fixtures::linear();
    let now = Micros(1_000);
    store.register_graph(&graph).await.unwrap();
    let run = store
        .create_run(plan_start(&graph, "run-1".into(), Value::Null, now).unwrap())
        .await
        .unwrap();
    assert_eq!(store.load_run(&run).await.unwrap().unwrap().version, 1);
    for (node, version) in [("a", 2), ("b", 3)] {
        let claimed = store
            .claim_ready(ClaimRequest {
                worker_id: "w1".into(),
                now,
                lease_micros: 10_000,
                limit: 8,
                graphs: vec![graph.id().clone()],
            })
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let task = &claimed[0];
        assert_eq!(task.task.node_id.as_str(), node);
        assert_eq!(task.task.status, TaskStatus::Running);
        assert_eq!(task.proof.attempt, 1);
        let snapshot = store.load_run(&run).await.unwrap().unwrap();
        let commit = plan_outcome(
            &graph,
            &snapshot,
            &task.task.task_id,
            NodeOutcome::Done(Outcome::done("ok")),
            now,
        )
        .unwrap();
        let result = store.apply(commit, Some(task.proof.clone())).await.unwrap();
        assert_eq!(result.run_version, version);
        if node == "a" {
            assert_eq!(result.inserted_tasks, vec!["run-1/b/0".into()]);
        }
    }
    assert_eq!(
        store.load_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    let events = store.events(&run, 0, 100).await.unwrap();
    assert_eq!(
        events.iter().map(|e| e.event.kind()).collect::<Vec<_>>(),
        [
            "RunStarted",
            "TaskScheduled",
            "TaskClaimed",
            "TaskCompleted",
            "TaskScheduled",
            "TaskClaimed",
            "TaskCompleted",
            "RunCompleted"
        ]
    );
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event.seq, index as u64 + 1);
    }
    let runs = store
        .list_runs(RunFilter {
            status: Some(RunStatus::Completed),
            ..RunFilter::all(10)
        })
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run_id, run);
}
