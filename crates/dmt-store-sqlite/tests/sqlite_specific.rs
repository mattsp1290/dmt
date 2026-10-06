mod common;
use common::{TempDb, fresh};
use dmt_core::{Micros, RunStatus, TaskStatus, fixtures, plan_cancel};
use dmt_store::{RunFilter, Store};
use dmt_store_conformance::harness::{self as h, LEASE, T0};

#[tokio::test]
async fn graphs_round_trip() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    for graph in fixtures::all() {
        store.register_graph(&graph).await.unwrap();
        let loaded = store
            .load_graph(graph.id(), graph.version())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, graph);
        assert_eq!(loaded.definition_hash(), graph.definition_hash());
    }
}
#[tokio::test]
async fn claim_filter_accepts_many_graphs() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let graph = fixtures::linear();
    let run = h::start_run("many graphs", &store, &graph, "run-1", T0).await;
    let mut request = h::claim_request("w1", T0, &[&graph], 1);
    request
        .graphs
        .extend((0..19).map(|i| format!("other-{i}").into()));
    let claimed = store.claim_ready(request).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task.run_id, run);
}
#[tokio::test]
async fn claim_walk_crosses_page_boundary() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let graph = fixtures::linear();
    let mut ids = Vec::new();
    for i in 0..70 {
        let run = h::start_run("pages", &store, &graph, &format!("run-{i:03}"), T0).await;
        ids.push(h::task_id(&run, "a", 0));
    }
    let first = store
        .claim_ready(h::claim_request("w1", T0, &[&graph], 3))
        .await
        .unwrap();
    let rest = store
        .claim_ready(h::claim_request("w1", T0, &[&graph], usize::MAX))
        .await
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|c| c.task.task_id.clone())
            .collect::<Vec<_>>(),
        ids[..3]
    );
    assert_eq!(
        rest.iter()
            .map(|c| c.task.task_id.clone())
            .collect::<Vec<_>>(),
        ids[3..]
    );
}
#[tokio::test]
async fn claim_skips_do_not_count_across_pages() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let graph = fixtures::linear();
    let mut skipped = Vec::new();
    for i in 0..68 {
        let run = h::start_run("skip pages", &store, &graph, &format!("run-{i:03}"), T0).await;
        let id = h::task_id(&run, "a", 0);
        h::claim_one("skip pages", &store, "w1", T0, &graph, &id).await;
        let snapshot = h::snapshot("skip pages", &store, &run).await;
        store
            .apply(
                plan_cancel(&graph, &snapshot, "cancel".into(), T0).unwrap(),
                None,
            )
            .await
            .unwrap();
        skipped.push(id);
    }
    let mut expected = Vec::new();
    for name in ["run-zz1", "run-zz2"] {
        let run = h::start_run("skip pages", &store, &graph, name, T0).await;
        expected.push(h::task_id(&run, "a", 0));
    }
    let before: Vec<_> = store.events(&h::run("run-000"), 0, 100).await.unwrap();
    let claimed = store
        .claim_ready(h::claim_request(
            "w2",
            T0.saturating_add(LEASE + 1),
            &[&graph],
            2,
        ))
        .await
        .unwrap();
    assert_eq!(
        claimed
            .iter()
            .map(|c| c.task.task_id.clone())
            .collect::<Vec<_>>(),
        expected
    );
    for id in skipped {
        let task = store.load_task(&id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Cancelled);
        assert_eq!((task.lease_owner, task.lease_until), (None, None));
    }
    assert_eq!(
        store.events(&h::run("run-000"), 0, 100).await.unwrap(),
        before
    );
}
#[tokio::test]
async fn bounds() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let graph = fixtures::linear();
    let run = h::start_run("bounds", &store, &graph, "run-1", T0).await;
    assert_eq!(store.events(&run, u64::MAX, 10).await.unwrap(), []);
    assert_eq!(store.events(&run, 0, 0).await.unwrap(), []);
    assert_eq!(
        store.events(&run, 0, usize::MAX).await.unwrap(),
        h::events("bounds", &store, &run).await
    );
    assert_eq!(
        store
            .list_runs(RunFilter::all(usize::MAX))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .due_signals(Micros(i64::MAX), usize::MAX)
            .await
            .unwrap(),
        []
    );
}
#[tokio::test]
async fn optional_output_round_trips() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let graph = fixtures::linear();
    let run = h::start_run("output", &store, &graph, "run-1", T0).await;
    assert_eq!(h::snapshot("output", &store, &run).await.output, None);
    for node in ["a", "b"] {
        let id = h::task_id(&run, node, 0);
        let claimed = h::claim_one("output", &store, "w1", T0, &graph, &id).await;
        let commit = h::plan_done("output", &store, &graph, &id, "ok", T0).await;
        let expected = commit
            .run_state
            .as_ref()
            .and_then(|state| state.output.clone());
        store.apply(commit, Some(claimed.proof)).await.unwrap();
        let snapshot = h::snapshot("output", &store, &run).await;
        assert_eq!(snapshot.output, expected);
    }
    assert_eq!(
        h::snapshot("output", &store, &run).await.status,
        RunStatus::Completed
    );
}

#[tokio::test]
async fn optional_json_distinguishes_null() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let graph = fixtures::linear();
    let run = h::start_run("null output", &store, &graph, "run-1", T0).await;
    for node in ["a", "b"] {
        let id = h::task_id(&run, node, 0);
        let claimed = h::claim_one("null output", &store, "w1", T0, &graph, &id).await;
        let mut commit = h::plan_done("null output", &store, &graph, &id, "ok", T0).await;
        if node == "b" {
            commit.run_state.as_mut().unwrap().output = Some(serde_json::Value::Null);
        }
        store.apply(commit, Some(claimed.proof)).await.unwrap();
    }
    assert_eq!(
        h::snapshot("null output", &store, &run).await.output,
        Some(serde_json::Value::Null)
    );
}

#[tokio::test]
async fn persisted_run_version_boundary_is_atomic() {
    use sqlx::{Connection, sqlite::SqliteConnectOptions};
    const SET_VERSION: &str = "UPDATE dmt_runs SET version=?1 WHERE id=?2";
    let db = TempDb::new();
    let store = fresh(&db).await;
    let graph = fixtures::linear();
    let run = h::start_run("version boundary", &store, &graph, "run-1", T0).await;
    let id = h::task_id(&run, "a", 0);
    h::claim_one("version boundary", &store, "w1", T0, &graph, &id).await;
    let mut commit = h::plan_done("version boundary", &store, &graph, &id, "ok", T0).await;
    let mut connection =
        sqlx::SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(db.path()))
            .await
            .unwrap();
    sqlx::query(SET_VERSION)
        .bind(i64::MAX - 1)
        .bind(run.as_str())
        .execute(&mut connection)
        .await
        .unwrap();
    commit.expected_run_version = u64::try_from(i64::MAX - 1).unwrap();
    assert_eq!(
        store.apply(commit, None).await.unwrap().run_version,
        u64::try_from(i64::MAX).unwrap()
    );
    let next = h::task_id(&run, "b", 0);
    h::claim_one("version boundary", &store, "w1", T0, &graph, &next).await;
    let commit = h::plan_done("version boundary", &store, &graph, &next, "ok", T0).await;
    let before = store.load_run(&run).await.unwrap();
    let before_task = store.load_task(&next).await.unwrap();
    let before_events = store.events(&run, 0, usize::MAX).await.unwrap();
    assert!(
        matches!(store.apply(commit, None).await, Err(dmt_store::StoreError::InvalidCommit(message)) if message == "run version overflow")
    );
    assert_eq!(store.load_run(&run).await.unwrap(), before);
    assert_eq!(store.load_task(&next).await.unwrap(), before_task);
    assert_eq!(
        store.events(&run, 0, usize::MAX).await.unwrap(),
        before_events
    );
}
