mod common;
use common::TempDb;
use dmt_core::{Commit, Graph, TaskStatus, fixtures, plan_start};
use dmt_store::{ClaimedTask, RunFilter, Store, StoreError};
use dmt_store_conformance::harness::{self as h, T0};
use dmt_store_sqlite::{SqliteOptions, SqliteStore};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::time::Duration;
const BEGIN: &str = "BEGIN IMMEDIATE";
const ROLLBACK: &str = "ROLLBACK";
struct Setup {
    _db: TempDb,
    store: SqliteStore,
    graph: Graph,
    claimed: ClaimedTask,
    commit: Commit,
    blocker: SqliteConnection,
}
async fn setup() -> Setup {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let store = SqliteStore::open(
        db.path(),
        SqliteOptions::default().busy_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let graph = fixtures::linear();
    let run = h::start_run("busy", &store, &graph, "run-1", T0).await;
    let claimed = h::claim_one("busy", &store, "w1", T0, &graph, &h::task_id(&run, "a", 0)).await;
    let commit = h::plan_done("busy", &store, &graph, &claimed.task.task_id, "ok", T0).await;
    h::start_run("busy", &store, &graph, "run-2", T0).await;
    let blocker = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(db.path()))
        .await
        .unwrap();
    Setup {
        _db: db,
        store,
        graph,
        claimed,
        commit,
        blocker,
    }
}
async fn lock(setup: &mut Setup) {
    sqlx::query(BEGIN)
        .execute(&mut setup.blocker)
        .await
        .unwrap();
}
async fn unlock(setup: &mut Setup) {
    sqlx::query(ROLLBACK)
        .execute(&mut setup.blocker)
        .await
        .unwrap();
}
async fn assert_busy<T>(future: impl std::future::Future<Output = Result<T, StoreError>>) {
    let result = tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("blocked operation exceeded five seconds");
    assert!(matches!(result, Err(StoreError::Busy)));
}
#[tokio::test]
async fn external_write_lock_returns_busy() {
    let mut s = setup().await;
    lock(&mut s).await;
    let new_graph = fixtures::retry_chain(1);
    assert_busy(s.store.register_graph(&new_graph)).await;
    assert_busy(
        s.store.create_run(
            plan_start(&s.graph, h::run("new-run"), serde_json::Value::Null, T0).unwrap(),
        ),
    )
    .await;
    assert_busy(
        s.store
            .apply(s.commit.clone(), Some(s.claimed.proof.clone())),
    )
    .await;
    assert_busy(s.store.heartbeat(&s.claimed.proof, T0, h::LEASE)).await;
    assert_busy(
        s.store
            .claim_ready(h::claim_request("w2", T0, &[&s.graph], 1)),
    )
    .await;
    unlock(&mut s).await;
    let ready = s
        .store
        .load_task(&h::task_id(&h::run("run-2"), "a", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.status, TaskStatus::Ready);
    assert_eq!(
        s.store
            .load_task(&s.claimed.task.task_id)
            .await
            .unwrap()
            .unwrap(),
        s.claimed.task
    );
}
#[tokio::test]
async fn reads_do_not_need_the_write_lock() {
    let mut s = setup().await;
    lock(&mut s).await;
    s.store
        .load_graph(s.graph.id(), s.graph.version())
        .await
        .unwrap();
    s.store.load_run(&s.claimed.task.run_id).await.unwrap();
    s.store.load_task(&s.claimed.task.task_id).await.unwrap();
    s.store
        .events(&s.claimed.task.run_id, 0, 100)
        .await
        .unwrap();
    s.store.list_runs(RunFilter::all(100)).await.unwrap();
    s.store
        .find_open_signal(&s.claimed.task.run_id, "gate")
        .await
        .unwrap();
    s.store.due_signals(T0, 100).await.unwrap();
    s.store.exhausted_tasks(100).await.unwrap();
    unlock(&mut s).await;
}
#[tokio::test]
async fn idle_claim_does_not_take_the_write_lock() {
    let mut s = setup().await;
    h::claim_one(
        "idle",
        &s.store,
        "w1",
        T0,
        &s.graph,
        &h::task_id(&h::run("run-2"), "a", 0),
    )
    .await;
    lock(&mut s).await;
    assert_eq!(
        s.store
            .claim_ready(h::claim_request("w2", T0, &[&s.graph], 1))
            .await
            .unwrap(),
        []
    );
    unlock(&mut s).await;
}
#[tokio::test]
async fn busy_clears_after_release() {
    let mut s = setup().await;
    lock(&mut s).await;
    assert_busy(
        s.store
            .apply(s.commit.clone(), Some(s.claimed.proof.clone())),
    )
    .await;
    unlock(&mut s).await;
    h::retry_busy(|| {
        s.store
            .apply(s.commit.clone(), Some(s.claimed.proof.clone()))
    })
    .await
    .unwrap();
    h::assert_contiguous(
        "busy",
        &h::events("busy", &s.store, &s.claimed.task.run_id).await,
    );
}
