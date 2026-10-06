mod common;
use common::{
    TempDb,
    dump::{self, dump},
    fresh,
    scenarios::{self, Operation, Scenario},
    state::mixed_state,
};
use dmt_store::{RunFilter, Store, StoreError};
use dmt_store_sqlite::{FaultPoint, SqliteOptions, SqliteStore};
use std::time::Duration;

async fn fault_loop(scenario: Scenario) {
    let reference_db = TempDb::new();
    let reference = fresh(&reference_db).await;
    let operation = scenarios::prepare(&reference, scenario).await;
    let pre = dump(reference_db.path()).await;
    let expected = operation.run(&reference).await.unwrap();
    let post = dump(reference_db.path()).await;
    assert_ne!(pre, post);
    // The bound converts a broken statement counter into a clear failure.
    for (fired, n) in (1..=1000).enumerate() {
        let db = TempDb::new();
        let store = fresh(&db).await;
        let operation = scenarios::prepare(&store, scenario).await;
        store.close().await;
        assert_eq!(dump(db.path()).await, pre);
        let store = SqliteStore::open(
            db.path(),
            SqliteOptions::default().fault(FaultPoint::AfterStatement(n)),
        )
        .await
        .unwrap();
        let result = operation.run(&store).await;
        if !store.fault_fired() {
            assert_eq!(result.unwrap(), expected);
            assert_eq!(dump(db.path()).await, post);
            assert!(
                fired >= operation.lower_bound(),
                "only {fired} statements injected for {scenario:?}"
            );
            return;
        }
        assert!(
            matches!(result,Err(StoreError::Backend(ref message)) if message.contains("injected fault")),
            "{result:?}"
        );
        assert_eq!(dump(db.path()).await, pre, "{scenario:?} statement {n}");
        if let Operation::Create(commit) = &operation {
            assert_eq!(store.load_run(&commit.run_id).await.unwrap(), None);
        }
        assert_eq!(operation.run(&store).await.unwrap(), expected);
        assert_eq!(
            dump(db.path()).await,
            post,
            "retry {scenario:?} statement {n}"
        );
        store.close().await;
    }
    panic!("fault never exhausted");
}
#[tokio::test]
async fn apply_fan_out_is_atomic() {
    fault_loop(Scenario::FanOut).await;
}
#[tokio::test]
async fn apply_join_contribution_is_atomic() {
    fault_loop(Scenario::Join).await;
}
#[tokio::test]
async fn apply_wait_open_is_atomic() {
    fault_loop(Scenario::Wait).await;
}
#[tokio::test]
async fn apply_signal_resolution_is_atomic() {
    fault_loop(Scenario::Signal).await;
}
#[tokio::test]
async fn create_run_is_atomic() {
    fault_loop(Scenario::Create).await;
}
#[tokio::test]
async fn claim_ready_is_atomic() {
    fault_loop(Scenario::Claim).await;
}

#[tokio::test]
async fn dropped_apply_future_rolls_back() {
    let reference_db = TempDb::new();
    let reference = fresh(&reference_db).await;
    let operation = scenarios::prepare(&reference, Scenario::FanOut).await;
    let pre = dump(reference_db.path()).await;
    operation.run(&reference).await.unwrap();
    let post = dump(reference_db.path()).await;
    for (stalled, n) in (1..=1000).enumerate() {
        let db = TempDb::new();
        let store = fresh(&db).await;
        let operation = scenarios::prepare(&store, Scenario::FanOut).await;
        store.close().await;
        let store = SqliteStore::open(
            db.path(),
            SqliteOptions::default().fault(FaultPoint::StallAfterStatement(n)),
        )
        .await
        .unwrap();
        let completed = {
            let mut future = Box::pin(operation.run(&store));
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    tokio::select! {
                        result = &mut future => { result.unwrap(); break true; },
                        () = tokio::time::sleep(Duration::from_millis(1)) => {
                            if store.fault_fired() { break false; }
                        }
                    }
                }
            })
            .await
            .expect("apply neither completed nor stalled")
            // future is dropped here before the next operation uses the writer.
        };
        if completed {
            assert!(!store.fault_fired());
            assert_eq!(dump(db.path()).await, post);
            assert!(stalled >= operation.lower_bound());
            return;
        }
        assert_eq!(dump(db.path()).await, pre);
        tokio::time::timeout(Duration::from_secs(10), operation.run(&store))
            .await
            .expect("writer was not released")
            .unwrap();
        assert_eq!(dump(db.path()).await, post);
        store.close().await;
    }
    panic!("stall never exhausted");
}

#[tokio::test]
async fn stalled_writer_times_out_as_busy() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let operation = scenarios::prepare(&store, Scenario::FanOut).await;
    store.close().await;
    let store = SqliteStore::open(
        db.path(),
        SqliteOptions::default()
            .busy_timeout(Duration::from_millis(100))
            .fault(FaultPoint::StallAfterStatement(1)),
    )
    .await
    .unwrap();
    let clone = store.clone();
    let task = tokio::spawn(async move { operation.run(&clone).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !store.fault_fired() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let graph = dmt_core::fixtures::linear();
    let result = tokio::time::timeout(Duration::from_secs(5), store.register_graph(&graph))
        .await
        .unwrap();
    assert!(matches!(result, Err(StoreError::Busy)));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    dmt_store_conformance::harness::retry_busy(|| store.register_graph(&graph))
        .await
        .unwrap();
}

#[tokio::test]
async fn dump_covers_every_column() {
    use sqlx::{Column, Row};
    let db = TempDb::new();
    let store = fresh(&db).await;
    mixed_state(&store).await;
    let lines = dump(db.path()).await;
    let mut connection = dump::connection(db.path()).await;
    for (table, statement, info) in dump::TABLES {
        let columns: Vec<String> = sqlx::query_scalar(info)
            .fetch_all(&mut connection)
            .await
            .unwrap();
        let rows = sqlx::query(statement)
            .fetch_all(&mut connection)
            .await
            .unwrap();
        assert!(!rows.is_empty(), "{table} must have a row");
        let dumped: Vec<_> = lines
            .iter()
            .filter(|line| line.starts_with(&format!("{table}:")))
            .collect();
        assert_eq!(dumped.len(), rows.len());
        for (row, line) in rows.iter().zip(dumped) {
            assert_eq!(row.columns().len(), columns.len());
            for (index, column) in columns.iter().enumerate() {
                assert_eq!(row.columns()[index].name(), column);
                let prefix = if index == 0 {
                    format!("{table}:{column}=")
                } else {
                    format!(",{column}=")
                };
                assert_eq!(
                    line.matches(&prefix).count(),
                    1,
                    "missing column {column}: {line}"
                );
            }
        }
    }
}

#[tokio::test]
async fn faults_do_not_fire_on_reads() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let state = mixed_state(&store).await;
    store.close().await;
    let store = SqliteStore::open(
        db.path(),
        SqliteOptions::default().fault(FaultPoint::AfterStatement(1)),
    )
    .await
    .unwrap();
    let graph = &state.graphs[0];
    store.load_graph(graph.id(), graph.version()).await.unwrap();
    store.load_run(&state.runs[0]).await.unwrap();
    store.load_task(&state.tasks[0]).await.unwrap();
    store.events(&state.runs[0], 0, 100).await.unwrap();
    store.list_runs(RunFilter::all(100)).await.unwrap();
    store
        .find_open_signal(&state.runs[0], "gate")
        .await
        .unwrap();
    store
        .due_signals(dmt_core::Micros(i64::MAX), 100)
        .await
        .unwrap();
    store.exhausted_tasks(100).await.unwrap();
    assert!(!store.fault_fired());
    assert!(
        matches!(store.register_graph(graph).await,Err(StoreError::Backend(ref message)) if message.contains("injected fault"))
    );
    assert!(store.fault_fired());
    store.register_graph(graph).await.unwrap();
}
