mod common;
use common::{
    TempDb, fresh,
    state::{continue_runs, mixed_state, observe},
};
use dmt_store::Store;
use dmt_store_sqlite::{SqliteOptions, SqliteStore};

#[tokio::test]
async fn reopen_after_close_is_identical() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let state = mixed_state(&store).await;
    let before = observe(&store, &state).await;
    store.close().await;
    let reopened = SqliteStore::open(db.path(), SqliteOptions::default())
        .await
        .unwrap();
    assert_eq!(observe(&reopened, &state).await, before);
}
#[tokio::test]
async fn reopen_without_close_is_identical() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let state = mixed_state(&store).await;
    let before = observe(&store, &state).await;
    let reopened = SqliteStore::open(db.path(), SqliteOptions::default())
        .await
        .unwrap();
    assert_eq!(observe(&reopened, &state).await, before);
}
#[tokio::test]
async fn run_continues_after_restart() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let state = mixed_state(&store).await;
    let before = store.events(&state.runs[0], 0, 10_000).await.unwrap();
    store.close().await;
    let reopened = SqliteStore::open(db.path(), SqliteOptions::default())
        .await
        .unwrap();
    continue_runs(&reopened, &state).await;
    let after = reopened.events(&state.runs[0], 0, 10_000).await.unwrap();
    assert!(after.len() > before.len());
    assert_eq!(after[..before.len()], before);
}
#[tokio::test]
async fn reopen_from_unclean_copy_recovers_wal() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let state = mixed_state(&store).await;
    let before = observe(&store, &state).await;
    let wal = format!("{}-wal", db.path().display());
    assert!(std::fs::metadata(&wal).unwrap().len() > 0);
    let copy = TempDb::new();
    std::fs::copy(db.path(), copy.path()).unwrap();
    std::fs::copy(wal, format!("{}-wal", copy.path().display())).unwrap();
    assert!(!std::path::Path::new(&format!("{}-shm", copy.path().display())).exists());
    let reopened = SqliteStore::open(copy.path(), SqliteOptions::default())
        .await
        .unwrap();
    assert_eq!(observe(&reopened, &state).await, before);
    continue_runs(&reopened, &state).await;
}
#[tokio::test]
async fn second_migrate_keeps_data() {
    let db = TempDb::new();
    let store = fresh(&db).await;
    let state = mixed_state(&store).await;
    let before = observe(&store, &state).await;
    store.close().await;
    SqliteStore::migrate(db.path()).await.unwrap();
    let reopened = SqliteStore::open(db.path(), SqliteOptions::default())
        .await
        .unwrap();
    assert_eq!(observe(&reopened, &state).await, before);
}
