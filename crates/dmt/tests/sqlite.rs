mod common;
#[tokio::test]
async fn quick_start_completes_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dmt.db");
    dmt::SqliteStore::migrate(&path).await.unwrap();
    let store = std::sync::Arc::new(
        dmt::SqliteStore::open(&path, dmt::SqliteOptions::default())
            .await
            .unwrap(),
    );
    common::quick_start(store.clone()).await;
    store.close().await;
    #[cfg(feature = "test-faults")]
    let _ = dmt::sqlite::FaultPoint::AfterStatement(1);
}
