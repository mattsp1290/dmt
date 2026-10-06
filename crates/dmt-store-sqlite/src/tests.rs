use super::*;
const SYNCHRONOUS: &str = "PRAGMA synchronous";
const FOREIGN_KEYS: &str = "PRAGMA foreign_keys";
const BUSY_TIMEOUT: &str = "PRAGMA busy_timeout";
const INSERT: &str = "INSERT INTO dmt_graphs VALUES ('g',1,'hash','{}')";
const COUNT: &str = "SELECT count(*) FROM dmt_graphs";

async fn store(options: SqliteOptions) -> (tempfile::TempDir, SqliteStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::open(path, options).await.unwrap();
    (dir, store)
}
#[tokio::test]
async fn writer_connection_settings() {
    let (_dir, store) = store(SqliteOptions::default()).await;
    let mode: String = sqlx::query_scalar(sql::PRAGMA_JOURNAL_MODE)
        .fetch_one(&store.writer)
        .await
        .unwrap();
    assert_eq!(mode, "wal");
    for (query, expected) in [(SYNCHRONOUS, 1), (FOREIGN_KEYS, 1), (BUSY_TIMEOUT, 5000)] {
        let value: i64 = sqlx::query_scalar(query)
            .fetch_one(&store.writer)
            .await
            .unwrap();
        assert_eq!(value, expected);
    }
}
#[tokio::test]
async fn synchronous_full_option() {
    let (_dir, store) = store(SqliteOptions::default().synchronous_full(true)).await;
    let value: i64 = sqlx::query_scalar(SYNCHRONOUS)
        .fetch_one(&store.writer)
        .await
        .unwrap();
    assert_eq!(value, 2);
}
#[tokio::test]
async fn busy_timeout_option() {
    let (_dir, store) =
        store(SqliteOptions::default().busy_timeout(Duration::from_millis(250))).await;
    for pool in [&store.writer, &store.reader] {
        let value: i64 = sqlx::query_scalar(BUSY_TIMEOUT)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(value, 250);
    }
}
#[tokio::test]
async fn reader_pool_is_read_only() {
    let (_dir, store) = store(SqliteOptions::default()).await;
    assert!(sqlx::query(INSERT).execute(&store.reader).await.is_err());
    let count: i64 = sqlx::query_scalar(COUNT)
        .fetch_one(&store.writer)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
