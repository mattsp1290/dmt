mod common;
use common::{TempDb, fresh};
use dmt_store::StoreError;
use dmt_store_sqlite::{SCHEMA_VERSION, SqliteOptions, SqliteStore};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};

const VERSION: &str = "SELECT sqlite_version()";
const STRICT: &str = "SELECT strict FROM pragma_table_list WHERE name = 'dmt_runs'";
const META: &str = "SELECT value FROM dmt_schema_meta WHERE key = 'schema_version'";
const MIGRATIONS: &str = "SELECT count(*) FROM _sqlx_migrations";
const INSERT: &str = "INSERT INTO dmt_graphs VALUES ('g',1,'hash','{}')";
const COUNT: &str = "SELECT count(*) FROM dmt_graphs";
const SET_VERSION: &str = "UPDATE dmt_schema_meta SET value = ?";
const DELETE_META: &str = "DELETE FROM dmt_schema_meta";
const DELETE_MODE: &str = "PRAGMA journal_mode = DELETE";
const JOURNAL: &str = "PRAGMA journal_mode";
const UNKNOWN_MIGRATION: &str = "INSERT INTO _sqlx_migrations (version,description,success,checksum,execution_time) VALUES (2,'unknown',1,x'00',0)";

async fn raw(db: &TempDb) -> SqliteConnection {
    SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(db.path()))
        .await
        .unwrap()
}
async fn open(db: &TempDb) -> Result<SqliteStore, StoreError> {
    SqliteStore::open(db.path(), SqliteOptions::default()).await
}
fn backend(error: StoreError) -> String {
    match error {
        StoreError::Backend(message) => message,
        other => panic!("expected Backend, got {other:?}"),
    }
}
fn sidecar(db: &TempDb, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", db.path().display()))
}
#[tokio::test]
async fn bundled_sqlite_is_recent() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    let version: String = sqlx::query_scalar(VERSION)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let parts: Vec<u32> = version
        .split('.')
        .map(|part| part.parse().unwrap())
        .collect();
    assert!((parts[0], parts[1]) >= (3, 38));
    let strict: i64 = sqlx::query_scalar(STRICT)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(strict, 1);
}
#[tokio::test]
async fn migrate_creates_and_is_idempotent() {
    let db = TempDb::new();
    assert!(!db.path().exists());
    SqliteStore::migrate(db.path()).await.unwrap();
    assert!(db.path().exists());
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    let value: String = sqlx::query_scalar(META).fetch_one(&mut conn).await.unwrap();
    assert_eq!(value, "1");
    assert_eq!(value.parse::<u32>().unwrap(), SCHEMA_VERSION);
    let count: i64 = sqlx::query_scalar(MIGRATIONS)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
#[tokio::test]
async fn migrate_preserves_data() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    sqlx::query(INSERT).execute(&mut conn).await.unwrap();
    SqliteStore::migrate(db.path()).await.unwrap();
    let count: i64 = sqlx::query_scalar(COUNT)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
#[tokio::test]
async fn open_requires_existing_file() {
    let db = TempDb::new();
    assert!(backend(open(&db).await.unwrap_err()).contains("not found"));
    assert!(!db.path().exists());
}
#[tokio::test]
async fn open_requires_migration() {
    let db = TempDb::new();
    std::fs::File::create(db.path()).unwrap();
    assert!(backend(open(&db).await.unwrap_err()).contains("not migrated"));
}
#[tokio::test]
async fn open_refuses_newer_schema() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    sqlx::query(SET_VERSION)
        .bind("2")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(backend(open(&db).await.unwrap_err()).contains("newer"));
}
#[tokio::test]
async fn open_refuses_missing_schema_row() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    sqlx::query(DELETE_META).execute(&mut conn).await.unwrap();
    assert!(backend(open(&db).await.unwrap_err()).contains("not migrated"));
}
#[tokio::test]
async fn open_refuses_unknown_schema_values() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    for value in ["abc", "", "0"] {
        sqlx::query(SET_VERSION)
            .bind(value)
            .execute(&mut conn)
            .await
            .unwrap();
        assert!(backend(open(&db).await.unwrap_err()).contains("schema version"));
    }
}
#[tokio::test]
async fn open_does_not_modify_unmigrated_file() {
    let db = TempDb::new();
    std::fs::File::create(db.path()).unwrap();
    assert!(open(&db).await.is_err());
    assert_eq!(std::fs::metadata(db.path()).unwrap().len(), 0);
    assert!(!sidecar(&db, "-wal").exists());
    assert!(!sidecar(&db, "-shm").exists());
}
#[tokio::test]
async fn open_refuses_non_wal_database() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    sqlx::query(DELETE_MODE).execute(&mut conn).await.unwrap();
    conn.close().await.unwrap();
    assert!(backend(open(&db).await.unwrap_err()).contains("WAL"));
    SqliteStore::migrate(db.path()).await.unwrap();
    open(&db).await.unwrap().close().await;
}
#[tokio::test]
async fn migrate_refuses_newer_database() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    sqlx::query(UNKNOWN_MIGRATION)
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(matches!(
        SqliteStore::migrate(db.path()).await,
        Err(StoreError::Backend(_))
    ));
}
#[tokio::test]
async fn rejects_memory_and_uri_paths() {
    for name in [":memory:", "", "file:x.db"] {
        assert!(matches!(
            SqliteStore::migrate(name).await,
            Err(StoreError::Backend(_))
        ));
        assert!(matches!(
            SqliteStore::open(name, SqliteOptions::default()).await,
            Err(StoreError::Backend(_))
        ));
    }
    assert!(!Path::new(":memory:").exists());
    assert!(!Path::new("file:x.db").exists());
}
#[cfg(unix)]
#[tokio::test]
async fn database_files_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let db = TempDb::new();
    let store = fresh(&db).await;
    let mut conn = raw(&db).await;
    sqlx::query(INSERT).execute(&mut conn).await.unwrap();
    for path in [
        db.path().to_path_buf(),
        sidecar(&db, "-wal"),
        sidecar(&db, "-shm"),
    ] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    conn.close().await.unwrap();
    store.close().await;
}
#[tokio::test]
async fn journal_mode_is_wal() {
    let db = TempDb::new();
    SqliteStore::migrate(db.path()).await.unwrap();
    let mut conn = raw(&db).await;
    let mode: String = sqlx::query_scalar(JOURNAL)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(mode, "wal");
}
#[tokio::test]
async fn close_then_reopen() {
    let db = TempDb::new();
    fresh(&db).await.close().await;
    open(&db).await.unwrap().close().await;
}

#[tokio::test]
async fn rejected_newer_schema_preserves_unclean_database_and_wal() {
    let source = TempDb::new();
    SqliteStore::migrate(source.path()).await.unwrap();
    let mut connection = raw(&source).await;
    sqlx::query(SET_VERSION)
        .bind("2")
        .execute(&mut connection)
        .await
        .unwrap();
    let copy = TempDb::new();
    std::fs::copy(source.path(), copy.path()).unwrap();
    std::fs::copy(sidecar(&source, "-wal"), sidecar(&copy, "-wal")).unwrap();
    assert!(!sidecar(&copy, "-shm").exists());
    let main_before = std::fs::read(copy.path()).unwrap();
    let wal_before = std::fs::read(sidecar(&copy, "-wal")).unwrap();
    assert_ne!(wal_before, Vec::<u8>::new());
    assert!(backend(open(&copy).await.unwrap_err()).contains("newer"));
    assert_eq!(std::fs::read(copy.path()).unwrap(), main_before);
    assert_eq!(std::fs::read(sidecar(&copy, "-wal")).unwrap(), wal_before);
    // Read-only WAL validation can create the disposable shared-memory index.
    if sidecar(&copy, "-shm").exists() {
        assert!(std::fs::metadata(sidecar(&copy, "-shm")).unwrap().len() > 0);
    }
}
