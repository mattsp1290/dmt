//! Durable SQLite persistence for dmt.
mod apply;
mod checks;
mod claim;
mod codec;
#[cfg(feature = "test-faults")]
mod faults;
#[cfg(feature = "test-faults")]
#[doc(hidden)]
pub use faults::FaultPoint;
mod error;
mod options;
mod read;
mod sql;
mod store_impl;
mod tx;
mod writes;

use dmt_store::StoreError;
use error::map_sqlx;
pub use options::SqliteOptions;
use sqlx::{
    Connection, SqliteConnection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{fs::OpenOptions, io::ErrorKind, path::Path, time::Duration};

/// The supported embedded schema version.
pub const SCHEMA_VERSION: u32 = 1;
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// A single writer and four read-only connections to a SQLite file.
#[derive(Debug, Clone)]
pub struct SqliteStore {
    writer: SqlitePool,
    reader: SqlitePool,
    #[cfg(feature = "test-faults")]
    faults: std::sync::Arc<faults::FaultState>,
}

impl SqliteStore {
    /// Create a private database file if absent and apply embedded migrations.
    ///
    /// # Errors
    /// Returns a backend error for an invalid path, I/O failure, or migration failure.
    pub async fn migrate(path: impl AsRef<Path>) -> Result<(), StoreError> {
        let path = path.as_ref();
        validate_path(path)?;
        let mut file = OpenOptions::new();
        file.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file.mode(0o600);
        }
        match file.open(path) {
            Ok(_) => (),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => (),
            Err(error) => return Err(StoreError::Backend(format!("{}: {error}", path.display()))),
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .map_err(map_sqlx)?;
        let result = MIGRATOR
            .run_direct(None, &mut connection, false)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()));
        let close = connection.close().await.map_err(map_sqlx);
        result.and(close)
    }

    /// Open an existing migrated WAL database.
    ///
    /// # Errors
    /// Returns an error for invalid paths, unsupported schemas, non-WAL databases,
    /// or connection failures. Never creates a database file.
    pub async fn open(path: impl AsRef<Path>, options: SqliteOptions) -> Result<Self, StoreError> {
        let path = path.as_ref();
        validate_path(path)?;
        if !path.exists() {
            return Err(StoreError::Backend(format!(
                "database file not found: {}; call SqliteStore::migrate first",
                path.display()
            )));
        }
        // Reject unsupported schemas before any writable connection can checkpoint a WAL.
        let validation = SqlitePoolOptions::new()
            .max_connections(1)
            .acquire_timeout(options.busy_timeout.max(Duration::from_secs(1)))
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(false)
                    .read_only(true)
                    .busy_timeout(options.busy_timeout),
            )
            .await
            .map_err(map_sqlx)?;
        let result = check_schema(&validation).await;
        validation.close().await;
        result?;
        let timeout = options.busy_timeout.max(Duration::from_secs(1));
        let connect = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .busy_timeout(options.busy_timeout)
            .synchronous(if options.synchronous_full {
                SqliteSynchronous::Full
            } else {
                SqliteSynchronous::Normal
            });
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .acquire_timeout(timeout)
            .connect_with(connect)
            .await
            .map_err(map_sqlx)?;
        if let Err(error) = check_schema(&writer).await {
            writer.close().await;
            return Err(error);
        }
        let connect = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .read_only(true)
            .busy_timeout(options.busy_timeout);
        let reader = match SqlitePoolOptions::new()
            .max_connections(4)
            .acquire_timeout(timeout)
            .connect_with(connect)
            .await
        {
            Ok(reader) => reader,
            Err(error) => {
                writer.close().await;
                return Err(map_sqlx(error));
            }
        };
        Ok(Self {
            writer,
            reader,
            #[cfg(feature = "test-faults")]
            faults: std::sync::Arc::new(faults::FaultState::new(options.fault)),
        })
    }

    /// Close both pools, waiting for outstanding connections.
    pub async fn close(&self) {
        self.reader.close().await;
        self.writer.close().await;
    }
}

fn validate_path(path: &Path) -> Result<(), StoreError> {
    let name = path
        .to_str()
        .ok_or_else(|| StoreError::Backend("database path must be UTF-8".into()))?;
    if name.is_empty() || name == ":memory:" || name.starts_with("file:") {
        return Err(StoreError::Backend(
            "in-memory and URI databases are not supported".into(),
        ));
    }
    Ok(())
}

async fn check_schema(writer: &SqlitePool) -> Result<(), StoreError> {
    let value: Option<String> = sqlx::query_scalar(sql::SELECT_SCHEMA_VERSION)
        .fetch_optional(writer)
        .await
        .map_err(|error| match error {
            sqlx::Error::Database(ref db) if db.message().contains("no such table") => {
                not_migrated()
            }
            _ => map_sqlx(error),
        })?;
    let value = value.ok_or_else(not_migrated)?;
    match value.parse::<u32>() {
        Ok(SCHEMA_VERSION) => (),
        Ok(version) if version > SCHEMA_VERSION => {
            return Err(StoreError::Backend(format!(
                "database schema version {version} is newer than supported version {SCHEMA_VERSION}"
            )));
        }
        _ => {
            return Err(StoreError::Backend(format!(
                "unsupported database schema version: {value:?}"
            )));
        }
    }
    let mode: String = sqlx::query_scalar(sql::PRAGMA_JOURNAL_MODE)
        .fetch_one(writer)
        .await
        .map_err(map_sqlx)?;
    if mode != "wal" {
        return Err(StoreError::Backend(
            "database is not in WAL mode; call SqliteStore::migrate first".into(),
        ));
    }
    Ok(())
}

fn not_migrated() -> StoreError {
    StoreError::Backend("database is not migrated; call SqliteStore::migrate first".into())
}

#[cfg(test)]
mod tests;

#[cfg(feature = "test-faults")]
impl SqliteStore {
    /// Whether the configured one-shot fault has fired.
    #[doc(hidden)]
    #[must_use]
    pub fn fault_fired(&self) -> bool {
        self.faults.fired()
    }
}
