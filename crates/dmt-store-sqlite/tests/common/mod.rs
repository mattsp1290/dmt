#![allow(dead_code)] // Each test binary uses a subset of the shared helpers.
use dmt_store_sqlite::{SqliteOptions, SqliteStore};
use std::path::{Path, PathBuf};

pub struct TempDb {
    _dir: tempfile::TempDir,
    path: PathBuf,
}
impl TempDb {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dmt.db");
        Self { _dir: dir, path }
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}
pub async fn fresh(db: &TempDb) -> SqliteStore {
    SqliteStore::migrate(db.path()).await.unwrap();
    SqliteStore::open(db.path(), SqliteOptions::default())
        .await
        .unwrap()
}

use async_trait::async_trait;
use dmt_store_conformance::StoreFactory;
use std::sync::atomic::{AtomicUsize, Ordering};
pub struct SqliteFactory {
    root: tempfile::TempDir,
    next: AtomicUsize,
}
impl SqliteFactory {
    pub fn new() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
            next: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl StoreFactory for SqliteFactory {
    type S = SqliteStore;
    async fn fresh(&self) -> Self::S {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        let path = self.root.path().join(format!("case-{index}.db"));
        SqliteStore::migrate(&path)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        SqliteStore::open(&path, SqliteOptions::default())
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
    }
}

#[cfg(feature = "test-faults")]
pub mod dump;
#[cfg(feature = "test-faults")]
pub mod scenarios;
pub mod state;
