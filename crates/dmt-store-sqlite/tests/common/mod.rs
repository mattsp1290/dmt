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
