use crate::{
    crash::CrashAt,
    graph,
    handlers::{self, Effects},
};
use dmt::{
    BuildError, Engine, EngineConfig, EngineError, EngineHandle, RunFilter, RunSummary,
    SqliteOptions, SqliteStore, Store, StoreError,
};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
pub const DB_FILE: &str = "pipeline.db";
pub const DEFAULT_WORKERS: usize = 3;
pub const DEFAULT_LEASE: Duration = Duration::from_secs(2);
pub const HEARTBEAT: Duration = Duration::from_millis(500);
pub const POLL: Duration = Duration::from_millis(100);
pub struct Options {
    pub data: PathBuf,
    pub workers: usize,
    pub lease: Duration,
}
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Build(#[from] BuildError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("{0}")]
    Usage(String),
}
impl AppError {
    #[must_use]
    pub fn is_graph_mismatch(&self) -> bool {
        matches!(
            self,
            Self::Engine(EngineError::Store(StoreError::GraphMismatch { .. }))
        )
    }
}
/// Build the example's engine settings.
/// # Panics
/// In debug builds, panics if the lease is shorter than twice the heartbeat.
#[must_use]
pub fn config(options: &Options, crash: Option<&CrashAt>) -> EngineConfig {
    debug_assert!(HEARTBEAT * 2 <= options.lease);
    EngineConfig {
        workers: options.workers,
        claim_limit: 1,
        poll_interval: POLL,
        lease: options.lease,
        heartbeat_every: HEARTBEAT,
        fault: crash.and_then(CrashAt::fault_point),
        ..EngineConfig::default()
    }
}
/// Open an existing database, optionally creating and migrating it first.
/// # Errors
/// Returns directory, migration, or database errors.
pub async fn open_store(data: &Path, create: bool) -> Result<Arc<SqliteStore>, StoreError> {
    let path = data.join(DB_FILE);
    if create {
        std::fs::create_dir_all(data).map_err(|e| StoreError::Backend(e.to_string()))?;
        SqliteStore::migrate(&path).await?;
    }
    SqliteStore::open(path, SqliteOptions::default())
        .await
        .map(Arc::new)
}
/// Register the graph on every engine, including worker-less command handles.
/// # Errors
/// Returns construction, registration, or engine-start errors.
pub async fn start(
    store: Arc<SqliteStore>,
    options: &Options,
    effects: Option<Arc<Effects>>,
) -> Result<EngineHandle, AppError> {
    let crash = effects.as_ref().and_then(|e| e.crash.as_ref());
    let mut builder = Engine::builder()
        .store(store)
        .graph(graph::pipeline())
        .config(config(options, crash));
    if let Some(effects) = effects {
        builder = handlers::register(builder, &effects);
    }
    Ok(builder.build()?.start().await?)
}
/// Select non-terminal runs of the pipeline graph before starting workers.
/// # Errors
/// Returns database errors.
pub async fn open_runs(store: &SqliteStore) -> Result<Vec<RunSummary>, StoreError> {
    Ok(store
        .list_runs(RunFilter {
            status: None,
            graph_id: Some(graph::GRAPH_ID.into()),
            limit: 10_000,
        })
        .await?
        .into_iter()
        .filter(|r| !r.status.is_terminal())
        .collect())
}
