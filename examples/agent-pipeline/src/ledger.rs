//! Example-grade ledger: the durable line itself is the fake external effect.
use dmt::{NodeContext, NodeOutcome};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
pub const LEDGER_FILE: &str = "ledger.jsonl";
pub const INVOCATIONS_FILE: &str = "invocations.jsonl";
#[derive(Debug, Serialize, Deserialize)]
pub struct Entry {
    pub step_key: String,
    pub outcome: NodeOutcome,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Invocation {
    pub step_key: String,
    pub node_id: String,
    pub attempt: u32,
}
pub struct Ledger {
    dir: PathBuf,
    lock: Mutex<()>,
}
impl Ledger {
    #[must_use]
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            lock: Mutex::new(()),
        }
    }
    /// Log every dispatch, including repeats.
    /// # Errors
    /// Returns file, serialization, or poisoned-lock errors.
    pub fn record_invocation(&self, ctx: &NodeContext) -> io::Result<()> {
        let _guard = self
            .lock
            .lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        append(
            &self.dir.join(INVOCATIONS_FILE),
            &Invocation {
                step_key: ctx.step_key.to_string(),
                node_id: ctx.node_id.to_string(),
                attempt: ctx.attempt,
            },
        )
    }
    /// Atomically look up, perform, and durably record one fake effect.
    /// # Errors
    /// Returns file, corrupt-record, or poisoned-lock errors.
    pub fn perform_once(
        &self,
        step_key: &str,
        perform: impl FnOnce() -> NodeOutcome,
    ) -> io::Result<(NodeOutcome, bool)> {
        let _guard = self
            .lock
            .lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        if let Some(entry) = read_entries(&self.dir)?
            .into_iter()
            .find(|e| e.step_key == step_key)
        {
            return Ok((entry.outcome, false));
        }
        let outcome = perform();
        append(
            &self.dir.join(LEDGER_FILE),
            &Entry {
                step_key: step_key.into(),
                outcome: outcome.clone(),
            },
        )?;
        Ok((outcome, true))
    }
}
fn append(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    file.write_all(&line)?;
    file.sync_all()
}
fn read<T: DeserializeOwned>(path: &Path) -> io::Result<Vec<T>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    contents
        .lines()
        .map(|line| {
            serde_json::from_str(line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })
        .collect()
}
/// Read the effect ledger, treating missing files as empty.
/// # Errors
/// Returns file or corrupt-record errors.
pub fn read_entries(dir: &Path) -> io::Result<Vec<Entry>> {
    read(&dir.join(LEDGER_FILE))
}
/// Read all dispatch attempts, without deduplication.
/// # Errors
/// Returns file or corrupt-record errors.
pub fn read_invocations(dir: &Path) -> io::Result<Vec<Invocation>> {
    read(&dir.join(INVOCATIONS_FILE))
}

#[cfg(test)]
mod tests;
