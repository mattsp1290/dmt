// Each test crate uses a subset of the shared harness.
#![allow(dead_code)]
use agent_pipeline::{app::DB_FILE, crash};
use dmt::{EventRecord, RunId, RunSnapshot, SqliteOptions, SqliteStore, Store};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
pub struct Cli {
    pub data: PathBuf,
}
pub struct Outcome {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr_path: PathBuf,
}
impl Outcome {
    pub fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr_path).unwrap()
    }
    pub fn success(&self) {
        assert!(
            self.status.success(),
            "{}: {}\n{}",
            self.status,
            self.stderr(),
            self.stdout
        );
    }
}
impl Cli {
    pub fn new(data: &Path) -> Self {
        Self {
            data: data.to_owned(),
        }
    }
    pub fn spawn(&self, name: &str, args: &[&str], env: &[(&str, &str)]) -> Held {
        let stderr_path = self.data.join(format!("{name}.stderr"));
        let mut child = Command::new(env!("CARGO_BIN_EXE_agent-pipeline"))
            .args(["--data"])
            .arg(&self.data)
            .args(["--workers", "1", "--lease-secs", "2"])
            .args(args)
            .current_dir(&self.data)
            .env_remove(crash::ENV)
            .envs(env.iter().copied())
            .stdout(Stdio::piped())
            .stderr(File::create(&stderr_path).unwrap())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, lines) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut collected = Vec::new();
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
                let _ = send.send(line.clone());
                collected.push(line);
            }
            collected
        });
        Held {
            child,
            lines,
            reader: Some(reader),
            stderr_path,
            deadline: Instant::now() + Duration::from_secs(30),
        }
    }
    pub fn step(&self, name: &str, args: &[&str], env: &[(&str, &str)]) -> Outcome {
        self.spawn(name, args, env).finish()
    }
}
pub struct Held {
    child: Child,
    lines: Receiver<String>,
    reader: Option<JoinHandle<Vec<String>>>,
    stderr_path: PathBuf,
    deadline: Instant,
}
impl Held {
    fn diagnostic(&self) -> String {
        fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
    pub fn wait_for_line(&mut self, prefix: &str) -> String {
        loop {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(remaining)
                .unwrap_or_else(|e| panic!("waiting for {prefix}: {e}: {}", self.diagnostic()));
            if line.starts_with(prefix) {
                return line;
            }
        }
    }
    fn join_reader(&mut self) -> String {
        self.reader.take().unwrap().join().unwrap().join("\n") + "\n"
    }
    pub fn finish(mut self) -> Outcome {
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                let stdout = self.join_reader();
                return Outcome {
                    status,
                    stdout,
                    stderr_path: self.stderr_path.clone(),
                };
            }
            assert!(
                Instant::now() < self.deadline,
                "child timed out: {}",
                self.diagnostic()
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
    pub fn kill(mut self) {
        self.child.kill().unwrap();
        let status = self.child.wait().unwrap();
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        self.join_reader();
    }
}
impl Drop for Held {
    fn drop(&mut self) {
        // Also reap on assertion failures, preventing leaked workers from sharing the DB.
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
pub fn assert_aborted(outcome: &Outcome) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            outcome.status.signal(),
            Some(6),
            "{}\n{}",
            outcome.stderr(),
            outcome.stdout
        );
    }
    #[cfg(not(unix))]
    assert!(!outcome.status.success(), "{}", outcome.stderr());
}
pub fn run_id(stdout: &str) -> String {
    line(stdout, "run ")
        .unwrap()
        .strip_prefix("run ")
        .unwrap()
        .to_owned()
}
pub fn line<'a>(stdout: &'a str, prefix: &str) -> Option<&'a str> {
    stdout.lines().find(|l| l.starts_with(prefix))
}
pub async fn events(data: &Path, run: &RunId) -> Vec<EventRecord> {
    let store = SqliteStore::open(data.join(DB_FILE), SqliteOptions::default())
        .await
        .unwrap();
    let result = store.events(run, 0, 10_000).await.unwrap();
    store.close().await;
    result
}
pub async fn snapshot(data: &Path, run: &RunId) -> RunSnapshot {
    let store = SqliteStore::open(data.join(DB_FILE), SqliteOptions::default())
        .await
        .unwrap();
    let result = store.load_run(run).await.unwrap().unwrap();
    store.close().await;
    result
}
