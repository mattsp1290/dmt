mod common;
use common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::ManualClock;
#[cfg(feature = "test-faults")]
use dmt_store::Store;
use dmt_store_sqlite::{SqliteOptions, SqliteStore};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
fn config() -> EngineConfig {
    EngineConfig {
        poll_interval: Duration::from_millis(10),
        sweep_interval: Duration::from_millis(20),
        heartbeat_every: Duration::from_millis(100),
        lease: Duration::from_secs(1),
        ..EngineConfig::default()
    }
}
async fn open(path: &Path) -> Arc<SqliteStore> {
    Arc::new(
        SqliteStore::open(path, SqliteOptions::default())
            .await
            .unwrap(),
    )
}
fn assert_sequences(records: &[dmt_store::EventRecord]) {
    assert_eq!(
        records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (1..=u64::try_from(records.len()).unwrap()).collect::<Vec<_>>()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_completes_on_temp_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dmt.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = open(&path).await;
    let joins = Arc::new(Mutex::new(Vec::new()));
    let j = joins.clone();
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::fan_out(JoinPolicy::All),
        config(),
        &[
            ("a", handler(|_, _| async { done() })),
            (
                "fo",
                handler(|_, _| async {
                    Ok(NodeOutcome::FanOut(vec![json!(0), json!(1), json!(2)]))
                }),
            ),
            ("br", handler(|_, _| async { done() })),
            (
                "jn",
                handler(move |_, input| {
                    let j = j.clone();
                    async move {
                        let NodeInput::Join(input) = input else {
                            panic!()
                        };
                        j.lock().unwrap().push(input);
                        done()
                    }
                }),
            ),
        ],
    )
    .await;
    let run = h.start_run(&"fan-out".into(), Value::Null).await.unwrap();
    completed(&h, &run).await;
    let records = h.events(&run, 0, 100).await.unwrap();
    assert_sequences(&records);
    let mut completions = std::collections::BTreeMap::new();
    for r in &records {
        if let RunEvent::TaskCompleted { task_id, .. } = &r.event {
            *completions.entry(task_id).or_insert(0) += 1;
        }
    }
    assert_eq!(completions.len(), 6);
    assert!(completions.values().all(|n| *n == 1));
    assert_eq!(kind_count(&h, &run, "JoinSatisfied").await, 1);
    assert_eq!(
        joins.lock().unwrap()[0]
            .results
            .iter()
            .map(BranchResult::index)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    h.shutdown(Duration::from_secs(5)).await.unwrap();
    store.close().await;
}
async fn reclaim_after_stop(drop_handle: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dmt.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = open(&path).await;
    let clock = Arc::new(ManualClock::new(T0));
    let recorder = Recorder::default();
    let r = recorder.clone();
    let witness = Arc::new(());
    let w = witness.clone();
    let joins = Recorder::default();
    let j = joins.clone();
    let handlers = [
        ("a", handler(|_, _| async { done() })),
        (
            "fo",
            handler(|_, _| async { Ok(NodeOutcome::FanOut(vec![json!(0), json!(1), json!(2)])) }),
        ),
        (
            "br",
            handler(move |ctx, input| {
                let r = r.clone();
                let w = w.clone();
                async move {
                    let attempt = ctx.attempt;
                    r.record(ctx);
                    if matches!(input, NodeInput::Branch { index: 1, .. }) && attempt == 1 {
                        std::future::pending::<()>().await;
                    }
                    drop(w);
                    done()
                }
            }),
        ),
        (
            "jn",
            handler(move |ctx, _| {
                j.record(ctx);
                async { done() }
            }),
        ),
    ];
    let first = start(
        store.clone(),
        clock.clone(),
        fixtures::fan_out(JoinPolicy::All),
        config(),
        &handlers,
    )
    .await;
    let run = first
        .start_run(&"fan-out".into(), Value::Null)
        .await
        .unwrap();
    eventually("branches settled", || async {
        recorder.count() == 3
            && [0, 2].into_iter().all(|index| {
                recorder
                    .entries()
                    .iter()
                    .any(|ctx| ctx.step_key.as_str().ends_with(&format!("/br/0/{index}")))
            })
            && kind_count(&first, &run, "BranchContributed").await == 2
    })
    .await;
    if drop_handle {
        drop(first);
        eventually("handler future dropped", || async {
            Arc::strong_count(&witness) == 2
        })
        .await;
    } else {
        first.abort().await;
        drop(first);
    }
    store.close().await;
    let _ = clock.advance(2_000_000);
    let second_store = open(&path).await;
    let second = start(
        second_store.clone(),
        clock,
        fixtures::fan_out(JoinPolicy::All),
        config(),
        &handlers,
    )
    .await;
    completed(&second, &run).await;
    assert_branch_attempts(&recorder);
    assert_reclaimed_events(&second, &run).await;
    assert_eq!(joins.count(), 1);
    second.shutdown(Duration::from_secs(5)).await.unwrap();
    second_store.close().await;
}
async fn assert_reclaimed_events(h: &EngineHandle, run: &RunId) {
    let target: TaskId = format!("{run}/br/0/1").into();
    let e = events(h, run).await;
    assert_eq!(
        e.iter()
            .filter_map(|e| {
                if let RunEvent::TaskClaimed {
                    task_id, attempt, ..
                } = e
                {
                    (task_id == &target).then_some(*attempt)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        e.iter()
            .filter_map(|e| {
                if let RunEvent::TaskCompleted {
                    task_id, attempt, ..
                } = e
                {
                    (task_id == &target).then_some(*attempt)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
        [2]
    );
    assert_eq!(kind_count(h, run, "JoinSatisfied").await, 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_engine_task_is_reclaimed_by_second_engine() {
    reclaim_after_stop(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_engine_task_is_reclaimed_by_second_engine() {
    reclaim_after_stop(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_while_parked_resumes_on_signal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dmt.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = open(&path).await;
    let clock = Arc::new(ManualClock::new(T0));
    let recorder = Recorder::default();
    let r = recorder.clone();
    let hnd = handler(move |ctx, _| {
        r.record(ctx);
        async { done() }
    });
    let handlers = [("plan", hnd.clone()), ("implement", hnd)];
    let first = start(
        store.clone(),
        clock.clone(),
        fixtures::loop_via_wait(),
        config(),
        &handlers,
    )
    .await;
    let run = first.start_run(&"loop".into(), Value::Null).await.unwrap();
    assert_eq!(
        first
            .wait_quiescent(&run, Duration::from_secs(5))
            .await
            .unwrap(),
        Quiescent::Parked
    );
    first.shutdown(Duration::from_secs(5)).await.unwrap();
    store.close().await;
    let store = open(&path).await;
    let second = start(
        store.clone(),
        clock,
        fixtures::loop_via_wait(),
        config(),
        &handlers,
    )
    .await;
    assert_eq!(
        second
            .wait_quiescent(&run, Duration::from_secs(5))
            .await
            .unwrap(),
        Quiescent::Parked
    );
    second
        .signal(
            &run,
            "signoff",
            SignalPayload {
                label: "approved".into(),
                payload: Value::Null,
            },
        )
        .await
        .unwrap();
    completed(&second, &run).await;
    assert_sequences(&second.events(&run, 0, 100).await.unwrap());
    assert_eq!(recorder.count(), 2);
    assert_eq!(kind_count(&second, &run, "RunParked").await, 1);
    assert_eq!(kind_count(&second, &run, "RunResumed").await, 1);
    second.shutdown(Duration::from_secs(5)).await.unwrap();
    store.close().await;
}
#[cfg(feature = "test-faults")]
fn file_handler(dir: &Path) -> Arc<dyn NodeHandler> {
    let path = dir.join("invocations.txt");
    handler(move |ctx, _| {
        let path = path.clone();
        async move {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            writeln!(file, "{}", ctx.node_id).unwrap();
            file.sync_all().unwrap();
            done()
        }
    })
}
#[cfg(feature = "test-faults")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fault_child_process() {
    let Some(dir) = std::env::var_os("DMT_RUNTIME_FAULT_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let store = open(&dir.join("dmt.db")).await;
    let hnd = file_handler(&dir);
    let h = start(
        store,
        Arc::new(dmt_store::SystemClock),
        fixtures::linear(),
        EngineConfig {
            fault: Some(FaultPoint::BeforeApply {
                node_id: "a".into(),
                attempt: 1,
            }),
            ..config()
        },
        &[("a", hnd.clone()), ("b", hnd)],
    )
    .await;
    let run = h.start_run(&"linear".into(), Value::Null).await.unwrap();
    let _ = h.wait_quiescent(&run, Duration::from_secs(20)).await;
    panic!("fault did not fire");
}
#[cfg(feature = "test-faults")]
async fn run_fault_child(dir: &Path) {
    use std::process::{Command, Stdio};
    let stderr_path = dir.join("child.stderr");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "fault_child_process",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("DMT_RUNTIME_FAULT_DIR", dir)
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if tokio::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!(
                "child timed out: {}",
                std::fs::read_to_string(stderr_path).unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "{}",
            std::fs::read_to_string(stderr_path).unwrap()
        );
    }
    #[cfg(not(unix))]
    assert!(!status.success(), "child did not abort");
}
#[cfg(feature = "test-faults")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_apply_abort_is_recovered_after_restart() {
    use dmt_store::{Clock, RunFilter, SystemClock};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dmt.db");
    SqliteStore::migrate(&path).await.unwrap();
    run_fault_child(dir.path()).await;
    let store = open(&path).await;
    let runs = store.list_runs(RunFilter::all(10)).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, RunStatus::Active);
    let run = &runs[0].run_id;
    let task = store.load_task(&task_id(run, "a")).await.unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    assert_eq!(task.attempt, 1);
    assert!(task.outcome.is_none());
    assert!(
        !store
            .events(run, 0, 100)
            .await
            .unwrap()
            .iter()
            .any(|e| matches!(e.event, RunEvent::TaskCompleted { .. }))
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("invocations.txt")).unwrap(),
        "a\n"
    );
    let hnd = file_handler(dir.path());
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(
            SystemClock.now().saturating_add(60_000_000),
        )),
        fixtures::linear(),
        config(),
        &[("a", hnd.clone()), ("b", hnd)],
    )
    .await;
    completed(&h, run).await;
    let records = h.events(run, 0, 100).await.unwrap();
    assert_sequences(&records);
    let target = task_id(run, "a");
    assert_eq!(
        records
            .iter()
            .filter_map(|e| {
                if let RunEvent::TaskClaimed {
                    task_id, attempt, ..
                } = &e.event
                {
                    (task_id == &target).then_some(*attempt)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        records
            .iter()
            .filter_map(|e| {
                if let RunEvent::TaskCompleted {
                    task_id, attempt, ..
                } = &e.event
                {
                    (task_id == &target).then_some(*attempt)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
        [2]
    );
    assert!(matches!(
        records.last().unwrap().event,
        RunEvent::RunCompleted { .. }
    ));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("invocations.txt"))
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["a", "a", "b"]
    );
    h.shutdown(Duration::from_secs(5)).await.unwrap();
    store.close().await;
}

fn assert_branch_attempts(recorder: &Recorder) {
    for index in 0..3 {
        let attempts: Vec<_> = recorder
            .entries()
            .iter()
            .filter(|c| c.step_key.as_str().ends_with(&format!("/br/0/{index}")))
            .map(|c| c.attempt)
            .collect();
        assert_eq!(attempts, if index == 1 { vec![1, 2] } else { vec![1] });
    }
}
