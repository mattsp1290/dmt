use super::*;
use dmt::{CancellationToken, Outcome};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};
fn outcome() -> NodeOutcome {
    NodeOutcome::Done(Outcome::done("ok"))
}
fn context() -> NodeContext {
    NodeContext {
        run_id: "r".into(),
        graph_id: "g".into(),
        graph_version: 1,
        node_id: "plan".into(),
        task_id: "r/plan/0".into(),
        step_key: dmt::StepKey::task(&"r".into(), &"plan".into(), 0),
        attempt: 1,
        cancel: CancellationToken::new(),
    }
}
#[test]
fn round_trip_and_repeat() {
    let dir = tempfile::tempdir().unwrap();
    let ledger = Ledger::open(dir.path());
    ledger.record_invocation(&context()).unwrap();
    ledger.record_invocation(&context()).unwrap();
    assert_eq!(
        ledger.perform_once("r/plan/0", outcome).unwrap(),
        (outcome(), true)
    );
    assert_eq!(
        ledger
            .perform_once("r/plan/0", || panic!("repeat performed"))
            .unwrap(),
        (outcome(), false)
    );
    assert_eq!(read_entries(dir.path()).unwrap().len(), 1);
    let invocations = read_invocations(dir.path()).unwrap();
    assert_eq!(invocations.len(), 2);
    assert_eq!(invocations[0].step_key, "r/plan/0");
    assert_eq!(invocations[0].node_id, "plan");
    assert_eq!(invocations[0].attempt, 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for name in [LEDGER_FILE, INVOCATIONS_FILE] {
            assert_eq!(
                fs::metadata(dir.path().join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
}
#[test]
fn malformed_line_is_invalid_data() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join(LEDGER_FILE), "{broken\n").unwrap();
    assert_eq!(
        Ledger::open(dir.path())
            .perform_once("r/plan/0", outcome)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
}
#[test]
fn concurrent_same_key_performs_once() {
    let dir = tempfile::tempdir().unwrap();
    let ledger = Arc::new(Ledger::open(dir.path()));
    let barrier = Arc::new(Barrier::new(3));
    let count = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let ledger = ledger.clone();
            let barrier = barrier.clone();
            let count = count.clone();
            std::thread::spawn(move || {
                barrier.wait();
                ledger
                    .perform_once("key", || {
                        count.fetch_add(1, Ordering::SeqCst);
                        outcome()
                    })
                    .unwrap()
            })
        })
        .collect();
    barrier.wait();
    let performed = threads
        .into_iter()
        .map(|t| usize::from(t.join().unwrap().1))
        .sum::<usize>();
    assert_eq!(performed, 1);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(read_entries(dir.path()).unwrap().len(), 1);
}
