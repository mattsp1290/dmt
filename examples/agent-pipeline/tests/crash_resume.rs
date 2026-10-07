mod proof;
mod support;
use agent_pipeline::{
    crash::ENV,
    ledger::{read_entries, read_invocations},
};
use dmt::{RunId, RunStatus};
use support::{Cli, assert_aborted, run_id};
fn show(cli: &Cli, run: &str, signal: Option<&str>) {
    let output = cli.step("show", &["show", "--run", run], &[]);
    output.success();
    if let Some(signal) = signal {
        assert!(output.stdout.contains("status parked\n"));
        assert!(
            output.stdout.contains(&format!("signal {signal} open")),
            "{}",
            output.stdout
        );
    } else {
        assert!(output.stdout.contains("status completed\n"));
        assert!(!output.stdout.lines().any(|l| l.starts_with("signal ")));
    }
}
fn signal(cli: &Cli, run: &str, name: &str, label: &str) {
    cli.step(
        "signal",
        &["signal", "--run", run, "--name", name, "--label", label],
        &[],
    )
    .success();
}
fn resume(cli: &Cli, run: &str, terminal: bool) {
    let output = cli.step("resume", &["resume", "--run", run], &[]);
    output.success();
    assert!(output.stdout.contains(&if terminal {
        format!("terminal {run} completed")
    } else {
        format!("parked {run}")
    }));
}
fn assert_keys(cli: &Cli, run: &str, suffixes: &[&str]) {
    let entries = read_entries(&cli.data).unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|e| &e.step_key)
            .collect::<std::collections::BTreeSet<_>>(),
        suffixes
            .iter()
            .map(|s| format!("{run}/{s}"))
            .collect::<Vec<_>>()
            .iter()
            .collect()
    );
    assert_eq!(entries.len(), suffixes.len());
}
fn assert_attempts(cli: &Cli, run: &str, suffix: &str, expected: &[u32]) {
    let invocations = read_invocations(&cli.data).unwrap();
    let attempts: std::collections::BTreeSet<_> = invocations
        .iter()
        .filter(|i| i.step_key == format!("{run}/{suffix}"))
        .map(|i| i.attempt)
        .collect();
    for attempt in expected {
        assert!(attempts.contains(attempt), "{suffix}: {attempts:?}");
    }
}
fn step_1_run(cli: &Cli) -> String {
    let output = cli.step("run", &["run"], &[]);
    output.success();
    let run = run_id(&output.stdout);
    assert!(output.stdout.contains(&format!("parked {run}")));
    show(cli, &run, Some("signoff signoff/0"));
    assert_keys(cli, &run, &["plan/0", "iterate-plan/0"]);
    run
}
fn step_2_changes(cli: &Cli, run: &str) {
    signal(cli, run, "signoff", "changes_requested");
    resume(cli, run, false);
    show(cli, run, Some("signoff signoff/1"));
    assert_keys(cli, run, &["plan/0", "iterate-plan/0", "iterate-plan/1"]);
}
fn step_3_kill_parked(cli: &Cli, run: &str) {
    let mut child = cli.spawn("hold", &["resume", "--hold"], &[]);
    assert_eq!(child.wait_for_line("parked "), format!("parked {run}"));
    child.kill();
    show(cli, run, Some("signoff signoff/1"));
    assert_keys(cli, run, &["plan/0", "iterate-plan/0", "iterate-plan/1"]);
}
fn step_4_crash_review(cli: &Cli, run: &str) {
    signal(cli, run, "signoff", "approved");
    assert_aborted(&cli.step("crash-review", &["resume"], &[(ENV, "review:1")]));
    assert_keys(
        cli,
        run,
        &[
            "plan/0",
            "iterate-plan/0",
            "iterate-plan/1",
            "implement/0",
            "review-fanout/0",
            "review/0/0",
            "review/0/1",
        ],
    );
    assert_attempts(cli, run, "review/0/1", &[1]);
}
fn step_5_crash_before_apply(cli: &Cli, run: &str) {
    assert_aborted(&cli.step("crash-open-pr", &["resume"], &[(ENV, "open-pr:after")]));
    proof::assert_ledger(&cli.data, run);
    assert_attempts(cli, run, "review/0/1", &[1, 2]);
    assert_attempts(cli, run, "open-pr/0", &[1]);
}
fn step_6_resume_to_ack(cli: &Cli, run: &str) {
    resume(cli, run, false);
    show(cli, run, Some("ack ack/0"));
    proof::assert_ledger(&cli.data, run);
    assert_attempts(cli, run, "open-pr/0", &[1, 2]);
}
fn step_7_complete(cli: &Cli, run: &str) {
    signal(cli, run, "ack", "acknowledged");
    resume(cli, run, true);
    show(cli, run, None);
}
#[tokio::test(flavor = "multi_thread")]
async fn kill_and_resume_completes_with_exactly_once_effects() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    let run = step_1_run(&cli);
    step_2_changes(&cli, &run);
    step_3_kill_parked(&cli, &run);
    step_4_crash_review(&cli, &run);
    step_5_crash_before_apply(&cli, &run);
    step_6_resume_to_ack(&cli, &run);
    step_7_complete(&cli, &run);
    proof::assert_ledger(dir.path(), &run);
    proof::assert_invocations(dir.path(), &run);
    let id = RunId::from(run.as_str());
    proof::assert_events(&support::events(dir.path(), &id).await, &run);
    let snapshot = support::snapshot(dir.path(), &id).await;
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(snapshot.tasks, []);
    assert_eq!(snapshot.signals, []);
}
