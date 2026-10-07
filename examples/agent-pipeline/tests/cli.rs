mod support;
use support::{Cli, run_id};
fn run(cli: &Cli) -> String {
    let output = cli.step("run", &["run"], &[]);
    output.success();
    let id = run_id(&output.stdout);
    assert!(output.stdout.contains(&format!("parked {id}")));
    id
}
fn demo(cli: &Cli) -> String {
    let output = cli.step("demo", &["demo", "--workers", "3"], &[]);
    output.success();
    assert!(output.stdout.starts_with("run "));
    assert_eq!(output.stdout.lines().last(), Some("RunCompleted"));
    assert!(output.stdout.contains("event 1 RunStarted"));
    run_id(&output.stdout)
}
#[test]
fn demo_prints_run_completed() {
    let dir = tempfile::tempdir().unwrap();
    demo(&Cli::new(dir.path()));
}
#[test]
fn run_then_show_reports_parked_signoff() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    let id = run(&cli);
    let out = cli.step("show", &["show", "--run", &id], &[]);
    out.success();
    assert!(out.stdout.contains("status parked\n"));
    assert!(out.stdout.contains("signal signoff signoff/0 open"));
}
#[test]
fn global_options_accepted_before_and_after_subcommand() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    run(&cli);
    cli.step(
        "after",
        &[
            "run",
            "--data",
            dir.path().to_str().unwrap(),
            "--workers",
            "1",
        ],
        &[],
    )
    .success();
}
#[test]
fn signal_rejects_invalid_pair() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    let out = cli.step(
        "invalid",
        &[
            "signal",
            "--run",
            "x",
            "--name",
            "signoff",
            "--label",
            "acknowledged",
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stderr().starts_with("error: "));
    assert!(!dir.path().join("pipeline.db").exists());
}
#[test]
fn signal_for_unopened_wait_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    let id = run(&cli);
    cli.step(
        "signal",
        &[
            "signal", "--run", &id, "--name", "signoff", "--label", "approved",
        ],
        &[],
    )
    .success();
    let out = cli.step(
        "early",
        &[
            "signal",
            "--run",
            &id,
            "--name",
            "ack",
            "--label",
            "acknowledged",
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        out.stderr(),
        format!("error: no open signal ack for run {id}\n")
    );
    let out = cli.step("resume", &["resume"], &[]);
    out.success();
    assert!(out.stdout.contains(&format!("parked {id}")));
}
#[test]
fn resume_with_no_open_runs_prints_none() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    demo(&cli);
    let out = cli.step("resume", &["resume"], &[]);
    out.success();
    assert_eq!(out.stdout, "no open runs\n");
}
#[test]
fn resume_run_on_terminal_run_exits_by_status() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    let id = demo(&cli);
    let out = cli.step("resume", &["resume", "--run", &id], &[]);
    out.success();
    assert_eq!(out.stdout, format!("terminal {id} completed\n"));
    let out = cli.step("missing", &["resume", "--run", "nope"], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(out.stderr(), "error: run nope not found\n");
}
#[tokio::test]
async fn graph_mismatch_exits_3() {
    use dmt::{EndStatus, GraphBuilder, SqliteOptions, SqliteStore, Store};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pipeline.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::open(&path, SqliteOptions::default())
        .await
        .unwrap();
    let graph = GraphBuilder::new("agent-pipeline", 1)
        .start("only")
        .task("only")
        .end("done", EndStatus::Completed)
        .edge("only", "done")
        .build()
        .unwrap();
    store.register_graph(&graph).await.unwrap();
    store.close().await;
    let cli = Cli::new(dir.path());
    let out = cli.step("mismatch", &["resume"], &[]);
    assert_eq!(out.status.code(), Some(3));
    assert!(out.stderr().contains("different definition"));
}
#[test]
fn invalid_crash_at_exits_2() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    let out = cli.step("invalid", &["run"], &[(agent_pipeline::crash::ENV, "nope")]);
    assert_eq!(out.status.code(), Some(2));
}
#[test]
fn missing_database_exits_1() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    let out = cli.step("missing", &["show", "--run", "x"], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(!dir.path().join("pipeline.db").exists());
}

#[test]
fn parser_errors_are_single_line_and_help_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::new(dir.path());
    for args in [vec!["run", "--workers", "nope"], vec![]] {
        let out = cli.step("parser-error", &args, &[]);
        assert_eq!(out.status.code(), Some(2));
        let stderr = out.stderr();
        assert_eq!(stderr.lines().count(), 1, "{stderr}");
        assert!(stderr.starts_with("error: "));
        assert!(!stderr.starts_with("error: error: "));
    }
    let help = cli.step("help", &["--help"], &[]);
    help.success();
    assert!(help.stdout.contains("Usage:"));
    assert_eq!(help.stderr(), "");
}
