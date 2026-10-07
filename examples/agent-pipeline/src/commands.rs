use crate::{cli::Command, status_code};
use agent_pipeline::{
    app::{self, AppError},
    graph,
};
use dmt::{EngineHandle, Quiescent, RunId, RunStatus, SignalPayload, SqliteStore, Store};
use serde_json::Value;
use std::time::Duration;
pub struct Selected {
    pub runs: Vec<RunId>,
    pub exit: Option<u8>,
}
pub async fn select(command: &Command, store: &SqliteStore) -> Result<Selected, AppError> {
    let mut selected = Selected {
        runs: Vec::new(),
        exit: None,
    };
    if let Command::Resume { run, .. } = command {
        if let Some(run) = run {
            let id = RunId::from(run.as_str());
            let snapshot = store
                .load_run(&id)
                .await?
                .ok_or_else(|| dmt::EngineError::RunNotFound { run_id: id.clone() })?;
            if snapshot.status.is_terminal() {
                println!("terminal {id} {}", snapshot.status);
                selected.exit = Some(status_code(snapshot.status));
            } else {
                selected.runs.push(id);
            }
        } else {
            selected.runs = app::open_runs(store)
                .await?
                .into_iter()
                .map(|r| r.run_id)
                .collect();
            // Even an empty resume registers the graph, detecting GraphMismatch.
        }
    }
    Ok(selected)
}
pub async fn dispatch(
    command: &Command,
    handle: &EngineHandle,
    runs: Vec<RunId>,
    input: Value,
    timeout: Duration,
) -> Result<u8, AppError> {
    match command {
        Command::Demo { .. } | Command::Run { .. } => {
            let run = handle.start_run(&graph::GRAPH_ID.into(), input).await?;
            println!("run {run}");
            if matches!(command, Command::Demo { .. }) {
                demo(handle, &run, timeout).await
            } else {
                wait(handle, &run, timeout, false).await
            }
        }
        Command::Resume { hold, .. } => {
            if runs.is_empty() {
                println!("no open runs");
            }
            let mut code = 0;
            for run in runs {
                println!("run {run}");
                code = code.max(wait(handle, &run, timeout, *hold).await?);
            }
            Ok(code)
        }
        Command::Signal { run, name, label } => {
            handle
                .signal(
                    &run.as_str().into(),
                    name,
                    SignalPayload {
                        label: label.clone(),
                        payload: Value::Null,
                    },
                )
                .await?;
            println!("signalled {run} {name} {label}");
            Ok(0)
        }
        Command::Show { run } => show(handle, &run.as_str().into()).await,
    }
}
async fn wait(
    handle: &EngineHandle,
    run: &RunId,
    timeout: Duration,
    hold: bool,
) -> Result<u8, AppError> {
    loop {
        match handle.wait_quiescent(run, timeout).await? {
            Quiescent::Terminal(status) => {
                println!("terminal {run} {status}");
                return Ok(status_code(status));
            }
            Quiescent::Parked => {
                println!("parked {run}");
                if !hold {
                    return Ok(0);
                }
                loop {
                    tokio::time::sleep(app::POLL).await;
                    let snapshot =
                        handle
                            .run(run)
                            .await?
                            .ok_or_else(|| dmt::EngineError::RunNotFound {
                                run_id: run.clone(),
                            })?;
                    if snapshot.status != RunStatus::Parked {
                        break;
                    }
                }
            }
        }
    }
}
async fn demo(handle: &EngineHandle, run: &RunId, timeout: Duration) -> Result<u8, AppError> {
    let mut cursor = 0;
    loop {
        let quiescent = handle.wait_quiescent(run, timeout).await?;
        for event in handle.events(run, cursor, 10_000).await? {
            println!("event {} {}", event.seq, event.event.kind());
            cursor = event.seq;
        }
        match quiescent {
            Quiescent::Terminal(status) => {
                println!(
                    "{}",
                    match status {
                        RunStatus::Completed => "RunCompleted",
                        RunStatus::Cancelled => "RunCancelled",
                        _ => "RunFailed",
                    }
                );
                return Ok(status_code(status));
            }
            Quiescent::Parked => {
                let snapshot =
                    handle
                        .run(run)
                        .await?
                        .ok_or_else(|| dmt::EngineError::RunNotFound {
                            run_id: run.clone(),
                        })?;
                let signal = snapshot
                    .signals
                    .first()
                    .ok_or_else(|| AppError::Usage("parked without a signal".into()))?;
                let label = if signal.name == graph::SIGNOFF {
                    graph::APPROVED
                } else {
                    graph::ACKNOWLEDGED
                };
                handle
                    .signal(
                        run,
                        &signal.name,
                        SignalPayload {
                            label: label.into(),
                            payload: Value::Null,
                        },
                    )
                    .await?;
            }
        }
    }
}
async fn show(handle: &EngineHandle, run: &RunId) -> Result<u8, AppError> {
    let mut snapshot = handle
        .run(run)
        .await?
        .ok_or_else(|| dmt::EngineError::RunNotFound {
            run_id: run.clone(),
        })?;
    println!(
        "run {run}\ngraph {}@{}\nstatus {}",
        snapshot.graph_id, snapshot.graph_version, snapshot.status
    );
    snapshot.tasks.sort_by(|a, b| a.task_id.cmp(&b.task_id));
    for task in snapshot.tasks {
        println!(
            "task {} {} attempt {}",
            task.task_id, task.status, task.attempt
        );
    }
    for signal in snapshot.signals {
        println!("signal {} {} open", signal.name, signal.key);
    }
    for event in handle.events(run, 0, 10_000).await? {
        println!("event {} {}", event.seq, event.event.kind());
    }
    Ok(0)
}
