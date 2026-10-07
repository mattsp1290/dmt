mod cli;
mod commands;
use agent_pipeline::{
    app::{self, AppError, Options},
    crash, graph,
    handlers::Effects,
    ledger::Ledger,
};
use clap::Parser;
use cli::{Cli, Command};
use dmt::{EngineError, RunStatus};
use std::{process::ExitCode, sync::Arc, time::Duration};
fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) {
                print!("{error}");
                return ExitCode::SUCCESS;
            }
            eprintln!("error: {error}");
            return ExitCode::from(2);
        }
    };
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(AppError::Io)
        .and_then(|rt| rt.block_on(execute(&cli)));
    match result {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            let code = if error.is_graph_mismatch() {
                3
            } else {
                match &error {
                    AppError::Usage(_) => 2,
                    AppError::Engine(EngineError::Timeout) => 4,
                    _ => 1,
                }
            };
            match &error {
                AppError::Engine(EngineError::SignalNotFound { run_id, name }) => {
                    eprintln!("error: no open signal {name} for run {run_id}");
                }
                _ => eprintln!("error: {error}"),
            }
            ExitCode::from(code)
        }
    }
}
fn status_code(status: RunStatus) -> u8 {
    u8::from(status != RunStatus::Completed)
}
async fn execute(cli: &Cli) -> Result<u8, AppError> {
    let worker_command = matches!(
        cli.command,
        Command::Demo { .. } | Command::Run { .. } | Command::Resume { .. }
    );
    if worker_command && cli.workers == 0 {
        return Err(AppError::Usage("--workers must be at least 1".into()));
    }
    if let Command::Signal { name, label, .. } = &cli.command {
        let valid = matches!(
            (name.as_str(), label.as_str()),
            (graph::SIGNOFF, graph::APPROVED | graph::CHANGES_REQUESTED)
                | (graph::ACK, graph::ACKNOWLEDGED)
        );
        if !valid {
            return Err(AppError::Usage(format!(
                "invalid signal pair {name} {label}"
            )));
        }
    }
    let crash = if worker_command {
        crash::from_env().map_err(AppError::Usage)?
    } else {
        None
    };
    let temp = if cli.data.is_none() && matches!(cli.command, Command::Demo { .. }) {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let data = cli
        .data
        .clone()
        .or_else(|| temp.as_ref().map(|d| d.path().to_owned()))
        .ok_or_else(|| AppError::Usage("--data is required".into()))?;
    let options = Options {
        data,
        workers: if worker_command { cli.workers } else { 0 },
        lease: Duration::from_secs(cli.lease_secs),
    };
    let input = match &cli.command {
        Command::Demo { input } | Command::Run { input } => {
            serde_json::from_str(input).map_err(|e| AppError::Usage(e.to_string()))?
        }
        _ => serde_json::Value::Null,
    };
    let create = matches!(cli.command, Command::Demo { .. } | Command::Run { .. });
    let store = app::open_store(&options.data, create).await?;
    let result = async {
        // Select before workers start: --run changes reporting, not worker scope.
        let selected = commands::select(&cli.command, &store).await?;
        if let Some(code) = selected.exit {
            return Ok(code);
        }
        let effects = worker_command.then(|| {
            Arc::new(Effects {
                ledger: Ledger::open(&options.data),
                crash,
            })
        });
        let handle = app::start(store.clone(), &options, effects).await?;
        let result = commands::dispatch(
            &cli.command,
            &handle,
            selected.runs,
            input,
            Duration::from_secs(cli.timeout_secs),
        )
        .await;
        if let Err(error) = handle.shutdown(Duration::from_secs(5)).await {
            eprintln!("warning: {error}");
        }
        result
    }
    .await;
    store.close().await;
    result
}
