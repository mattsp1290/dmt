use clap::{Parser, Subcommand};
use std::path::PathBuf;
#[derive(Parser)]
#[command(about = "Durable agent pipeline with fake, idempotent handlers")]
pub struct Cli {
    #[arg(long, global = true)]
    pub data: Option<PathBuf>,
    #[arg(long, global = true, default_value_t = 3)]
    pub workers: usize,
    #[arg(long, global = true, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..))]
    pub lease_secs: u64,
    #[arg(long, global = true, default_value_t = 600)]
    pub timeout_secs: u64,
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    Demo {
        #[arg(long, default_value = "null")]
        input: String,
    },
    Run {
        #[arg(long, default_value = "null")]
        input: String,
    },
    Resume {
        #[arg(long)]
        run: Option<String>,
        #[arg(long)]
        hold: bool,
    },
    Signal {
        #[arg(long)]
        run: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        label: String,
    },
    Show {
        #[arg(long)]
        run: String,
    },
}
