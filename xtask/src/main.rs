use std::{
    env,
    path::Path,
    process::{self, Command},
};

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["check"] => check(false),
        ["check", "--offline"] => check(true),
        _ => {
            eprintln!("usage: cargo xtask check [--offline]");
            process::exit(2);
        }
    }
}

fn check(offline: bool) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be inside the workspace");
    run(workspace, &["fmt", "--all", "--check"]);
    let mut clippy = vec!["clippy", "--workspace", "--all-targets", "--all-features"];
    let mut test = vec!["test", "--workspace", "--all-features"];
    if offline {
        clippy.push("--offline");
        test.push("--offline");
    }
    clippy.extend(["--", "-D", "warnings"]);
    run(workspace, &clippy);
    run(workspace, &test);
}

fn run(workspace: &Path, args: &[&str]) {
    println!("$ cargo {}", args.join(" "));
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    match Command::new(cargo)
        .args(args)
        .current_dir(workspace)
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(status) => process::exit(status.code().unwrap_or(1)),
        Err(error) => {
            eprintln!("failed to run cargo: {error}");
            process::exit(1);
        }
    }
}
