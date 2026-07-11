//! The bug mine. Deterministic simulation testing for fafo:
//!
//!   dst run   --seed N [--config dst.json] [--wills-strict]
//!             one simulated cluster life; exit 0 = clean, crash = bug
//!   dst check --seed N ...
//!             determinism self-test: run the seed twice, the trace
//!             hashes must match
//!   dst mine  [--jobs J] [--seconds S] [--seed BASE] ...
//!             all cores, one subprocess per seed (so aborts and panics
//!             anywhere are caught), crashes confirmed by re-running the
//!             seed, then logged to crashes/seed-N.log
//!
//! A crash is a bug. A bug is $100. The seed in the crash log replays it.

use fafo::sim::{DstConfig, Rng, run_blocking};
use std::io::Write;
use std::process::Command;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("help");
    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1).cloned())
    };
    let has = |name: &str| args.iter().any(|a| a == name);

    let fuzz = has("--fuzz");
    let mut cfg: DstConfig = match (flag("--config"), fuzz) {
        (Some(path), _) => serde_json::from_str(&std::fs::read_to_string(&path).expect("read config"))
            .expect("parse config"),
        // --fuzz derives the whole cluster shape from the seed; the concrete
        // seed is filled in below (and per-seed in `mine`).
        (None, true) => DstConfig::fuzzed(flag("--seed").as_deref().and_then(|s| s.parse().ok()).unwrap_or(1)),
        (None, false) => DstConfig::default(),
    };
    if let Some(seed) = flag("--seed") {
        let seed = seed.parse().expect("--seed takes a u64");
        cfg = if fuzz { DstConfig::fuzzed(seed) } else { DstConfig { seed, ..cfg } };
    }
    if has("--wills-strict") {
        cfg.wills_survive_node_crash = true;
    }
    // Reproduce the original memory-only will bug: persistence off, oracle
    // on. The default (persistence on) is the fix.
    if has("--no-durable-wills") {
        cfg.durable_wills = false;
        cfg.wills_survive_node_crash = true;
    }

    let extra_flags: Vec<String> = ["--wills-strict", "--no-durable-wills", "--fuzz"]
        .iter()
        .filter(|f| has(f))
        .map(|f| f.to_string())
        .collect();
    if mode == "config" {
        println!("{}", serde_json::to_string_pretty(&cfg).unwrap());
        return;
    }
    match mode {
        "run" => run_one(cfg),
        "check" => check(cfg),
        "mine" => mine(cfg, &flag("--jobs"), &flag("--seconds"), &extra_flags, fuzz),
        _ => {
            eprintln!(
                "usage: dst run|check|mine [--seed N] [--config f.json] \
                 [--jobs J] [--seconds S] [--wills-strict] [--no-durable-wills]"
            );
            std::process::exit(2);
        }
    }
}

/// Any panic anywhere in the simulation — an oracle, a fafo_assert deep
/// in a worker task, an unwrap in the harness — must take the whole
/// process with it, loudly, with the seed attached. That's what makes
/// "nonzero exit = bug" airtight.
fn install_crash_hook(seed: u64) {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("\n==== DST CRASH (seed {seed}) ====");
        default(info);
        eprintln!("reproduce with: cargo run --release --bin dst -- run --seed {seed}");
        std::process::abort();
    }));
}

fn run_one(cfg: DstConfig) {
    install_crash_hook(cfg.seed);
    let report = run_blocking(cfg);
    println!(
        "seed {} clean: {} events, trace {:016x}",
        report.seed, report.events, report.trace_hash
    );
}

fn check(cfg: DstConfig) {
    install_crash_hook(cfg.seed);
    let a = run_blocking(cfg.clone());
    let b = run_blocking(cfg);
    if a.trace_hash != b.trace_hash || a.events != b.events {
        eprintln!(
            "NONDETERMINISM at seed {}: run A = {} events / {:016x}, run B = {} events / {:016x}",
            a.seed, a.events, a.trace_hash, b.events, b.trace_hash
        );
        std::process::exit(1);
    }
    println!(
        "seed {} deterministic: {} events, trace {:016x} twice",
        a.seed, a.events, a.trace_hash
    );
}

fn mine(
    cfg: DstConfig,
    jobs: &Option<String>,
    seconds: &Option<String>,
    extra_flags: &[String],
    fuzz: bool,
) {
    let jobs: usize = jobs
        .as_deref()
        .map(|j| j.parse().expect("--jobs"))
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4));
    let deadline = seconds
        .as_deref()
        .map(|s| std::time::Instant::now() + std::time::Duration::from_secs(s.parse().expect("--seconds")));
    let exe = std::env::current_exe().expect("current_exe");
    std::fs::create_dir_all("crashes").expect("crashes dir");

    // In fixed-config mode a bounty claim is (config, seed): persist the
    // config. In --fuzz mode the seed alone derives the config, so there is
    // nothing to persist — the crash log carries the exact repro command.
    let config_path = "crashes/mine-config.json";
    if !fuzz {
        std::fs::write(config_path, serde_json::to_string_pretty(&cfg).unwrap()).unwrap();
    }

    let mut seeder = Rng::new(cfg.seed);
    let mut running: Vec<(u64, std::process::Child)> = Vec::new();
    let (mut launched, mut clean, mut bugs) = (0u64, 0u64, 0u64);
    let mode_label = if fuzz { "config-fuzzing" } else { config_path };
    println!("mining with {jobs} jobs ({mode_label}); a crash is a bug, a bug is $100");

    loop {
        let out_of_time = deadline.is_some_and(|d| std::time::Instant::now() > d);
        while running.len() < jobs && !out_of_time {
            let seed = seeder.next();
            let mut cmd = Command::new(&exe);
            cmd.args(["run", "--seed", &seed.to_string()]);
            if !fuzz {
                cmd.args(["--config", config_path]);
            }
            for f in extra_flags {
                cmd.arg(f);
            }
            let child = cmd
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("spawn miner");
            running.push((seed, child));
            launched += 1;
        }
        if running.is_empty() {
            break;
        }
        // Reap whoever finished; sleep briefly if nobody has.
        let mut still = Vec::new();
        for (seed, mut child) in running {
            match child.try_wait().expect("try_wait") {
                None => still.push((seed, child)),
                Some(status) if status.success() => clean += 1,
                Some(_) => {
                    bugs += 1;
                    let out = child.wait_with_output().expect("crash output");
                    let log = format!("crashes/seed-{seed}.log");
                    let mut f = std::fs::File::create(&log).expect("crash log");
                    let repro = if fuzz {
                        format!("dst run --seed {seed} --fuzz")
                    } else {
                        format!("dst run --seed {seed} --config {config_path}")
                    };
                    writeln!(f, "seed: {seed}\nreproduce: {repro}\n").unwrap();
                    f.write_all(&out.stderr).unwrap();
                    println!("BUG: seed {seed} crashed -> {log}");
                }
            }
        }
        running = still;
        if launched % 50 == 0 || running.len() == jobs {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if launched.is_multiple_of(100) && running.len() < jobs {
            println!("mined {launched} seeds: {clean} clean, {bugs} bugs");
        }
    }
    println!("done: {launched} seeds, {clean} clean, {bugs} bugs");
    if bugs > 0 {
        std::process::exit(1);
    }
}
