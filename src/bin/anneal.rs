//! Watch placement learn — and survive a stop-the-world. Phase 1 anneals a
//! clustered workload from scratch; then the node is shut down and a fresh
//! one boots over the same blob store (placement loaded from worker
//! checkpoints); phase 2 should open at the annealed rate, not the random
//! baseline.
//!
//!   cargo run --release --bin anneal
//!   HYST=0 LOGICAL=64 CLIQUES=32 TXNS=10000 cargo run --release --bin anneal

use fafo::cluster::{self, Node, Op};
use fafo::store::{BlobStore, FsBlobStore};
use std::sync::Arc;

/// Deterministic splitmix64: no rand dependency, reproducible runs.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn submit(node: &Node, objects: Vec<String>, ops: Vec<Op>) {
    cluster::submit(node, objects, ops, false, std::env::var("OPT").is_ok())
        .await
        .expect("txn succeeds");
}

fn insert_op(object: &str) -> Op {
    Op {
        object: object.to_string(),
        sql: "INSERT INTO t (n) VALUES (1)".to_string(),
        params: vec![],
    }
}

async fn boot(root: &std::path::Path, logical: usize, hyst: u64, tag: &str) -> Node {
    let store: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(root.join("blobs")).unwrap());
    cluster::start(cluster::NodeConfig {
        logical,
        hysteresis: hyst,
        secret: "anneal".into(),
        ..cluster::NodeConfig::new(store, root.join(format!("live-{tag}")))
    })
    .await
    .unwrap()
}

async fn drive(node: &Node, rng: &mut Rng, txns: usize, cliques: usize, clique_size: usize, cross_pct: u64) {
    let window = 1_000;
    let name = |c: usize, m: usize| format!("c{c}_m{m}");
    let mut s = node.stats().await;
    let (mut prev_total, mut prev_cross, mut prev_takes, mut prev_returns) =
        (s.total_txns, s.cross_worker_txns, s.takes, s.returns);
    println!(
        "{:>12} {:>10} {:>8} {:>8}",
        "txns", "cross %", "takes", "returns"
    );
    for i in 0..txns {
        let picks: Vec<String> = if rng.below(100) < cross_pct {
            let c1 = (rng.below(cliques as u64).min(rng.below(cliques as u64))) as usize;
            let c2 = (rng.below(cliques as u64).min(rng.below(cliques as u64))) as usize;
            vec![
                name(c1, rng.below(clique_size as u64) as usize),
                name(c2, rng.below(clique_size as u64) as usize),
            ]
        } else {
            let c = (rng.below(cliques as u64).min(rng.below(cliques as u64))) as usize;
            let m1 = rng.below(clique_size as u64) as usize;
            let m2 = (m1 + 1 + rng.below(clique_size as u64 - 1) as usize) % clique_size;
            vec![name(c, m1), name(c, m2)]
        };
        let ops = picks.iter().map(|p| insert_op(p)).collect();
        submit(node, picks, ops).await;

        if (i + 1) % window == 0 {
            s = node.stats().await;
            let dt = s.total_txns - prev_total;
            println!(
                "{:>12} {:>9.1}% {:>8} {:>8}",
                format!("{}..{}", i + 1 - window, i + 1),
                100.0 * (s.cross_worker_txns - prev_cross) as f64 / dt as f64,
                s.takes - prev_takes,
                s.returns - prev_returns,
            );
            (prev_total, prev_cross, prev_takes, prev_returns) =
                (s.total_txns, s.cross_worker_txns, s.takes, s.returns);
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let logical = env_or("LOGICAL", 64);
    let cliques = env_or("CLIQUES", 32);
    let clique_size = 3;
    let txns = env_or("TXNS", 10_000);
    let cross_pct = env_or("CROSS_PCT", 10) as u64;
    let hyst = env_or("HYST", 200) as u64;

    let root = std::env::temp_dir().join(format!("fafo-anneal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    println!(
        "logical={logical} cliques={cliques}x{clique_size} txns={txns}+{txns} cross-clique={cross_pct}% hyst={hyst}"
    );

    let mut rng = Rng(42);
    println!("\n== phase 1: cold start, random placement");
    let node = boot(&root, logical, hyst, "p1").await;
    for c in 0..cliques {
        for m in 0..clique_size {
            let id = format!("c{c}_m{m}");
            submit(
                &node,
                vec![id.clone()],
                vec![Op {
                    object: id.clone(),
                    sql: "CREATE TABLE t (n INTEGER)".to_string(),
                    params: vec![],
                }],
            )
            .await;
        }
    }
    drive(&node, &mut rng, txns, cliques, clique_size, cross_pct).await;
    let s = node.stats().await;
    println!(
        "phase 1 total: {:.1}% cross-worker",
        100.0 * s.cross_worker_txns as f64 / s.total_txns as f64
    );

    println!("\n== stop the world");
    node.shutdown().await;

    println!("== phase 2: fresh node over the same blobs (placement from checkpoints)");
    let node = boot(&root, logical, hyst, "p2").await;
    drive(&node, &mut rng, txns, cliques, clique_size, cross_pct).await;
    let s = node.stats().await;
    println!(
        "phase 2 total: {:.1}% cross-worker — learning survived the restart",
        100.0 * s.cross_worker_txns as f64 / s.total_txns as f64
    );
    let mut busy: Vec<_> = s.per_worker.iter().filter(|w| w.txns > 0).collect();
    busy.sort_by_key(|w| std::cmp::Reverse(w.txns));
    println!("busiest workers (txns/exceptions owned):");
    for w in busy.iter().take(10) {
        println!("  w{:<3} {:>6} txns  {:>3} exceptions", w.worker, w.txns, w.owned_exceptions);
    }
    println!("  ({} workers executed at least one txn)", busy.len());
    node.shutdown().await;

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}
