//! Group-commit A/B: hammer hot objects with concurrent writes, pessimistic
//! (every txn pays a full commit round trip) vs optimistic (txns coalesce
//! into boats, one commit record per boat).
//!
//!   cargo run --release --bin boatbench
//!   TXNS=5000 CONC=100 OBJECTS=4 cargo run --release --bin boatbench

use fafo::cluster::{self, Node, NodeConfig, Op};
use fafo::store::{BlobStore, FsBlobStore};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Wraps a store with artificial per-op latency to simulate remote object
/// storage (LAT_MS=25 approximates an R2 round trip).
struct SlowStore(FsBlobStore, Duration);

#[async_trait::async_trait]
impl BlobStore for SlowStore {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        tokio::time::sleep(self.1).await;
        self.0.get(key).await
    }
    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        tokio::time::sleep(self.1).await;
        self.0.put(key, bytes).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        tokio::time::sleep(self.1).await;
        self.0.delete(key).await
    }
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        self.0.list(prefix).await
    }
    async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
        tokio::time::sleep(self.1).await;
        self.0.create(key, bytes).await
    }
}

async fn boot(root: &std::path::Path, blobs: &str, tag: &str) -> Node {
    let lat = Duration::from_millis(env_or("LAT_MS", 0) as u64);
    let fs = FsBlobStore::new(root.join(format!("blobs-{blobs}"))).unwrap();
    let store: Arc<dyn BlobStore> = if lat.is_zero() {
        Arc::new(fs)
    } else {
        Arc::new(SlowStore(fs, lat))
    };
    cluster::start(NodeConfig {
        logical: 8,
        secret: "bench".into(),
        ..NodeConfig::new(store, root.join(format!("live-{tag}")))
    })
    .await
    .unwrap()
}

async fn run_mode(node: &Node, objects: &[String], txns: usize, conc: usize, optimistic: bool) -> f64 {
    let start = Instant::now();
    let mut inflight = tokio::task::JoinSet::new();
    for i in 0..txns {
        while inflight.len() >= conc {
            inflight.join_next().await.unwrap().unwrap();
        }
        let node = node.clone();
        let object = objects[i % objects.len()].clone();
        inflight.spawn(async move {
            cluster::submit(
                &node,
                vec![object.clone()],
                vec![Op {
                    object,
                    sql: "INSERT INTO t (n) VALUES (1)".into(),
                    params: vec![],
                }],
                false,
                optimistic,
            )
            .await
            .expect("write succeeds");
        });
    }
    while inflight.join_next().await.is_some() {}
    txns as f64 / start.elapsed().as_secs_f64()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let txns = env_or("TXNS", 5_000);
    let conc = env_or("CONC", 100);
    let n_objects = env_or("OBJECTS", 4);

    let root = std::env::temp_dir().join(format!("fafo-boatbench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let lat = env_or("LAT_MS", 0);
    println!(
        "{txns} writes, {conc} concurrent, {n_objects} hot object(s), fs blob store{}",
        if lat > 0 {
            format!(" + {lat}ms simulated latency per blob op")
        } else {
            String::new()
        }
    );
    println!();

    for optimistic in [false, true] {
        let tag = if optimistic { "opt" } else { "pess" };
        let node = boot(&root, tag, tag).await;
        let objects: Vec<String> = (0..n_objects).map(|i| format!("hot{i}")).collect();
        let seed = env_or("SEED_KB", 0);
        for o in &objects {
            let mut ops = vec![Op {
                object: o.clone(),
                sql: "CREATE TABLE t (n INTEGER, b TEXT)".into(),
                params: vec![],
            }];
            if seed > 0 {
                // Pre-grow the object so snapshots would be expensive.
                ops.push(Op {
                    object: o.clone(),
                    sql: "INSERT INTO t (n, b) VALUES (0, ?1)".into(),
                    params: vec![serde_json::json!("x".repeat(seed * 1024))],
                });
            }
            cluster::submit(&node, vec![o.clone()], ops, false, false)
                .await
                .unwrap();
        }

        let before = node.stats().await;
        let tps = run_mode(&node, &objects, txns, conc, optimistic).await;
        node.shutdown().await; // flushes the final boat
        let after = node.stats().await;
        let ships = after.ships - before.ships;
        let mb_shipped = (after.bytes_shipped - before.bytes_shipped) as f64 / 1e6;

        // Correctness: everything acked must be durable.
        let verify = boot(&root, tag, &format!("{tag}-v")).await;
        let mut total = 0i64;
        for o in &objects {
            let res = cluster::submit(
                &verify,
                vec![o.clone()],
                vec![Op {
                    object: o.clone(),
                    sql: "SELECT COUNT(*) AS c FROM t WHERE n = 1".into(),
                    params: vec![],
                }],
                true,
                false,
            )
            .await
            .unwrap();
            let v = serde_json::to_value(&res.results).unwrap();
            total += v[0]["rows"][0]["c"].as_i64().unwrap();
        }
        verify.shutdown().await;
        assert_eq!(total as usize, txns, "all acked writes durable");

        println!(
            "{:>11}: {:>8.0} txn/s   {:>5} boats   {:>6.1} txns/boat   {:>8.1} MB shipped   all {} writes durable ✓",
            if optimistic { "optimistic" } else { "pessimistic" },
            tps,
            ships,
            txns as f64 / ships as f64,
            mb_shipped,
            txns
        );
    }

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}
