//! Cluster topology: W logical workers, any number of processes/containers.
//!
//! Logical workers are the stable coordinate system (the Kafka-partition
//! trick): placement learning attaches to logical worker ids, and live
//! nodes claim logical workers via epoch leases in the blob store. Kill a
//! node and its workers get claimed by survivors at a bumped epoch; run one
//! node and it claims everything. Nothing couples to a node or thread count.
//!
//! Global coordination happens in exactly two places, both off the hot path:
//!   - lease claims race through BlobStore::create (create-if-absent CAS)
//!   - object ownership is recorded in per-worker checkpoints
//!     (`_worker/<i>.json`), written remove-side-first on every transfer so
//!     an object is never durably claimed by two workers; a crash between
//!     the two writes orphans the object, which merely falls back to its
//!     hash-default worker — placement is a hint, the data is safe either way.
//!
//! Fencing: each node runs a lease guard that watches its claimed epochs;
//! if any is superseded, the node fail-stops (process::exit). The residual
//! unsafety window is the guard poll interval plus one write RTT — a paused
//! process could commit within it. Documented in the README.

use crate::api::ApiError;
use crate::store::BlobStore;
use crate::worker::{self, WorkerMsg};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};

// ---------------------------------------------------------------- wire types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Op {
    pub object: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OpResult {
    Rows { rows: Vec<Value> },
    Affected { rows_affected: usize },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TxnResponse {
    pub txn_id: String,
    pub results: Vec<OpResult>,
}

/// Placement metadata that travels with an object during ownership transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferMeta {
    /// The giver considered the object settled (long tenure): the receiver
    /// should treat this as a visit (return it home) unless repeat visits
    /// say otherwise.
    pub settled: bool,
    /// Where the object should return to. Its original home if it was
    /// already on a trip, else the giver.
    pub home: usize,
    pub visit: Option<VisitInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisitInfo {
    /// Logical worker the visits were to (globally meaningful).
    pub worker: usize,
    pub count: u32,
    /// Receiver-side clock value at the last visit.
    pub last: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TakeError {
    /// Not the owner (any more); hint = who we think owns it now.
    NotMine { hint: Option<usize> },
    Failed(String),
}

#[derive(Debug, Serialize)]
pub struct StatsSnapshot {
    pub logical_workers: usize,
    pub claimed_here: Vec<usize>,
    pub total_txns: u64,
    pub cross_worker_txns: u64,
    pub takes: u64,
    pub returns: u64,
    pub per_worker: Vec<WorkerStat>,
}

#[derive(Debug, Serialize)]
pub struct WorkerStat {
    pub worker: usize,
    pub txns: u64,
    pub owned_exceptions: usize,
}

// ------------------------------------------------------------------ routing

/// Object -> logical worker. Hash default unless a checkpoint/transfer says
/// otherwise. This map is a per-node cache of hints: stale entries get
/// corrected by NotMine bounces, and correctness never depends on it —
/// the owning worker is the authority for what it owns.
pub struct Routing {
    pub logical: usize,
    pub exceptions: HashMap<String, usize>,
    /// Logical worker -> base URL of the node holding its lease.
    pub addrs: HashMap<usize, String>,
}

impl Routing {
    pub fn owner_of(&self, object: &str) -> usize {
        self.exceptions
            .get(object)
            .copied()
            .unwrap_or_else(|| default_worker(object, self.logical))
    }

    /// Exception count for one worker — the cheap local proxy for load.
    pub fn exception_load(&self, worker: usize) -> usize {
        self.exceptions.values().filter(|&&w| w == worker).count()
    }

    /// Pressure: is this worker already hosting more than its share of
    /// migrated objects? Cohesion must not win every argument or the whole
    /// system collapses onto one mega-worker (observed empirically: 87/96
    /// objects on w0). Cap ≈ 2x the fair share.
    pub fn crowded(&self, worker: usize) -> bool {
        let total = self.exceptions.len();
        let cap = ((2 * total) / self.logical).max(4);
        self.exception_load(worker) >= cap
    }
}

/// Deterministic across processes: DefaultHasher::new() uses fixed keys.
pub fn default_worker(object: &str, logical: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    object.hash(&mut h);
    (h.finish() % logical as u64) as usize
}

// --------------------------------------------------------------------- node

pub enum ClaimSpec {
    All,
    Workers(Vec<usize>),
    /// Claim up to N workers that are free (unclaimed, tombstoned, or dead).
    /// The right mode for identical container instances: no per-instance
    /// configuration, the fleet divides the workers among itself.
    Auto(usize),
}

impl ClaimSpec {
    /// "all", "auto:16", "7", or "0-15".
    pub fn parse(spec: &str, logical: usize) -> Self {
        if spec == "all" {
            return Self::All;
        }
        if let Some(k) = spec.strip_prefix("auto:").and_then(|k| k.parse().ok()) {
            return Self::Auto(k);
        }
        if let Some((a, b)) = spec.split_once('-')
            && let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>())
        {
            return Self::Workers((a..=b.min(logical.saturating_sub(1))).collect());
        }
        Self::Workers(spec.parse::<usize>().map(|w| vec![w]).unwrap_or_default())
    }
}

pub struct NodeConfig {
    pub store: Arc<dyn BlobStore>,
    pub live_dir: PathBuf,
    pub logical: usize,
    pub claim: ClaimSpec,
    /// Listen address, e.g. "127.0.0.1:0" (dev) or "0.0.0.0:8080" (container).
    pub bind: String,
    /// Base URL peers should use to reach this node. Defaults to
    /// http://<local_addr>; on Cloudflare set it to the Worker-routed URL
    /// for this instance (e.g. https://fafo.example.com/internal/instance/3).
    pub advertise: Option<String>,
    pub hysteresis: u64,
    /// Shared secret for /internal/rpc. Every node in a cluster must agree.
    pub secret: String,
    /// Optional bearer token required on the public API.
    pub api_token: Option<String>,
}

pub struct NodeInner {
    pub store: Arc<dyn BlobStore>,
    pub routing: RwLock<Routing>,
    /// Senders for logical workers claimed by THIS node.
    pub local: RwLock<HashMap<usize, mpsc::UnboundedSender<WorkerMsg>>>,
    /// Logical clock for tenure/visit windows (per-node; hints only).
    pub clock: AtomicU64,
    pub hysteresis: u64,
    /// Base URL peers use to reach this node.
    pub advertise: String,
    pub secret: String,
    pub api_token: Option<String>,
    pub http: reqwest::Client,
    pub stats: Stats,
    /// Epochs of leases this node holds; watched by the lease guard.
    pub epochs: RwLock<HashMap<usize, u64>>,
    /// Set by the lease guard just before fail-stop; checked at the commit
    /// point as a last line of defense.
    pub fenced: AtomicBool,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

pub type Node = Arc<NodeInner>;

#[derive(Default)]
pub struct Stats {
    pub total_txns: AtomicU64,
    pub cross_worker_txns: AtomicU64,
    pub takes: AtomicU64,
    pub returns: AtomicU64,
}

#[derive(Serialize, Deserialize)]
struct ClusterMeta {
    logical_workers: usize,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    owned: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Lease {
    addr: String,
}

pub fn checkpoint_key(worker: usize) -> String {
    format!("_worker/{worker}.json")
}

fn lease_key(worker: usize, epoch: u64) -> String {
    format!("_lease/w{worker}/e{epoch}.json")
}

fn tombstone_key(worker: usize, epoch: u64) -> String {
    format!("_lease/w{worker}/e{epoch}.released")
}

const LEASE_GUARD_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

struct LeaseState {
    epoch: u64,
    addr: String,
    released: bool,
}

async fn latest_lease(store: &dyn BlobStore, worker: usize) -> anyhow::Result<Option<LeaseState>> {
    let prefix = format!("_lease/w{worker}/");
    let keys = store.list(&prefix).await?;
    let mut best: Option<u64> = None;
    for key in &keys {
        if let Some(epoch) = key
            .strip_prefix(&prefix)
            .and_then(|k| k.strip_prefix('e'))
            .and_then(|k| k.strip_suffix(".json"))
            .and_then(|k| k.parse::<u64>().ok())
            && best.is_none_or(|e| epoch > e)
        {
            best = Some(epoch);
        }
    }
    let Some(epoch) = best else {
        return Ok(None);
    };
    let Some(bytes) = store.get(&lease_key(worker, epoch)).await? else {
        return Ok(None);
    };
    let lease: Lease = serde_json::from_slice(&bytes)?;
    let released = keys.contains(&tombstone_key(worker, epoch));
    Ok(Some(LeaseState {
        epoch,
        addr: lease.addr,
        released,
    }))
}

/// Boot a node: agree on W (create-once cluster meta), recover the commit
/// log, load placement from checkpoints, claim logical workers via epoch
/// leases, spawn worker tasks, serve HTTP (public API + /internal/rpc), and
/// start the lease guard.
pub async fn start(cfg: NodeConfig) -> anyhow::Result<Node> {
    // Cluster config is create-once: first node wins, everyone else adopts.
    let meta_bytes = serde_json::to_vec(&ClusterMeta {
        logical_workers: cfg.logical,
    })?;
    cfg.store.create("_meta/cluster.json", &meta_bytes).await?;
    let logical = match cfg.store.get("_meta/cluster.json").await? {
        Some(bytes) => serde_json::from_slice::<ClusterMeta>(&bytes)?.logical_workers,
        None => cfg.logical,
    };

    // Any node may recover: roll-forward is idempotent (same bytes, same
    // deletes), so concurrent booters are harmless.
    worker::recover(cfg.store.as_ref()).await?;

    let _ = std::fs::remove_dir_all(&cfg.live_dir);
    std::fs::create_dir_all(&cfg.live_dir)?;

    // Placement knowledge: union of all workers' checkpoints. An object
    // claimed by two checkpoints (crash between transfer writes) goes to the
    // lower worker id — arbitrary but deterministic, and safe because the
    // data lives in the blob store either way.
    let mut exceptions: HashMap<String, usize> = HashMap::new();
    for key in cfg.store.list("_worker/").await? {
        let Some(w) = key
            .strip_prefix("_worker/")
            .and_then(|k| k.strip_suffix(".json"))
            .and_then(|k| k.parse::<usize>().ok())
        else {
            continue;
        };
        let Some(bytes) = cfg.store.get(&key).await? else {
            continue;
        };
        let Ok(cp) = serde_json::from_slice::<Checkpoint>(&bytes) else {
            continue;
        };
        for object in cp.owned {
            if default_worker(&object, logical) == w {
                continue;
            }
            match exceptions.get(&object) {
                Some(&prev) if prev <= w => {
                    println!("boot: {object} dual-claimed by w{prev} and w{w}; keeping w{prev}");
                }
                _ => {
                    exceptions.insert(object, w);
                }
            }
        }
    }

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    let advertise = cfg
        .advertise
        .clone()
        .unwrap_or_else(|| format!("http://{}", listener.local_addr().unwrap()));

    // Current lease holders for every worker (so we can route to peers).
    let mut addrs = HashMap::new();
    for w in 0..logical {
        if let Some(lease) = latest_lease(cfg.store.as_ref(), w).await? {
            addrs.insert(w, lease.addr);
        }
    }

    let node: Node = Arc::new(NodeInner {
        store: cfg.store.clone(),
        routing: RwLock::new(Routing {
            logical,
            exceptions,
            addrs,
        }),
        local: RwLock::new(HashMap::new()),
        clock: AtomicU64::new(0),
        hysteresis: cfg.hysteresis,
        advertise: advertise.clone(),
        secret: cfg.secret,
        api_token: cfg.api_token,
        http: reqwest::Client::new(),
        stats: Stats::default(),
        epochs: RwLock::new(HashMap::new()),
        fenced: AtomicBool::new(false),
        tasks: Mutex::new(Vec::new()),
    });

    // The HTTP server must be up before claiming: peers health-check us,
    // and a claimed-but-unreachable node reads as dead.
    let app = crate::api::router(node.clone());
    node.tasks.lock().unwrap().push(tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    }));

    let candidates: Vec<usize> = match &cfg.claim {
        ClaimSpec::All => (0..logical).collect(),
        ClaimSpec::Workers(ws) => ws.iter().copied().filter(|w| *w < logical).collect(),
        ClaimSpec::Auto(_) => {
            // Rotate the scan by our address hash so concurrent booters
            // start claiming from different offsets (fewer create races).
            let mut h = std::collections::hash_map::DefaultHasher::new();
            advertise.hash(&mut h);
            let start = (h.finish() % logical as u64) as usize;
            (0..logical).map(|i| (start + i) % logical).collect()
        }
    };
    let quota = match &cfg.claim {
        ClaimSpec::Auto(k) => *k,
        _ => usize::MAX,
    };

    let mut claimed = 0usize;
    for w in candidates {
        if claimed >= quota {
            break;
        }
        let next_epoch = match latest_lease(cfg.store.as_ref(), w).await? {
            Some(lease) => {
                // Claimable when: cleanly released, held by our own
                // predecessor identity (rolling deploy of a named
                // instance), or the holder is dead.
                let claimable = lease.released
                    || lease.addr == advertise
                    || !crate::rpc::health(&node, &lease.addr).await;
                if !claimable {
                    continue;
                }
                lease.epoch + 1
            }
            None => 1,
        };
        let lease_bytes = serde_json::to_vec(&Lease {
            addr: advertise.clone(),
        })?;
        if !cfg
            .store
            .create(&lease_key(w, next_epoch), &lease_bytes)
            .await?
        {
            continue; // lost the claim race
        }
        let tx = worker::spawn(node.clone(), w, cfg.live_dir.join(format!("w{w}")))?;
        node.local.write().unwrap().insert(w, tx);
        node.epochs.write().unwrap().insert(w, next_epoch);
        node.routing
            .write()
            .unwrap()
            .addrs
            .insert(w, advertise.clone());
        claimed += 1;
    }

    // Lease guard: fail-stop if any of our epochs gets superseded. Losing a
    // lease means another node is now the writer for that worker; continuing
    // would violate single-writer. Exiting the whole process is the correct
    // (fail-stop) response, not a recoverable error.
    let guard_node = node.clone();
    node.tasks.lock().unwrap().push(tokio::spawn(async move {
        loop {
            tokio::time::sleep(LEASE_GUARD_INTERVAL).await;
            let epochs: Vec<(usize, u64)> = {
                let e = guard_node.epochs.read().unwrap();
                e.iter().map(|(w, e)| (*w, *e)).collect()
            };
            for (w, mine) in epochs {
                match latest_lease(guard_node.store.as_ref(), w).await {
                    Ok(Some(lease)) if lease.epoch > mine => {
                        guard_node.fenced.store(true, Ordering::SeqCst);
                        eprintln!(
                            "FENCED: lease for worker {w} superseded (epoch {} > {mine}); fail-stopping",
                            lease.epoch
                        );
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
        }
    }));

    println!(
        "node {} claimed {:?} of {} logical workers",
        advertise,
        node.claimed(),
        logical
    );
    Ok(node)
}

impl NodeInner {
    pub fn claimed(&self) -> Vec<usize> {
        let mut v: Vec<usize> = self.local.read().unwrap().keys().copied().collect();
        v.sort_unstable();
        v
    }

    /// Graceful shutdown: stop serving, drop worker senders so worker tasks
    /// drain and exit, and tombstone our leases so the next claimant doesn't
    /// need a failed health check to take over. Checkpoints are already
    /// current — they're written synchronously on every ownership change.
    pub async fn shutdown(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        self.local.write().unwrap().clear();
        let epochs: Vec<(usize, u64)> = {
            let e = self.epochs.read().unwrap();
            e.iter().map(|(w, e)| (*w, *e)).collect()
        };
        for (w, epoch) in epochs {
            let _ = self.store.create(&tombstone_key(w, epoch), b"released").await;
        }
        self.epochs.write().unwrap().clear();
    }

    pub async fn stats(&self) -> StatsSnapshot {
        let senders: Vec<(usize, mpsc::UnboundedSender<WorkerMsg>)> = {
            let local = self.local.read().unwrap();
            local.iter().map(|(w, tx)| (*w, tx.clone())).collect()
        };
        let mut per_worker = Vec::new();
        for (w, tx) in senders {
            let (rtx, rrx) = oneshot::channel();
            if tx.send(WorkerMsg::Stats { resp: rtx }).is_ok()
                && let Ok((txns, owned_exceptions)) = rrx.await
            {
                per_worker.push(WorkerStat {
                    worker: w,
                    txns,
                    owned_exceptions,
                });
            }
        }
        per_worker.sort_by_key(|s| s.worker);
        StatsSnapshot {
            logical_workers: self.routing.read().unwrap().logical,
            claimed_here: self.claimed(),
            total_txns: self.stats.total_txns.load(Ordering::Relaxed),
            cross_worker_txns: self.stats.cross_worker_txns.load(Ordering::Relaxed),
            takes: self.stats.takes.load(Ordering::Relaxed),
            returns: self.stats.returns.load(Ordering::Relaxed),
            per_worker,
        }
    }
}

/// Re-read a worker's lease from the blob store and refresh the address
/// cache. Used whenever routing has no (or a stale) address for a worker —
/// e.g. a node that claimed its lease after we booted.
pub async fn resolve_addr(node: &Node, worker: usize) -> Option<String> {
    let lease = latest_lease(node.store.as_ref(), worker).await.ok()??;
    node.routing
        .write()
        .unwrap()
        .addrs
        .insert(worker, lease.addr.clone());
    Some(lease.addr)
}

/// Validate and route a transaction: pick the plurality owner as target,
/// dispatch to it locally or proxy to the node holding its lease.
pub async fn submit(
    node: &Node,
    objects: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
) -> Result<TxnResponse, ApiError> {
    if objects.is_empty() {
        return Err(ApiError::bad_request("declare at least one object"));
    }
    let mut ids = objects;
    ids.sort();
    ids.dedup();
    for id in &ids {
        if !crate::object::valid_id(id) {
            return Err(ApiError::bad_request(format!("invalid object id: {id:?}")));
        }
    }
    for op in &ops {
        if ids.binary_search(&op.object).is_err() {
            return Err(ApiError::bad_request(format!(
                "op touches undeclared object {:?} — declare it in `objects`",
                op.object
            )));
        }
    }
    node.clock.fetch_add(1, Ordering::Relaxed);
    node.stats.total_txns.fetch_add(1, Ordering::Relaxed);
    submit_routed(node, ids, ops, read_only).await
}

/// Routing half of submit, callable from the RPC handler (already validated).
pub async fn submit_routed(
    node: &Node,
    ids: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
) -> Result<TxnResponse, ApiError> {
    // Plurality owner wins; ties break toward the less-loaded worker
    // (pressure), then lowest id (determinism). Two-object cross txns are
    // always 1-1 ties, so the load tie-break is the main balancing force.
    let target = {
        let routing = node.routing.read().unwrap();
        let mut votes: HashMap<usize, usize> = HashMap::new();
        for id in &ids {
            *votes.entry(routing.owner_of(id)).or_default() += 1;
        }
        votes
            .into_iter()
            .min_by_key(|&(w, count)| (std::cmp::Reverse(count), routing.exception_load(w), w))
            .map(|(w, _)| w)
            .unwrap()
    };

    let local_tx = node.local.read().unwrap().get(&target).cloned();
    if let Some(tx) = local_tx {
        let (rtx, rrx) = oneshot::channel();
        tx.send(WorkerMsg::Submit {
            participants: ids,
            ops,
            read_only,
            resp: rtx,
        })
        .map_err(|_| ApiError::internal("worker is gone"))?;
        rrx.await
            .map_err(|_| ApiError::internal("transaction dropped"))?
    } else {
        let cached = node.routing.read().unwrap().addrs.get(&target).cloned();
        let addr = match cached {
            Some(addr) => addr,
            None => resolve_addr(node, target).await.ok_or_else(|| {
                ApiError::internal(format!("no live node holds logical worker {target}"))
            })?,
        };
        match crate::rpc::forward_txn(node, &addr, ids.clone(), ops.clone(), read_only).await {
            // Transport failure: the cached address may belong to a dead
            // world. Re-read the lease and retry once at the new holder.
            Err(e) if e.message.starts_with("rpc to") => match resolve_addr(node, target).await {
                Some(fresh) if fresh != addr => {
                    crate::rpc::forward_txn(node, &fresh, ids, ops, read_only).await
                }
                _ => Err(e),
            },
            other => other,
        }
    }
}

/// Convenience for tests: find an id that hash-defaults to a given worker.
pub fn id_on_worker(worker: usize, logical: usize, tag: &str) -> String {
    (0..)
        .map(|i| format!("{tag}{i}"))
        .find(|id| default_worker(id, logical) == worker)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FsBlobStore;

    async fn boot(root: &std::path::Path, logical: usize, claim: ClaimSpec, tag: &str) -> Node {
        let store: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(root.join("blobs")).unwrap());
        start(NodeConfig {
            store,
            live_dir: root.join(format!("live-{tag}")),
            logical,
            claim,
            bind: "127.0.0.1:0".into(),
            advertise: None,
            hysteresis: 200,
            secret: "test-secret".into(),
            api_token: None,
        })
        .await
        .unwrap()
    }

    async fn exec(
        node: &Node,
        objects: &[&str],
        ops: &[(&str, &str)],
    ) -> Result<TxnResponse, ApiError> {
        submit(
            node,
            objects.iter().map(|s| s.to_string()).collect(),
            ops.iter()
                .map(|(object, sql)| Op {
                    object: object.to_string(),
                    sql: sql.to_string(),
                    params: vec![],
                })
                .collect(),
            false,
        )
        .await
    }

    async fn balance(node: &Node, id: &str) -> i64 {
        let res = exec(node, &[id], &[(id, "SELECT balance FROM account")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        v[0]["rows"][0]["balance"].as_i64().unwrap()
    }

    async fn make_account(node: &Node, id: &str) {
        let create = format!(
            "CREATE TABLE account (balance INTEGER NOT NULL CHECK (balance >= 0)) -- {id}"
        );
        exec(
            node,
            &[id],
            &[
                (id, create.as_str()),
                (id, "INSERT INTO account (balance) VALUES (100)"),
            ],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn atomic_cross_worker_commit_and_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 8, ClaimSpec::All, "a").await;
        let alice = id_on_worker(1, 8, "alice");
        let bob = id_on_worker(6, 8, "bob");
        make_account(&node, &alice).await;
        make_account(&node, &bob).await;

        exec(
            &node,
            &[&alice, &bob],
            &[
                (&alice, "UPDATE account SET balance = balance - 60"),
                (&bob, "UPDATE account SET balance = balance + 60"),
            ],
        )
        .await
        .unwrap();
        assert_eq!(balance(&node, &alice).await, 40);
        assert_eq!(balance(&node, &bob).await, 160);

        // Credit lands first, then the debit fails the CHECK: all rolls back.
        let err = exec(
            &node,
            &[&alice, &bob],
            &[
                (&bob, "UPDATE account SET balance = balance + 500"),
                (&alice, "UPDATE account SET balance = balance - 500"),
            ],
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(balance(&node, &alice).await, 40);
        assert_eq!(balance(&node, &bob).await, 160);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_transfers_conserve_money() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 8, ClaimSpec::All, "a").await;
        let alice = id_on_worker(2, 8, "alice");
        let bob = id_on_worker(5, 8, "bob");
        make_account(&node, &alice).await;
        make_account(&node, &bob).await;

        let mut handles = Vec::new();
        for i in 0..100 {
            let node = node.clone();
            let (alice, bob) = (alice.clone(), bob.clone());
            handles.push(tokio::spawn(async move {
                let (from, to) = if i % 2 == 0 {
                    (alice.clone(), bob.clone())
                } else {
                    (bob.clone(), alice.clone())
                };
                let debit = format!("UPDATE account SET balance = balance - 3 -- {from}");
                let credit = format!("UPDATE account SET balance = balance + 3 -- {to}");
                let _ = exec(
                    &node,
                    &[from.as_str(), to.as_str()],
                    &[
                        (from.as_str(), debit.as_str()),
                        (to.as_str(), credit.as_str()),
                    ],
                )
                .await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let total = balance(&node, &alice).await + balance(&node, &bob).await;
        assert_eq!(total, 200);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn two_nodes_share_the_world_and_resume() {
        let dir = tempfile::tempdir().unwrap();
        let node_a = boot(dir.path(), 8, ClaimSpec::Workers((0..4).collect()), "a").await;
        let node_b = boot(dir.path(), 8, ClaimSpec::Workers((4..8).collect()), "b").await;

        let alice = id_on_worker(1, 8, "alice"); // node A territory
        let bob = id_on_worker(6, 8, "bob"); // node B territory
        make_account(&node_a, &alice).await;
        make_account(&node_b, &bob).await;

        // Cross-node transaction: submitted at A, participants span nodes.
        exec(
            &node_a,
            &[&alice, &bob],
            &[
                (&alice, "UPDATE account SET balance = balance - 60"),
                (&bob, "UPDATE account SET balance = balance + 60"),
            ],
        )
        .await
        .unwrap();

        // Both nodes agree, wherever the objects ended up living.
        assert_eq!(balance(&node_a, &alice).await, 40);
        assert_eq!(balance(&node_b, &alice).await, 40);
        assert_eq!(balance(&node_a, &bob).await, 160);
        assert_eq!(balance(&node_b, &bob).await, 160);

        // Stop the world (gracefully: leases tombstoned).
        node_a.shutdown().await;
        node_b.shutdown().await;

        // Resume with a different shape: one auto-claiming node takes all 8
        // (tombstones make the old leases claimable without health checks).
        let node_c = boot(dir.path(), 8, ClaimSpec::Auto(8), "c").await;
        assert_eq!(node_c.claimed().len(), 8);
        assert_eq!(balance(&node_c, &alice).await, 40);
        assert_eq!(balance(&node_c, &bob).await, 160);
        node_c.shutdown().await;
    }

    #[tokio::test]
    async fn auto_claim_divides_the_fleet() {
        let dir = tempfile::tempdir().unwrap();
        let node_a = boot(dir.path(), 8, ClaimSpec::Auto(4), "a").await;
        let node_b = boot(dir.path(), 8, ClaimSpec::Auto(4), "b").await;
        let mut all = node_a.claimed();
        all.extend(node_b.claimed());
        all.sort_unstable();
        assert_eq!(all, (0..8).collect::<Vec<_>>(), "no overlap, full coverage");
        node_a.shutdown().await;
        node_b.shutdown().await;
    }
}
