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
    /// Worker ranges covered by this node's block leases.
    pub claimed_here: Vec<(usize, usize)>,
    pub total_txns: u64,
    pub cross_worker_txns: u64,
    pub takes: u64,
    pub returns: u64,
    /// Boats shipped; txns/ships = group-commit amortization.
    pub ships: u64,
    pub bytes_shipped: u64,
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
    /// Lease blocks over the worker space (see ClusterMeta::blocks).
    pub blocks: usize,
    pub exceptions: HashMap<String, usize>,
    /// Lease block -> base URL of the node holding it. Block-keyed so the
    /// map stays O(blocks) however large the logical worker space is.
    pub addrs: HashMap<usize, String>,
}

impl Routing {
    pub fn addr_of_worker(&self, worker: usize) -> Option<String> {
        self.addrs
            .get(&block_of(worker, self.logical, self.blocks))
            .cloned()
    }

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
    /// Backpressure watermark: bytes of unshipped state per worker above
    /// which optimistic txns are demoted to boat-riders. Has no effect at
    /// all below the watermark.
    pub max_unshipped: u64,
    /// Container resource budgets (disk ledger, boat byte cap, activation
    /// permits). `Limits::detect()` sizes them from cgroups/env.
    pub limits: crate::limits::Limits,
}

pub const DEFAULT_MAX_UNSHIPPED: u64 = 256 * 1024 * 1024;

pub struct NodeInner {
    pub store: Arc<dyn BlobStore>,
    pub routing: RwLock<Routing>,
    /// Senders for logical workers SPAWNED on this node. Workers are
    /// virtual: owning a block claims its whole worker range, but a task
    /// only exists once a worker is first touched (Orleans-style), so a
    /// million logical workers cost nothing until used.
    pub local: RwLock<HashMap<usize, mpsc::UnboundedSender<WorkerMsg>>>,
    /// Where spawned workers keep live files (needed for lazy spawning).
    pub live_dir: PathBuf,
    /// Logical clock for tenure/visit windows (per-node; hints only).
    pub clock: AtomicU64,
    pub hysteresis: u64,
    /// Base URL peers use to reach this node.
    pub advertise: String,
    pub secret: String,
    pub api_token: Option<String>,
    pub max_unshipped: u64,
    pub limits: crate::limits::Limits,
    /// Node-wide local-file accounting; enforces the disk budget.
    pub disk: Mutex<crate::limits::DiskLedger>,
    /// Caps concurrent cold-object fetches (each holds an image in RAM).
    pub activation_permits: Arc<tokio::sync::Semaphore>,
    pub http: reqwest::Client,
    pub stats: Stats,
    /// Epochs of block leases this node holds; watched by the lease guard.
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
    /// Boats shipped. txns/ships is the group-commit amortization factor.
    pub ships: AtomicU64,
    /// Payload bytes uploaded by boats (snapshots + deltas). With delta
    /// shipping this stays proportional to what changed, not object size.
    pub bytes_shipped: AtomicU64,
}

#[derive(Serialize, Deserialize)]
struct ClusterMeta {
    logical_workers: usize,
    /// Lease granularity: the worker space is divided into this many fixed
    /// blocks, and leases are per-block. Claims, the lease guard, and
    /// tombstones are O(blocks) — never O(logical_workers) — which is what
    /// lets LOGICAL_WORKERS be a million without the metadata caring.
    /// Fixed at cluster creation; the fixed grid is also what keeps
    /// create-if-absent sound (arbitrary ranges could overlap).
    #[serde(default = "default_blocks_legacy")]
    blocks: usize,
}

fn default_blocks_legacy() -> usize {
    64 // pre-blocks clusters had per-worker leases with W=64
}

/// Workers [b*W/B, (b+1)*W/B) belong to block b.
pub fn block_of(worker: usize, logical: usize, blocks: usize) -> usize {
    worker * blocks / logical
}

pub fn block_range(block: usize, logical: usize, blocks: usize) -> (usize, usize) {
    (block * logical / blocks, (block + 1) * logical / blocks)
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

fn lease_key(block: usize, epoch: u64) -> String {
    format!("_lease/b{block}/e{epoch}.json")
}

fn tombstone_key(block: usize, epoch: u64) -> String {
    format!("_lease/b{block}/e{epoch}.released")
}

const LEASE_GUARD_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

struct LeaseState {
    epoch: u64,
    addr: String,
    released: bool,
}

async fn latest_lease(store: &dyn BlobStore, block: usize) -> anyhow::Result<Option<LeaseState>> {
    let prefix = format!("_lease/b{block}/");
    let keys = store.list(&prefix).await?;
    lease_from_keys(store, block, &keys).await
}

async fn lease_from_keys(
    store: &dyn BlobStore,
    block: usize,
    keys: &[String],
) -> anyhow::Result<Option<LeaseState>> {
    let prefix = format!("_lease/b{block}/");
    let mut best: Option<u64> = None;
    for key in keys {
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
    let Some(bytes) = store.get(&lease_key(block, epoch)).await? else {
        return Ok(None);
    };
    let lease: Lease = serde_json::from_slice(&bytes)?;
    let released = keys.contains(&tombstone_key(block, epoch));
    Ok(Some(LeaseState {
        epoch,
        addr: lease.addr,
        released,
    }))
}

/// Every block's current lease, from ONE list of the lease space —
/// boot cost is O(live blocks), not O(logical workers).
async fn load_leases(
    store: &dyn BlobStore,
    blocks: usize,
) -> anyhow::Result<HashMap<usize, LeaseState>> {
    let keys = store.list("_lease/").await?;
    let mut by_block: HashMap<usize, Vec<String>> = HashMap::new();
    for key in keys {
        if let Some(b) = key
            .strip_prefix("_lease/b")
            .and_then(|k| k.split('/').next())
            .and_then(|k| k.parse::<usize>().ok())
            && b < blocks
        {
            by_block.entry(b).or_default().push(key);
        }
    }
    let mut out = HashMap::new();
    for (block, keys) in by_block {
        if let Some(state) = lease_from_keys(store, block, &keys).await? {
            out.insert(block, state);
        }
    }
    Ok(out)
}

/// Boot a node: agree on W (create-once cluster meta), recover the commit
/// log, load placement from checkpoints, claim logical workers via epoch
/// leases, spawn worker tasks, serve HTTP (public API + /internal/rpc), and
/// start the lease guard.
pub async fn start(cfg: NodeConfig) -> anyhow::Result<Node> {
    // Cluster config is create-once: first node wins, everyone else adopts.
    let meta_bytes = serde_json::to_vec(&ClusterMeta {
        logical_workers: cfg.logical,
        blocks: cfg.logical.min(256),
    })?;
    cfg.store.create("_meta/cluster.json", &meta_bytes).await?;
    let (logical, blocks) = match cfg.store.get("_meta/cluster.json").await? {
        Some(bytes) => {
            let meta: ClusterMeta = serde_json::from_slice(&bytes)?;
            (meta.logical_workers, meta.blocks)
        }
        None => (cfg.logical, cfg.logical.min(256)),
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

    // Current block-lease holders (one list; so we can route to peers).
    let leases = load_leases(cfg.store.as_ref(), blocks).await?;
    let addrs: HashMap<usize, String> = leases
        .iter()
        .map(|(b, lease)| (*b, lease.addr.clone()))
        .collect();

    let node: Node = Arc::new(NodeInner {
        store: cfg.store.clone(),
        routing: RwLock::new(Routing {
            logical,
            blocks,
            exceptions,
            addrs,
        }),
        local: RwLock::new(HashMap::new()),
        live_dir: cfg.live_dir.clone(),
        clock: AtomicU64::new(0),
        hysteresis: cfg.hysteresis,
        advertise: advertise.clone(),
        secret: cfg.secret,
        api_token: cfg.api_token,
        max_unshipped: cfg.max_unshipped,
        limits: cfg.limits,
        disk: Mutex::new(crate::limits::DiskLedger::new(cfg.limits.disk_budget)),
        activation_permits: Arc::new(tokio::sync::Semaphore::new(cfg.limits.activation_permits)),
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

    // Claiming operates on BLOCKS: O(≤256) blob ops however big W is.
    let candidate_blocks: Vec<usize> = match &cfg.claim {
        ClaimSpec::All => (0..blocks).collect(),
        ClaimSpec::Workers(ws) => {
            let mut bs: Vec<usize> = ws
                .iter()
                .filter(|w| **w < logical)
                .map(|w| block_of(*w, logical, blocks))
                .collect();
            bs.sort_unstable();
            bs.dedup();
            bs
        }
        ClaimSpec::Auto(_) => {
            // Rotate the scan by our address hash so concurrent booters
            // start claiming from different offsets (fewer create races).
            let mut h = std::collections::hash_map::DefaultHasher::new();
            advertise.hash(&mut h);
            let start = (h.finish() % blocks as u64) as usize;
            (0..blocks).map(|i| (start + i) % blocks).collect()
        }
    };
    // Auto quota is expressed in workers; convert to blocks, rounding up.
    let quota_blocks = match &cfg.claim {
        ClaimSpec::Auto(k) => (k * blocks).div_ceil(logical).max(1),
        _ => usize::MAX,
    };

    // Health-check each foreign holder once, not once per block.
    let mut addr_alive: HashMap<String, bool> = HashMap::new();
    let mut claimed = 0usize;
    for b in candidate_blocks {
        if claimed >= quota_blocks {
            break;
        }
        let next_epoch = match leases.get(&b) {
            Some(lease) => {
                // Claimable when: cleanly released, held by our own
                // predecessor identity (rolling deploy of a named
                // instance), or the holder is dead.
                let claimable = if lease.released || lease.addr == advertise {
                    true
                } else {
                    let alive = match addr_alive.get(&lease.addr) {
                        Some(a) => *a,
                        None => {
                            let a = crate::rpc::health(&node, &lease.addr).await;
                            addr_alive.insert(lease.addr.clone(), a);
                            a
                        }
                    };
                    !alive
                };
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
            .create(&lease_key(b, next_epoch), &lease_bytes)
            .await?
        {
            continue; // lost the claim race
        }
        // No worker tasks are spawned here: workers are virtual and
        // materialize on first touch (local_sender).
        node.epochs.write().unwrap().insert(b, next_epoch);
        node.routing
            .write()
            .unwrap()
            .addrs
            .insert(b, advertise.clone());
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
            for (b, mine) in epochs {
                match latest_lease(guard_node.store.as_ref(), b).await {
                    Ok(Some(lease)) if lease.epoch > mine => {
                        guard_node.fenced.store(true, Ordering::SeqCst);
                        eprintln!(
                            "FENCED: lease for block {b} superseded (epoch {} > {mine}); fail-stopping",
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
        "node {} claimed worker ranges {:?} of {} logical workers ({} blocks)",
        advertise,
        node.claimed_ranges(),
        logical,
        blocks
    );
    Ok(node)
}

/// Get (or lazily spawn) the local worker task for a logical worker this
/// node's leases cover. Returns None if the worker isn't ours. This is what
/// makes workers virtual: a claimed-but-untouched worker costs nothing.
pub fn local_sender(node: &Node, worker: usize) -> Option<mpsc::UnboundedSender<WorkerMsg>> {
    if let Some(tx) = node.local.read().unwrap().get(&worker) {
        return Some(tx.clone());
    }
    if !node.owns_worker(worker) {
        return None;
    }
    let mut local = node.local.write().unwrap();
    if let Some(tx) = local.get(&worker) {
        return Some(tx.clone()); // lost a benign race; someone spawned it
    }
    let tx = worker::spawn(
        node.clone(),
        worker,
        node.live_dir.join(format!("w{worker}")),
    )
    .ok()?;
    local.insert(worker, tx.clone());
    Some(tx)
}

impl NodeInner {
    /// Enforce the disk budget: the ledger deletes LRU cache files itself;
    /// if live files alone still bust the budget, the heaviest worker gets
    /// a Shed request (deactivate idle clean objects -> cache -> deletable).
    pub fn enforce_disk(&self) {
        let enforcement = self.disk.lock().unwrap().enforce();
        if let Some(worker) = enforcement.shed_from_worker
            && let Some(tx) = self.local.read().unwrap().get(&worker)
        {
            let _ = tx.send(WorkerMsg::Shed);
        }
    }

    /// Worker ranges this node's block leases cover. Kept as ranges — with
    /// a million logical workers, enumerating ids would be self-harm.
    pub fn claimed_ranges(&self) -> Vec<(usize, usize)> {
        let routing = self.routing.read().unwrap();
        let mut blocks: Vec<usize> = self.epochs.read().unwrap().keys().copied().collect();
        blocks.sort_unstable();
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for b in blocks {
            let (start, end) = block_range(b, routing.logical, routing.blocks);
            match ranges.last_mut() {
                Some((_, prev_end)) if *prev_end == start => *prev_end = end,
                _ => ranges.push((start, end)),
            }
        }
        ranges
    }

    /// Total workers this node's leases cover.
    pub fn claimed_workers(&self) -> usize {
        self.claimed_ranges().iter().map(|(s, e)| e - s).sum()
    }

    /// Test/deme helper: enumerate claimed worker ids (small W only).
    pub fn claimed(&self) -> Vec<usize> {
        self.claimed_ranges()
            .iter()
            .flat_map(|&(s, e)| s..e)
            .collect()
    }

    /// Does a block lease this node holds cover this worker?
    pub fn owns_worker(&self, worker: usize) -> bool {
        let routing = self.routing.read().unwrap();
        let b = block_of(worker, routing.logical, routing.blocks);
        drop(routing);
        self.epochs.read().unwrap().contains_key(&b)
    }

    /// Graceful shutdown: flush every worker's final boat (unshipped
    /// optimistic txns become durable), stop serving, and tombstone our
    /// leases so the next claimant doesn't need a failed health check to
    /// take over. Checkpoints are already current — they're written
    /// synchronously on every ownership change.
    pub async fn shutdown(&self) {
        let senders: Vec<mpsc::UnboundedSender<WorkerMsg>> =
            { self.local.read().unwrap().values().cloned().collect() };
        let mut flushed = Vec::new();
        for tx in senders {
            let (rtx, rrx) = oneshot::channel();
            if tx.send(WorkerMsg::Shutdown { resp: rtx }).is_ok() {
                flushed.push(rrx);
            }
        }
        for f in flushed {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), f).await;
        }
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
            claimed_here: self.claimed_ranges(),
            total_txns: self.stats.total_txns.load(Ordering::Relaxed),
            cross_worker_txns: self.stats.cross_worker_txns.load(Ordering::Relaxed),
            takes: self.stats.takes.load(Ordering::Relaxed),
            returns: self.stats.returns.load(Ordering::Relaxed),
            ships: self.stats.ships.load(Ordering::Relaxed),
            bytes_shipped: self.stats.bytes_shipped.load(Ordering::Relaxed),
            per_worker,
        }
    }
}

/// Re-read a worker's lease from the blob store and refresh the address
/// cache. Used whenever routing has no (or a stale) address for a worker —
/// e.g. a node that claimed its lease after we booted.
pub async fn resolve_addr(node: &Node, worker: usize) -> Option<String> {
    let block = {
        let routing = node.routing.read().unwrap();
        block_of(worker, routing.logical, routing.blocks)
    };
    let lease = latest_lease(node.store.as_ref(), block).await.ok()??;
    node.routing
        .write()
        .unwrap()
        .addrs
        .insert(block, lease.addr.clone());
    Some(lease.addr)
}

/// Validate and route a transaction: pick the plurality owner as target,
/// dispatch to it locally or proxy to the node holding its lease.
pub async fn submit(
    node: &Node,
    objects: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
    optimistic: bool,
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
    submit_routed(node, ids, ops, read_only, optimistic).await
}

/// Routing half of submit, callable from the RPC handler (already validated).
pub async fn submit_routed(
    node: &Node,
    ids: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
    optimistic: bool,
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

    let local_tx = local_sender(node, target);
    if let Some(tx) = local_tx {
        let (rtx, rrx) = oneshot::channel();
        tx.send(WorkerMsg::Submit {
            participants: ids,
            ops,
            read_only,
            optimistic,
            resp: rtx,
        })
        .map_err(|_| ApiError::internal("worker is gone"))?;
        rrx.await
            .map_err(|_| ApiError::internal("transaction dropped"))?
    } else {
        let cached = node.routing.read().unwrap().addr_of_worker(target);
        let addr = match cached {
            Some(addr) => addr,
            None => resolve_addr(node, target).await.ok_or_else(|| {
                ApiError::internal(format!("no live node holds logical worker {target}"))
            })?,
        };
        match crate::rpc::forward_txn(node, &addr, ids.clone(), ops.clone(), read_only, optimistic)
            .await
        {
            // Transport failure: the cached address may belong to a dead
            // world. Re-read the lease and retry once at the new holder.
            Err(e) if e.message.starts_with("rpc to") => match resolve_addr(node, target).await {
                Some(fresh) if fresh != addr => {
                    crate::rpc::forward_txn(node, &fresh, ids, ops, read_only, optimistic).await
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
            max_unshipped: DEFAULT_MAX_UNSHIPPED,
            limits: crate::limits::Limits::detect(),
        })
        .await
        .unwrap()
    }

    async fn exec_mode(
        node: &Node,
        objects: &[&str],
        ops: &[(&str, &str)],
        optimistic: bool,
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
            optimistic,
        )
        .await
    }

    async fn exec(
        node: &Node,
        objects: &[&str],
        ops: &[(&str, &str)],
    ) -> Result<TxnResponse, ApiError> {
        exec_mode(node, objects, ops, false).await
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
    async fn optimistic_boats_coalesce_and_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let log = id_on_worker(1, 4, "log");
        exec(&node, &[&log], &[(&log, "CREATE TABLE t (n INTEGER)")])
            .await
            .unwrap();

        // 200 concurrent optimistic writes: acked on local apply, coalesced
        // into boats sized by whatever accumulates during each ship RTT.
        let mut handles = Vec::new();
        for _ in 0..200 {
            let node = node.clone();
            let log = log.clone();
            handles.push(tokio::spawn(async move {
                exec_mode(&node, &[&log], &[(&log, "INSERT INTO t (n) VALUES (1)")], true)
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Pessimistic barrier: acked only when its boat (and therefore
        // everything before it) is durable.
        exec(&node, &[&log], &[(&log, "INSERT INTO t (n) VALUES (2)")])
            .await
            .unwrap();

        let s = node.stats().await;
        assert!(
            s.ships < 201,
            "boats should coalesce: {} ships for 201 writes",
            s.ships
        );

        node.shutdown().await;

        // Everything acked must be durable after a graceful stop.
        let node2 = boot(dir.path(), 4, ClaimSpec::All, "b").await;
        let res = exec(&node2, &[&log], &[(&log, "SELECT COUNT(*) AS c FROM t")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 201);
        node2.shutdown().await;
    }

    #[tokio::test]
    async fn takes_wait_for_unshipped_boats() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let a = id_on_worker(0, 4, "obja");
        let b = id_on_worker(3, 4, "objb");
        exec(&node, &[&a], &[(&a, "CREATE TABLE t (n INTEGER)")])
            .await
            .unwrap();
        exec(&node, &[&b], &[(&b, "CREATE TABLE t (n INTEGER)")])
            .await
            .unwrap();

        // Optimistic write to `a`, immediately followed by a cross-worker
        // txn that migrates `a` to b's worker. The take must wait for the
        // boat, or the new owner would activate a stale snapshot.
        exec_mode(&node, &[&a], &[(&a, "INSERT INTO t (n) VALUES (41)")], true)
            .await
            .unwrap();
        let res = exec(
            &node,
            &[&a, &b],
            &[
                (&a, "SELECT COUNT(*) AS c FROM t"),
                (&b, "INSERT INTO t (n) VALUES (1)"),
            ],
        )
        .await
        .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 1, "optimistic write visible after take");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn large_objects_ship_deltas_and_survive() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let tenant = id_on_worker(1, 4, "tenant");

        // A db-per-tenant-sized object: well past DELTA_MIN_BYTES.
        let big = "x".repeat(200_000);
        submit(
            &node,
            vec![tenant.clone()],
            vec![
                Op {
                    object: tenant.clone(),
                    sql: "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)".into(),
                    params: vec![],
                },
                Op {
                    object: tenant.clone(),
                    sql: "INSERT INTO docs (body) VALUES (?1)".into(),
                    params: vec![serde_json::json!(big)],
                },
            ],
            false,
            false,
        )
        .await
        .unwrap();

        // Small writes against the big object must ship deltas, not the file.
        let before = node.stats().await.bytes_shipped;
        for i in 0..5 {
            submit(
                &node,
                vec![tenant.clone()],
                vec![Op {
                    object: tenant.clone(),
                    sql: "INSERT INTO docs (body) VALUES (?1)".into(),
                    params: vec![serde_json::json!(format!("small-{i}"))],
                }],
                false,
                false,
            )
            .await
            .unwrap();
        }
        let delta_bytes = node.stats().await.bytes_shipped - before;
        assert!(
            delta_bytes < 100_000,
            "5 small writes to a ~200KB object shipped {delta_bytes} bytes — deltas aren't working"
        );
        let deltas = node.store.list(&crate::delta::delta_prefix(&tenant)).await.unwrap();
        assert!(!deltas.is_empty(), "delta chain should exist");

        // Enough writes to force a compaction (chain > COMPACT_CHAIN).
        for i in 0..20 {
            submit(
                &node,
                vec![tenant.clone()],
                vec![Op {
                    object: tenant.clone(),
                    sql: "INSERT INTO docs (body) VALUES (?1)".into(),
                    params: vec![serde_json::json!(format!("more-{i}"))],
                }],
                false,
                false,
            )
            .await
            .unwrap();
        }
        let chain = node.store.list(&crate::delta::delta_prefix(&tenant)).await.unwrap();
        assert!(
            chain.len() <= crate::delta::COMPACT_CHAIN as usize + 1,
            "compaction should bound the chain, got {} deltas",
            chain.len()
        );

        node.shutdown().await;

        // Restart: base + delta chain reconstructs the exact state.
        let node2 = boot(dir.path(), 4, ClaimSpec::All, "b").await;
        let res = exec(
            &node2,
            &[&tenant],
            &[
                (&tenant, "SELECT COUNT(*) AS c FROM docs"),
                (&tenant, "SELECT LENGTH(body) AS l FROM docs WHERE id = 1"),
            ],
        )
        .await
        .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 26, "1 big + 5 small + 20 more");
        assert_eq!(v[1]["rows"][0]["l"], 200_000, "big row intact byte-for-byte");
        node2.shutdown().await;
    }

    /// Counts full GETs per key, to prove the commuter cache skips them.
    struct CountingStore(FsBlobStore, std::sync::Mutex<HashMap<String, u32>>);

    #[async_trait::async_trait]
    impl BlobStore for CountingStore {
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            *self.1.lock().unwrap().entry(key.to_string()).or_default() += 1;
            self.0.get(key).await
        }
        async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            self.0.put(key, bytes).await
        }
        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.0.delete(key).await
        }
        async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            self.0.list(prefix).await
        }
        async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
            self.0.create(key, bytes).await
        }
        async fn get_range(&self, key: &str, o: u64, l: u64) -> anyhow::Result<Option<Vec<u8>>> {
            self.0.get_range(key, o, l).await // ranged peeks not counted
        }
    }

    #[tokio::test]
    async fn commuter_cache_skips_base_redownload() {
        let dir = tempfile::tempdir().unwrap();
        let counting = Arc::new(CountingStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::sync::Mutex::new(HashMap::new()),
        ));
        let store: Arc<dyn BlobStore> = counting.clone();
        let node = start(NodeConfig {
            store,
            live_dir: dir.path().join("live"),
            logical: 4,
            claim: ClaimSpec::All,
            bind: "127.0.0.1:0".into(),
            advertise: None,
            hysteresis: 3, // low tenure bar so the cross-txn displaces + returns
            secret: "test".into(),
            api_token: None,
            max_unshipped: DEFAULT_MAX_UNSHIPPED,
            limits: crate::limits::Limits::detect(),
        })
        .await
        .unwrap();

        let tenant = id_on_worker(0, 4, "tenant");
        let partner = id_on_worker(3, 4, "partner");
        let big = "x".repeat(200_000);
        submit(
            &node,
            vec![tenant.clone()],
            vec![
                Op {
                    object: tenant.clone(),
                    sql: "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)".into(),
                    params: vec![],
                },
                Op {
                    object: tenant.clone(),
                    sql: "INSERT INTO docs (body) VALUES (?1)".into(),
                    params: vec![serde_json::json!(big)],
                },
            ],
            false,
            false,
        )
        .await
        .unwrap();
        exec(&node, &[&partner], &[(&partner, "CREATE TABLE t (n INTEGER)")])
            .await
            .unwrap();
        // Build tenure past hysteresis so displacement triggers a return.
        for _ in 0..5 {
            exec(&node, &[&tenant], &[(&tenant, "INSERT INTO docs (body) VALUES ('x')")])
                .await
                .unwrap();
        }

        let base_key = crate::object::object_key(&tenant);
        let gets_before = *counting.1.lock().unwrap().get(&base_key).unwrap_or(&0);

        // Cross-worker txn: tenant is taken to partner's worker (activates
        // there — a real download), then hysteresis sends it home, where the
        // commuter cache + delta chain must avoid re-downloading the base.
        exec(
            &node,
            &[&tenant, &partner],
            &[
                (&tenant, "INSERT INTO docs (body) VALUES ('trip')"),
                (&partner, "INSERT INTO t (n) VALUES (1)"),
            ],
        )
        .await
        .unwrap();
        // Touch it back home (forces re-activation at the home worker).
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let owner = node.routing.read().unwrap().owner_of(&tenant);
            if owner == 0 {
                break;
            }
        }
        exec(&node, &[&tenant], &[(&tenant, "INSERT INTO docs (body) VALUES ('home')")])
            .await
            .unwrap();

        let gets_after = *counting.1.lock().unwrap().get(&base_key).unwrap_or(&0);
        let full_downloads = gets_after - gets_before;
        assert!(
            full_downloads <= 1,
            "round trip should cost at most ONE full base download (the take); got {full_downloads}"
        );

        // And the data is intact everywhere it traveled.
        let res = exec(&node, &[&tenant], &[(&tenant, "SELECT COUNT(*) AS c FROM docs")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 8); // 1 big + 5 tenure + trip + home
        node.shutdown().await;
    }

    #[tokio::test]
    async fn boats_split_along_txn_groups_under_byte_cap() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());
        let mut limits = crate::limits::Limits::derive(4 << 30, 8 << 30);
        limits.max_boat_bytes = 1; // every component alone busts the cap
        let node = start(NodeConfig {
            store,
            live_dir: dir.path().join("live"),
            logical: 4,
            claim: ClaimSpec::All,
            bind: "127.0.0.1:0".into(),
            advertise: None,
            hysteresis: 200,
            secret: "test".into(),
            api_token: None,
            max_unshipped: DEFAULT_MAX_UNSHIPPED,
            limits,
        })
        .await
        .unwrap();

        // Force everything onto ONE worker so all txns share a boat queue:
        // one multi-object txn (an atomic group) + independent singles.
        let a = id_on_worker(2, 4, "ga");
        let b = id_on_worker(2, 4, "gb");
        let c = id_on_worker(2, 4, "gc");
        for id in [&a, &b, &c] {
            exec(&node, &[id], &[(id, "CREATE TABLE t (n INTEGER)")])
                .await
                .unwrap();
        }
        let ships_before = node.stats().await.ships;
        // Concurrent: a cross-object txn {a,b} and a single {c}. With a
        // 1-byte cap they must ship as separate boats — but {a,b} must
        // never be split across two.
        let n1 = node.clone();
        let (a1, b1) = (a.clone(), b.clone());
        let t1 = tokio::spawn(async move {
            exec(
                &n1,
                &[&a1, &b1],
                &[
                    (&a1, "INSERT INTO t (n) VALUES (1)"),
                    (&b1, "INSERT INTO t (n) VALUES (1)"),
                ],
            )
            .await
            .unwrap();
        });
        let n2 = node.clone();
        let c2 = c.clone();
        let t2 = tokio::spawn(async move {
            exec(&n2, &[&c2], &[(&c2, "INSERT INTO t (n) VALUES (1)")])
                .await
                .unwrap();
        });
        t1.await.unwrap();
        t2.await.unwrap();
        node.shutdown().await;
        let ships = node.stats().await.ships - ships_before;
        assert!(ships >= 1, "shipped something");

        // Restart: everything acked must be durable, and the {a,b} txn
        // must be atomically present.
        let node2 = boot(dir.path(), 4, ClaimSpec::All, "b").await;
        for id in [&a, &b, &c] {
            let res = exec(&node2, &[id], &[(id, "SELECT COUNT(*) AS n FROM t")])
                .await
                .unwrap();
            let v = serde_json::to_value(&res.results).unwrap();
            assert_eq!(v[0]["rows"][0]["n"], 1, "{id} durable");
        }
        node2.shutdown().await;
    }

    #[tokio::test]
    async fn disk_pressure_sheds_idle_objects() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());
        let mut limits = crate::limits::Limits::derive(4 << 30, 8 << 30);
        limits.disk_budget = 40 * 1024; // ~2 small SQLite files
        let node = start(NodeConfig {
            store,
            live_dir: dir.path().join("live"),
            logical: 4,
            claim: ClaimSpec::All,
            bind: "127.0.0.1:0".into(),
            advertise: None,
            hysteresis: 200,
            secret: "test".into(),
            api_token: None,
            max_unshipped: DEFAULT_MAX_UNSHIPPED,
            limits,
        })
        .await
        .unwrap();

        // Ten objects, each a ~12-16KB SQLite file: way over a 40KB budget.
        for i in 0..10 {
            let id = format!("obj{i}");
            exec(
                &node,
                &[&id],
                &[
                    (&id, "CREATE TABLE t (n INTEGER)"),
                    (&id, "INSERT INTO t (n) VALUES (1)"),
                ],
            )
            .await
            .unwrap();
        }
        // Give shed/enforce cycles a moment to settle.
        let mut used = u64::MAX;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            used = node.disk.lock().unwrap().used();
            if used <= 40 * 1024 {
                break;
            }
        }
        assert!(
            used <= 40 * 1024,
            "ledger should shed+reclaim to budget, still using {used}"
        );

        // Shedded objects reactivate correctly on next touch.
        for i in 0..10 {
            let id = format!("obj{i}");
            let res = exec(&node, &[&id], &[(&id, "SELECT COUNT(*) AS n FROM t")])
                .await
                .unwrap();
            let v = serde_json::to_value(&res.results).unwrap();
            assert_eq!(v[0]["rows"][0]["n"], 1, "{id} intact after shed cycle");
        }
        node.shutdown().await;
    }

    #[tokio::test]
    async fn one_million_logical_workers_costs_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let boot_start = std::time::Instant::now();
        // Two nodes over a million-worker space: leases are per-block
        // (≤256), workers are virtual (spawned on first touch), so this
        // boots in milliseconds instead of hours.
        let node_a = boot(
            dir.path(),
            1_000_000,
            ClaimSpec::Auto(500_000),
            "a",
        )
        .await;
        let node_b = boot(dir.path(), 1_000_000, ClaimSpec::Auto(500_000), "b").await;
        let boot_elapsed = boot_start.elapsed();
        assert!(
            boot_elapsed.as_secs() < 20,
            "boot took {boot_elapsed:?}; per-worker costs are back"
        );
        assert_eq!(
            node_a.claimed_workers() + node_b.claimed_workers(),
            1_000_000,
            "the fleet covers the whole space"
        );

        // Real transactions across the space (and across nodes) still work;
        // only the touched workers ever materialize.
        let alice = id_on_worker(3, 1_000_000, "alice");
        let bob = id_on_worker(999_777, 1_000_000, "bob");
        make_account(&node_a, &alice).await;
        make_account(&node_a, &bob).await;
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
        assert_eq!(balance(&node_b, &alice).await, 40);
        assert_eq!(balance(&node_b, &bob).await, 160);

        let spawned = node_a.local.read().unwrap().len() + node_b.local.read().unwrap().len();
        assert!(
            spawned <= 8,
            "only touched workers should materialize, got {spawned}"
        );
        node_a.shutdown().await;
        node_b.shutdown().await;
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
