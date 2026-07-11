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
use crate::Map;
use crate::store::BlobStore;
use crate::worker::{self, WorkerMsg};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    /// Long-poll replies only: hash of `results`, fed back as `baseline` in
    /// the next poll for gapless change detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
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
    /// Handoff generation this deed was cut at: strictly greater than any
    /// generation the giver ever held or granted for this object. A deed
    /// at gen <= what a worker already knows is stale and must be refused.
    #[serde(default)]
    pub generation: u64,
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
    /// Long-polls currently parked on this worker's objects.
    pub parked_polls: usize,
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
    pub exceptions: Map<String, usize>,
    /// Lease block -> base URL of the node holding it. Block-keyed so the
    /// map stays O(blocks) however large the logical worker space is.
    pub addrs: Map<usize, String>,
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

/// FNV-1a 64: deterministic AND portable — the Worker router implements
/// these same ten lines in TypeScript to send requests straight to the
/// owning instance, deleting the inter-instance hairpin for any object
/// still at its hash-default home.
pub fn default_worker(object: &str, logical: usize) -> usize {
    (fnv1a(object.as_bytes()) % logical as u64) as usize
}

pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
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
    /// Fencing TTL. A commit may only pass the commit point within this
    /// long of a verified lease (else it re-verifies inline), and a node
    /// taking over a NON-tombstoned lease waits this long before its first
    /// write — so a paused-then-resumed old holder always lands its final
    /// commits before the new holder's first read. Closes the split-brain
    /// window under bounded-clock-rate assumptions.
    pub fence_ttl: std::time::Duration,
    /// How this node reaches peers. None = HTTP (production). The
    /// simulator injects an in-process transport it can delay, drop, and
    /// partition.
    pub transport: Option<Arc<dyn crate::rpc::Transport>>,
    /// Serve the public HTTP API. The simulator turns this off: no
    /// sockets means no OS nondeterminism.
    pub serve_http: bool,
    /// What losing a lease means. True (production): process::exit —
    /// fail-stop is the correct response to a superseded epoch. False
    /// (simulation): crash only this node, the sim must keep running.
    pub exit_on_fence: bool,
    /// Check the wall clock (in addition to the monotonic one) at the
    /// fencing recency gate. True in production — it catches system
    /// suspends the monotonic clock sleeps through. False under the
    /// simulator, where tokio's paused clock is the only real one and a
    /// CPU-contended host would otherwise fence healthy virtual nodes
    /// (found as a once-per-sweep heisen-failure: wall time outran
    /// FENCE_TTL while virtual time was fine).
    pub wall_fence: bool,
}

impl NodeConfig {
    /// Production-shaped defaults; override what the situation needs.
    pub fn new(store: Arc<dyn BlobStore>, live_dir: impl Into<PathBuf>) -> Self {
        Self {
            store,
            live_dir: live_dir.into(),
            logical: 4096,
            claim: ClaimSpec::All,
            bind: "127.0.0.1:0".into(),
            advertise: None,
            hysteresis: 200,
            secret: "dev-secret".into(),
            api_token: None,
            max_unshipped: DEFAULT_MAX_UNSHIPPED,
            limits: crate::limits::Limits::detect(),
            fence_ttl: std::time::Duration::from_secs(10),
            transport: None,
            serve_http: true,
            exit_on_fence: true,
            wall_fence: true,
        }
    }
}

pub const DEFAULT_MAX_UNSHIPPED: u64 = 256 * 1024 * 1024;

pub struct NodeInner {
    pub store: Arc<dyn BlobStore>,
    pub routing: RwLock<Routing>,
    /// Senders for logical workers SPAWNED on this node. Workers are
    /// virtual: owning a block claims its whole worker range, but a task
    /// only exists once a worker is first touched (Orleans-style), so a
    /// million logical workers cost nothing until used.
    pub local: RwLock<Map<usize, mpsc::UnboundedSender<WorkerMsg>>>,
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
    /// Per-worker transit maps loaded from checkpoints at boot, so a
    /// rebooted giver still remembers which handoffs were in flight.
    pub boot_transit: Mutex<Map<usize, Map<String, (usize, u64)>>>,
    /// Per-worker owned sets (with generations) reconciled at boot; each
    /// worker task consumes its slice when it spawns.
    pub boot_owned: Mutex<Map<usize, Map<String, u64>>>,
    /// Caps concurrent cold-object fetches (each holds an image in RAM).
    pub activation_permits: Arc<tokio::sync::Semaphore>,
    /// How this node reaches peers (HTTP in production, in-process under
    /// the simulator).
    pub transport: Arc<dyn crate::rpc::Transport>,
    /// Short name derived from the advertise address plus this boot's
    /// lease epoch; prefixes staging ids so they are unique across the
    /// cluster AND across restarts (a restarted node reusing ids let one
    /// generation's commit records clobber another's — simulator, seed 3),
    /// while staying deterministic under simulation (uuids were not).
    pub tag: std::sync::OnceLock<String>,
    /// Staging-id sequence for boats.
    pub ship_seq: AtomicU64,
    pub stats: Stats,
    /// Epochs of block leases this node holds; watched by the lease guard.
    pub epochs: RwLock<Map<usize, u64>>,
    /// Set by the lease guard just before fail-stop; checked at the commit
    /// point as a last line of defense.
    pub fenced: AtomicBool,
    pub fence_ttl: std::time::Duration,
    /// When our leases were last verified current, double-stamped: the
    /// monotonic clock catches process pauses (it keeps ticking while we
    /// don't); the wall clock catches system suspend (where monotonic
    /// may sleep too). Stale on EITHER axis forces re-verification.
    /// (tokio Instants, so the simulator's virtual clock governs them.)
    pub verified: Mutex<(tokio::time::Instant, std::time::SystemTime)>,
    /// No commit may pass before this instant: set to claim time + TTL when
    /// taking over a non-tombstoned lease, giving a paused predecessor's
    /// recency gate time to expire first.
    pub earliest_write: Mutex<tokio::time::Instant>,
    /// Fail-stop policy: exit the process (production) or crash just this
    /// node (simulation).
    exit_on_fence: bool,
    wall_fence: bool,
    /// Every task this node spawned: HTTP server, lease guard, workers,
    /// boats, takes. crash() aborts them all — kill -9 with a scalpel.
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

/// Which lease block covers a worker. This mapping names the lease keys in
/// the blob store, so it is frozen — existing clusters depend on it.
pub fn block_of(worker: usize, logical: usize, blocks: usize) -> usize {
    worker * blocks / logical
}

/// The worker range a block covers: exactly the workers block_of sends to
/// it. The ceilings matter when blocks don't divide W evenly — plain floor
/// division here would disagree with block_of at the seams (block_of(3906)
/// is 0 for W=1M, B=256; a floored range would put 3906 in block 1).
pub fn block_range(block: usize, logical: usize, blocks: usize) -> (usize, usize) {
    let start = (block * logical).div_ceil(blocks);
    let end = ((block + 1) * logical).div_ceil(blocks);
    (start, end)
}

#[derive(Serialize, Deserialize, Default)]
#[serde(from = "CheckpointWire")]
pub(crate) struct Checkpoint {
    /// object -> the handoff generation at which this worker admitted it.
    /// Generations strictly increase with every release of an object, so
    /// "highest gen wins" arbitrates every stale-claim question — dual
    /// claims at boot, late adopts, twice-spent deeds — with an integer
    /// compare instead of heuristics. (The simulator burned down every
    /// cheaper scheme: unversioned claims left cycles of stale transit
    /// markers that a chain-follower resolved arbitrarily — and wrongly.)
    pub(crate) owned: Map<String, u64>,
    /// Objects this worker released whose new owner may not have durably
    /// claimed them yet: object -> (destination, generation). This is what
    /// makes the transfer gap VISIBLE — without it, an object mid-handoff
    /// looks durably unclaimed and the hash-default fallback would
    /// manufacture a second owner.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub(crate) transit: Map<String, (usize, u64)>,
}

/// Wire-compat shim: checkpoints written before handoff generations stored
/// `owned` as a bare array and `transit` as object -> worker. Silently
/// ignoring them on upgrade would be amnesia — the exact failure mode the
/// simulator punished — so legacy entries deserialize at generation 1
/// (any post-upgrade handoff outbids them).
#[derive(Deserialize)]
struct CheckpointWire {
    #[serde(default)]
    owned: OwnedWire,
    #[serde(default)]
    transit: TransitWire,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OwnedWire {
    Gens(Map<String, u64>),
    Legacy(Vec<String>),
}

impl Default for OwnedWire {
    fn default() -> Self {
        OwnedWire::Gens(Map::default())
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TransitWire {
    Gens(Map<String, (usize, u64)>),
    Legacy(Map<String, usize>),
}

impl Default for TransitWire {
    fn default() -> Self {
        TransitWire::Gens(Map::default())
    }
}

impl From<CheckpointWire> for Checkpoint {
    fn from(w: CheckpointWire) -> Self {
        Checkpoint {
            owned: match w.owned {
                OwnedWire::Gens(m) => m,
                OwnedWire::Legacy(v) => v.into_iter().map(|o| (o, 1)).collect(),
            },
            transit: match w.transit {
                TransitWire::Gens(m) => m,
                TransitWire::Legacy(m) => m.into_iter().map(|(o, w)| (o, (w, 1))).collect(),
            },
        }
    }
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
) -> anyhow::Result<Map<usize, LeaseState>> {
    let keys = store.list("_lease/").await?;
    let mut by_block: Map<usize, Vec<String>> = Map::default();
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
    let mut out = Map::default();
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
    let mut exceptions: Map<String, usize> = Map::default();
    let mut boot_transit: Map<usize, Map<String, (usize, u64)>> = Map::default();
    let mut boot_owned: Map<usize, Map<String, u64>> = Map::default();
    let mut claim_gens: Map<String, (usize, u64)> = Map::default();
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
        if !cp.transit.is_empty() {
            boot_transit.insert(w, cp.transit.clone());
        }
        for (object, generation) in cp.owned {
            match claim_gens.get(&object) {
                Some(&(prev_w, prev_g)) => {
                    // Dual claim: the higher generation is the later
                    // handoff, i.e. the real owner. (Ties can't happen —
                    // one release, one generation — but break them
                    // deterministically anyway.)
                    let keep = if generation > prev_g || (generation == prev_g && w < prev_w) {
                        claim_gens.insert(object.clone(), (w, generation));
                        w
                    } else {
                        prev_w
                    };
                    println!(
                        "boot: {object} dual-claimed by w{prev_w}@g{prev_g} and w{w}@g{generation}; keeping w{keep}"
                    );
                }
                None => {
                    claim_gens.insert(object.clone(), (w, generation));
                }
            }
        }
    }
    for (object, (w, generation)) in &claim_gens {
        boot_owned
            .entry(*w)
            .or_default()
            .insert(object.clone(), *generation);
        if default_worker(object, logical) != *w {
            exceptions.insert(object.clone(), *w);
        }
    }

    // No HTTP means no sockets at all (the simulator's world); it also
    // means the advertise address must be given, since there is no bound
    // port to derive one from.
    let listener = if cfg.serve_http {
        Some(tokio::net::TcpListener::bind(&cfg.bind).await?)
    } else {
        None
    };
    let advertise = match (cfg.advertise.clone(), &listener) {
        (Some(a), _) => a,
        (None, Some(l)) => format!("http://{}", l.local_addr()?),
        (None, None) => anyhow::bail!("advertise is required when not serving HTTP"),
    };

    // Current block-lease holders (one list; so we can route to peers).
    let leases = load_leases(cfg.store.as_ref(), blocks).await?;
    let addrs: Map<usize, String> = leases
        .iter()
        .map(|(b, lease)| (*b, lease.addr.clone()))
        .collect();

    let secret = cfg.secret;
    let node: Node = Arc::new(NodeInner {
        store: cfg.store.clone(),
        routing: RwLock::new(Routing {
            logical,
            blocks,
            exceptions,
            addrs,
        }),
        local: RwLock::new(Map::default()),
        live_dir: cfg.live_dir.clone(),
        clock: AtomicU64::new(0),
        hysteresis: cfg.hysteresis,
        advertise: advertise.clone(),
        secret: secret.clone(),
        api_token: cfg.api_token,
        max_unshipped: cfg.max_unshipped,
        limits: cfg.limits,
        disk: Mutex::new(crate::limits::DiskLedger::new(cfg.limits.disk_budget)),
        boot_transit: Mutex::new(boot_transit),
        boot_owned: Mutex::new(boot_owned),
        activation_permits: Arc::new(tokio::sync::Semaphore::new(cfg.limits.activation_permits)),
        transport: match cfg.transport {
            Some(t) => t,
            None => Arc::new(crate::rpc::Http::new(secret.clone())),
        },
        tag: std::sync::OnceLock::new(),
        ship_seq: AtomicU64::new(0),
        stats: Stats::default(),
        epochs: RwLock::new(Map::default()),
        fenced: AtomicBool::new(false),
        fence_ttl: cfg.fence_ttl,
        verified: Mutex::new((tokio::time::Instant::now(), std::time::SystemTime::now())),
        earliest_write: Mutex::new(tokio::time::Instant::now()),
        exit_on_fence: cfg.exit_on_fence,
        wall_fence: cfg.wall_fence,
        tasks: Mutex::new(Vec::new()),
    });

    // The HTTP server must be up before claiming: peers health-check us,
    // and a claimed-but-unreachable node reads as dead.
    if let Some(listener) = listener {
        let app = crate::api::router(node.clone());
        node.spawn_tracked(async move {
            let _ = axum::serve(listener, app).await;
        });
    }

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
    let mut addr_alive: Map<String, bool> = Map::default();
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
                // Taking over WITHOUT a tombstone means the holder may be
                // paused, not dead: wait out its fencing TTL before our
                // first write, so its last stale commits (if any) land
                // strictly before we read or write anything.
                if !lease.released {
                    let mut ew = node.earliest_write.lock().unwrap();
                    *ew = (*ew).max(tokio::time::Instant::now() + cfg.fence_ttl);
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

    // Name this boot: address hash + the highest epoch we just claimed.
    // Epochs only go up, so two boots of one node can never share a tag.
    let boot_epoch = node.epochs.read().unwrap().values().max().copied().unwrap_or(0);
    let _ = node.tag.set(format!(
        "{:08x}e{boot_epoch}",
        fnv1a(advertise.as_bytes()) as u32
    ));

    // Claiming just created/verified our leases: stamp it.
    *node.verified.lock().unwrap() = (tokio::time::Instant::now(), std::time::SystemTime::now());

    // Lease guard: fail-stop if any of our epochs gets superseded. Losing a
    // lease means another node is now the writer for that worker; continuing
    // would violate single-writer. Exiting the whole process is the correct
    // (fail-stop) response, not a recoverable error. Each clean sweep also
    // refreshes the recency stamps the commit gate checks.
    let guard_node = node.clone();
    node.spawn_tracked(async move {
        loop {
            tokio::time::sleep(LEASE_GUARD_INTERVAL).await;
            verify_leases(&guard_node).await;
        }
    });

    println!(
        "node {} claimed worker ranges {:?} of {} logical workers ({} blocks)",
        advertise,
        node.claimed_ranges(),
        logical,
        blocks
    );
    Ok(node)
}

/// Re-check every lease this node holds. Superseded -> fail-stop (the flag
/// is set first so in-flight commits refuse the commit point). All current
/// -> refresh the recency stamps the commit gate relies on.
pub async fn verify_leases(node: &Node) -> bool {
    let epochs: Vec<(usize, u64)> = {
        let e = node.epochs.read().unwrap();
        e.iter().map(|(b, e)| (*b, *e)).collect()
    };
    // ONE list covers every block (per-block lists — and later per-block
    // GETs — were both the guard's R2 bill and multi-second latency
    // outliers at the commit gate). Epochs are parsed straight from the
    // key names: a lease key only exists with its content fully written
    // (create-if-absent), so no blob reads are needed to compare.
    let Ok(keys) = node.store.list("_lease/").await else {
        return true; // transient store error: keep the old stamp, retry later
    };
    let mut max_epoch: Map<usize, u64> = Map::default();
    for key in &keys {
        // _lease/b<block>/e<epoch>.json (tombstones end .released)
        let Some(rest) = key.strip_prefix("_lease/b") else {
            continue;
        };
        let Some((block, entry)) = rest.split_once('/') else {
            continue;
        };
        let (Ok(block), Some(epoch)) = (
            block.parse::<usize>(),
            entry
                .strip_prefix('e')
                .and_then(|e| e.strip_suffix(".json"))
                .and_then(|e| e.parse::<u64>().ok()),
        ) else {
            continue;
        };
        let best = max_epoch.entry(block).or_insert(epoch);
        *best = (*best).max(epoch);
    }
    for (b, mine) in epochs {
        let latest = max_epoch.get(&b).copied().unwrap_or(0);
        if latest > mine {
            fail_stop(
                node,
                &format!("lease for block {b} superseded (epoch {latest} > {mine})"),
            );
            return false;
        }
    }
    *node.verified.lock().unwrap() = (tokio::time::Instant::now(), std::time::SystemTime::now());
    true
}

/// Are the recency stamps too old to trust for a commit? Stale on either
/// clock: monotonic catches process pauses, wall catches system suspend.
pub fn lease_stale(node: &Node) -> bool {
    let (mono, wall) = *node.verified.lock().unwrap();
    if mono.elapsed() > node.fence_ttl {
        return true;
    }
    if !node.wall_fence {
        return false;
    }
    std::time::SystemTime::now()
        .duration_since(wall)
        .map(|d| d > node.fence_ttl)
        .unwrap_or(false) // wall clock went backwards (NTP): trust monotonic
}

fn fail_stop(node: &Node, msg: &str) {
    node.fenced.store(true, Ordering::SeqCst);
    eprintln!("FENCED: {msg}; fail-stopping");
    if node.exit_on_fence {
        // Under test, exiting would take the whole test runner with us;
        // the fenced flag (which the commit gate honors) stands in.
        #[cfg(not(test))]
        std::process::exit(1);
    } else {
        // Simulation: this node dies, the world keeps turning.
        node.crash();
    }
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
    /// Spawn a task that dies with this node. Everything a node starts —
    /// server, guard, workers, boats, takes — goes through here so that
    /// crash() can take it all down the way the kernel would.
    pub fn spawn_tracked<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|t| !t.is_finished()); // don't hoard the dead
        tasks.push(tokio::spawn(fut));
    }

    /// kill -9: abort every task without flushing, tombstoning, or saying
    /// goodbye. Unshipped optimistic writes die (their contract), leases
    /// stay un-tombstoned (successors must health-check and wait out the
    /// fence TTL), and armed wills go down with the ship — exactly the
    /// behavior the simulator exists to interrogate.
    pub fn crash(&self) {
        self.fenced.store(true, Ordering::SeqCst);
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        self.local.write().unwrap().clear();
    }

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

    /// Deadlock forensics: every live worker's queues and parked txns.
    pub async fn debug_dump(&self) -> String {
        let senders: Vec<(usize, mpsc::UnboundedSender<WorkerMsg>)> = {
            let local = self.local.read().unwrap();
            local.iter().map(|(w, tx)| (*w, tx.clone())).collect()
        };
        let mut out = format!("node {}:\n", self.advertise);
        for (_w, tx) in senders {
            let (rtx, rrx) = oneshot::channel();
            if tx.send(WorkerMsg::Dump { resp: rtx }).is_ok()
                && let Ok(dump) = rrx.await
            {
                out.push_str(&dump);
            }
        }
        out
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
                && let Ok((txns, owned_exceptions, parked_polls)) = rrx.await
            {
                per_worker.push(WorkerStat {
                    worker: w,
                    txns,
                    owned_exceptions,
                    parked_polls,
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
    submit_as(node, None, objects, ops, read_only, optimistic).await
}

/// submit with a capability attached: the txn runs under SQLite's
/// authorizer at the owning worker, so every engine action (through CTEs
/// and trigger cascades) must be covered by the token's verbs.
pub async fn submit_as(
    node: &Node,
    cap: Option<Arc<crate::grants::Capability>>,
    objects: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
    optimistic: bool,
) -> Result<TxnResponse, ApiError> {
    let ids = validate_txn(objects, &ops)?;
    node.clock.fetch_add(1, Ordering::Relaxed);
    node.stats.total_txns.fetch_add(1, Ordering::Relaxed);
    submit_routed(node, cap, ids, ops, read_only, optimistic).await
}

/// Shared txn validation: sorted, deduped participant ids, every op's
/// object declared. Used by submit and by will registration (which must
/// reject a bad will while the client can still hear about it).
pub fn validate_txn(objects: Vec<String>, ops: &[Op]) -> Result<Vec<String>, ApiError> {
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
    for op in ops {
        if ids.binary_search(&op.object).is_err() {
            return Err(ApiError::bad_request(format!(
                "op touches undeclared object {:?} — declare it in `objects`",
                op.object
            )));
        }
    }
    Ok(ids)
}

/// Routing half of submit, callable from the RPC handler (already validated).
pub async fn submit_routed(
    node: &Node,
    cap: Option<Arc<crate::grants::Capability>>,
    ids: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
    optimistic: bool,
) -> Result<TxnResponse, ApiError> {
    // Plurality owner wins; ties break toward the less-loaded worker
    // (pressure), then lowest id (determinism). Two-object cross txns are
    // always 1-1 ties, so the load tie-break is the main balancing force.
    let target = plurality_target(node, &ids);
    let local_tx = local_sender(node, target);
    if let Some(tx) = local_tx {
        let (rtx, rrx) = oneshot::channel();
        tx.send(WorkerMsg::Submit {
            participants: ids,
            ops,
            read_only,
            poll: None,
            cap,
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
        match crate::rpc::forward_txn(
            node,
            &addr,
            cap.as_deref().cloned(),
            ids.clone(),
            ops.clone(),
            read_only,
            optimistic,
        )
        .await
        {
            // Transport failure: the cached address may belong to a dead
            // world. Re-read the lease and retry once at the new holder.
            Err(e) if e.message.starts_with("rpc to") => match resolve_addr(node, target).await {
                Some(fresh) if fresh != addr => {
                    crate::rpc::forward_txn(
                        node,
                        &fresh,
                        cap.as_deref().cloned(),
                        ids,
                        ops,
                        read_only,
                        optimistic,
                    )
                    .await
                }
                _ => Err(e),
            },
            other => other,
        }
    }
}

fn plurality_target(node: &Node, ids: &[String]) -> usize {
    let routing = node.routing.read().unwrap();
    let mut votes: Map<usize, usize> = Map::default();
    for id in ids {
        *votes.entry(routing.owner_of(id)).or_default() += 1;
    }
    votes
        .into_iter()
        .min_by_key(|&(w, count)| (std::cmp::Reverse(count), routing.exception_load(w), w))
        .map(|(w, _)| w)
        .expect("validated txns have participants")
}

/// A transaction that arrived over the RPC never forwards again: one hop,
/// then THIS node coordinates. Any worker can (takes pull the remote
/// participants in, healing routing hints as they chase). Re-forwarding
/// looked harmless but is a live loop: two nodes with mutually stale
/// exception hints bounce the txn forever, each hop nesting inside the
/// last — the simulator caught it on its very first seed.
pub async fn submit_received(
    node: &Node,
    cap: Option<Arc<crate::grants::Capability>>,
    ids: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
    optimistic: bool,
) -> Result<TxnResponse, ApiError> {
    let preferred = plurality_target(node, &ids);
    let tx = local_sender(node, preferred).or_else(|| {
        // The plurality owner isn't ours (the sender's hint was stale):
        // coordinate at our lowest claimed worker, deterministically.
        let block = node.epochs.read().unwrap().keys().min().copied()?;
        let (start, _) = {
            let routing = node.routing.read().unwrap();
            block_range(block, routing.logical, routing.blocks)
        };
        local_sender(node, start)
    });
    let Some(tx) = tx else {
        return Err(ApiError::internal(
            "no local worker to coordinate a received transaction",
        ));
    };
    let (rtx, rrx) = oneshot::channel();
    tx.send(WorkerMsg::Submit {
        participants: ids,
        ops,
        read_only,
        poll: None,
        cap,
        optimistic,
        resp: rtx,
    })
    .map_err(|_| ApiError::internal("worker is gone"))?;
    rrx.await
        .map_err(|_| ApiError::internal("transaction dropped"))?
}

/// Long-poll a read-only query on one object: the reply arrives when the
/// condition holds — non-empty results, or (with `baseline`) a result hash
/// different from the one the client last saw. The initial check rides the
/// object's txn queue, so parking is gapless: no write can slip between
/// "checked: empty" and "re-checked on every later write".
///
/// Polls are node-local (a parked reply slot can't ride the HTTP RPC): the
/// caller must sit on the owning node. WS clients pin with /ws?for=; the
/// edge router sends /objects/{id}/* to the owning instance already.
#[allow(clippy::too_many_arguments)]
pub async fn submit_poll(
    node: &Node,
    object: String,
    sql: String,
    params: Vec<Value>,
    durable: bool,
    baseline: Option<String>,
    conn: u64,
    frame: u64,
) -> Result<TxnResponse, ApiError> {
    if !crate::object::valid_id(&object) {
        return Err(ApiError::bad_request(format!(
            "invalid object id: {object:?}"
        )));
    }
    let target = node.routing.read().unwrap().owner_of(&object);
    let Some(tx) = local_sender(node, target) else {
        return Err(ApiError::bad_request(format!(
            "this node does not own {object:?} — poll the owning instance (WS: /ws?for={object})"
        )));
    };
    node.clock.fetch_add(1, Ordering::Relaxed);
    let (rtx, rrx) = oneshot::channel();
    tx.send(WorkerMsg::Submit {
        participants: vec![object.clone()],
        ops: vec![Op {
            object,
            sql,
            params,
        }],
        read_only: true,
        poll: Some(worker::PollOpts {
            durable,
            baseline,
            conn,
            frame,
        }),
        cap: None,
        optimistic: false,
        resp: rtx,
    })
    .map_err(|_| ApiError::internal("worker is gone"))?;
    rrx.await
        .map_err(|_| ApiError::internal("poll canceled"))?
}

/// Abandon a parked poll. Fire-and-forget: a missing worker means the poll
/// is already gone, and an already-fired poll is a no-op.
pub fn cancel_poll(node: &Node, object: &str, conn: u64, frame: u64) {
    let target = node.routing.read().unwrap().owner_of(object);
    if let Some(tx) = local_sender(node, target) {
        let _ = tx.send(WorkerMsg::CancelPoll {
            object: object.to_string(),
            conn,
            frame,
        });
    }
}

/// The durable truth about who claims an object, from the checkpoint set.
/// Placement hints can go stale (transfers whose parties died mid-
/// handshake leave cycles of lies); the checkpoints cannot — every
/// transfer renounces durably (with a transit marker) before granting.
/// Rare-path only: one LIST plus one GET per checkpointed worker.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Claim {
    /// A worker's checkpoint lists it as owned (worker, generation).
    Owned(usize, u64),
    /// Mid-handoff: the giver's transit marker names the destination
    /// (destination, generation).
    Transit(usize, u64),
    /// No durable claim anywhere: the hash-default home may adopt.
    Unclaimed,
}

pub async fn durable_claim(store: &dyn BlobStore, object: &str) -> Claim {
    let Ok(keys) = store.list("_worker/").await else {
        // Can't read the truth: claim nothing, adopt nothing.
        return Claim::Transit(usize::MAX, u64::MAX);
    };
    // Highest generation wins; at equal generation an admitted claim
    // (owned) supersedes the giver's marker for the same handoff. Stale
    // markers from earlier hops lose on generation alone — no chain
    // walking, no cycles, no heuristics.
    let mut best: Option<Claim> = None;
    let rank = |c: &Claim| match *c {
        Claim::Owned(_, g) => (g, 1u8),
        Claim::Transit(_, g) => (g, 0u8),
        Claim::Unclaimed => (0, 0),
    };
    for key in keys {
        let Some(w) = key
            .strip_prefix("_worker/")
            .and_then(|k| k.strip_suffix(".json"))
            .and_then(|k| k.parse::<usize>().ok())
        else {
            continue;
        };
        let Ok(Some(bytes)) = store.get(&key).await else {
            continue;
        };
        let Ok(cp) = serde_json::from_slice::<Checkpoint>(&bytes) else {
            continue;
        };
        let mut consider = |c: Claim| {
            if best.as_ref().is_none_or(|b| rank(&c) > rank(b)) {
                best = Some(c);
            }
        };
        if let Some(&g) = cp.owned.get(object) {
            consider(Claim::Owned(w, g));
        }
        if let Some(&(to, g)) = cp.transit.get(object) {
            consider(Claim::Transit(to, g));
        }
    }
    best.unwrap_or(Claim::Unclaimed)
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

    async fn boot_hyst(
        root: &std::path::Path,
        logical: usize,
        claim: ClaimSpec,
        tag: &str,
        hysteresis: u64,
    ) -> Node {
        let store: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(root.join("blobs")).unwrap());
        start(NodeConfig {
            logical,
            claim,
            hysteresis,
            secret: "test-secret".into(),
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store, root.join(format!("live-{tag}")))
        })
        .await
        .unwrap()
    }

    async fn boot(root: &std::path::Path, logical: usize, claim: ClaimSpec, tag: &str) -> Node {
        boot_hyst(root, logical, claim, tag, 200).await
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

    #[test]
    fn blocks_tile_the_worker_space_exactly() {
        // Every worker belongs to exactly one block, and that block's
        // range contains it — even when blocks don't divide W evenly.
        for (logical, blocks) in [(8, 4), (4096, 256), (1_000_000, 256), (100, 7), (256, 256)] {
            let mut covered = 0;
            for b in 0..blocks {
                let (start, end) = block_range(b, logical, blocks);
                assert!(start <= end);
                covered += end - start;
                for w in start..end {
                    assert_eq!(block_of(w, logical, blocks), b, "W={logical} B={blocks} w={w}");
                }
            }
            assert_eq!(covered, logical, "W={logical} B={blocks}: no gaps, no overlap");
        }
    }

    #[test]
    fn default_worker_is_frozen_fnv1a() {
        // The Worker edge router reimplements this hash in TypeScript to
        // route requests straight to the owning instance. These values
        // changing means every deployed router is suddenly wrong.
        assert_eq!(default_worker("alice", 4096), 2311);
        assert_eq!(default_worker("bob", 4096), 2644);
        assert_eq!(default_worker("alice", 1), 0);
    }

    #[test]
    fn claim_specs_parse_the_documented_forms() {
        assert!(matches!(ClaimSpec::parse("all", 8), ClaimSpec::All));
        assert!(matches!(ClaimSpec::parse("auto:16", 8), ClaimSpec::Auto(16)));
        assert!(matches!(ClaimSpec::parse("7", 8), ClaimSpec::Workers(w) if w == vec![7]));
        assert!(
            matches!(ClaimSpec::parse("2-5", 8), ClaimSpec::Workers(w) if w == vec![2, 3, 4, 5])
        );
        // Ranges clamp to the worker space; nonsense claims nothing.
        assert!(
            matches!(ClaimSpec::parse("6-99", 8), ClaimSpec::Workers(w) if w == vec![6, 7])
        );
        assert!(matches!(ClaimSpec::parse("5-2", 8), ClaimSpec::Workers(w) if w.is_empty()));
        assert!(matches!(ClaimSpec::parse("junk", 8), ClaimSpec::Workers(w) if w.is_empty()));
    }

    #[test]
    fn txn_validation_sorts_dedups_and_rejects() {
        let op = |object: &str| Op {
            object: object.into(),
            sql: "SELECT 1".into(),
            params: vec![],
        };
        let ids = validate_txn(
            vec!["bob".into(), "alice".into(), "bob".into()],
            &[op("alice"), op("bob")],
        )
        .unwrap();
        assert_eq!(ids, vec!["alice", "bob"], "sorted and deduped");

        assert!(validate_txn(vec![], &[]).is_err(), "no participants");
        assert!(validate_txn(vec!["_meta".into()], &[]).is_err(), "reserved id");
        let err = validate_txn(vec!["alice".into()], &[op("eve")]).unwrap_err();
        assert!(err.message.contains("undeclared"), "op outside the declared set");
    }

    #[test]
    fn pressure_caps_how_much_one_worker_may_hoard() {
        let mut routing = Routing {
            logical: 4,
            blocks: 4,
            exceptions: Map::default(),
            addrs: Map::default(),
        };
        assert!(!routing.crowded(0), "an empty cluster is never crowded");
        // 8 exceptions over 4 workers: fair share 2, cap max(2*8/4, 4) = 4.
        for i in 0..8 {
            routing.exceptions.insert(format!("obj{i}"), if i < 5 { 0 } else { 1 });
        }
        assert_eq!(routing.exception_load(0), 5);
        assert!(routing.crowded(0), "5 of 8 on one worker is a mega-worker forming");
        assert!(!routing.crowded(1));
        assert_eq!(routing.owner_of("obj3"), 0, "exceptions override the hash");
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
    struct CountingStore(FsBlobStore, std::sync::Mutex<Map<String, u32>>);

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
            std::sync::Mutex::new(Map::default()),
        ));
        let store: Arc<dyn BlobStore> = counting.clone();
        let node = start(NodeConfig {
            logical: 4,
            claim: ClaimSpec::All,
            hysteresis: 3, // low tenure bar so the cross-txn displaces + returns,
            secret: "test".into(),
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store, dir.path().join("live"))
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
            logical: 4,
            claim: ClaimSpec::All,
            secret: "test".into(),
            limits,
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store, dir.path().join("live"))
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
            logical: 4,
            claim: ClaimSpec::All,
            secret: "test".into(),
            limits,
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store, dir.path().join("live"))
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

    async fn boot_fenced(
        root: &std::path::Path,
        tag: &str,
        fence_ttl: std::time::Duration,
    ) -> Node {
        let store: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(root.join("blobs")).unwrap());
        start(NodeConfig {
            logical: 4,
            secret: "test".into(),
            fence_ttl,
            ..NodeConfig::new(store, root.join(format!("live-{tag}")))
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn takeover_of_unreleased_lease_waits_out_the_fence_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());
        // A dead holder left non-tombstoned leases behind (kill -9 world).
        let corpse = serde_json::to_vec(&Lease {
            addr: "http://127.0.0.1:9".into(),
        })
        .unwrap();
        for b in 0..4 {
            store.put(&format!("_lease/b{b}/e1.json"), &corpse).await.unwrap();
        }

        let ttl = std::time::Duration::from_millis(400);
        let node = boot_fenced(dir.path(), "a", ttl).await;
        let claimed_at = std::time::Instant::now();

        // First WRITE must not land before the predecessor's TTL expires.
        exec(&node, &["obj"], &[("obj", "CREATE TABLE t (n INTEGER)")])
            .await
            .unwrap();
        let elapsed = claimed_at.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(300),
            "first commit should wait out the fence TTL, took {elapsed:?}"
        );
        node.shutdown().await;
    }

    #[tokio::test]
    async fn stale_lease_is_refused_at_the_commit_point() {
        let dir = tempfile::tempdir().unwrap();
        let ttl = std::time::Duration::from_millis(200);
        let node = boot_fenced(dir.path(), "a", ttl).await;

        let alice = id_on_worker(1, 4, "alice"); // W=4 => block 1
        make_account(&node, &alice).await;

        // Simulate a takeover happening while this node is "paused": another
        // node writes a higher epoch for alice's block.
        let usurper = serde_json::to_vec(&Lease {
            addr: "http://127.0.0.1:9".into(),
        })
        .unwrap();
        node.store.put("_lease/b1/e2.json", &usurper).await.unwrap();

        // Let the recency stamp go stale (the guard's 5s tick hasn't run),
        // then try to commit: the gate must verify inline, discover the
        // usurper, flag the node fenced, and refuse the commit point.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let err = exec(
            &node,
            &[&alice],
            &[(&alice, "UPDATE account SET balance = balance - 1")],
        )
        .await
        .unwrap_err();
        assert!(
            err.message.contains("superseded") || err.message.contains("fenced"),
            "commit must be refused, got: {}",
            err.message
        );
        assert!(
            node.fenced.load(std::sync::atomic::Ordering::SeqCst),
            "node should consider itself fenced"
        );
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

    #[tokio::test]
    async fn migration_and_return_ride_the_rpc_between_nodes() {
        let dir = tempfile::tempdir().unwrap();
        // Low hysteresis so the displaced object earns "settled" fast and
        // goes home within the test.
        let node_a = boot_hyst(dir.path(), 4, ClaimSpec::Workers(vec![0, 1]), "a", 3).await;
        let node_b = boot_hyst(dir.path(), 4, ClaimSpec::Workers(vec![2, 3]), "b", 3).await;
        let a1 = id_on_worker(1, 4, "a1");
        let a2 = id_on_worker(1, 4, "a2");
        let b1 = id_on_worker(2, 4, "b1");
        make_account(&node_a, &a1).await;
        make_account(&node_a, &a2).await;
        make_account(&node_b, &b1).await;
        // Tenure for b1 at its home worker, past the hysteresis bar.
        for _ in 0..5 {
            exec(&node_b, &[&b1], &[(&b1, "UPDATE account SET balance = balance + 0")])
                .await
                .unwrap();
        }

        // Plurality at A's worker 1: b1 is TAKEN from node B over the RPC,
        // the txn runs at A, and hysteresis then sends b1 home — an Adopt
        // over the RPC in the other direction.
        exec(
            &node_a,
            &[&a1, &a2, &b1],
            &[
                (&a1, "UPDATE account SET balance = balance - 5"),
                (&a2, "UPDATE account SET balance = balance - 5"),
                (&b1, "UPDATE account SET balance = balance + 10"),
            ],
        )
        .await
        .unwrap();
        assert!(node_a.stats().await.takes >= 1, "the take crossed nodes");

        let mut home = false;
        for _ in 0..250 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if node_b.routing.read().unwrap().owner_of(&b1) == 2
                && node_a.stats().await.returns >= 1
            {
                home = true;
                break;
            }
        }
        assert!(home, "hysteresis must return b1 to worker 2 via RPC adopt");
        assert_eq!(balance(&node_b, &b1).await, 110, "state followed the round trip");
        node_a.shutdown().await;
        node_b.shutdown().await;
    }

    #[tokio::test]
    async fn a_partial_cluster_reports_unroutable_workers() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::Workers(vec![0]), "a").await;
        let local = id_on_worker(0, 4, "mine");
        let foreign = id_on_worker(3, 4, "theirs");
        make_account(&node, &local).await;

        // Routing to a worker nobody holds: said plainly, not hung.
        let err = exec(&node, &[&foreign], &[(&foreign, "SELECT 1")]).await.unwrap_err();
        assert!(err.message.contains("no live node holds logical worker"), "{}", err.message);

        // A txn that must ACQUIRE the unroutable participant fails after
        // the take retries drain, releasing what it already held — and a
        // good txn queued behind it on the same object runs the moment the
        // doomed one lets go.
        let n2 = node.clone();
        let (l2, f2) = (local.clone(), foreign.clone());
        let doomed = tokio::spawn(async move {
            exec(
                &n2,
                &[&l2, &f2],
                &[(&l2, "UPDATE account SET balance = balance - 1")],
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let queued_objects = [local.as_str()];
        let queued_ops = [(local.as_str(), "UPDATE account SET balance = balance + 5")];
        let queued = exec(&node, &queued_objects, &queued_ops);
        let (doomed, queued) = tokio::join!(doomed, queued);
        let err = doomed.unwrap().unwrap_err();
        assert!(err.message.contains("acquisition failed"), "{}", err.message);
        queued.unwrap();
        assert_eq!(balance(&node, &local).await, 105, "released cleanly, queue served");

        // Polls are node-local: the owning instance must be asked.
        let err = poll_q(&node, &foreign, "SELECT 1", vec![], false, None).await.unwrap_err();
        assert!(err.message.contains("poll the owning instance"), "{}", err.message);
        cancel_poll(&node, &foreign, 1, 1); // fire-and-forget, even unrouted
        node.shutdown().await;
    }

    #[tokio::test]
    async fn a_stale_peer_address_heals_on_retry() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path().join("blobs")).unwrap();
        // A dead-but-fast address: bind an ephemeral port, then drop the
        // listener so connections are refused instead of hanging.
        let dead = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", l.local_addr().unwrap())
        };
        // A corpse lease for block 3 at that address: node A will boot
        // with it cached in its routing.
        store
            .put(
                "_lease/b3/e1.json",
                serde_json::to_vec(&Lease { addr: dead }).unwrap().as_slice(),
            )
            .await
            .unwrap();
        // Short fence TTL: B's takeover of the corpse's (non-tombstoned)
        // lease must wait it out before its first write.
        let boot_short = |claim: Vec<usize>, tag: &'static str| {
            let root = dir.path().to_path_buf();
            async move {
                let store: Arc<dyn BlobStore> =
                    Arc::new(FsBlobStore::new(root.join("blobs")).unwrap());
                start(NodeConfig {
                    logical: 4,
                    claim: ClaimSpec::Workers(claim),
                    secret: "test-secret".into(),
                    fence_ttl: std::time::Duration::from_millis(300),
                    ..NodeConfig::new(store, root.join(format!("live-{tag}")))
                })
                .await
                .unwrap()
            }
        };
        let node_a = boot_short(vec![0], "a").await;
        // B health-checks the corpse, finds it dead, and claims block 3 at
        // a bumped epoch — a takeover A hears nothing about.
        let node_b = boot_short(vec![3], "b").await;
        assert_eq!(node_b.claimed_workers(), 1);

        // A's first forward goes to the dead address, fails at transport,
        // re-reads the lease, and retries once at the fresh holder.
        let obj = id_on_worker(3, 4, "healed");
        exec(&node_a, &[&obj], &[(&obj, "CREATE TABLE t (n INTEGER)")])
            .await
            .unwrap();
        let res = exec(&node_b, &[&obj], &[(&obj, "SELECT COUNT(*) AS c FROM t")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 0, "the txn landed at the real holder");
        node_a.shutdown().await;
        node_b.shutdown().await;
    }

    #[tokio::test]
    async fn live_holders_keep_their_leases() {
        let dir = tempfile::tempdir().unwrap();
        let node_a = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        // B wants everything, but every block's holder answers /healthz.
        let node_b = boot(dir.path(), 4, ClaimSpec::All, "b").await;
        assert_eq!(node_b.claimed_workers(), 0, "may not steal from the living");
        assert_eq!(node_a.claimed_workers(), 4);
        node_a.shutdown().await;
        node_b.shutdown().await;
    }

    #[tokio::test]
    async fn legacy_cluster_meta_defaults_to_64_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path().join("blobs")).unwrap();
        // A cluster created before lease blocks existed: no `blocks` field.
        store
            .create("_meta/cluster.json", br#"{"logical_workers":8}"#)
            .await
            .unwrap();
        let node = boot(dir.path(), 8, ClaimSpec::All, "a").await;
        {
            let routing = node.routing.read().unwrap();
            assert_eq!(routing.blocks, 64, "legacy clusters ran per-worker leases at W=64");
            assert_eq!(routing.logical, 8, "and the stored W wins over the env");
        }
        node.shutdown().await;
    }

    #[tokio::test]
    async fn boot_resolves_dual_claims_by_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path().join("blobs")).unwrap();
        // A crash mid-handoff can leave an object in two checkpoints. The
        // higher handoff generation is the later transfer — the real owner.
        let obj = id_on_worker(0, 4, "dual");
        store
            .put("_worker/2.json", format!(r#"{{"owned":{{"{obj}":7}}}}"#).as_bytes())
            .await
            .unwrap();
        store
            .put("_worker/1.json", format!(r#"{{"owned":{{"{obj}":3}}}}"#).as_bytes())
            .await
            .unwrap();
        // Legacy pre-generation checkpoint (bare array = generation 1):
        // still readable — silently dropping it would be induced amnesia.
        let legacy = id_on_worker(0, 4, "legacy");
        store
            .put("_worker/3.json", format!(r#"{{"owned":["{legacy}"]}}"#).as_bytes())
            .await
            .unwrap();
        store.put("_worker/stray-file", b"{}").await.unwrap(); // not a worker: ignored
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        assert_eq!(
            node.routing.read().unwrap().owner_of(&obj),
            2,
            "higher generation wins the dual claim"
        );
        assert_eq!(
            node.routing.read().unwrap().owner_of(&legacy),
            3,
            "legacy checkpoints still claim their objects"
        );
        node.shutdown().await;
    }

    #[tokio::test]
    async fn claimed_ranges_merge_only_adjacent_blocks() {
        let dir = tempfile::tempdir().unwrap();
        // 99 is out of range and silently dropped; 0 and 2 don't touch.
        let node = boot(dir.path(), 4, ClaimSpec::Workers(vec![0, 2, 99]), "a").await;
        assert_eq!(node.claimed_ranges(), vec![(0, 1), (2, 3)]);
        assert_eq!(node.claimed_workers(), 2);
        assert_eq!(node.claimed(), vec![0, 2]);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn the_lease_guard_shrugs_at_junk_keys_and_store_blips() {
        /// A store whose LIST can be told to fail (one guard sweep's view
        /// of a transient outage).
        struct FlakyList(FsBlobStore, std::sync::atomic::AtomicBool);
        #[async_trait::async_trait]
        impl BlobStore for FlakyList {
            async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                self.0.get(key).await
            }
            async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.0.put(key, bytes).await
            }
            async fn delete(&self, key: &str) -> anyhow::Result<()> {
                self.0.delete(key).await
            }
            async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
                anyhow::ensure!(!self.1.load(Ordering::Relaxed), "injected list outage");
                self.0.list(prefix).await
            }
            async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
                self.0.create(key, bytes).await
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let flaky = Arc::new(FlakyList(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::sync::atomic::AtomicBool::new(false),
        ));
        let store: Arc<dyn BlobStore> = flaky.clone();
        let node = boot_with_store(dir.path(), store, 4).await;
        for junk in [
            "_lease/loose-file",
            "_lease/bnope/e1.json",
            "_lease/b1/not-an-epoch.json",
            "_lease/b1/e1.released", // tombstones are not epochs
        ] {
            node.store.put(junk, b"junk").await.unwrap();
        }
        assert!(verify_leases(&node).await, "junk must not read as supersession");
        assert!(!node.fenced.load(Ordering::SeqCst));

        // A transient store outage keeps the old stamp and retries later —
        // it must not fence a healthy node.
        flaky.1.store(true, Ordering::Relaxed);
        assert!(verify_leases(&node).await, "an outage is not a supersession");
        assert!(!node.fenced.load(Ordering::SeqCst));
        flaky.1.store(false, Ordering::Relaxed);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn a_corrupt_delta_fails_activation_not_the_process() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let obj = id_on_worker(1, 4, "victim");
        make_account(&node, &obj).await;

        // Poison the chain with a delta newer than every valid state, then
        // force a cold activation.
        node.store
            .put(&crate::delta::delta_key(&obj, u32::MAX), b"garbage")
            .await
            .unwrap();
        local_sender(&node, 1).unwrap().send(WorkerMsg::Shed).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let err = exec(&node, &[&obj], &[(&obj, "SELECT 1")]).await.unwrap_err();
        assert!(err.message.contains("activation failed"), "{}", err.message);

        // The documented recovery: remove the poison, retry, all is well.
        node.store.delete(&crate::delta::delta_key(&obj, u32::MAX)).await.unwrap();
        assert_eq!(balance(&node, &obj).await, 100);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn racing_takers_converge_without_a_second_transfer() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let l1 = id_on_worker(1, 4, "left");
        let l2 = id_on_worker(1, 4, "right");
        let shared = id_on_worker(2, 4, "shared");
        exec(&node, &[&l1], &[(&l1, "CREATE TABLE t (n INTEGER)")]).await.unwrap();
        exec(&node, &[&l2], &[(&l2, "CREATE TABLE t (n INTEGER)")]).await.unwrap();
        exec(&node, &[&shared], &[(&shared, "CREATE TABLE t (n INTEGER)")]).await.unwrap();

        // Two txns at worker 1 race to take the same object from worker 2.
        // One take wins; the loser's take chases the NotMine hint, finds
        // the object already local, and rides the queue like anyone else.
        let objects_a = [l1.as_str(), shared.as_str()];
        let ops_a = [
            (l1.as_str(), "INSERT INTO t (n) VALUES (1)"),
            (shared.as_str(), "INSERT INTO t (n) VALUES (1)"),
        ];
        let objects_b = [l2.as_str(), shared.as_str()];
        let ops_b = [
            (l2.as_str(), "INSERT INTO t (n) VALUES (1)"),
            (shared.as_str(), "INSERT INTO t (n) VALUES (1)"),
        ];
        let (ra, rb) = tokio::join!(
            exec(&node, &objects_a, &ops_a),
            exec(&node, &objects_b, &ops_b),
        );
        ra.unwrap();
        rb.unwrap();

        let res = exec(&node, &[&shared], &[(&shared, "SELECT COUNT(*) AS c FROM t")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 2, "both racers committed exactly once");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn takes_and_returns_survive_a_dead_peer() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(300),
        ));
        let node_a = start(NodeConfig {
            logical: 4,
            claim: ClaimSpec::Workers(vec![0, 1]),
            hysteresis: 3,
            secret: "test".into(),
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store.clone(), dir.path().join("live-a"))
        })
        .await
        .unwrap();
        let node_b = start(NodeConfig {
            logical: 4,
            claim: ClaimSpec::Workers(vec![2, 3]),
            hysteresis: 3,
            secret: "test".into(),
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store, dir.path().join("live-b"))
        })
        .await
        .unwrap();

        let a1 = id_on_worker(1, 4, "pa");
        let a2 = id_on_worker(1, 4, "pb");
        let b1 = id_on_worker(2, 4, "pc");
        make_account(&node_a, &a1).await;
        make_account(&node_a, &a2).await;
        make_account(&node_b, &b1).await;
        for _ in 0..5 {
            exec(&node_b, &[&b1], &[(&b1, "UPDATE account SET balance = balance + 0")])
                .await
                .unwrap();
        }

        // Drag b1 to node A (a live cross-node take), then kill B while
        // A's boat is still in flight — the hysteresis return that follows
        // has nowhere to go. The object is orphaned, not corrupted.
        exec(
            &node_a,
            &[&a1, &a2, &b1],
            &[(&b1, "UPDATE account SET balance = balance + 1")],
        )
        .await
        .unwrap();
        node_b.shutdown().await;
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        // A fresh take toward the dead node's worker: the cached address
        // fails, the lease re-read finds a tombstone, and the txn reports
        // acquisition failure instead of hanging.
        let cold = id_on_worker(3, 4, "cold");
        let err = exec(
            &node_a,
            &[&a1, &cold],
            &[(&a1, "UPDATE account SET balance = balance + 0")],
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("acquisition failed"), "{}", err.message);
        node_a.shutdown().await;
    }

    #[tokio::test]
    async fn three_quick_visits_earn_a_rehome() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot_hyst(dir.path(), 4, ClaimSpec::All, "a", 3).await;
        let anchor1 = id_on_worker(2, 4, "anch");
        let anchor2 = id_on_worker(2, 4, "anchb");
        let commuter = id_on_worker(1, 4, "commuter");
        make_account(&node, &anchor1).await;
        make_account(&node, &anchor2).await;
        make_account(&node, &commuter).await;
        // Build tenure so every displacement reads as "settled elsewhere".
        for _ in 0..5 {
            exec(&node, &[&commuter], &[(&commuter, "UPDATE account SET balance = balance + 0")])
                .await
                .unwrap();
        }

        // Drag the commuter to worker 2 three times inside the visit
        // window. Visits 1 and 2 bounce home; visit 3 moves it in.
        for visit in 1..=3 {
            exec(
                &node,
                &[&anchor1, &anchor2, &commuter],
                &[(&commuter, "UPDATE account SET balance = balance + 1")],
            )
            .await
            .unwrap();
            if visit < 3 {
                let mut returned = false;
                for _ in 0..250 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if node.routing.read().unwrap().owner_of(&commuter) == 1 {
                        returned = true;
                        break;
                    }
                }
                assert!(returned, "visit {visit} should bounce home");
                // Rebuild tenure at home: only a SETTLED object's next
                // displacement counts as a fresh visit.
                for _ in 0..4 {
                    exec(
                        &node,
                        &[&commuter],
                        &[(&commuter, "UPDATE account SET balance = balance + 0")],
                    )
                    .await
                    .unwrap();
                }
            }
        }
        // Give any (wrong) return a chance to happen, then check it moved in.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            node.routing.read().unwrap().owner_of(&commuter),
            2,
            "third visit inside the window rehomes the object"
        );
        assert_eq!(balance(&node, &commuter).await, 103);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn a_fenced_node_refuses_the_commit_point() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let obj = id_on_worker(1, 4, "fenceme");
        make_account(&node, &obj).await;

        // The lease guard flips this when a lease is superseded; the commit
        // gate is the last line of defense even with fresh recency stamps.
        node.fenced.store(true, Ordering::SeqCst);
        let err = exec(&node, &[&obj], &[(&obj, "UPDATE account SET balance = 0")])
            .await
            .unwrap_err();
        assert!(err.message.contains("fenced"), "{}", err.message);
    }

    #[tokio::test]
    async fn takes_queue_fairly_behind_unshipped_boats() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(200),
        ));
        let node = boot_with_store(dir.path(), store, 4).await;
        let obj = id_on_worker(1, 4, "contested");
        exec(&node, &[&obj], &[(&obj, "CREATE TABLE t (n INTEGER)")]).await.unwrap();

        // Dirty the object; its boat is now in flight for ~200ms.
        exec_mode(&node, &[&obj], &[(&obj, "INSERT INTO t (n) VALUES (1)")], true)
            .await
            .unwrap();

        // Two takes and a txn pile up behind the unshipped state, FIFO.
        let w1 = local_sender(&node, 1).unwrap();
        let (t1_tx, t1_rx) = oneshot::channel();
        w1.send(WorkerMsg::Take { object: obj.clone(), taker: 2, resp: t1_tx }).unwrap();
        let (t2_tx, t2_rx) = oneshot::channel();
        w1.send(WorkerMsg::Take { object: obj.clone(), taker: 3, resp: t2_tx }).unwrap();
        let n2 = node.clone();
        let o2 = obj.clone();
        let txn = tokio::spawn(async move {
            exec(&n2, &[&o2], &[(&o2, "INSERT INTO t (n) VALUES (2)")]).await
        });

        // Boat lands: the first take wins, the second is bounced to the new
        // owner, and the queued txn chases the object and still commits.
        let meta = t1_rx.await.unwrap().unwrap();
        assert_eq!(meta.home, 1, "first visit away from home");
        match t2_rx.await.unwrap().unwrap_err() {
            TakeError::NotMine { hint } => assert_eq!(hint, Some(2), "bounced to the winner"),
            other => panic!("expected NotMine, got {other:?}"),
        }
        txn.await.unwrap().unwrap();

        // A late take to the old owner gets the message-level NotMine.
        let (t3_tx, t3_rx) = oneshot::channel();
        let owner_now = node.routing.read().unwrap().owner_of(&obj);
        let old = if owner_now == 1 { 2 } else { 1 };
        local_sender(&node, old)
            .unwrap()
            .send(WorkerMsg::Take { object: obj.clone(), taker: 3, resp: t3_tx })
            .unwrap();
        assert!(matches!(t3_rx.await.unwrap(), Err(TakeError::NotMine { .. })));

        let res = exec(&node, &[&obj], &[(&obj, "SELECT COUNT(*) AS c FROM t")]).await.unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 2, "no write was lost in the shuffle");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn the_byte_cap_defers_whole_components_to_the_next_boat() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(150),
        ));
        let mut limits = crate::limits::Limits::derive(4 << 30, 8 << 30);
        limits.max_boat_bytes = 1; // every component alone busts the cap
        let node = start(NodeConfig {
            logical: 4,
            claim: ClaimSpec::All,
            secret: "test".into(),
            limits,
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store, dir.path().join("live"))
        })
        .await
        .unwrap();
        let a = id_on_worker(2, 4, "defa");
        let b = id_on_worker(2, 4, "defb");
        let c = id_on_worker(2, 4, "defc");
        for id in [&a, &b, &c] {
            exec(&node, &[id], &[(id, "CREATE TABLE t (n INTEGER)")]).await.unwrap();
        }

        // One optimistic write launches a boat; while it flies (150ms), two
        // MORE independent writes accumulate. The next maybe_launch sees two
        // components against a 1-byte cap: it must take one and defer the
        // other whole, never splitting or dropping either.
        exec_mode(&node, &[&a], &[(&a, "INSERT INTO t (n) VALUES (1)")], true).await.unwrap();
        let objects_b = [b.as_str()];
        let ops_b = [(b.as_str(), "INSERT INTO t (n) VALUES (1)")];
        let objects_c = [c.as_str()];
        let ops_c = [(c.as_str(), "INSERT INTO t (n) VALUES (1)")];
        let (rb, rc) = tokio::join!(
            exec_mode(&node, &objects_b, &ops_b, true),
            exec_mode(&node, &objects_c, &ops_c, true),
        );
        rb.unwrap();
        rc.unwrap();
        // A pessimistic barrier: acked only once everything before it landed.
        exec(&node, &[&a], &[(&a, "INSERT INTO t (n) VALUES (2)")]).await.unwrap();
        node.shutdown().await;

        let node2 = boot(dir.path(), 4, ClaimSpec::All, "b").await;
        for (id, expect) in [(&a, 2), (&b, 1), (&c, 1)] {
            let res = exec(&node2, &[id], &[(id, "SELECT COUNT(*) AS c FROM t")]).await.unwrap();
            let v = serde_json::to_value(&res.results).unwrap();
            assert_eq!(v[0]["rows"][0]["c"], expect, "{id} durable");
        }
        node2.shutdown().await;
    }

    #[tokio::test]
    async fn a_vanished_live_file_reverts_instead_of_shipping_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(200),
        ));
        let node = boot_with_store(dir.path(), store, 4).await;
        let a = id_on_worker(1, 4, "solid");
        let b = id_on_worker(1, 4, "doomed");
        exec(&node, &[&a], &[(&a, "CREATE TABLE t (n INTEGER)")]).await.unwrap();
        exec(&node, &[&b], &[(&b, "CREATE TABLE t (n INTEGER)")]).await.unwrap();

        // Boat 1 in flight for `a`; a pessimistic write to `b` becomes
        // pending. Sabotage b's live file before its boat can snapshot it.
        exec_mode(&node, &[&a], &[(&a, "INSERT INTO t (n) VALUES (1)")], true).await.unwrap();
        let n2 = node.clone();
        let b2 = b.clone();
        let pending = tokio::spawn(async move {
            exec(&n2, &[&b2], &[(&b2, "INSERT INTO t (n) VALUES (1)")]).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        std::fs::remove_file(node.live_dir.join("w1").join(format!("{b}.db"))).unwrap();

        let err = pending.await.unwrap().unwrap_err();
        assert!(err.message.contains("snapshot failed"), "{}", err.message);
        // The object reverted to its durable state and works again.
        let res = exec(&node, &[&b], &[(&b, "SELECT COUNT(*) AS c FROM t")]).await.unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 0, "reverted to the empty durable table");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn a_sinking_boat_fails_the_writes_that_followed_it() {
        let dir = tempfile::tempdir().unwrap();
        let flaky = Arc::new(FlakyStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::sync::atomic::AtomicBool::new(false),
        ));
        // Slow it down so followers reliably pile up behind the doomed boat.
        struct SlowFlaky(Arc<FlakyStore>, std::time::Duration);
        #[async_trait::async_trait]
        impl BlobStore for SlowFlaky {
            async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                self.0.get(key).await
            }
            async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                tokio::time::sleep(self.1).await;
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
                self.0.get_range(key, o, l).await
            }
        }
        let store: Arc<dyn BlobStore> =
            Arc::new(SlowFlaky(flaky.clone(), std::time::Duration::from_millis(200)));
        let node = boot_with_store(dir.path(), store, 4).await;
        let chan = id_on_worker(1, 4, "chan");
        let bystander = id_on_worker(1, 4, "bystander");
        make_channel(&node, &chan).await;
        exec(&node, &[&bystander], &[(&bystander, "CREATE TABLE t (n INTEGER)")]).await.unwrap();
        // Acks land at the commit record; give the (slow) promotions
        // behind them time to finish before the store starts failing.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // The doomed boat: an optimistic ack, store now failing.
        flaky.1.store(true, Ordering::Relaxed);

        // Disk pressure during the outage: unshipped objects are exempt
        // from shedding (their live file is ahead of the blob store).
        local_sender(&node, 1).unwrap().send(WorkerMsg::Shed).unwrap();

        // A take during the outage is REFUSED: ownership you cannot
        // durably renounce must not move (granting anyway once forked an
        // object under two live writers). The object stays here, usable.
        let (ttx, trx) = oneshot::channel();
        local_sender(&node, 1)
            .unwrap()
            .send(WorkerMsg::Take { object: bystander.clone(), taker: 2, resp: ttx })
            .unwrap();
        let refused = trx.await.unwrap();
        assert!(
            matches!(refused, Err(TakeError::Failed(_)) | Err(TakeError::NotMine { .. })),
            "transfer must abort when the renounce can't be made durable: {refused:?}"
        );
        publish(&node, &chan, "doomed", true).await;
        // A pessimistic follower applies onto state that is about to
        // un-happen; it must hear "reverted", not hang on a dead reply slot.
        let n2 = node.clone();
        let c2 = chan.clone();
        let follower = tokio::spawn(async move {
            exec(&n2, &[&c2], &[(&c2, "INSERT INTO msgs (body) VALUES ('follower')")]).await
        });
        let err = follower.await.unwrap().unwrap_err();
        assert!(
            err.message.contains("reverted") || err.message.contains("commit failed"),
            "{}",
            err.message
        );

        flaky.1.store(false, Ordering::Relaxed);
        let res = exec(&node, &[&chan], &[(&chan, "SELECT COUNT(*) AS c FROM msgs")]).await.unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 0, "both writes un-happened together");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_fails_parked_polls_so_clients_repoll_elsewhere() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;
        let n2 = node.clone();
        let c2 = chan.clone();
        let parked = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs WHERE id > 99", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        node.shutdown().await;
        let err = parked.await.unwrap().unwrap_err();
        assert!(err.message.contains("shutting down"), "{}", err.message);
    }

    #[tokio::test]
    async fn a_deferred_constraint_fails_at_commit_and_poisons_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(200),
        ));
        let node = boot_with_store(dir.path(), store, 4).await;
        let obj = id_on_worker(1, 4, "fk");
        // A DEFERRED constraint is checked only at COMMIT: the failure
        // arrives after every op succeeded, on the commit path itself.
        exec(
            &node,
            &[&obj],
            &[
                (&obj, "CREATE TABLE parent (id INTEGER PRIMARY KEY)"),
                (
                    &obj,
                    "CREATE TABLE child (p INTEGER REFERENCES parent(id) \
                     DEFERRABLE INITIALLY DEFERRED)",
                ),
            ],
        )
        .await
        .unwrap();

        // Let the schema boat finish promoting before anything fails, so
        // "revert to durable" reverts to a state WITH the tables.
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;

        // An acked-optimistic parent sits alongside when the poisoned txn
        // arrives. COMMIT refuses the deferred constraint; a plain
        // ROLLBACK erases exactly the poisoned txn — the parent survives.
        exec_mode(&node, &[&obj], &[(&obj, "INSERT INTO parent (id) VALUES (1)")], true)
            .await
            .unwrap();
        let err = exec(
            &node,
            &[&obj],
            // The parent for 999 never arrives; COMMIT is where SQLite notices.
            &[(&obj, "INSERT INTO child (p) VALUES (999)")],
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("local commit failed"), "{}", err.message);
        let res = exec(
            &node,
            &[&obj],
            &[
                (&obj, "SELECT COUNT(*) AS c FROM child"),
                (&obj, "SELECT COUNT(*) AS c FROM parent"),
            ],
        )
        .await
        .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 0, "the poisoned insert un-happened");
        assert_eq!(v[1]["rows"][0]["c"], 1, "the acked parent survived the rollback");

        // Cross-object: the first participant COMMITS before the second's
        // deferred constraint fires. It cannot be uncommitted, so it
        // reverts to durable state — the txn vanishes atomically from both,
        // taking any unshipped optimistic writes on it along (the sunk-boat
        // contract). ("early" sorts before "fk"; commits run sorted.)
        let other = id_on_worker(1, 4, "early");
        exec(&node, &[&other], &[(&other, "CREATE TABLE log (n INTEGER)")]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(700)).await; // promotion lands
        // Two optimistic writes: the first launches a boat, the second is
        // still DIRTY when the poison lands — its accounting must clear.
        exec_mode(&node, &[&other], &[(&other, "INSERT INTO log (n) VALUES (7)")], true)
            .await
            .unwrap();
        exec_mode(&node, &[&other], &[(&other, "INSERT INTO log (n) VALUES (7)")], true)
            .await
            .unwrap();
        let err = exec(
            &node,
            &[&other, &obj],
            &[
                (&other, "INSERT INTO log (n) VALUES (99)"),
                (&obj, "INSERT INTO child (p) VALUES (999)"),
            ],
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("local commit failed"), "{}", err.message);
        let res = exec(
            &node,
            &[&other],
            &[(&other, "SELECT COUNT(*) AS c FROM log WHERE n = 99")],
        )
        .await
        .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 0, "the committed half reverted with the txn");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn big_boats_stage_and_clean_up_when_the_commit_fails() {
        let dir = tempfile::tempdir().unwrap();
        let flaky = Arc::new(FlakyStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::sync::atomic::AtomicBool::new(false),
        ));
        let store: Arc<dyn BlobStore> = flaky.clone();
        let node = boot_with_store(dir.path(), store, 4).await;
        let big = id_on_worker(1, 4, "bigfail");
        exec(&node, &[&big], &[(&big, "CREATE TABLE docs (body TEXT)")]).await.unwrap();

        // >96 KB of payload forces the staged (non-inline) commit path;
        // with every put failing, staging must be attempted AND swept.
        flaky.1.store(true, Ordering::Relaxed);
        let blob = "x".repeat(200_000);
        let err = submit(
            &node,
            vec![big.clone()],
            vec![Op {
                object: big.clone(),
                sql: "INSERT INTO docs (body) VALUES (?1)".into(),
                params: vec![serde_json::json!(blob)],
            }],
            false,
            false,
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("commit failed"), "{}", err.message);

        flaky.1.store(false, Ordering::Relaxed);
        assert!(
            node.store.list("staging/").await.unwrap().is_empty(),
            "failed boats must not leak staging blobs"
        );
        node.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_a_table_under_a_parked_poll_reports_the_error() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;

        let n2 = node.clone();
        let c2 = chan.clone();
        let parked = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs WHERE id > 99", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The schema changes under the query: the poll can never run again,
        // so it must error out rather than park forever.
        exec(&node, &[&chan], &[(&chan, "DROP TABLE msgs")]).await.unwrap();
        let err = parked.await.unwrap().unwrap_err();
        assert!(err.message.contains("msgs"), "names the vanished table: {}", err.message);
        node.shutdown().await;
    }

    // ================================================================ polls
    //
    // The long-poll battery. A poll is a read-only query whose reply is
    // held until its condition holds: non-empty results, or (with a
    // baseline) a result hash different from the last one seen.

    async fn poll_q(
        node: &Node,
        object: &str,
        sql: &str,
        params: Vec<Value>,
        durable: bool,
        baseline: Option<&str>,
    ) -> Result<TxnResponse, ApiError> {
        submit_poll(
            node,
            object.to_string(),
            sql.to_string(),
            params,
            durable,
            baseline.map(str::to_string),
            0,
            0,
        )
        .await
    }

    fn poll_rows(res: &TxnResponse) -> Vec<Value> {
        match res.results.first() {
            Some(OpResult::Rows { rows }) => rows.clone(),
            _ => Vec::new(),
        }
    }

    async fn parked_polls(node: &Node) -> usize {
        node.stats()
            .await
            .per_worker
            .iter()
            .map(|w| w.parked_polls)
            .sum()
    }

    async fn make_channel(node: &Node, id: &str) {
        exec(
            node,
            &[id],
            &[(
                id,
                "CREATE TABLE msgs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)",
            )],
        )
        .await
        .unwrap();
    }

    async fn publish(node: &Node, id: &str, body: &str, optimistic: bool) {
        let sql = format!("INSERT INTO msgs (body) VALUES ('{body}')");
        exec_mode(node, &[id], &[(id, sql.as_str())], optimistic)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn poll_returns_immediately_then_parks_then_fires() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;
        publish(&node, &chan, "hello", false).await;

        // Condition already true: immediate reply.
        let res = poll_q(&node, &chan, "SELECT * FROM msgs WHERE id > 0", vec![], false, None)
            .await
            .unwrap();
        assert_eq!(poll_rows(&res).len(), 1);

        // Condition false: parks. Prove it's parked (no reply in 100ms),
        // then a publish fires it with exactly the new row.
        let n2 = node.clone();
        let c2 = chan.clone();
        let mut parked = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs WHERE id > ?1", vec![Value::from(1)], false, None).await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut parked)
                .await
                .is_err(),
            "poll should park while the condition is false"
        );
        assert_eq!(parked_polls(&node).await, 1);
        publish(&node, &chan, "world", true).await;
        let res = parked.await.unwrap().unwrap();
        let rows = poll_rows(&res);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], 2);
        assert_eq!(rows[0]["body"], "world");
        assert_eq!(parked_polls(&node).await, 0);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn poll_cursor_loop_loses_no_wakeups_under_hammer() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(2, 4, "hot");
        make_channel(&node, &chan).await;

        // 100 concurrent optimistic publishers racing one consumer running
        // the documented cursor loop. Every message must arrive exactly
        // once, in order — the registration-at-serialization-point
        // guarantee means no wakeup is ever lost.
        let mut writers = Vec::new();
        for i in 0..100 {
            let n = node.clone();
            let c = chan.clone();
            writers.push(tokio::spawn(async move {
                publish(&n, &c, &format!("m{i}"), true).await;
            }));
        }
        let mut seen: Vec<i64> = Vec::new();
        let mut cursor = 0i64;
        while seen.len() < 100 {
            let res = poll_q(
                &node,
                &chan,
                "SELECT id FROM msgs WHERE id > ?1 ORDER BY id",
                vec![Value::from(cursor)],
                false,
                None,
            )
            .await
            .unwrap();
            for row in poll_rows(&res) {
                let id = row["id"].as_i64().unwrap();
                seen.push(id);
                cursor = id;
            }
        }
        for w in writers {
            w.await.unwrap();
        }
        assert_eq!(seen, (1..=100).collect::<Vec<i64>>(), "no gaps, no dupes, in order");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn change_detection_bootstraps_sees_deletes_ignores_noops() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let pres = id_on_worker(1, 4, "presence");
        exec(
            &node,
            &[&pres],
            &[
                (&pres, "CREATE TABLE p (u TEXT PRIMARY KEY)"),
                (&pres, "INSERT INTO p VALUES ('alice'), ('bob')"),
            ],
        )
        .await
        .unwrap();
        let view = "SELECT u FROM p ORDER BY u";

        // Empty baseline never matches: immediate bootstrap snapshot.
        let res = poll_q(&node, &pres, view, vec![], false, Some("")).await.unwrap();
        assert_eq!(poll_rows(&res).len(), 2);
        let h1 = res.hash.clone().unwrap();

        // Same baseline: parks. A write that does NOT change the result
        // must not fire it; a DELETE (shrinking the result!) must.
        let n2 = node.clone();
        let p2 = pres.clone();
        let v2 = view.to_string();
        let h1c = h1.clone();
        let mut parked = tokio::spawn(async move {
            poll_q(&n2, &p2, &v2, vec![], false, Some(&h1c)).await
        });
        exec_mode(&node, &[&pres], &[(&pres, "UPDATE p SET u = 'alice' WHERE u = 'alice'")], true)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut parked)
                .await
                .is_err(),
            "a write that leaves the result identical must not fire the poll"
        );
        exec_mode(&node, &[&pres], &[(&pres, "DELETE FROM p WHERE u = 'bob'")], true)
            .await
            .unwrap();
        let res = parked.await.unwrap().unwrap();
        assert_eq!(poll_rows(&res).len(), 1, "the leave is visible");
        assert_ne!(res.hash.clone().unwrap(), h1);
        node.shutdown().await;
    }

    /// Delays every blob write, so there's a window where state is applied
    /// locally but not yet durable.
    struct SlowStore(FsBlobStore, std::time::Duration);

    #[async_trait::async_trait]
    impl BlobStore for SlowStore {
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            tokio::time::sleep(self.1).await;
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
            self.0.get_range(key, o, l).await
        }
    }

    async fn boot_with_store(
        root: &std::path::Path,
        store: Arc<dyn BlobStore>,
        logical: usize,
    ) -> Node {
        start(NodeConfig {
            logical,
            secret: "test".into(),
            fence_ttl: std::time::Duration::from_secs(60),
            ..NodeConfig::new(store, root.join("live"))
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn durable_poll_fires_only_after_the_boat_lands() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(300),
        ));
        let node = boot_with_store(dir.path(), store, 4).await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await; // pessimistic: waits out boat 1

        // Optimistic publish acks locally; its boat is now in flight for
        // ~300ms. A non-durable poll sees the row instantly; a durable one
        // must hold until the commit record is down.
        publish(&node, &chan, "hello", true).await;
        let res = poll_q(&node, &chan, "SELECT * FROM msgs", vec![], false, None)
            .await
            .unwrap();
        assert_eq!(poll_rows(&res).len(), 1, "optimistic read sees applied state");

        let started = std::time::Instant::now();
        let n2 = node.clone();
        let c2 = chan.clone();
        let mut durable = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs", vec![], true, None).await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut durable)
                .await
                .is_err(),
            "durable poll must not fire from undurable state"
        );
        let res = tokio::time::timeout(std::time::Duration::from_secs(5), durable)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(poll_rows(&res).len(), 1);
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(200),
            "reply only after the ship round trip"
        );

        // Quiescent object: durable state == live state, immediate reply.
        let res = poll_q(&node, &chan, "SELECT * FROM msgs", vec![], true, None)
            .await
            .unwrap();
        assert_eq!(poll_rows(&res).len(), 1);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn parked_polls_fail_on_migration_and_repoll_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 8, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 8, "chan");
        let d1 = id_on_worker(6, 8, "d1");
        let d2 = id_on_worker(6, 8, "d2");
        make_channel(&node, &chan).await;
        make_account(&node, &d1).await;
        make_account(&node, &d2).await;

        let n2 = node.clone();
        let c2 = chan.clone();
        let parked = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs WHERE id > 99", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(parked_polls(&node).await, 1);

        // Plurality drags chan to worker 6: its parked poll must fail
        // loudly, not dangle.
        exec(
            &node,
            &[&chan, &d1, &d2],
            &[(&chan, "INSERT INTO msgs (body) VALUES ('x')")],
        )
        .await
        .unwrap();
        let err = parked.await.unwrap().unwrap_err();
        assert!(
            err.message.contains("re-poll"),
            "poll should say re-poll, got: {}",
            err.message
        );

        // The documented client contract: just poll again. Retry through
        // any further migrations (hysteresis may bounce the object home).
        let res = loop {
            match poll_q(&node, &chan, "SELECT * FROM msgs WHERE id > 0", vec![], false, None).await
            {
                Ok(res) => break res,
                Err(e) if e.message.contains("re-poll") => continue,
                Err(e) => panic!("unexpected: {}", e.message),
            }
        };
        assert_eq!(poll_rows(&res).len(), 1);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn parked_polls_survive_shedding() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;

        let n2 = node.clone();
        let c2 = chan.clone();
        let parked = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(parked_polls(&node).await, 1);

        // Deactivate the (clean, idle) object out from under the poll.
        let owner = { node.routing.read().unwrap().owner_of(&chan) };
        local_sender(&node, owner)
            .unwrap()
            .send(WorkerMsg::Shed)
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(parked_polls(&node).await, 1, "poll waits through eviction");

        // The next write reactivates the object and the poll fires.
        publish(&node, &chan, "back", false).await;
        let res = parked.await.unwrap().unwrap();
        assert_eq!(poll_rows(&res).len(), 1);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn poll_rejects_writes_and_bad_objects() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;

        let err = poll_q(&node, &chan, "INSERT INTO msgs (body) VALUES ('sneaky')", vec![], false, None)
            .await
            .unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);

        let err = poll_q(&node, "_lease/nope", "SELECT 1", vec![], false, None)
            .await
            .unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn abandoned_and_canceled_polls_get_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;

        // Abandoned: the client task dies (reply slot dropped). The park
        // survives until the next write re-check sweeps it — no reply, no
        // panic, no leak.
        let n2 = node.clone();
        let c2 = chan.clone();
        let abandoned = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs WHERE id > 99", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        abandoned.abort();
        let _ = abandoned.await;
        assert_eq!(parked_polls(&node).await, 1, "swept lazily, not synchronously");
        publish(&node, &chan, "sweep", false).await;
        assert_eq!(parked_polls(&node).await, 0);

        // Canceled: explicit cancel resolves the caller with an error.
        let n3 = node.clone();
        let c3 = chan.clone();
        let canceled = tokio::spawn(async move {
            submit_poll(
                &n3,
                c3,
                "SELECT * FROM msgs WHERE id > 99".into(),
                vec![],
                false,
                None,
                7,
                42,
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel_poll(&node, &chan, 7, 42);
        let err = canceled.await.unwrap().unwrap_err();
        assert!(err.message.contains("canceled"), "got: {}", err.message);
        assert_eq!(parked_polls(&node).await, 0);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_polls_fire_independently() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let acct = id_on_worker(1, 4, "acct");
        make_account(&node, &acct).await; // balance 100

        let n1 = node.clone();
        let a1 = acct.clone();
        let low = tokio::spawn(async move {
            poll_q(&n1, &a1, "SELECT balance FROM account WHERE balance < 50", vec![], false, None).await
        });
        let n2 = node.clone();
        let a2 = acct.clone();
        let mut high = tokio::spawn(async move {
            poll_q(&n2, &a2, "SELECT balance FROM account WHERE balance > 500", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(parked_polls(&node).await, 2);

        // Drop to 40: fires "low" only.
        exec(&node, &[&acct], &[(&acct, "UPDATE account SET balance = 40")])
            .await
            .unwrap();
        let res = low.await.unwrap().unwrap();
        assert_eq!(poll_rows(&res)[0]["balance"], 40);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut high)
                .await
                .is_err(),
            "the other condition still doesn't hold"
        );
        exec(&node, &[&acct], &[(&acct, "UPDATE account SET balance = 900")])
            .await
            .unwrap();
        let res = high.await.unwrap().unwrap();
        assert_eq!(poll_rows(&res)[0]["balance"], 900);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn transactional_outbox_wakes_the_subscriber_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 8, ClaimSpec::All, "a").await;
        let alice = id_on_worker(1, 8, "alice");
        let chan = id_on_worker(6, 8, "orders");
        let anchor = id_on_worker(6, 8, "anchor");
        make_account(&node, &alice).await;
        make_account(&node, &anchor).await;
        make_channel(&node, &chan).await;

        let n2 = node.clone();
        let c2 = chan.clone();
        let sub = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT body FROM msgs WHERE id > 0", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // One atomic txn: state change + publish. The anchor keeps the
        // channel's worker in the plurality so the parked poll isn't
        // migrated away mid-test.
        exec(
            &node,
            &[&alice, &chan, &anchor],
            &[
                (&alice, "UPDATE account SET balance = balance - 60"),
                (&chan, "INSERT INTO msgs (body) VALUES ('order:alice:60')"),
            ],
        )
        .await
        .unwrap();
        let res = sub.await.unwrap().unwrap();
        assert_eq!(poll_rows(&res)[0]["body"], "order:alice:60");
        assert_eq!(balance(&node, &alice).await, 40);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn poll_on_cold_object_activates_it() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;
        publish(&node, &chan, "persisted", false).await;
        node.shutdown().await;

        // Fresh node, empty local disk: the poll itself must pull the
        // object out of the blob store.
        let node2 = boot(dir.path(), 4, ClaimSpec::All, "b").await;
        let res = poll_q(&node2, &chan, "SELECT body FROM msgs", vec![], false, None)
            .await
            .unwrap();
        assert_eq!(poll_rows(&res)[0]["body"], "persisted");
        node2.shutdown().await;
    }

    /// Store that can be told to start failing writes: sinks the boat.
    struct FlakyStore(FsBlobStore, std::sync::atomic::AtomicBool);

    #[async_trait::async_trait]
    impl BlobStore for FlakyStore {
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            if self.1.load(Ordering::Relaxed) {
                anyhow::bail!("injected put failure");
            }
            self.0.put(key, bytes).await
        }
        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.0.delete(key).await
        }
        async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            self.0.list(prefix).await
        }
        async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
            if self.1.load(Ordering::Relaxed) {
                anyhow::bail!("injected create failure");
            }
            self.0.create(key, bytes).await
        }
        async fn get_range(&self, key: &str, o: u64, l: u64) -> anyhow::Result<Option<Vec<u8>>> {
            self.0.get_range(key, o, l).await
        }
    }

    /// Fails puts to final object keys only (objects/*), leaving staging
    /// and commit records alone: the boat commits but promotion fails.
    struct StubbornStore(FsBlobStore, std::sync::atomic::AtomicBool);

    #[async_trait::async_trait]
    impl BlobStore for StubbornStore {
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            if self.1.load(Ordering::Relaxed) && key.starts_with("objects/") {
                anyhow::bail!("injected promotion failure");
            }
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
            self.0.get_range(key, o, l).await
        }
    }

    #[tokio::test]
    async fn failed_promotion_self_heals_on_the_next_boat() {
        let dir = tempfile::tempdir().unwrap();
        let stubborn = Arc::new(StubbornStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::sync::atomic::AtomicBool::new(false),
        ));
        let store: Arc<dyn BlobStore> = stubborn.clone();
        let node = boot_with_store(dir.path(), store, 4).await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;
        let base_key = crate::object::object_key(&chan);
        let counter = |bytes: Option<Vec<u8>>| {
            bytes.as_deref().map(crate::delta::change_counter).unwrap_or(0)
        };
        let before = counter(stubborn.0.get(&base_key).await.unwrap());

        // The commit record lands (the write acks) but promotion fails:
        // the blob store is now behind acked local state.
        stubborn.1.store(true, Ordering::Relaxed);
        publish(&node, &chan, "stuck", false).await;

        // Heal the store: the worker must re-ship current state on its
        // own — no reboot, no further writes from clients.
        stubborn.1.store(false, Ordering::Relaxed);
        let mut healed = false;
        for _ in 0..250 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if counter(stubborn.0.get(&base_key).await.unwrap()) > before {
                healed = true;
                break;
            }
        }
        assert!(healed, "base snapshot must catch up after a failed promotion");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn parked_polls_fail_when_the_boat_sinks() {
        let dir = tempfile::tempdir().unwrap();
        let flaky = Arc::new(FlakyStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::sync::atomic::AtomicBool::new(false),
        ));
        let store: Arc<dyn BlobStore> = flaky.clone();
        let node = boot_with_store(dir.path(), store, 4).await;
        let chan = id_on_worker(1, 4, "chan");
        make_channel(&node, &chan).await;

        let n2 = node.clone();
        let c2 = chan.clone();
        let parked = tokio::spawn(async move {
            poll_q(&n2, &c2, "SELECT * FROM msgs WHERE id > 99", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The optimistic write acks, then its boat sinks: the object
        // reverts to last-durable and the parked poll — which was judged
        // against state that no longer exists — must fail, not dangle.
        flaky.1.store(true, Ordering::Relaxed);
        publish(&node, &chan, "doomed", true).await;
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), parked)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(err.message.contains("re-poll"), "got: {}", err.message);

        // Heal the store; the channel reverts to durable state (no rows)
        // and keeps working.
        flaky.1.store(false, Ordering::Relaxed);
        let res = poll_q(&node, &chan, "SELECT COUNT(*) AS c FROM msgs", vec![], false, Some(""))
            .await
            .unwrap();
        assert_eq!(poll_rows(&res)[0]["c"], 0, "the doomed row un-happened");
        node.shutdown().await;
    }

    // ====================================================== ephemeral state
    //
    // TEMP tables: same SQL, same polls, never dirty the object, never ride
    // a boat, never cost a storage op. Detected by the main file's change
    // counter staying put across the commit.

    #[tokio::test]
    async fn temp_writes_ship_nothing_and_still_wake_polls() {
        let dir = tempfile::tempdir().unwrap();
        // A brutally slow store makes any accidental ship obvious.
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(300),
        ));
        let node = boot_with_store(dir.path(), store, 4).await;
        let room = id_on_worker(1, 4, "room");
        make_channel(&node, &room).await; // durable schema, ships
        let ships_before = node.stats().await.ships;

        // Pessimistic TEMP writes must ack immediately: there is nothing
        // to make durable, so not even a pessimistic txn waits for a boat.
        let started = std::time::Instant::now();
        exec(
            &node,
            &[&room],
            &[(&room, "CREATE TEMP TABLE typing (u TEXT PRIMARY KEY, at INTEGER)")],
        )
        .await
        .unwrap();
        exec(
            &node,
            &[&room],
            &[(&room, "INSERT INTO typing VALUES ('alice', 1)")],
        )
        .await
        .unwrap();
        assert!(
            started.elapsed() < std::time::Duration::from_millis(150),
            "temp-only pessimistic txns must not wait for the 300ms store"
        );

        // ...but they DO wake pollers: signals ride the same protocol.
        let n2 = node.clone();
        let r2 = room.clone();
        let parked = tokio::spawn(async move {
            poll_q(&n2, &r2, "SELECT u FROM typing WHERE u = 'bob'", vec![], false, None).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        exec(&node, &[&room], &[(&room, "INSERT INTO typing VALUES ('bob', 2)")])
            .await
            .unwrap();
        let res = parked.await.unwrap().unwrap();
        assert_eq!(poll_rows(&res)[0]["u"], "bob");

        // TEMP joins MAIN in one query.
        publish(&node, &room, "hello", false).await;
        let res = poll_q(
            &node,
            &room,
            "SELECT (SELECT COUNT(*) FROM msgs) AS durable, (SELECT COUNT(*) FROM typing) AS ephemeral",
            vec![],
            false,
            None,
        )
        .await
        .unwrap();
        assert_eq!(poll_rows(&res)[0]["durable"], 1);
        assert_eq!(poll_rows(&res)[0]["ephemeral"], 2);

        // Only the durable publish shipped; every temp write was free.
        let ships_after = node.stats().await.ships;
        assert_eq!(ships_after, ships_before + 1, "temp writes never launched a boat");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn temp_state_evaporates_on_eviction_main_survives() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let room = id_on_worker(1, 4, "room");
        make_channel(&node, &room).await;
        publish(&node, &room, "durable", false).await;
        exec(
            &node,
            &[&room],
            &[
                (&room, "CREATE TEMP TABLE typing (u TEXT)"),
                (&room, "INSERT INTO typing VALUES ('alice')"),
            ],
        )
        .await
        .unwrap();

        // TEMP writes leave the object clean, so shedding still works —
        // ephemeral state doesn't pin memory or disk.
        let owner = { node.routing.read().unwrap().owner_of(&room) };
        local_sender(&node, owner)
            .unwrap()
            .send(WorkerMsg::Shed)
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Reactivation restores durable state; the temp table is gone —
        // that's the contract (and the query error says so plainly).
        let err = exec(&node, &[&room], &[(&room, "SELECT * FROM typing")])
            .await
            .unwrap_err();
        assert!(err.message.contains("typing"), "got: {}", err.message);
        let res = exec(&node, &[&room], &[(&room, "SELECT body FROM msgs")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["body"], "durable");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn noop_writes_dont_ship_either() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(SlowStore(
            FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            std::time::Duration::from_millis(300),
        ));
        let node = boot_with_store(dir.path(), store, 4).await;
        let room = id_on_worker(1, 4, "room");
        make_channel(&node, &room).await;
        let ships_before = node.stats().await.ships;

        // Matches zero rows -> no page changes -> counter unmoved -> free.
        let started = std::time::Instant::now();
        exec(
            &node,
            &[&room],
            &[(&room, "UPDATE msgs SET body = 'x' WHERE id = 999")],
        )
        .await
        .unwrap();
        assert!(started.elapsed() < std::time::Duration::from_millis(150));
        assert_eq!(node.stats().await.ships, ships_before);

        // A write that actually changes bytes still ships.
        publish(&node, &room, "real", false).await;
        assert_eq!(node.stats().await.ships, ships_before + 1);
        node.shutdown().await;
    }

    // ==================================================== capability verbs
    //
    // Inserts, updates, and deletes are different powers. Enforcement is
    // SQLite's authorizer at prepare time — CTEs and trigger cascades are
    // classified by the engine itself, not by keyword sniffing.

    fn test_cap(objects: &str, verbs: &[&str]) -> Arc<crate::grants::Capability> {
        Arc::new(crate::grants::Capability {
            grants: vec![crate::grants::Grant {
                objects: objects.into(),
                verbs: verbs.iter().map(|s| s.to_string()).collect(),
            }],
            exp: u64::MAX,
            sub: None,
        })
    }

    async fn exec_as(
        node: &Node,
        cap: Arc<crate::grants::Capability>,
        object: &str,
        sql: &str,
    ) -> Result<TxnResponse, ApiError> {
        submit_as(
            node,
            Some(cap),
            vec![object.to_string()],
            vec![Op {
                object: object.to_string(),
                sql: sql.to_string(),
                params: vec![],
            }],
            false,
            false,
        )
        .await
    }

    #[tokio::test]
    async fn insert_only_tokens_append_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let log = id_on_worker(1, 4, "log");
        exec(
            &node,
            &[&log],
            &[(&log, "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)")],
        )
        .await
        .unwrap();

        let appender = test_cap(&log, &["insert"]);
        exec_as(&node, appender.clone(), &log, "INSERT INTO events (body) VALUES ('e1')")
            .await
            .unwrap();

        for sql in [
            "UPDATE events SET body = 'rewritten' WHERE id = 1",
            "DELETE FROM events",
            "DROP TABLE events",
            "CREATE TABLE sneaky (x)",
            "PRAGMA journal_mode = DELETE",
        ] {
            let err = exec_as(&node, appender.clone(), &log, sql).await.unwrap_err();
            assert_eq!(
                err.status,
                axum::http::StatusCode::BAD_REQUEST,
                "{sql} must be denied"
            );
            assert!(
                err.message.contains("not authorized") || err.message.contains("rolled back"),
                "{sql}: {}",
                err.message
            );
        }

        // History intact, append still works.
        exec_as(&node, appender, &log, "INSERT INTO events (body) VALUES ('e2')")
            .await
            .unwrap();
        let res = exec(&node, &[&log], &[(&log, "SELECT COUNT(*) AS c FROM events")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 2);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn trigger_cascades_cannot_smuggle_verbs() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let obj = id_on_worker(2, 4, "audited");
        // Root installs a trigger: inserting into inbox DELETEs from audit.
        // An insert-only token inserting into inbox would transitively
        // delete — the authorizer sees the trigger's DELETE at prepare
        // time and refuses the whole statement.
        exec(
            &node,
            &[&obj],
            &[
                (&obj, "CREATE TABLE inbox (body TEXT)"),
                (&obj, "CREATE TABLE audit (note TEXT)"),
                (&obj, "INSERT INTO audit VALUES ('important')"),
                (
                    &obj,
                    "CREATE TRIGGER purge AFTER INSERT ON inbox BEGIN DELETE FROM audit; END",
                ),
            ],
        )
        .await
        .unwrap();

        let appender = test_cap(&obj, &["insert"]);
        let err = exec_as(&node, appender, &obj, "INSERT INTO inbox VALUES ('hi')")
            .await
            .unwrap_err();
        assert!(
            err.message.contains("not authorized"),
            "trigger cascade must be caught: {}",
            err.message
        );

        // With delete granted too, the same statement (and its cascade) runs.
        let both = test_cap(&obj, &["insert", "delete"]);
        exec_as(&node, both, &obj, "INSERT INTO inbox VALUES ('hi')")
            .await
            .unwrap();
        let res = exec(&node, &[&obj], &[(&obj, "SELECT COUNT(*) AS c FROM audit")])
            .await
            .unwrap();
        let v = serde_json::to_value(&res.results).unwrap();
        assert_eq!(v[0]["rows"][0]["c"], 0, "cascade ran once authorized");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn update_tokens_can_read_their_where_clause_but_ctes_cant_sneak_inserts() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 4, ClaimSpec::All, "a").await;
        let obj = id_on_worker(1, 4, "acct2");
        make_account(&node, &obj).await; // balance 100

        // UPDATE needs to read the WHERE column: allowed for any verb
        // holder on the object (you can't meaningfully update blind).
        let updater = test_cap(&obj, &["update"]);
        exec_as(
            &node,
            updater.clone(),
            &obj,
            "UPDATE account SET balance = 50 WHERE balance = 100",
        )
        .await
        .unwrap();
        assert_eq!(balance(&node, &obj).await, 50);

        // A WITH ... INSERT is still an INSERT, whatever it starts with.
        let err = exec_as(
            &node,
            updater,
            &obj,
            "WITH x(v) AS (SELECT 1) INSERT INTO account SELECT v FROM x",
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("not authorized"), "{}", err.message);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn cross_object_txns_enforce_per_object_verbs() {
        let dir = tempfile::tempdir().unwrap();
        let node = boot(dir.path(), 8, ClaimSpec::All, "a").await;
        let acct = id_on_worker(1, 8, "acct3");
        let outbox = id_on_worker(6, 8, "outbox3");
        make_account(&node, &acct).await;
        make_channel(&node, &outbox).await;

        // update on the account, insert on the channel: the outbox shape,
        // with each object's connection gated by its own verbs.
        let cap = Arc::new(crate::grants::Capability {
            grants: vec![
                crate::grants::Grant {
                    objects: acct.clone(),
                    verbs: vec!["update".into()],
                },
                crate::grants::Grant {
                    objects: outbox.clone(),
                    verbs: vec!["insert".into()],
                },
            ],
            exp: u64::MAX,
            sub: None,
        });
        submit_as(
            &node,
            Some(cap.clone()),
            vec![acct.clone(), outbox.clone()],
            vec![
                Op {
                    object: acct.clone(),
                    sql: "UPDATE account SET balance = balance - 10".into(),
                    params: vec![],
                },
                Op {
                    object: outbox.clone(),
                    sql: "INSERT INTO msgs (body) VALUES ('spent 10')".into(),
                    params: vec![],
                },
            ],
            false,
            false,
        )
        .await
        .unwrap();

        // Swap the verbs across objects: both directions must fail.
        let err = submit_as(
            &node,
            Some(cap),
            vec![acct.clone(), outbox.clone()],
            vec![
                Op {
                    object: acct.clone(),
                    sql: "INSERT INTO account (balance) VALUES (5)".into(),
                    params: vec![],
                },
                Op {
                    object: outbox,
                    sql: "INSERT INTO msgs (body) VALUES ('x')".into(),
                    params: vec![],
                },
            ],
            false,
            false,
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("not authorized"), "{}", err.message);
        assert_eq!(balance(&node, &acct).await, 90, "the atomic txn rolled back whole");
        node.shutdown().await;
    }
}
