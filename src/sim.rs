//! Deterministic simulation testing (DST): a whole fafo cluster in one
//! thread, under a virtual clock, with seeded faults everywhere — and a
//! set of oracles that panic the moment any promise the system makes is
//! broken. Same seed, same run, bit for bit; a crash IS a repro.
//!
//! The moving parts:
//!   SimStore     the cluster's blob store: in-memory truth + seeded
//!                latency and write-failure windows
//!   SimNet       the "network": routes rpc calls straight into the
//!                target node's handler, with delays, partitions, and
//!                dead-node refusals
//!   World        boots nodes, schedules faults, runs the workload,
//!                keeps the client-side model the oracles audit against
//!
//! Determinism rests on: one current_thread runtime with paused time
//! (virtual clocks, instant deadlock detection), no sockets, no uuids,
//! fixed-key hash maps, and a single SplitMix64 stream consumed in
//! scheduler order. `dst check` proves it per seed by running twice and
//! comparing trace hashes.

use crate::api::{self, Auth, Session};
use crate::cluster::{self, ClaimSpec, Node, NodeConfig, Op};
use crate::rpc::{Request, Response, Transport};
use crate::store::{BlobStore, MemBlobStore};
use crate::{Map, Set};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout};

// ------------------------------------------------------------------ rng

/// SplitMix64: tiny, seedable, and good enough to storm a cluster with.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    #[allow(clippy::should_implement_trait)] // a seeded generator, not an iterator
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    /// True with probability pct/100.
    pub fn chance(&mut self, pct: u32) -> bool {
        self.next() % 100 < pct as u64
    }

    /// Uniform in [0, n).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

type SharedRng = Arc<Mutex<Rng>>;

fn chance(rng: &SharedRng, pct: u32) -> bool {
    rng.lock().unwrap().chance(pct)
}

fn below(rng: &SharedRng, n: u64) -> u64 {
    rng.lock().unwrap().below(n)
}

// --------------------------------------------------------------- config

/// Everything a run is, in one reproducible bundle. JSON on disk, seed on
/// the command line.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DstConfig {
    pub seed: u64,
    pub nodes: usize,
    pub logical_workers: usize,
    pub accounts: usize,
    pub initial_balance: i64,
    /// Transfer transactions per workload phase (two phases per run).
    pub transfers: usize,
    pub optimistic_pct: u32,
    /// Concurrent transfer clients.
    pub clients: usize,
    /// Messages the publisher pushes through the pub/sub channel.
    pub channel_msgs: usize,
    /// Will sessions per phase: half die politely, half ride a node crash.
    pub wills: usize,
    /// Max virtual latency per blob op / per rpc call, in ms.
    pub store_latency_ms: u64,
    pub net_latency_ms: u64,
    /// During a storage fault window, % of writes that fail.
    pub store_fail_pct: u32,
    /// Node crashes over the run (restarts follow if `restarts`).
    pub crashes: usize,
    pub restarts: bool,
    pub fence_ttl_ms: u64,
    /// Liveness bound: any single submission hanging longer than this
    /// (virtual) is a deadlock, and a crash.
    pub op_timeout_ms: u64,
    /// How long an unrefreshed durable will lives before a sweeper fires
    /// it. The oracle waits a few multiples of this for crash-orphaned
    /// wills to land before declaring one lost.
    pub will_ttl_ms: u64,
    /// The canary, now the contract: a will armed on a node that CRASHES
    /// must still fire, because durable wills persist it and a surviving
    /// node's sweeper fires it. Default true — this is the property the
    /// durable-will machinery exists to hold. Set false only to silence
    /// the will oracle while investigating something else.
    pub wills_survive_node_crash: bool,
    /// Persist wills durably (the fix). Turn off to reproduce the original
    /// memory-only bug: `dst run --no-durable-wills` fires the will oracle,
    /// the default (on) satisfies it — the fix, proven both directions.
    pub durable_wills: bool,

    // ---- the multi-workload battery (all run CONCURRENTLY, both phases)
    /// ERC20 token accounts (`tok-{i}`) + a supply object. Invariant: the
    /// sum of balances equals total supply equals the supply ledger — a
    /// conservation law whose reference total itself moves (mint/burn).
    #[serde(default = "d_erc20_accounts")]
    pub erc20_accounts: usize,
    /// ERC20 ops per phase (mint / burn / transfer mix).
    #[serde(default = "d_erc20_ops")]
    pub erc20_ops: usize,
    /// Escrow sagas per phase: open (funder -> escrow object), then settle
    /// (release to payee | refund to funder) — with deliberate RACES on
    /// half of them: both directions attempted concurrently, exactly one
    /// may win. Exactly-once settlement is the sharpest anti-fork oracle.
    #[serde(default = "d_escrows")]
    pub escrows: usize,
    /// Idempotent counters (`ctr-{i}`): n must equal COUNT(incs) always;
    /// retries dedup through the UNIQUE key — the documented idempotency
    /// pattern, now an oracle.
    #[serde(default = "d_counters")]
    pub counters: usize,
    #[serde(default = "d_counter_incs")]
    pub counter_incs: usize,
    /// Per-node clock-rate skew, in percent around 1.0: each node boots
    /// with a rate drawn from [1-skew, 1+skew]. The fencing model's stated
    /// assumption is bounded RATE error; the supported envelope (±8%,
    /// ratio ≤ 1.17) sits inside the engine's SKEW_TOLERANCE (1.25).
    #[serde(default = "d_clock_skew_pct")]
    pub clock_skew_pct: u32,
    /// Pause faults per run: a live node is isolated (RPC-dead to peers,
    /// still running) long enough for a peer to take over its lease, then
    /// healed — the classic "zombie loses its lease and rejoins" adversary.
    /// Correct fencing must keep the zombie from writing after takeover.
    #[serde(default = "d_pause_faults")]
    pub pause_faults: usize,
    /// Chaos: pauses ALSO freeze the paused node's clock (host-suspend /
    /// VM-pause), so on wake it believes ~no time passed. This can defeat
    /// clock-based fencing (a documented limitation) — on by default only
    /// under --clock-chaos.
    #[serde(default)]
    pub pause_freezes_clock: bool,
    /// Chaos mode: rates in [0.6, 1.6] (ratio up to 2.67), deliberately
    /// beyond SKEW_TOLERANCE. This is `--no-durable-wills` for clocks: the
    /// run is EXPECTED to fork, proving the simulator can see clock-rate
    /// violations at all. Never on by default.
    #[serde(default)]
    pub clock_chaos: bool,
    /// Watched feeds (`feed-{i}`), each consumed three ways at once:
    /// a cursor poll loop, a DURABLE cursor loop (its deliveries must
    /// survive any crash), and a change-detection (baseline-hash) watcher.
    #[serde(default = "d_feeds")]
    pub feeds: usize,
    #[serde(default = "d_feed_appends")]
    pub feed_appends: usize,
}

fn d_erc20_accounts() -> usize { 6 }
fn d_erc20_ops() -> usize { 40 }
fn d_escrows() -> usize { 8 }
fn d_counters() -> usize { 4 }
fn d_counter_incs() -> usize { 30 }
fn d_clock_skew_pct() -> u32 { 8 }
fn d_pause_faults() -> usize { 1 }
fn d_feeds() -> usize { 2 }
fn d_feed_appends() -> usize { 15 }

impl Default for DstConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            nodes: 3,
            logical_workers: 16,
            accounts: 10,
            initial_balance: 100,
            transfers: 120,
            optimistic_pct: 50,
            clients: 4,
            channel_msgs: 30,
            wills: 4,
            store_latency_ms: 4,
            net_latency_ms: 4,
            store_fail_pct: 20,
            crashes: 2,
            restarts: true,
            fence_ttl_ms: 1000,
            op_timeout_ms: 120_000,
            will_ttl_ms: 2000,
            wills_survive_node_crash: true,
            durable_wills: true,
            erc20_accounts: d_erc20_accounts(),
            erc20_ops: d_erc20_ops(),
            escrows: d_escrows(),
            counters: d_counters(),
            counter_incs: d_counter_incs(),
            feeds: d_feeds(),
            feed_appends: d_feed_appends(),
            clock_skew_pct: d_clock_skew_pct(),
            clock_chaos: false,
            pause_faults: d_pause_faults(),
            pause_freezes_clock: false,
        }
    }
}

impl DstConfig {
    /// Derive a whole cluster shape from the seed alone. Every knob is
    /// randomized within a range that stays *legal but hostile* — small
    /// fence windows against high latency, degenerate single-node worlds,
    /// fault storms up to 90%, tight worker spaces that force churn. The
    /// seed determines both the config and the run, so a crash is still
    /// replayable from the seed and `--fuzz` alone. This is parameter-space
    /// search on top of schedule-space search: the default config is one
    /// point; this explores the rest.
    pub fn fuzzed(seed: u64) -> Self {
        let mut r = Rng::new(seed ^ CONFIG_SALT);
        let nodes = 1 + r.below(5) as usize; // 1..=5
        let logical_workers = [4usize, 8, 16, 16, 32][r.below(5) as usize];
        let net_latency_ms = 1 + r.below(30);
        let store_latency_ms = 1 + r.below(30);
        // Fence TTL stays within the supported operating envelope: below
        // ~2x the guard sweep (fence_ttl/3) fencing has no safety margin,
        // which is a MISCONFIGURATION, not a bug to hunt. Everything at or
        // above 800ms is legal and hammered.
        let fence_ttl_ms = [800u64, 1200, 1800, 2400, 3000][r.below(5) as usize];
        Self {
            seed,
            nodes,
            logical_workers,
            accounts: 6 + r.below(8) as usize,
            initial_balance: 100,
            transfers: 40 + r.below(120) as usize,
            optimistic_pct: r.below(101) as u32,
            clients: 2 + r.below(5) as usize,
            channel_msgs: 10 + r.below(30) as usize,
            wills: r.below(6) as usize,
            store_latency_ms,
            net_latency_ms,
            store_fail_pct: r.below(60) as u32,
            crashes: r.below(4) as usize,
            // Restarts always on: the platform (Cloudflare Containers) always
            // reschedules a crashed instance. Permanent node loss with no
            // replacement is out of the operating envelope — the orphan
            // reclaim sweep heals it, but "fewer nodes forever" is not a
            // supported steady state to fuzz against.
            restarts: true,
            fence_ttl_ms,
            op_timeout_ms: 120_000,
            will_ttl_ms: fence_ttl_ms.max(1000) * 2,
            wills_survive_node_crash: true,
            durable_wills: true,
            erc20_accounts: 4 + r.below(6) as usize,
            erc20_ops: 20 + r.below(40) as usize,
            escrows: r.below(12) as usize,
            counters: 1 + r.below(5) as usize,
            counter_incs: 10 + r.below(40) as usize,
            feeds: 1 + r.below(3) as usize,
            feed_appends: 8 + r.below(20) as usize,
            clock_skew_pct: r.below(9) as u32, // 0..=8%: inside SKEW_TOLERANCE
            clock_chaos: false,
            // The pause adversary IS in the envelope: a live node loses its
            // lease (partition + a peer's reboot-takeover) and rejoins.
            // Fencing must keep its post-takeover writes out (commit-point
            // verify) AND its refused writes must not survive in the
            // commuter cache (fetch_image bounds the cache to durable).
            pause_faults: r.below(3) as usize,
            pause_freezes_clock: false,
        }
    }
}

/// A fixed salt so a fuzzed config's internal RNG stream doesn't alias the
/// run's own RNG stream (which is seeded by `seed` directly).
const CONFIG_SALT: u64 = 0x00c0_ffee_f00d_face;

// ---------------------------------------------------------------- trace

/// A running hash of everything a client could observe, plus a tail of
/// recent events for the crash report. Two runs of one seed must produce
/// the same hash — `dst check` enforces it.
pub struct Trace {
    state: Mutex<(u64, u64, Vec<String>)>, // (hash, events, tail)
}

impl Trace {
    fn new() -> Self {
        Self {
            state: Mutex::new((0xcbf29ce484222325, 0, Vec::new())),
        }
    }

    pub fn record(&self, event: String) {
        if std::env::var_os("FAFO_DST_LOG").is_some() {
            eprintln!("[dst] {event}");
        }
        let mut s = self.state.lock().unwrap();
        for b in event.as_bytes() {
            s.0 ^= *b as u64;
            s.0 = s.0.wrapping_mul(0x100000001b3);
        }
        s.1 += 1;
        s.2.push(event);
        if s.2.len() > 400 {
            s.2.remove(0);
        }
    }

    pub fn hash(&self) -> u64 {
        self.state.lock().unwrap().0
    }

    pub fn events(&self) -> u64 {
        self.state.lock().unwrap().1
    }

    pub fn dump_tail(&self) -> String {
        self.state.lock().unwrap().2.join("\n")
    }
}

// ------------------------------------------------------------- sim store

/// The cluster's shared blob store: in-memory truth, seeded latency on
/// everything, seeded failures on writes while the fault window is open.
/// (Reads stay reliable-but-slow: a store that can't be read at all just
/// stalls the world, which is a less interesting bug surface than writes
/// that vanish.)
pub struct SimStore {
    inner: MemBlobStore,
    rng: SharedRng,
    latency_ms: u64,
    fail_pct: u32,
    failing: AtomicBool,
}

impl SimStore {
    fn new(rng: SharedRng, cfg: &DstConfig) -> Self {
        Self {
            inner: MemBlobStore::default(),
            rng,
            latency_ms: cfg.store_latency_ms,
            fail_pct: cfg.store_fail_pct,
            failing: AtomicBool::new(false),
        }
    }

    pub fn set_failing(&self, on: bool) {
        self.failing.store(on, Ordering::SeqCst);
    }

    async fn delay(&self) {
        if self.latency_ms > 0 {
            let ms = below(&self.rng, self.latency_ms + 1);
            sleep(Duration::from_millis(ms)).await;
        }
    }

    fn maybe_fail(&self, op: &str, key: &str) -> anyhow::Result<()> {
        if self.failing.load(Ordering::SeqCst) && chance(&self.rng, self.fail_pct) {
            anyhow::bail!("simulated store fault: {op} {key}");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BlobStore for SimStore {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.delay().await;
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.delay().await;
        self.maybe_fail("put", key)?;
        self.inner.put(key, bytes).await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.delay().await;
        self.maybe_fail("delete", key)?;
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        self.delay().await;
        self.inner.list(prefix).await
    }

    async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
        self.delay().await;
        self.maybe_fail("create", key)?;
        self.inner.create(key, bytes).await
    }

    async fn version(&self, key: &str) -> anyhow::Result<u64> {
        self.delay().await;
        self.inner.version(key).await
    }

    async fn put_cas(&self, key: &str, expected: u64, bytes: &[u8]) -> anyhow::Result<Option<u64>> {
        // Delay and fault happen on the request; the CAS itself stays atomic
        // in the inner store (no await between check and write).
        self.delay().await;
        self.maybe_fail("put_cas", key)?;
        self.inner.put_cas(key, expected, bytes).await
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> anyhow::Result<Option<Vec<u8>>> {
        self.delay().await;
        self.inner.get_range(key, offset, len).await
    }
}

// --------------------------------------------------------------- sim net

/// The network between nodes: a routing table straight into each node's
/// rpc handler, with seeded delay, directional partitions, and refusal
/// for the dead. One SimTransport per node knows who "me" is, so
/// partitions can be asymmetric — the nasty kind.
pub struct SimNet {
    nodes: Mutex<Map<String, Node>>,
    /// Directional blocks: (from, to) pairs that drop everything.
    blocked: Mutex<Set<(String, String)>>,
    rng: SharedRng,
    latency_ms: u64,
}

impl SimNet {
    fn new(rng: SharedRng, cfg: &DstConfig) -> Self {
        Self {
            nodes: Mutex::new(Map::default()),
            blocked: Mutex::new(Set::default()),
            rng,
            latency_ms: cfg.net_latency_ms,
        }
    }

    fn register(&self, addr: &str, node: Node) {
        self.nodes.lock().unwrap().insert(addr.to_string(), node);
    }

    fn deregister(&self, addr: &str) {
        self.nodes.lock().unwrap().remove(addr);
    }

    pub fn partition(&self, a: &str, b: &str) {
        let mut blocked = self.blocked.lock().unwrap();
        blocked.insert((a.to_string(), b.to_string()));
        blocked.insert((b.to_string(), a.to_string()));
    }

    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }

    /// Cut one node off from every current peer (both directions): it can
    /// still reach the store, but no RPC in or out. Peers health-check it
    /// as dead; the node keeps running.
    pub fn isolate(&self, addr: &str) {
        let peers: Vec<String> = self.nodes.lock().unwrap().keys().cloned().collect();
        let mut blocked = self.blocked.lock().unwrap();
        for p in peers {
            if p != addr {
                blocked.insert((addr.to_string(), p.clone()));
                blocked.insert((p, addr.to_string()));
            }
        }
    }

    fn reachable(&self, from: &str, to: &str) -> Option<Node> {
        if self
            .blocked
            .lock()
            .unwrap()
            .contains(&(from.to_string(), to.to_string()))
        {
            return None;
        }
        self.nodes.lock().unwrap().get(to).cloned()
    }

    async fn delay(&self) {
        if self.latency_ms > 0 {
            let ms = below(&self.rng, self.latency_ms + 1);
            sleep(Duration::from_millis(ms)).await;
        }
    }
}

pub struct SimTransport {
    net: Arc<SimNet>,
    me: String,
}

#[async_trait::async_trait]
impl Transport for SimTransport {
    async fn call(&self, base: &str, req: &Request) -> anyhow::Result<Response> {
        self.net.delay().await;
        let Some(target) = self.net.reachable(&self.me, base) else {
            anyhow::bail!("rpc to {base}: unreachable");
        };
        // A serde round trip, exactly like the wire would do — it also
        // keeps the types honest about being serializable. Boxing the
        // handler is the task boundary HTTP would have provided: it stops
        // caller and callee futures from nesting into one giant frame.
        let req: Request = serde_json::from_value(serde_json::to_value(req)?)?;
        Ok(Box::pin(api::handle_rpc(&target, req)).await)
    }

    async fn health(&self, base: &str) -> bool {
        self.net.delay().await;
        self.net.reachable(&self.me, base).is_some()
    }
}

// ---------------------------------------------------------------- model

/// What the clients believe happened — the ground truth oracles audit
/// the cluster against.
#[derive(Default)]
struct Model {
    /// Every transfer ever issued: key -> (from, to, acked, pessimistic-acked).
    transfers: Mutex<Vec<TransferRecord>>,
    /// Channel publishes: key -> acked.
    publishes: Mutex<Vec<(String, bool)>>,
    /// Armed wills and how their connection ended.
    wills: Mutex<Vec<WillCase>>,
    /// Generic multi-leg atomic ops (ERC20): each leg is a ledger row
    /// (object, amount) that must exist with the others or not at all.
    ops: Mutex<Vec<OpRecord>>,
    /// Escrow sagas: key -> (funder, payee, amount, open_acked).
    escrows: Mutex<Vec<EscrowRecord>>,
    /// Idempotent counter increments: (counter object, key, acked, optimistic).
    incs: Mutex<Vec<(String, String, bool, bool)>>,
    /// Feed appends: (feed, key, acked, optimistic).
    appends: Mutex<Vec<(String, String, bool, bool)>>,
    /// Rows delivered by DURABLE polls: (feed, id, key). The contract:
    /// a durable delivery can never un-happen — every one of these must
    /// exist in the final table, crashes and all.
    durable_deliveries: Mutex<Vec<(String, i64, String)>>,
}

struct OpRecord {
    key: String,
    legs: Vec<(String, i64)>,
    acked: bool,
    optimistic: bool,
}

struct EscrowRecord {
    key: String,
    funder: String,
    payee: String,
    amount: i64,
    open_acked: bool,
}

struct TransferRecord {
    key: String,
    from: String,
    to: String,
    acked: bool,
    optimistic: bool,
}

struct WillCase {
    key: String,
    /// The node the session was opened against.
    node: usize,
    /// How the connection died: false = polite close (server alive),
    /// true = the node crashed under it.
    node_crashed: bool,
}

// ---------------------------------------------------------------- world

pub struct World {
    pub cfg: DstConfig,
    rng: SharedRng,
    pub store: Arc<SimStore>,
    pub net: Arc<SimNet>,
    nodes: Mutex<Vec<Option<Node>>>,
    generations: Mutex<Vec<u32>>,
    dir: tempfile::TempDir,
    pub trace: Arc<Trace>,
    /// The shared virtual clock every node's will deadlines measure
    /// against. Captured once, inside the runtime, so it advances with
    /// tokio's paused time.
    clock_base: tokio::time::Instant,
    model: Model,
}

pub struct RunReport {
    pub seed: u64,
    pub events: u64,
    pub trace_hash: u64,
}

fn addr_of(i: usize) -> String {
    format!("sim://n{i}")
}

impl World {
    fn new(cfg: DstConfig) -> Self {
        let rng: SharedRng = Arc::new(Mutex::new(Rng::new(cfg.seed)));
        let store = Arc::new(SimStore::new(rng.clone(), &cfg));
        let net = Arc::new(SimNet::new(rng.clone(), &cfg));
        let nodes = Mutex::new((0..cfg.nodes).map(|_| None).collect());
        let generations = Mutex::new(vec![0; cfg.nodes]);
        Self {
            rng,
            store,
            net,
            nodes,
            generations,
            dir: tempfile::tempdir().expect("tempdir"),
            trace: Arc::new(Trace::new()),
            clock_base: tokio::time::Instant::now(),
            model: Model::default(),
            cfg,
        }
    }

    async fn boot_node(&self, i: usize) -> anyhow::Result<()> {
        let quota = self.cfg.logical_workers.div_ceil(self.cfg.nodes);
        self.boot_node_claiming(i, ClaimSpec::Auto(quota)).await
    }

    async fn boot_node_claiming(&self, i: usize, claim: ClaimSpec) -> anyhow::Result<()> {
        let generation = {
            let mut g = self.generations.lock().unwrap();
            g[i] += 1;
            g[i]
        };
        let addr = addr_of(i);
        // Each boot draws this node's oscillator error. Chaos mode blows
        // straight past the engine's tolerance — expected to fork.
        let clock_rate = if self.cfg.clock_chaos {
            0.6 + (below(&self.rng, 101) as f64) / 100.0 // 0.60..=1.60
        } else if self.cfg.clock_skew_pct > 0 {
            let span = self.cfg.clock_skew_pct.min(50) as f64 / 100.0;
            1.0 - span + (below(&self.rng, 1001) as f64) / 1000.0 * 2.0 * span
        } else {
            1.0
        };
        let node = cluster::start(NodeConfig {
            logical: self.cfg.logical_workers,
            claim,
            advertise: Some(addr.clone()),
            hysteresis: 8,
            secret: "dst".into(),
            clock_rate,
            fence_ttl: Duration::from_millis(self.cfg.fence_ttl_ms),
            transport: Some(Arc::new(SimTransport {
                net: self.net.clone(),
                me: addr.clone(),
            })),
            serve_http: false,
            exit_on_fence: false,
            // Virtual time is the only meaningful clock in here; the wall
            // check would fence healthy nodes whenever the host is busy.
            wall_fence: false,
            will_ttl: Duration::from_millis(self.cfg.will_ttl_ms),
            durable_wills: self.cfg.durable_wills,
            // Every node measures will deadlines against one shared virtual
            // clock, so deadlines are comparable across the cluster AND the
            // run replays identically.
            clock_base: Some(self.clock_base),
            ..NodeConfig::new(
                self.store.clone() as Arc<dyn BlobStore>,
                self.dir.path().join(format!("n{i}-g{generation}")),
            )
        })
        .await?;
        self.net.register(&addr, node.clone());
        self.nodes.lock().unwrap()[i] = Some(node);
        self.trace.record(format!("boot n{i} g{generation}"));
        Ok(())
    }

    /// The pause adversary: isolate a live node (RPC-dead to peers, still
    /// running and still reaching the store), force a peer to take over its
    /// lease during the blackout, then heal. A correctly-fenced zombie must
    /// NOT commit anything after the takeover — its guard sees the superseded
    /// epoch and fail-stops, or its commit gate refuses. Optionally freezes
    /// the zombie's clock too (suspend), which can defeat clock fencing.
    async fn pause_fault(self: &Arc<Self>) {
        let live = self.live_indices();
        if live.len() < 2 {
            return; // need a survivor to take over
        }
        let victim = live[below(&self.rng, live.len() as u64) as usize];
        let taker = *live.iter().find(|&&i| i != victim).unwrap();
        let Some(vnode) = self.node(victim) else { return };

        self.net.isolate(&addr_of(victim));
        if self.cfg.pause_freezes_clock {
            vnode.node_clock.freeze();
        }
        self.trace.record(format!(
            "pause n{victim} (freeze={})",
            self.cfg.pause_freezes_clock
        ));

        // Force the takeover: bounce the taker so its boot-claim scans the
        // blocks, finds the victim RPC-dead, waits out the fence TTL, and
        // adopts them. (Reboot is how the safe system takes over at all.)
        self.crash_node(taker);
        sleep(Duration::from_millis(self.cfg.fence_ttl_ms * 3)).await;
        // Reboot the taker with its NORMAL auto quota (a real node claims
        // its share, never All): it adopts the isolated victim's blocks as
        // dead-holder orphans up to quota.
        let _ = self.boot_node(taker).await;
        // Let the taker actually write to the reclaimed objects.
        sleep(Duration::from_millis(self.cfg.fence_ttl_ms)).await;

        // Zombie wakes. If fencing is wrong, its next writes fork.
        if self.cfg.pause_freezes_clock {
            vnode.node_clock.thaw();
        }
        self.net.heal();
        self.trace.record(format!("resume n{victim}"));
        // The healed victim sees its leases superseded and fail-stops
        // (crashes internally). Model the platform rescheduling it: crash it
        // in the world's books and reboot, so its orphaned blocks get
        // reclaimed and objects there become reachable again.
        sleep(Duration::from_millis(self.cfg.fence_ttl_ms)).await;
        self.crash_node(victim);
        sleep(Duration::from_millis(self.cfg.fence_ttl_ms * 2)).await;
        let _ = self.boot_node(victim).await;
    }

    fn crash_node(&self, i: usize) {
        let node = self.nodes.lock().unwrap()[i].take();
        if let Some(node) = node {
            self.net.deregister(&addr_of(i));
            node.crash();
            self.trace.record(format!("crash n{i}"));
            // Sessions opened against this node die with it: their wills
            // were process memory. Mark them so the will oracle knows.
            for w in self.model.wills.lock().unwrap().iter_mut() {
                if w.node == i && !w.node_crashed {
                    w.node_crashed = true;
                }
            }
        }
    }

    fn any_node(&self) -> Option<Node> {
        let nodes = self.nodes.lock().unwrap();
        let live: Vec<&Node> = nodes.iter().flatten().collect();
        if live.is_empty() {
            return None;
        }
        let pick = below(&self.rng, live.len() as u64) as usize;
        Some(live[pick].clone())
    }

    fn node(&self, i: usize) -> Option<Node> {
        self.nodes.lock().unwrap()[i].clone()
    }

    fn live_indices(&self) -> Vec<usize> {
        let nodes = self.nodes.lock().unwrap();
        (0..nodes.len()).filter(|&i| nodes[i].is_some()).collect()
    }

    // ------------------------------------------------------- primitives

    /// Submit with node fallback + bounded retries. Returns Ok(true) if
    /// acked (including "already landed" via the idempotency constraint),
    /// Ok(false) if abandoned, and panics — the liveness oracle — if any
    /// single submission hangs.
    async fn submit_retry(
        &self,
        what: &str,
        objects: Vec<String>,
        ops: Vec<Op>,
        read_only: bool,
        optimistic: bool,
    ) -> anyhow::Result<Option<crate::cluster::TxnResponse>> {
        for _attempt in 0..60 {
            let Some(node) = self.any_node() else {
                sleep(Duration::from_millis(200)).await;
                continue;
            };
            let fut = cluster::submit(&node, objects.clone(), ops.clone(), read_only, optimistic);
            match timeout(Duration::from_millis(self.cfg.op_timeout_ms), fut).await {
                Err(_) => {
                    let mut dumps = String::new();
                    for i in self.live_indices() {
                        if let Some(n) = self.node(i) {
                            dumps.push_str(&n.debug_dump().await);
                        }
                    }
                    // For each object in the hung txn, dump the DURABLE claim
                    // and whether the named owner is a LIVE node — a take that
                    // hangs on a block owned by a dead node is the orphan-
                    // reclaim gap, and this line names it directly.
                    let mut claims = String::new();
                    for o in &objects {
                        let claim = cluster::durable_claim(self.store.as_ref(), o).await;
                        claims.push_str(&format!("  {o}: {claim:?}\n"));
                    }
                    panic!(
                        "LIVENESS: {what} hung for {}ms virtual — deadlock.\nworkers:\n{dumps}durable claims:\n{claims}trace tail:\n{}",
                        self.cfg.op_timeout_ms,
                        self.trace.dump_tail()
                    )
                }
                Ok(Ok(resp)) => return Ok(Some(resp)),
                Ok(Err(e)) if e.message.contains("UNIQUE") => {
                    // The retry raced an earlier success: it landed.
                    return Ok(None);
                }
                Ok(Err(e)) if e.message.contains("CHECK") => {
                    // Business rejection (overdraft): final, not a fault.
                    anyhow::bail!("rejected: {}", e.message);
                }
                Ok(Err(_)) => {
                    let backoff = 50 + below(&self.rng, 200);
                    sleep(Duration::from_millis(backoff)).await;
                }
            }
        }
        anyhow::bail!("abandoned after retries")
    }

    /// Read rows from one object, retrying across nodes. Panics on hang.
    async fn read(&self, object: &str, sql: &str) -> Vec<Value> {
        for _attempt in 0..120 {
            let Some(node) = self.any_node() else {
                sleep(Duration::from_millis(200)).await;
                continue;
            };
            let ops = vec![Op {
                object: object.to_string(),
                sql: sql.to_string(),
                params: vec![],
            }];
            let fut = cluster::submit(&node, vec![object.to_string()], ops, true, false);
            match timeout(Duration::from_millis(self.cfg.op_timeout_ms), fut).await {
                Err(_) => {
                    let claim = cluster::durable_claim(self.store.as_ref(), object).await;
                    let mut dumps = String::new();
                    for i in self.live_indices() {
                        if let Some(n) = self.node(i) {
                            dumps.push_str(&n.debug_dump().await);
                        }
                    }
                    panic!(
                        "LIVENESS: read of {object} hung. durable_claim={claim:?}\nnodes:\n{dumps}trace tail:\n{}",
                        self.trace.dump_tail()
                    )
                }
                Ok(Ok(mut resp)) => match resp.results.pop() {
                    Some(crate::cluster::OpResult::Rows { rows }) => return rows,
                    _ => panic!("read of {object} returned no rows result"),
                },
                Ok(Err(_)) => sleep(Duration::from_millis(100)).await,
            }
        }
        panic!(
            "read of {object} kept failing with live nodes present.\ntrace tail:\n{}",
            self.trace.dump_tail()
        );
    }

    // -------------------------------------------------------- workload

    async fn create_schemas(&self) {
        for a in 0..self.cfg.accounts {
            let object = account_name(a);
            let ops = vec![
                op(&object, "CREATE TABLE IF NOT EXISTS account (balance INTEGER NOT NULL CHECK (balance >= 0))", vec![]),
                op(&object, "INSERT INTO account (balance) SELECT ?1 WHERE NOT EXISTS (SELECT 1 FROM account)", vec![json!(self.cfg.initial_balance)]),
                op(&object, "CREATE TABLE IF NOT EXISTS writes (k TEXT PRIMARY KEY, amt INTEGER)", vec![]),
            ];
            self.submit_retry(&format!("schema {object}"), vec![object.clone()], ops, false, false)
                .await
                .expect("schema creation must eventually succeed");
        }
        for (object, schema) in [
            ("chan", "CREATE TABLE IF NOT EXISTS msgs (id INTEGER PRIMARY KEY AUTOINCREMENT, k TEXT UNIQUE)"),
            ("graveyard", "CREATE TABLE IF NOT EXISTS g (k TEXT PRIMARY KEY)"),
        ] {
            self.submit_retry(object, vec![object.to_string()], vec![op(object, schema, vec![])], false, false)
                .await
                .expect("schema creation must eventually succeed");
        }
        // ERC20: token accounts + the supply object. Everyone gets the
        // same ledger shape (`writes`) so one atomicity auditor covers all.
        for t in 0..self.cfg.erc20_accounts {
            let object = format!("tok-{t}");
            let ops = vec![
                op(&object, "CREATE TABLE IF NOT EXISTS account (balance INTEGER NOT NULL CHECK (balance >= 0))", vec![]),
                op(&object, "INSERT INTO account (balance) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM account)", vec![]),
                op(&object, "CREATE TABLE IF NOT EXISTS writes (k TEXT PRIMARY KEY, amt INTEGER)", vec![]),
            ];
            self.submit_retry(&format!("schema {object}"), vec![object.clone()], ops, false, false)
                .await
                .expect("schema creation must eventually succeed");
        }
        let ops = vec![
            op("tok-supply", "CREATE TABLE IF NOT EXISTS account (balance INTEGER NOT NULL CHECK (balance >= 0))", vec![]),
            op("tok-supply", "INSERT INTO account (balance) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM account)", vec![]),
            op("tok-supply", "CREATE TABLE IF NOT EXISTS writes (k TEXT PRIMARY KEY, amt INTEGER)", vec![]),
        ];
        self.submit_retry("schema tok-supply", vec!["tok-supply".into()], ops, false, false)
            .await
            .expect("schema creation must eventually succeed");
        // Escrow parties (escrow objects themselves are minted per saga).
        for e in 0..2 {
            let object = format!("esc-party-{e}");
            let ops = vec![
                op(&object, "CREATE TABLE IF NOT EXISTS account (balance INTEGER NOT NULL CHECK (balance >= 0))", vec![]),
                op(&object, "INSERT INTO account (balance) SELECT ?1 WHERE NOT EXISTS (SELECT 1 FROM account)", vec![json!(self.cfg.initial_balance)]),
                op(&object, "CREATE TABLE IF NOT EXISTS writes (k TEXT PRIMARY KEY, amt INTEGER)", vec![]),
            ];
            self.submit_retry(&format!("schema {object}"), vec![object.clone()], ops, false, false)
                .await
                .expect("schema creation must eventually succeed");
        }
        // Idempotent counters.
        for c in 0..self.cfg.counters {
            let object = format!("ctr-{c}");
            let ops = vec![
                op(&object, "CREATE TABLE IF NOT EXISTS c (n INTEGER NOT NULL)", vec![]),
                op(&object, "INSERT INTO c (n) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM c)", vec![]),
                op(&object, "CREATE TABLE IF NOT EXISTS incs (k TEXT PRIMARY KEY)", vec![]),
            ];
            self.submit_retry(&format!("schema {object}"), vec![object.clone()], ops, false, false)
                .await
                .expect("schema creation must eventually succeed");
        }
        // Watched feeds.
        for f in 0..self.cfg.feeds {
            let object = format!("feed-{f}");
            let ops = vec![op(
                &object,
                "CREATE TABLE IF NOT EXISTS msgs (id INTEGER PRIMARY KEY AUTOINCREMENT, k TEXT UNIQUE, mode TEXT)",
                vec![],
            )];
            self.submit_retry(&format!("schema {object}"), vec![object.clone()], ops, false, false)
                .await
                .expect("schema creation must eventually succeed");
        }
        self.trace.record("schemas ready".into());
    }

    async fn transfer(&self, key: String, from: String, to: String, amount: i64, optimistic: bool) {
        let ops = vec![
            op(&from, "UPDATE account SET balance = balance - ?1", vec![json!(amount)]),
            op(&from, "INSERT INTO writes (k, amt) VALUES (?1, -?2)", vec![json!(key.clone()), json!(amount)]),
            op(&to, "UPDATE account SET balance = balance + ?1", vec![json!(amount)]),
            op(&to, "INSERT INTO writes (k, amt) VALUES (?1, ?2)", vec![json!(key.clone()), json!(amount)]),
        ];
        self.trace
            .record(format!("begin {key} {from}->{to} opt={optimistic}"));
        let acked = self
            .submit_retry(
                &format!("transfer {key}"),
                vec![from.clone(), to.clone()],
                ops,
                false,
                optimistic,
            )
            .await
            .is_ok();
        self.trace
            .record(format!("transfer {key} {from}->{to} acked={acked}"));
        self.model.transfers.lock().unwrap().push(TransferRecord {
            key,
            from,
            to,
            acked,
            optimistic,
        });
    }

    async fn transfer_phase(self: &Arc<Self>, phase: u32) {
        let mut tasks = Vec::new();
        let per_client = self.cfg.transfers / self.cfg.clients;
        for c in 0..self.cfg.clients {
            let world = self.clone();
            tasks.push(tokio::spawn(async move {
                for t in 0..per_client {
                    let a = below(&world.rng, world.cfg.accounts as u64) as usize;
                    let b = (a + 1 + below(&world.rng, world.cfg.accounts as u64 - 1) as usize)
                        % world.cfg.accounts;
                    let amount = 1 + below(&world.rng, 3) as i64;
                    let optimistic = chance(&world.rng, world.cfg.optimistic_pct);
                    let key = format!("t{phase}-c{c}-{t}");
                    world
                        .transfer(key, account_name(a), account_name(b), amount, optimistic)
                        .await;
                    let pause = below(&world.rng, 40);
                    sleep(Duration::from_millis(pause)).await;
                }
            }));
        }
        for t in tasks {
            t.await.expect("transfer client must not die");
        }
    }

    async fn publish_all(self: &Arc<Self>) {
        for m in 0..self.cfg.channel_msgs {
            let key = format!("msg-{m}");
            let ops = vec![op("chan", "INSERT INTO msgs (k) VALUES (?1)", vec![json!(key.clone())])];
            let acked = self
                .submit_retry(&format!("publish {key}"), vec!["chan".into()], ops, false, false)
                .await
                .is_ok();
            self.model.publishes.lock().unwrap().push((key, acked));
            let pause = below(&self.rng, 30);
            sleep(Duration::from_millis(pause)).await;
        }
        self.trace.record("publisher done".into());
    }

    /// The documented cursor loop, against a cluster on fire: poll the
    /// owning node, advance by rowid, re-poll on every error.
    async fn subscribe_all(
        self: &Arc<Self>,
        publisher_done: Arc<AtomicBool>,
    ) -> Vec<String> {
        let mut received = Vec::new();
        let mut cursor = 0i64;
        // Progress-relative: wait out the publisher (bounded), then a bounded
        // drain to the durable tail.
        let mut drain_deadline: Option<tokio::time::Instant> = None;
        loop {
            if publisher_done.load(Ordering::SeqCst) && drain_deadline.is_none() {
                drain_deadline = Some(tokio::time::Instant::now() + LIVENESS_DRAIN);
            }
            if drain_deadline.is_some_and(|d| tokio::time::Instant::now() > d) {
                let want = self
                    .model
                    .publishes
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, acked)| *acked)
                    .count();
                panic!(
                    "LIVENESS: subscriber stuck at {} of {want} acked messages (drain).\ntrace tail:\n{}",
                    received.len(),
                    self.trace.dump_tail()
                );
            }
            let Some(node) = self.any_node() else {
                sleep(Duration::from_millis(200)).await;
                continue;
            };
            let poll = cluster::submit_poll(
                &node,
                "chan".into(),
                "SELECT id, k FROM msgs WHERE id > ?1 ORDER BY id LIMIT 10".into(),
                vec![json!(cursor)],
                // DURABLE. The oracle requires exactly-once delivery of every
                // acked (committed) publish. Only a durable reader can promise
                // that: a non-durable poll sees UNCOMMITTED rows that later roll
                // back, and because AUTOINCREMENT ids are not reused after a
                // rollback the cursor advances past an id a committed message
                // will later occupy — so the subscriber skips it forever (under
                // the pause fault: read uncommitted msg-1 at id 2, it aborts,
                // committed msg-5 takes id 2, subscriber never re-reads it). A
                // durable poll only ever sees the committed prefix, whose ids
                // are stable and contiguous, so the cursor never skips.
                true,
                None,
                u64::MAX, // sim subscriber's connection id space
                cursor as u64,
            );
            match timeout(Duration::from_millis(3000), poll).await {
                Err(_) => {
                    // Nothing new where we're parked; cancel and re-poll
                    // (the owner may have moved under us).
                    cluster::cancel_poll(&node, "chan", u64::MAX, cursor as u64);
                    // Publishing ended and a direct read agrees we're at the
                    // durable tail: done. We DRAIN rather than require every
                    // publish to have ACKed — under the pause fault a publish
                    // can be a LOST ACK (committed durably, client saw an
                    // error, recorded acked=false), so want==channel_msgs would
                    // hang forever on a message we already received. (The feed
                    // consumers already exit this way; the subscriber was the
                    // last oracle that didn't, turning every lost-ack publish
                    // under skew+pause into a false liveness crash.)
                    if publisher_done.load(Ordering::SeqCst) {
                        let max = self
                            .read("chan", "SELECT COALESCE(MAX(id), 0) AS m FROM msgs")
                            .await[0]["m"]
                            .as_i64()
                            .expect("max id");
                        if max <= cursor {
                            break;
                        }
                    }
                }
                Ok(Ok(resp)) => {
                    for result in &resp.results {
                        if let crate::cluster::OpResult::Rows { rows } = result {
                            for row in rows {
                                let id = row["id"].as_i64().expect("id");
                                let k = row["k"].as_str().expect("k").to_string();
                                assert!(id > cursor, "subscriber went backwards: {id} <= {cursor}");
                                cursor = id;
                                received.push(k);
                            }
                        }
                    }
                }
                Ok(Err(_)) => sleep(Duration::from_millis(100)).await,
            }
        }
        self.trace.record(format!("subscriber got {}", received.len()));
        received
    }

    /// Arm `count` wills. Polite ones close immediately (the will must
    /// fire). Doomed ones stay open on their node so a later crash takes
    /// the connection down WITH the server — the canary case.
    async fn arm_wills(&self, phase: u32, doomed_node: usize) -> Vec<Session> {
        let mut doomed = Vec::new();
        for w in 0..self.cfg.wills {
            let polite = w % 2 == 0;
            let node_idx = if polite {
                let live = self.live_indices();
                live[below(&self.rng, live.len() as u64) as usize]
            } else {
                doomed_node
            };
            let Some(node) = self.node(node_idx) else { continue };
            let key = format!("will-{phase}-{w}");
            let session = Session::open(node, Auth::Root);
            let frame = json!({
                "id": 1,
                // Idempotent by construction (INSERT OR IGNORE): a
                // crash-orphaned will fires at-least-once — the sweeper may
                // reclaim one whose firer died mid-commit — so its effect
                // must survive replay, exactly as the will docs advise.
                "will": { "ops": [{
                    "object": "graveyard",
                    "sql": "INSERT OR IGNORE INTO g (k) VALUES (?1)",
                    "params": [key]
                }]}
            });
            let reply = session.frame(&frame.to_string()).await.expect("arm reply");
            let reply: Value = serde_json::from_str(&reply).expect("arm reply json");
            assert_eq!(
                reply["result"]["will"], "armed",
                "will must arm on a live node: {reply}"
            );
            self.model.wills.lock().unwrap().push(WillCase {
                key: key.clone(),
                node: node_idx,
                node_crashed: false,
            });
            self.trace.record(format!("will {key} armed on n{node_idx} polite={polite}"));
            if polite {
                // The server-side teardown runs when the socket dies with
                // the node still alive — that's close().
                session.close().await;
            } else {
                doomed.push(session);
            }
        }
        doomed
    }

    /// Read that tolerates a missing table/object (a saga whose open never
    /// landed): Ok(None) for "definitively absent", retrying transient
    /// errors, panicking only on a liveness violation.
    async fn try_read(&self, object: &str, sql: &str) -> Option<Vec<Value>> {
        for _attempt in 0..120 {
            let Some(node) = self.any_node() else {
                sleep(Duration::from_millis(200)).await;
                continue;
            };
            let ops = vec![op(object, sql, vec![])];
            let fut = cluster::submit(&node, vec![object.to_string()], ops, true, false);
            match timeout(Duration::from_millis(self.cfg.op_timeout_ms), fut).await {
                Err(_) => panic!(
                    "LIVENESS: try_read of {object} hung.
trace tail:
{}",
                    self.trace.dump_tail()
                ),
                Ok(Ok(mut resp)) => match resp.results.pop() {
                    Some(crate::cluster::OpResult::Rows { rows }) => return Some(rows),
                    _ => return Some(Vec::new()),
                },
                Ok(Err(e)) if e.message.contains("no such table") => return None,
                Ok(Err(_)) => sleep(Duration::from_millis(100)).await,
            }
        }
        panic!(
            "try_read of {object} kept failing with live nodes present.
trace tail:
{}",
            self.trace.dump_tail()
        );
    }

    // ------------------------------------------------ workload: ERC20

    /// Mint / burn / transfer against token accounts plus a moving supply.
    /// Every op writes a ledger leg in each participant; the auditor
    /// demands all legs or none, and sum(balances) == supply == its ledger.
    async fn erc20_phase(self: &Arc<Self>, phase: u32) {
        let mut tasks = Vec::new();
        let per_client = self.cfg.erc20_ops / 2;
        for c in 0..2usize {
            let world = self.clone();
            tasks.push(tokio::spawn(async move {
                for t in 0..per_client {
                    let key = format!("tok{phase}-c{c}-{t}");
                    let a = below(&world.rng, world.cfg.erc20_accounts as u64) as usize;
                    let b = (a + 1 + below(&world.rng, world.cfg.erc20_accounts as u64 - 1) as usize)
                        % world.cfg.erc20_accounts;
                    let amt = 1 + below(&world.rng, 5) as i64;
                    let optimistic = chance(&world.rng, world.cfg.optimistic_pct);
                    let kind = below(&world.rng, 10);
                    let (participants, ops, legs) = if kind < 4 {
                        // mint: supply and the recipient both grow by amt.
                        let to = format!("tok-{a}");
                        (
                            vec!["tok-supply".to_string(), to.clone()],
                            vec![
                                op("tok-supply", "UPDATE account SET balance = balance + ?1", vec![json!(amt)]),
                                op("tok-supply", "INSERT INTO writes (k, amt) VALUES (?1, ?2)", vec![json!(key.clone()), json!(amt)]),
                                op(&to, "UPDATE account SET balance = balance + ?1", vec![json!(amt)]),
                                op(&to, "INSERT INTO writes (k, amt) VALUES (?1, ?2)", vec![json!(key.clone()), json!(amt)]),
                            ],
                            vec![("tok-supply".to_string(), amt), (to, amt)],
                        )
                    } else if kind < 6 {
                        // burn: both shrink; CHECKs reject an overburn.
                        let from = format!("tok-{a}");
                        (
                            vec!["tok-supply".to_string(), from.clone()],
                            vec![
                                op(&from, "UPDATE account SET balance = balance - ?1", vec![json!(amt)]),
                                op(&from, "INSERT INTO writes (k, amt) VALUES (?1, -?2)", vec![json!(key.clone()), json!(amt)]),
                                op("tok-supply", "UPDATE account SET balance = balance - ?1", vec![json!(amt)]),
                                op("tok-supply", "INSERT INTO writes (k, amt) VALUES (?1, -?2)", vec![json!(key.clone()), json!(amt)]),
                            ],
                            vec![("tok-supply".to_string(), -amt), (from, -amt)],
                        )
                    } else {
                        // transfer: supply untouched.
                        let from = format!("tok-{a}");
                        let to = format!("tok-{b}");
                        (
                            vec![from.clone(), to.clone()],
                            vec![
                                op(&from, "UPDATE account SET balance = balance - ?1", vec![json!(amt)]),
                                op(&from, "INSERT INTO writes (k, amt) VALUES (?1, -?2)", vec![json!(key.clone()), json!(amt)]),
                                op(&to, "UPDATE account SET balance = balance + ?1", vec![json!(amt)]),
                                op(&to, "INSERT INTO writes (k, amt) VALUES (?1, ?2)", vec![json!(key.clone()), json!(amt)]),
                            ],
                            vec![(from, -amt), (to, amt)],
                        )
                    };
                    let acked = world
                        .submit_retry(&format!("erc20 {key}"), participants, ops, false, optimistic)
                        .await
                        .is_ok();
                    world.trace.record(format!("erc20 {key} acked={acked}"));
                    world.model.ops.lock().unwrap().push(OpRecord {
                        key,
                        legs,
                        acked,
                        optimistic,
                    });
                    let pause = below(&world.rng, 40);
                    sleep(Duration::from_millis(pause)).await;
                }
            }));
        }
        for t in tasks {
            t.await.expect("erc20 client must not die");
        }
    }

    // ----------------------------------------------- workload: escrows

    /// Open moves money from a party into a per-saga escrow OBJECT; settle
    /// releases it to the other party or refunds it — and on half the
    /// sagas both directions race. The settlements PRIMARY KEY is the
    /// exactly-once arbiter: a forked escrow is the only way to lose.
    async fn escrow_phase(self: &Arc<Self>, phase: u32) {
        let mut tasks = Vec::new();
        for e in 0..self.cfg.escrows {
            let world = self.clone();
            tasks.push(tokio::spawn(async move {
                let key = format!("esc{phase}-{e}");
                let escrow = format!("escrow-{key}");
                let funder = format!("esc-party-{}", e % 2);
                let payee = format!("esc-party-{}", (e + 1) % 2);
                let amount = 1 + below(&world.rng, 4) as i64;
                let open_ops = vec![
                    op(&funder, "UPDATE account SET balance = balance - ?1", vec![json!(amount)]),
                    op(&funder, "INSERT INTO writes (k, amt) VALUES (?1, -?2)", vec![json!(format!("open-{key}")), json!(amount)]),
                    op(&escrow, "CREATE TABLE IF NOT EXISTS meta (amt INTEGER NOT NULL)", vec![]),
                    op(&escrow, "CREATE TABLE IF NOT EXISTS settlements (slot TEXT PRIMARY KEY, dir TEXT NOT NULL)", vec![]),
                    op(&escrow, "INSERT INTO meta (amt) VALUES (?1)", vec![json!(amount)]),
                ];
                let open_acked = world
                    .submit_retry(
                        &format!("escrow open {key}"),
                        vec![funder.clone(), escrow.clone()],
                        open_ops,
                        false,
                        false, // opens are pessimistic: settles build on them
                    )
                    .await
                    .is_ok();
                world.trace.record(format!("escrow {key} open acked={open_acked}"));
                world.model.escrows.lock().unwrap().push(EscrowRecord {
                    key: key.clone(),
                    funder: funder.clone(),
                    payee: payee.clone(),
                    amount,
                    open_acked,
                });
                if !open_acked {
                    return;
                }
                let pause = below(&world.rng, 60);
                sleep(Duration::from_millis(pause)).await;
                // Settle. Half the sagas race both directions on purpose.
                let race = e % 2 == 0;
                let mut settles = Vec::new();
                let dirs: Vec<(&str, String)> = if race {
                    vec![("release", payee.clone()), ("refund", funder.clone())]
                } else if chance(&world.rng, 50) {
                    vec![("release", payee.clone())]
                } else {
                    vec![("refund", funder.clone())]
                };
                for (dir, target) in dirs {
                    let world = world.clone();
                    let (key, escrow) = (key.clone(), escrow.clone());
                    settles.push(tokio::spawn(async move {
                        // The `settlements` PRIMARY KEY 'done' is the
                        // exactly-once gate: whichever direction commits the
                        // row first wins the escrow; the racing one hits the
                        // UNIQUE constraint, its whole txn rolls back, and no
                        // money moves. The credit amount is a literal the
                        // client knows (it opened the saga) — meta lives in
                        // the escrow object, but the balance moves on the
                        // target's connection.
                        let amt = {
                            let m = world.model.escrows.lock().unwrap();
                            m.iter().find(|r| r.key == key).map(|r| r.amount).unwrap()
                        };
                        let ops = vec![
                            op(&escrow, "INSERT INTO settlements (slot, dir) VALUES ('done', ?1)", vec![json!(dir)]),
                            op(&target, "UPDATE account SET balance = balance + ?1", vec![json!(amt)]),
                            op(&target, "INSERT INTO writes (k, amt) VALUES (?1, ?2)", vec![json!(format!("settle-{key}-{dir}")), json!(amt)]),
                        ];
                        let acked = world
                            .submit_retry(
                                &format!("escrow settle {key} {dir}"),
                                vec![escrow.clone(), target.clone()],
                                ops,
                                false,
                                false,
                            )
                            .await
                            .is_ok();
                        world
                            .trace
                            .record(format!("escrow {key} settle {dir} acked={acked}"));
                    }));
                }
                for sjoin in settles {
                    sjoin.await.expect("settle task");
                }
            }));
        }
        for t in tasks {
            t.await.expect("escrow saga must not die");
        }
    }

    // ---------------------------------------------- workload: counters

    async fn counter_phase(self: &Arc<Self>, phase: u32) {
        let mut tasks = Vec::new();
        let per_client = self.cfg.counter_incs / 2;
        for c in 0..2usize {
            let world = self.clone();
            tasks.push(tokio::spawn(async move {
                for t in 0..per_client {
                    let ctr = format!("ctr-{}", below(&world.rng, world.cfg.counters as u64));
                    let key = format!("inc{phase}-c{c}-{t}");
                    let optimistic = chance(&world.rng, world.cfg.optimistic_pct);
                    let ops = vec![
                        op(&ctr, "INSERT INTO incs (k) VALUES (?1)", vec![json!(key.clone())]),
                        op(&ctr, "UPDATE c SET n = n + 1", vec![]),
                    ];
                    let acked = world
                        .submit_retry(&format!("inc {key}"), vec![ctr.clone()], ops, false, optimistic)
                        .await
                        .is_ok();
                    world
                        .model
                        .incs
                        .lock()
                        .unwrap()
                        .push((ctr, key, acked, optimistic));
                    let pause = below(&world.rng, 30);
                    sleep(Duration::from_millis(pause)).await;
                }
            }));
        }
        for t in tasks {
            t.await.expect("counter client must not die");
        }
    }

    // ------------------------------------------- workload: watched feeds

    async fn feed_producer(self: &Arc<Self>, feed: usize, phase: u32) {
        let object = format!("feed-{feed}");
        for m in 0..self.cfg.feed_appends {
            let key = format!("f{feed}-p{phase}-{m}");
            let optimistic = chance(&self.rng, self.cfg.optimistic_pct);
            let mode = if optimistic { "opt" } else { "pess" };
            let ops = vec![op(
                &object,
                "INSERT INTO msgs (k, mode) VALUES (?1, ?2)",
                vec![json!(key.clone()), json!(mode)],
            )];
            let acked = self
                .submit_retry(&format!("append {key}"), vec![object.clone()], ops, false, optimistic)
                .await
                .is_ok();
            self.model
                .appends
                .lock()
                .unwrap()
                .push((object.clone(), key, acked, optimistic));
            let pause = below(&self.rng, 40);
            sleep(Duration::from_millis(pause)).await;
        }
        self.trace.record(format!("producer feed-{feed} p{phase} done"));
    }

    /// The documented cursor loop against one feed. `durable` selects the
    /// poll mode; durable deliveries are recorded for the never-un-happens
    /// oracle. Runs until it has drained everything that exists once the
    /// producers stop. Returns the transcript (asserted in-loop: strictly
    /// increasing ids, no duplicate keys).
    async fn consume_feed(
        self: &Arc<Self>,
        feed: usize,
        durable: bool,
        producers_done: Arc<AtomicBool>,
        conn_id: u64,
    ) -> Vec<(i64, String)> {
        let object = format!("feed-{feed}");
        let mut transcript: Vec<(i64, String)> = Vec::new();
        let mut seen: Set<String> = Set::default();
        let mut cursor = 0i64;
        // Progress-relative: wait out slow producers, then a bounded drain.
        let mut drain_deadline: Option<tokio::time::Instant> = None;
        loop {
            if producers_done.load(Ordering::SeqCst) && drain_deadline.is_none() {
                drain_deadline = Some(tokio::time::Instant::now() + LIVENESS_DRAIN);
            }
            if drain_deadline.is_some_and(|d| tokio::time::Instant::now() > d) {
                panic!(
                    "LIVENESS: consumer(durable={durable}) of {object} stuck at cursor {cursor} (drain).
trace tail:
{}",
                    self.trace.dump_tail()
                );
            }
            let Some(node) = self.any_node() else {
                sleep(Duration::from_millis(200)).await;
                continue;
            };
            let poll = cluster::submit_poll(
                &node,
                object.clone(),
                "SELECT id, k FROM msgs WHERE id > ?1 ORDER BY id LIMIT 8".into(),
                vec![json!(cursor)],
                durable,
                None,
                conn_id,
                cursor as u64,
            );
            match timeout(Duration::from_millis(2500), poll).await {
                Err(_) => {
                    cluster::cancel_poll(&node, &object, conn_id, cursor as u64);
                    // Parked with nothing new. If production has ended and
                    // a direct read agrees we're at the end, we're done.
                    if producers_done.load(Ordering::SeqCst) {
                        let max = self
                            .read(&object, "SELECT COALESCE(MAX(id), 0) AS m FROM msgs")
                            .await[0]["m"]
                            .as_i64()
                            .expect("max id");
                        if max <= cursor {
                            break;
                        }
                    }
                }
                Ok(Ok(resp)) => {
                    for result in &resp.results {
                        if let crate::cluster::OpResult::Rows { rows } = result {
                            for row in rows {
                                let id = row["id"].as_i64().expect("id");
                                let k = row["k"].as_str().expect("k").to_string();
                                assert!(
                                    id > cursor,
                                    "CONSUMER(durable={durable}) of {object} went backwards: {id} <= {cursor}"
                                );
                                assert!(
                                    seen.insert(k.clone()),
                                    "CONSUMER(durable={durable}) of {object} saw {k} twice"
                                );
                                cursor = id;
                                if durable {
                                    self.model
                                        .durable_deliveries
                                        .lock()
                                        .unwrap()
                                        .push((object.clone(), id, k.clone()));
                                }
                                transcript.push((id, k));
                            }
                        }
                    }
                }
                Ok(Err(_)) => sleep(Duration::from_millis(100)).await,
            }
        }
        self.trace.record(format!(
            "consumer feed-{feed} durable={durable} got {}",
            transcript.len()
        ));
        transcript
    }

    /// Change-detection watcher: the baseline-hash loop over an aggregate
    /// view. Every fire must present a hash different from the baseline it
    /// parked with (the server's contract), and at the end the view must
    /// match a direct read (bounded staleness: zero, at quiescence).
    async fn watch_feed_changes(
        self: &Arc<Self>,
        feed: usize,
        producers_done: Arc<AtomicBool>,
        conn_id: u64,
    ) {
        let object = format!("feed-{feed}");
        let sql = "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS m FROM msgs";
        let mut baseline = String::new();
        let mut fires = 0u64;
        // Progress-relative: wait out slow producers, then a bounded drain.
        let mut drain_deadline: Option<tokio::time::Instant> = None;
        loop {
            if producers_done.load(Ordering::SeqCst) && drain_deadline.is_none() {
                drain_deadline = Some(tokio::time::Instant::now() + LIVENESS_DRAIN);
            }
            if drain_deadline.is_some_and(|d| tokio::time::Instant::now() > d) {
                panic!(
                    "LIVENESS: change watcher of {object} stuck after {fires} fires (drain).
trace tail:
{}",
                    self.trace.dump_tail()
                );
            }
            let Some(node) = self.any_node() else {
                sleep(Duration::from_millis(200)).await;
                continue;
            };
            let poll = cluster::submit_poll(
                &node,
                object.clone(),
                sql.into(),
                vec![],
                false,
                Some(baseline.clone()),
                conn_id,
                fires,
            );
            match timeout(Duration::from_millis(2500), poll).await {
                Err(_) => {
                    cluster::cancel_poll(&node, &object, conn_id, fires);
                    if producers_done.load(Ordering::SeqCst) {
                        break;
                    }
                }
                Ok(Ok(resp)) => {
                    let hash = resp.hash.clone().unwrap_or_default();
                    assert_ne!(
                        hash, baseline,
                        "CHANGE WATCHER of {object} fired without a change"
                    );
                    baseline = hash;
                    fires += 1;
                }
                Ok(Err(_)) => sleep(Duration::from_millis(100)).await,
            }
        }
        self.trace
            .record(format!("change watcher feed-{feed} done after {fires} fires"));
    }

    // ---------------------------------------------------- workload oracles

    /// ERC20: sum of every token account's balance equals the supply
    /// object's balance equals the supply ledger's running sum — a
    /// conservation law whose reference total moves (mint/burn) — and
    /// every multi-leg op is whole (all legs present with matching signs,
    /// or none). Only valid at quiescence.
    async fn audit_erc20(&self, when: &str) {
        let mut ledger: Map<String, Vec<(String, i64)>> = Map::default();
        let mut account_sum = 0i64;
        for t in 0..self.cfg.erc20_accounts {
            let object = format!("tok-{t}");
            account_sum += self.read(&object, "SELECT balance FROM account").await[0]["balance"]
                .as_i64()
                .expect("balance");
            ledger.insert(object.clone(), self.read_ledger(&object).await);
        }
        let supply = self.read("tok-supply", "SELECT balance FROM account").await[0]["balance"]
            .as_i64()
            .expect("supply balance");
        ledger.insert("tok-supply".into(), self.read_ledger("tok-supply").await);
        let supply_ledger: i64 = ledger["tok-supply"].iter().map(|(_, a)| a).sum();

        assert_eq!(
            account_sum, supply,
            "ERC20 SUPPLY violated {when}: sum(balances)={account_sum} != supply={supply}.
trace tail:
{}",
            self.trace.dump_tail()
        );
        assert_eq!(
            supply, supply_ledger,
            "ERC20 SUPPLY LEDGER violated {when}: supply={supply} != ledger sum={supply_ledger}.
trace tail:
{}",
            self.trace.dump_tail()
        );

        let ops = self.model.ops.lock().unwrap();
        for o in ops.iter() {
            let present: Vec<bool> = o
                .legs
                .iter()
                .map(|(obj, amt)| {
                    ledger[obj]
                        .iter()
                        .any(|(k, a)| k == &o.key && a == amt)
                })
                .collect();
            let all = present.iter().all(|&p| p);
            let none = present.iter().all(|&p| !p);
            assert!(
                all || none,
                "ERC20 ATOMICITY violated {when}: op {} (acked={}, opt={}) legs present={present:?}.
trace tail:
{}",
                o.key, o.acked, o.optimistic, self.trace.dump_tail()
            );
        }
    }

    /// Escrow: every opened saga is settled exactly once or not at all —
    /// never both directions, never a torn settle (a `settlements` row
    /// without its balance credit, or vice versa) — and the two parties
    /// plus every live escrow's held `meta` conserve the parties' opening
    /// capital.
    async fn audit_escrows(&self, when: &str) {
        let escrows = self.model.escrows.lock().unwrap();
        let mut party_ledgers: Map<String, Vec<(String, i64)>> = Map::default();
        for e in 0..2usize {
            let object = format!("esc-party-{e}");
            party_ledgers.insert(object.clone(), self.read_ledger(&object).await);
        }
        for r in escrows.iter() {
            let object = format!("escrow-{}", r.key);
            let settlements = match self.try_read(&object, "SELECT dir FROM settlements").await {
                Some(rows) => rows,
                None => {
                    // No escrow object: the open never durably landed.
                    assert!(
                        !r.open_acked,
                        "ESCROW violated {when}: {} open acked but escrow object is absent.
trace tail:
{}",
                        r.key, self.trace.dump_tail()
                    );
                    continue;
                }
            };
            // Exactly-once: the PRIMARY KEY makes >1 impossible, but assert
            // it anyway — a fork would show as two rows.
            assert!(
                settlements.len() <= 1,
                "ESCROW EXACTLY-ONCE violated {when}: {} settled {} times.
trace tail:
{}",
                r.key, settlements.len(), self.trace.dump_tail()
            );
            if let Some(row) = settlements.first() {
                let dir = row["dir"].as_str().expect("dir");
                let target = if dir == "release" { &r.payee } else { &r.funder };
                let credited = party_ledgers[target]
                    .iter()
                    .any(|(k, a)| k == &format!("settle-{}-{dir}", r.key) && *a == r.amount);
                assert!(
                    credited,
                    "ESCROW TORN {when}: {} settled {dir} but the {target} credit is missing.
trace tail:
{}",
                    r.key, self.trace.dump_tail()
                );
            }
        }
        // Capital conservation: parties + funds still held in escrow.
        let mut party_balance = 0i64;
        for e in 0..2usize {
            party_balance += self.read(&format!("esc-party-{e}"), "SELECT balance FROM account").await[0]["balance"]
                .as_i64()
                .expect("party balance");
        }
        let mut held = 0i64;
        for r in escrows.iter() {
            if !r.open_acked {
                continue;
            }
            let object = format!("escrow-{}", r.key);
            let settled = self
                .try_read(&object, "SELECT COUNT(*) AS n FROM settlements")
                .await
                .map(|rows| rows[0]["n"].as_i64().unwrap_or(0))
                .unwrap_or(0);
            if settled == 0 {
                held += r.amount; // still locked in the escrow
            }
        }
        let expected = 2 * self.cfg.initial_balance;
        if party_balance + held != expected {
            // Dump DURABLE truth: if the durable balances/settlements sum to
            // `expected`, this is a STALE READ (a node cache behind the log);
            // if durable itself is off, a real escrow fork.
            let mut dbg = String::new();
            for e in 0..2usize {
                let o = format!("esc-party-{e}");
                let db = self.durable_scalar(&o, "SELECT balance FROM account").await;
                let dw = self.durable_writes(&o).await;
                dbg.push_str(&format!("  DURABLE {o}: balance={db:?} writes={dw:?}\n"));
                dbg.push_str(&format!("    {}\n", crate::objlog::dump_log(self.store.as_ref(), &o).await));
            }
            for r in escrows.iter().filter(|r| r.open_acked) {
                let o = format!("escrow-{}", r.key);
                let ds = self.durable_scalar(&o, "SELECT COUNT(*) FROM settlements").await;
                dbg.push_str(&format!("  DURABLE {o}: amount={} settled={ds:?}\n", r.amount));
            }
            eprintln!("ESCROW CAPITAL durable dump:\n{dbg}");
        }
        assert_eq!(
            party_balance + held, expected,
            "ESCROW CAPITAL violated {when}: parties {party_balance} + held {held} != {expected}.
trace tail:
{}",
            self.trace.dump_tail()
        );
    }

    /// Idempotent counters: n == COUNT(distinct inc keys) always. A
    /// lost-ack retry that ran twice would push n past the key count; a
    /// non-idempotent apply would too. The UNIQUE key is the whole defense.
    async fn audit_counters(&self, when: &str) {
        for c in 0..self.cfg.counters {
            let object = format!("ctr-{c}");
            let n = self.read(&object, "SELECT n FROM c").await[0]["n"].as_i64().expect("n");
            let keys = self.read(&object, "SELECT COUNT(*) AS k FROM incs").await[0]["k"]
                .as_i64()
                .expect("k");
            assert_eq!(
                n, keys,
                "COUNTER violated {when}: {object} n={n} != distinct incs={keys} (a retry double-applied).
trace tail:
{}",
                self.trace.dump_tail()
            );
        }
    }

    /// Every DURABLE delivery still exists. A durable poll only fires from
    /// shipped state, so a row it handed a consumer must survive every
    /// later crash — the whole point of the durable contract. (Optimistic
    /// deliveries carry no such promise and are not audited here.)
    async fn audit_durable_deliveries(&self) {
        let deliveries = self.model.durable_deliveries.lock().unwrap();
        for (feed, id, k) in deliveries.iter() {
            let present = self
                .read(feed, &format!("SELECT k FROM msgs WHERE id = {id}"))
                .await
                .first()
                .map(|r| r["k"].as_str() == Some(k.as_str()))
                .unwrap_or(false);
            assert!(
                present,
                "DURABLE DELIVERY LOST: {feed} row id={id} k={k} was delivered by a durable poll but is gone.
trace tail:
{}",
                self.trace.dump_tail()
            );
        }
    }

    /// Feed transcript completeness, the real watch guarantee: every
    /// message that is DURABLY in the final table was delivered to a cursor
    /// consumer. (An optimistically-acked append whose boat sank is
    /// legitimately gone — the optimistic contract — so we audit against
    /// what actually survived, not against the ack flag.) The inverse,
    /// "no phantom deliveries", is checked too: every delivered key exists.
    async fn audit_feed_completeness(&self, feed: usize, delivered: &Set<String>) {
        let object = format!("feed-{feed}");
        let present: Set<String> = self
            .read(&object, "SELECT k FROM msgs")
            .await
            .iter()
            .map(|r| r["k"].as_str().expect("k").to_string())
            .collect();
        for k in &present {
            assert!(
                delivered.contains(k),
                "FEED INCOMPLETE: durable message {k} on {object} never reached a consumer.
trace tail:
{}",
                self.trace.dump_tail()
            );
        }
        // No phantom check over the union: a NON-durable poll may legally
        // deliver an optimistically-applied row that a later crash reverts
        // (the optimistic-read contract). The strict "durable deliveries
        // never un-happen" property is audit_durable_deliveries, which
        // covers exactly the poll mode that promises it.
    }

    /// Fold an object straight from the durable store (bypassing every node
    /// cache) and read its writes-set. Diagnostic only — used at oracle
    /// failure to locate the fork.
    async fn durable_writes(&self, object: &str) -> Vec<(String, i64)> {
        let (image, _seq) = match crate::objlog::fold_committed(self.store.as_ref(), object).await {
            Ok(v) => v,
            Err(e) => return vec![("<fold-error>".to_string(), 0), (format!("{e}"), 0)],
        };
        let path = std::env::temp_dir().join(format!("dst-durable-{}-{object}.db", std::process::id()));
        if std::fs::write(&path, &image).is_err() {
            return vec![("<write-error>".to_string(), 0)];
        }
        let Ok(conn) = rusqlite::Connection::open(&path) else {
            return vec![("<open-error>".to_string(), 0)];
        };
        let mut out = Vec::new();
        if let Ok(mut stmt) = conn.prepare("SELECT k, amt FROM writes ORDER BY rowid") {
            if let Ok(rows) = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))) {
                for row in rows.flatten() {
                    out.push(row);
                }
            }
        }
        out
    }

    /// Fold an object straight from the durable store and run a scalar query
    /// (first column of first row as i64). Diagnostic only. Returns None if
    /// the fold fails or the query returns nothing.
    async fn durable_scalar(&self, object: &str, sql: &str) -> Option<i64> {
        let (image, _seq) = crate::objlog::fold_committed(self.store.as_ref(), object).await.ok()?;
        let path = std::env::temp_dir().join(format!("dst-durscalar-{}-{object}.db", std::process::id()));
        std::fs::write(&path, &image).ok()?;
        let conn = rusqlite::Connection::open(&path).ok()?;
        conn.query_row(sql, [], |r| r.get::<_, i64>(0)).ok()
    }

    async fn read_ledger(&self, object: &str) -> Vec<(String, i64)> {
        self.read(object, "SELECT k, amt FROM writes ORDER BY rowid")
            .await
            .iter()
            .map(|r| {
                (
                    r["k"].as_str().expect("k").to_string(),
                    r["amt"].as_i64().expect("amt"),
                )
            })
            .collect()
    }

    // ---------------------------------------------------------- oracles

    /// Money in the system is exactly what it started as, and every
    /// transfer is whole: its debit and credit exist together or not at
    /// all, with matching amounts. Only valid at quiescence.
    async fn audit_conservation(&self, when: &str) {
        let mut total = 0i64;
        let mut ledger: Map<String, Vec<(String, i64)>> = Map::default();
        for a in 0..self.cfg.accounts {
            let object = account_name(a);
            let rows = self.read(&object, "SELECT balance FROM account").await;
            total += rows[0]["balance"].as_i64().expect("balance");
            let writes = self.read(&object, "SELECT k, amt FROM writes ORDER BY rowid").await;
            let entries = writes
                .iter()
                .map(|r| (r["k"].as_str().expect("k").to_string(), r["amt"].as_i64().expect("amt")))
                .collect();
            ledger.insert(object, entries);
        }

        // Per-key atomicity: pair up debits and credits across accounts.
        // Collect the torn one (if any) first; then dump the DURABLE fold of
        // both accounts before panicking, to tell a stale READ (durable has
        // the write, a node cache lost it) apart from write-ERASURE (durable
        // itself lost it — a stale snapshot shipped over a committed entry).
        let torn = {
            let transfers = self.model.transfers.lock().unwrap();
            transfers.iter().find_map(|t| {
                let debit = ledger[&t.from].iter().find(|(k, _)| k == &t.key).cloned();
                let credit = ledger[&t.to].iter().find(|(k, _)| k == &t.key).cloned();
                match (&debit, &credit) {
                    (None, None) => None,
                    (Some((_, d)), Some((_, c))) if *d == -*c => None,
                    _ => Some((t.from.clone(), t.to.clone(), t.key.clone(), t.acked, t.optimistic, debit, credit)),
                }
            })
        };
        if let Some((from, to, key, acked, optimistic, debit, credit)) = torn {
            let df = self.durable_writes(&from).await;
            let dt = self.durable_writes(&to).await;
            let dd = df.iter().find(|(k, _)| k == &key);
            let dc = dt.iter().find(|(k, _)| k == &key);
            let mut dumps = String::new();
            for i in self.live_indices() {
                if let Some(n) = self.node(i) {
                    dumps.push_str(&n.debug_dump().await);
                }
            }
            let seq_from = crate::objlog::committed_seq(self.store.as_ref(), &from).await.unwrap_or(u64::MAX);
            let seq_to = crate::objlog::committed_seq(self.store.as_ref(), &to).await.unwrap_or(u64::MAX);
            eprintln!("DURABLE committed_seq: {from}={seq_from} {to}={seq_to}\nnodes:\n{dumps}");
            panic!(
                "ATOMICITY violated {when}: transfer {key} (acked={acked}, optimistic={optimistic}) is torn: \
                 read debit={debit:?} credit={credit:?}\n\
                 DURABLE {from}: writes={df:?} -> key {key} = {dd:?}\n\
                 DURABLE {to}: writes={dt:?} -> key {key} = {dc:?}\n\
                 (durable-has-write => stale READ; durable-missing => write ERASURE)\ntrace tail:\n{}",
                self.trace.dump_tail()
            );
        }

        let expected = self.cfg.accounts as i64 * self.cfg.initial_balance;
        assert_eq!(
            total, expected,
            "CONSERVATION violated {when}: {total} != {expected}.\ntrace tail:\n{}",
            self.trace.dump_tail()
        );
        self.trace.record(format!("conservation ok {when}"));
    }

    /// Every transfer is all-or-nothing across BOTH accounts, acked or
    /// not; every acked pessimistic transfer survived; polite wills fired;
    /// crash-doomed wills fired only if the config says they must.
    async fn audit_final(&self) {
        // Atomicity + durability, from the writes tables.
        let mut present: Map<String, Set<String>> = Map::default();
        for a in 0..self.cfg.accounts {
            let object = account_name(a);
            let rows = self.read(&object, "SELECT k FROM writes ORDER BY rowid").await;
            let keys: Set<String> = rows
                .iter()
                .map(|r| r["k"].as_str().expect("k").to_string())
                .collect();
            present.insert(object, keys);
        }
        {
            let transfers = self.model.transfers.lock().unwrap();
            for t in transfers.iter() {
                let in_from = present[&t.from].contains(&t.key);
                let in_to = present[&t.to].contains(&t.key);
                assert_eq!(
                    in_from, in_to,
                    "ATOMICITY violated: transfer {} present in {} but not {} \
                     (acked={}).\ntrace tail:\n{}",
                    t.key,
                    if in_from { &t.from } else { &t.to },
                    if in_from { &t.to } else { &t.from },
                    t.acked,
                    self.trace.dump_tail()
                );
                if t.acked && !t.optimistic {
                    assert!(
                        in_from,
                        "DURABILITY violated: acked pessimistic transfer {} vanished.\ntrace tail:\n{}",
                        t.key,
                        self.trace.dump_tail()
                    );
                }
            }
        }

        // Wills. Every armed will's connection is now dead — polite ones
        // closed, doomed ones rode a node crash. Each must have fired
        // exactly one graveyard row. Crash-orphaned wills fire on the
        // sweeper's schedule, so wait a few TTLs of virtual time for them
        // before declaring one lost (the wait itself is virtually free).
        let expected: Set<String> =
            self.model.wills.lock().unwrap().iter().map(|w| w.key.clone()).collect();
        // Progress-relative grace, not a fixed cap. A crash-orphaned will
        // fires on the sweeper's schedule, and under store faults each fire is
        // a durable write that may take many retried sweeps to land — a fixed
        // will_ttl*6 window cuts that fault-induced tail and reads a
        // still-firing will as lost (single-node high-fault fuzz). Instead
        // keep waiting as long as the graveyard is still GROWING (wills are
        // landing); give up only after a full grace window of NO progress.
        // A baseline where every will already fired breaks on the first check,
        // so this is inert there.
        let grace = Duration::from_millis(self.cfg.will_ttl_ms * 6);
        let mut deadline = tokio::time::Instant::now() + grace;
        let mut last_seen = 0usize;
        let graveyard = loop {
            let rows = self.read("graveyard", "SELECT k FROM g").await;
            let graveyard: Set<String> =
                rows.iter().map(|r| r["k"].as_str().expect("k").to_string()).collect();
            if expected.iter().all(|k| graveyard.contains(k)) {
                break graveyard;
            }
            if graveyard.len() > last_seen {
                last_seen = graveyard.len(); // progress: a will just fired
                deadline = tokio::time::Instant::now() + grace;
            }
            if tokio::time::Instant::now() > deadline {
                break graveyard; // no fire for a whole grace window: truly stuck
            }
            sleep(Duration::from_millis(self.cfg.will_ttl_ms / 2)).await;
        };
        for w in self.model.wills.lock().unwrap().iter() {
            if w.node_crashed && !self.cfg.wills_survive_node_crash {
                // Oracle silenced by config: record, don't fail.
                self.trace
                    .record(format!("will {} orphaned by crash of n{} (oracle off)", w.key, w.node));
                continue;
            }
            assert!(
                graveyard.contains(&w.key),
                "WILL violated: connection for {} is dead (node n{}, crashed={}) \
                 but its will never fired.\ntrace tail:\n{}",
                w.key,
                w.node,
                w.node_crashed,
                self.trace.dump_tail()
            );
        }
        // No phantom fires: the graveyard holds exactly the wills we
        // armed, nothing a bug conjured. (At-most-once double-fire is
        // masked by the idempotent INSERT OR IGNORE and neutralized by the
        // claim; this catches the opposite failure — a will that ran but
        // was never armed.)
        for k in &graveyard {
            assert!(
                expected.contains(k),
                "WILL violated: graveyard holds {k}, which no session armed.\ntrace tail:\n{}",
                self.trace.dump_tail()
            );
        }
        self.trace.record("final audit ok".into());
    }
}

/// Bounded window a consumer/watcher/subscriber gets to drain to the durable
/// tail AFTER production ends. Production itself is unbounded on purpose — a
/// SLOW producer under skew+pause (many retries serializing a fork) is not a
/// bug and must not trip a liveness crash, so the oracles wait for it however
/// long it takes. Only once producers stop does an inability to read the
/// committed tail become a genuine stuck. A producer that truly HANGS never
/// signals done: the run then never terminates and the mine wall-timeout
/// reports it. This makes "a crash is a bug" hold under skew+pause.
const LIVENESS_DRAIN: Duration = Duration::from_secs(600);

fn account_name(i: usize) -> String {
    format!("acct-{i}")
}

fn op(object: &str, sql: &str, params: Vec<Value>) -> Op {
    Op {
        object: object.to_string(),
        sql: sql.to_string(),
        params,
    }
}

// ------------------------------------------------------------------ run

/// One full simulated life of a cluster. Panics (loudly, with the trace
/// tail) on any oracle violation, internal assert, or deadlock.
pub async fn run(world: Arc<World>) -> RunReport {
    crate::PARANOIA.store(true, Ordering::SeqCst);
    let seed = world.cfg.seed;

    // Boot the fleet and lay the schema down.
    for i in 0..world.cfg.nodes {
        world.boot_node(i).await.expect("initial boot succeeds");
    }
    world.create_schemas().await;

    // Phase 1: the whole battery under a storage fault window and a
    // partition — transfers, ERC20, escrows, counters, and watched feeds
    // (produced + consumed three ways) all racing on one world.
    let doomed_node = below(&world.rng, world.cfg.nodes as u64) as usize;
    let doomed_wills = world.arm_wills(1, doomed_node).await;
    let mut phase1 = Vec::new();
    {
        let w = world.clone();
        phase1.push(tokio::spawn(async move { w.transfer_phase(1).await }));
        let w = world.clone();
        phase1.push(tokio::spawn(async move { w.erc20_phase(1).await }));
        let w = world.clone();
        phase1.push(tokio::spawn(async move { w.escrow_phase(1).await }));
        let w = world.clone();
        phase1.push(tokio::spawn(async move { w.counter_phase(1).await }));
    }
    // Feeds: producers + consumers run across BOTH phases, so start them
    // here and join after phase 2. producers_done gates consumer exit.
    let producers_done = Arc::new(AtomicBool::new(false));
    let mut feed_tasks = Vec::new();
    let mut consumer_tasks = Vec::new();
    for f in 0..world.cfg.feeds {
        let w = world.clone();
        feed_tasks.push(tokio::spawn(async move {
            w.feed_producer(f, 1).await;
            w.feed_producer(f, 2).await;
        }));
        // conn ids are namespaced per (feed, role) so cancels don't collide.
        let base = 0x1000_0000u64 + (f as u64) * 0x100;
        let (w, pd) = (world.clone(), producers_done.clone());
        consumer_tasks.push(tokio::spawn(async move {
            (f, false, w.consume_feed(f, false, pd, base).await)
        }));
        let (w, pd) = (world.clone(), producers_done.clone());
        consumer_tasks.push(tokio::spawn(async move {
            (f, true, w.consume_feed(f, true, pd, base + 1).await)
        }));
        let (w, pd) = (world.clone(), producers_done.clone());
        consumer_tasks.push(tokio::spawn(async move {
            w.watch_feed_changes(f, pd, base + 2).await;
            (f, false, Vec::new())
        }));
    }
    sleep(Duration::from_millis(300)).await;
    world.store.set_failing(true);
    if world.cfg.nodes >= 2 {
        world.net.partition(&addr_of(0), &addr_of(1));
        world.trace.record("partition n0<->n1".into());
    }
    sleep(Duration::from_millis(1500)).await;
    world.store.set_failing(false);
    world.net.heal();
    world.trace.record("faults healed".into());
    for t in phase1 {
        t.await.expect("phase 1 workload");
    }

    // Crash the node holding the doomed wills; drop their sessions the
    // way a dead server drops sockets: without ceremony.
    if world.cfg.crashes >= 1 {
        world.crash_node(doomed_node);
        drop(doomed_wills);
        if world.cfg.restarts {
            sleep(Duration::from_millis(world.cfg.fence_ttl_ms * 2)).await;
            let _ = world.boot_node(doomed_node).await;
        }
    }

    // Quiescent audit while the cluster is (possibly) one node short.
    // Feeds are still live (they span both phases), so they're audited
    // only at the end; everything else is quiescent here.
    world.audit_conservation("mid-run").await;
    world.audit_erc20("mid-run").await;
    world.audit_escrows("mid-run").await;
    world.audit_counters("mid-run").await;

    // Phase 2: the battery again + the original pub/sub test, then a crash.
    let mut phase2 = Vec::new();
    {
        let w = world.clone();
        phase2.push(tokio::spawn(async move { w.transfer_phase(2).await }));
        let w = world.clone();
        phase2.push(tokio::spawn(async move { w.erc20_phase(2).await }));
        let w = world.clone();
        phase2.push(tokio::spawn(async move { w.escrow_phase(2).await }));
        let w = world.clone();
        phase2.push(tokio::spawn(async move { w.counter_phase(2).await }));
    }
    let w = world.clone();
    let publisher = tokio::spawn(async move { w.publish_all().await });
    let publisher_done = Arc::new(AtomicBool::new(false));
    let w = world.clone();
    let pd = publisher_done.clone();
    let subscriber = tokio::spawn(async move { w.subscribe_all(pd).await });
    if world.cfg.crashes >= 2 {
        sleep(Duration::from_millis(700)).await;
        let live = world.live_indices();
        if live.len() > 1 {
            let victim = live[below(&world.rng, live.len() as u64) as usize];
            world.crash_node(victim);
            if world.cfg.restarts {
                sleep(Duration::from_millis(world.cfg.fence_ttl_ms * 2)).await;
                let _ = world.boot_node(victim).await;
            }
        }
    }
    // The pause adversary: a live node loses its lease to a peer and
    // rejoins. Fencing must keep the zombie's post-takeover writes out.
    for _ in 0..world.cfg.pause_faults {
        world.pause_fault().await;
    }
    for t in phase2 {
        t.await.expect("phase 2 workload");
    }
    publisher.await.expect("publisher");
    publisher_done.store(true, Ordering::SeqCst);
    let received = subscriber.await.expect("subscriber");

    // Producers are done; feeds can now drain and their consumers exit.
    for t in feed_tasks {
        t.await.expect("feed producer");
    }
    producers_done.store(true, Ordering::SeqCst);
    let mut feed_transcripts: Map<usize, Set<String>> = Map::default();
    for t in consumer_tasks {
        let (feed, _durable, transcript) = t.await.expect("feed consumer");
        let entry = feed_transcripts.entry(feed).or_default();
        for (_id, k) in transcript {
            entry.insert(k);
        }
    }

    // Subscriber oracle: everything acked arrived, nothing twice.
    {
        let mut seen = Set::default();
        for k in &received {
            assert!(seen.insert(k.clone()), "SUBSCRIBER saw {k} twice");
        }
        let publishes = world.model.publishes.lock().unwrap();
        for (k, acked) in publishes.iter() {
            if *acked {
                assert!(seen.contains(k), "SUBSCRIBER missed acked publish {k}");
            }
        }
    }

    world.audit_conservation("pre-shutdown").await;

    // Stop the world the hard way, then audit from a cold, fresh node:
    // recovery is part of what's under test.
    for i in 0..world.cfg.nodes {
        world.crash_node(i);
    }
    sleep(Duration::from_millis(world.cfg.fence_ttl_ms * 2)).await;
    world
        .boot_node_claiming(0, ClaimSpec::All)
        .await
        .expect("auditor boots");
    world.audit_conservation("after recovery").await;
    world.audit_erc20("after recovery").await;
    world.audit_escrows("after recovery").await;
    world.audit_counters("after recovery").await;
    world.audit_durable_deliveries().await;
    for (feed, delivered) in &feed_transcripts {
        world.audit_feed_completeness(*feed, delivered).await;
    }
    world.audit_final().await;

    RunReport {
        seed,
        events: world.trace.events(),
        trace_hash: world.trace.hash(),
    }
}

/// Run one seed on a fresh single-threaded runtime with a paused clock —
/// the determinism sandbox. This is the only entry the dst binary uses.
/// The runtime gets its own fat stack: a whole cluster's async machinery
/// polls nested on one thread, and debug-build frames are enormous.
pub fn run_blocking(cfg: DstConfig) -> RunReport {
    // Wall-clock watchdog: virtual time makes every legitimate wait free,
    // so a run burning real minutes is itself a bug (a stalled scheduler,
    // a busy loop). Dump the trace tail and crash.
    let seed = cfg.seed;
    let done = Arc::new(AtomicBool::new(false));
    let trace_for_watchdog: Arc<Mutex<Option<Arc<Trace>>>> = Arc::new(Mutex::new(None));
    {
        let done = done.clone();
        let trace = trace_for_watchdog.clone();
        std::thread::spawn(move || {
            for _ in 0..240 {
                std::thread::sleep(Duration::from_millis(500));
                if done.load(Ordering::SeqCst) {
                    return;
                }
            }
            let tail = trace
                .lock()
                .unwrap()
                .as_ref()
                .map(|t| t.dump_tail())
                .unwrap_or_default();
            eprintln!("WALL-CLOCK HANG (seed {seed}): 120s real elapsed.\ntrace tail:\n{tail}");
            std::process::abort();
        });
    }
    std::thread::Builder::new()
        .name("dst-world".into())
        .stack_size(256 << 20)
        .spawn({
            let done = done.clone();
            move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .expect("runtime");
                let report = rt.block_on(async {
                    let world = Arc::new(World::new(cfg));
                    *trace_for_watchdog.lock().unwrap() = Some(world.trace.clone());
                    run(world).await
                });
                done.store(true, Ordering::SeqCst);
                report
            }
        })
        .expect("world thread")
        .join()
        .expect("world thread must not panic quietly")
}
