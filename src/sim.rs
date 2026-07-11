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
    /// The canary: when true, a will armed on a node that CRASHES must
    /// still fire — which fafo today cannot honor (wills live in the
    /// node's memory). False skips that check (documented known issue)
    /// so mining hunts unknown bugs; flip it to watch the DST catch a
    /// real, known one at any seed with a crash-doomed will.
    pub wills_survive_node_crash: bool,
}

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
            wills_survive_node_crash: false,
        }
    }
}

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
        let node = cluster::start(NodeConfig {
            logical: self.cfg.logical_workers,
            claim,
            advertise: Some(addr.clone()),
            hysteresis: 8,
            secret: "dst".into(),
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
                    panic!(
                        "LIVENESS: {what} hung for {}ms virtual — deadlock.\nworkers:\n{dumps}\ntrace tail:\n{}",
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
                Err(_) => panic!(
                    "LIVENESS: read of {object} hung.\ntrace tail:\n{}",
                    self.trace.dump_tail()
                ),
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
    async fn subscribe_all(self: &Arc<Self>) -> Vec<String> {
        let mut received = Vec::new();
        let mut cursor = 0i64;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
        loop {
            let want: usize = {
                let p = self.model.publishes.lock().unwrap();
                p.iter().filter(|(_, acked)| *acked).count()
            };
            if received.len() >= want && want == self.cfg.channel_msgs {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "LIVENESS: subscriber stuck at {} of {want} acked messages.\ntrace tail:\n{}",
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
                false,
                None,
                u64::MAX, // sim subscriber's connection id space
                cursor as u64,
            );
            match timeout(Duration::from_millis(3000), poll).await {
                Err(_) => {
                    // Nothing new where we're parked; cancel and re-poll
                    // (the owner may have moved under us).
                    cluster::cancel_poll(&node, "chan", u64::MAX, cursor as u64);
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
                "will": { "ops": [{
                    "object": "graveyard",
                    "sql": "INSERT INTO g (k) VALUES (?1)",
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
        let transfers = self.model.transfers.lock().unwrap();
        for t in transfers.iter() {
            let debit = ledger[&t.from].iter().find(|(k, _)| k == &t.key);
            let credit = ledger[&t.to].iter().find(|(k, _)| k == &t.key);
            match (debit, credit) {
                (None, None) => {}
                (Some((_, d)), Some((_, c))) if *d == -*c => {}
                other => panic!(
                    "ATOMICITY violated {when}: transfer {} (acked={}, optimistic={}) is torn:                      debit={:?} credit={:?}.\ntrace tail:\n{}",
                    t.key, t.acked, t.optimistic, other.0, other.1,
                    self.trace.dump_tail()
                ),
            }
        }
        drop(transfers);

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
        drop(transfers);

        // Wills.
        let grave_rows = self.read("graveyard", "SELECT k FROM g").await;
        let graveyard: Set<String> = grave_rows
            .iter()
            .map(|r| r["k"].as_str().expect("k").to_string())
            .collect();
        for w in self.model.wills.lock().unwrap().iter() {
            if w.node_crashed && !self.cfg.wills_survive_node_crash {
                // Known gap: a will is process memory; the node took it
                // down. Recorded, not fatal — until the fix ships.
                self.trace
                    .record(format!("will {} orphaned by crash of n{} (known gap)", w.key, w.node));
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
        self.trace.record("final audit ok".into());
    }
}

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

    // Phase 1: transfers under a storage fault window and a partition.
    let doomed_node = below(&world.rng, world.cfg.nodes as u64) as usize;
    let doomed_wills = world.arm_wills(1, doomed_node).await;
    let w = world.clone();
    let workload = tokio::spawn(async move { w.transfer_phase(1).await });
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
    workload.await.expect("phase 1 clients");

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
    world.audit_conservation("mid-run").await;

    // Phase 2: transfers + the pub/sub battery, then another crash.
    let w = world.clone();
    let workload = tokio::spawn(async move { w.transfer_phase(2).await });
    let w = world.clone();
    let publisher = tokio::spawn(async move { w.publish_all().await });
    let w = world.clone();
    let subscriber = tokio::spawn(async move { w.subscribe_all().await });
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
    workload.await.expect("phase 2 clients");
    publisher.await.expect("publisher");
    let received = subscriber.await.expect("subscriber");

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
    let report = std::thread::Builder::new()
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
        .expect("world thread must not panic quietly");
    report
}
