//! A logical worker: one serial event loop that is both the admission
//! authority (per-object FIFO queues) and the executor for the objects it
//! owns. There is no global coordinator — a transaction whose participants
//! all live here never touches anything outside this task and the blob store.
//!
//! Admission is deterministic locking, decentralized: a transaction acquires
//! its participants in sorted object order. A local participant is a queue
//! position (proceed when at the head); a remote one is a Take — itself a
//! queued single-object operation at the current owner, so takes are ordered
//! and fair like everything else. Holding heads only of earlier objects
//! while waiting on a later one is ordered lock acquisition: deadlock-free.
//!
//! Every cross-worker transaction migrates its stray participants here
//! (that's also the placement learning rule), then runs locally. Ownership
//! transfer is cheap because the blob store already holds the state: the
//! giver evicts + checkpoints (remove side first), the receiver activates
//! from the blob on demand.
//!
//! Commit protocol per transaction (unchanged since v1):
//!   1. run every op inside a SQLite transaction on its participant
//!   2. commit locally, snapshot each database
//!   3. stage snapshots at            staging/<txn>/<id>.db
//!   4. put the commit record at      txns/<txn>.json          <- COMMIT POINT
//!   5. promote snapshots to          objects/<id>.db, clean up
//!
//! Crash after 4 → rolled forward by `recover` at boot. Before 4 → staging
//! garbage, swept at boot. Failure after local commit → evict participants
//! so memory never outruns the blob store.

use crate::api::ApiError;
use crate::cluster::{
    Node, Op, OpResult, TakeError, TransferMeta, TxnResponse, VisitInfo, checkpoint_key,
};
use crate::delta::{self, COMPACT_CHAIN, COMPACT_FRACTION_DENOM, DELTA_MIN_BYTES, Manifest};
use crate::grants;
use crate::object::{LiveObject, evict, fetch_image, materialize, object_key, purge};
use crate::store::BlobStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, oneshot};

/// Visits to the same worker within the window before a displacement stops
/// returning home and becomes a re-homing.
const REHOME_AFTER: u32 = 3;
const VISIT_WINDOW: u64 = 1000;
const TAKE_RETRIES: usize = 8;

pub enum WorkerMsg {
    Submit {
        /// Sorted, deduped; every op's object is among them.
        participants: Vec<String>,
        ops: Vec<Op>,
        read_only: bool,
        /// Present when this (read-only, single-object) txn is a long-poll:
        /// if its condition doesn't hold at the serialization point, it
        /// parks instead of replying and is re-checked after every write.
        poll: Option<PollOpts>,
        /// Capability-holder txns run under SQLite's authorizer: every
        /// action (through CTEs and trigger cascades) must be granted.
        cap: Option<Arc<grants::Capability>>,
        /// Optimistic txns are acked after local apply and ship in the next
        /// boat; pessimistic txns hold their ack until the boat is durable
        /// (and thereby act as flush barriers for everything before them).
        optimistic: bool,
        resp: oneshot::Sender<Result<TxnResponse, ApiError>>,
    },
    /// Another worker wants this object. Queued in the object's FIFO like
    /// any transaction; granted when it reaches the head.
    Take {
        object: String,
        taker: usize,
        resp: oneshot::Sender<Result<TransferMeta, TakeError>>,
    },
    /// This object is ours now (hysteresis return, or a grant completing).
    Adopt {
        object: String,
        meta: TransferMeta,
    },
    /// Abandon one parked poll (client went away or gave up).
    CancelPoll {
        object: String,
        conn: u64,
        frame: u64,
    },
    /// Disk pressure: deactivate idle clean objects (their files become
    /// commuter cache, which the ledger may then reclaim).
    Shed,
    /// Internal: a spawned take task finished.
    Taken {
        txn: u64,
        object: String,
        from: usize,
        result: TakenResult,
    },
    /// Internal: the in-flight boat finished shipping.
    ShipDone {
        objects: Vec<String>,
        ok: bool,
        /// Committed (the record landed) but the final object/delta put
        /// failed: the blob store is behind acked local state. The worker
        /// self-heals by re-shipping these from the live file.
        unpromoted: Vec<String>,
    },
    /// Internal: a spawned activation fetch finished. Blob I/O for cold
    /// objects happens off-loop so one cold tenant can't stall the others.
    Activated {
        txn: u64,
        object: String,
        result: Result<(Vec<u8>, u32), String>,
    },
    /// Flush the final boat, then stop.
    Shutdown {
        resp: oneshot::Sender<()>,
    },
    Stats {
        resp: oneshot::Sender<(u64, usize, usize)>,
    },
}

/// How a long-poll decides it is ready to reply.
///
/// Without `baseline`: reply when the result is non-empty — a condition
/// variable over SQL (`NOT EXISTS`, aggregates, thresholds all work).
/// With `baseline` (the hash from a previous reply): reply when the result
/// hash differs — change detection, Consul-blocking-query style, costing
/// the server one hash instead of a retained result set.
pub struct PollOpts {
    pub durable: bool,
    pub baseline: Option<String>,
    /// Identifies the subscription for cancellation: connection + frame id.
    pub conn: u64,
    pub frame: u64,
}

/// A parked long-poll: a read-only query waiting for its condition to hold.
/// Just SQL text and a reply slot — no retained results, no diff state.
struct PendingPoll {
    conn: u64,
    frame: u64,
    sql: String,
    params: Vec<Value>,
    durable: bool,
    baseline: Option<String>,
    resp: oneshot::Sender<Result<TxnResponse, ApiError>>,
}

pub enum TakenResult {
    Got(TransferMeta),
    /// Routing resolved to ourselves mid-flight (someone else brought it).
    AlreadyLocal,
    Failed(String),
}

enum Entry {
    Txn(u64),
    Take {
        taker: usize,
        resp: oneshot::Sender<Result<TransferMeta, TakeError>>,
    },
}

struct Parked {
    participants: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
    optimistic: bool,
    poll: Option<PollOpts>,
    cap: Option<Arc<grants::Capability>>,
    resp: oneshot::Sender<Result<TxnResponse, ApiError>>,
    /// How many participants (in sorted order) we hold queue heads for.
    acquired: usize,
    taking: bool,
    activating: bool,
    crossed: bool,
}

struct Meta {
    arrived_at: u64,
    return_to: Option<usize>,
    visit: Option<VisitInfo>,
}

type Waiter = (oneshot::Sender<Result<TxnResponse, ApiError>>, TxnResponse);

/// A locally-committed txn awaiting durability, for boat grouping.
struct AppliedTxn {
    participants: Vec<String>,
    /// Present for pessimistic (and demoted-optimistic) txns: acked when
    /// this txn's boat lands.
    waiter: Option<Waiter>,
}

struct Worker {
    node: Node,
    id: usize,
    live_dir: PathBuf,
    self_tx: mpsc::UnboundedSender<WorkerMsg>,
    /// Objects explicitly owned (checkpointed). Hash-default objects are
    /// owned implicitly via routing and only enter this set if they ever
    /// migrate away and come back.
    owned: HashSet<String>,
    objects: HashMap<String, LiveObject>,
    meta: HashMap<String, Meta>,
    queues: HashMap<String, VecDeque<Entry>>,
    parked: HashMap<u64, Parked>,
    next_txn: u64,
    txns_executed: u64,
    /// Boat state: objects with locally-committed, not-yet-shipped writes,
    /// with the size recorded when they first became dirty.
    dirty: HashMap<String, u64>,
    /// Approximate bytes of unshipped state; the backpressure watermark.
    /// Below max_unshipped nothing changes; above it, optimistic txns are
    /// quietly demoted to boat-riders, which paces producers to ship speed.
    dirty_bytes: u64,
    /// Applied-but-unshipped txns, in order. Boats are cut along
    /// txn-connected components of these, so one txn's participants can
    /// never straddle two commit records (atomic durability), while the
    /// byte cap keeps a single boat within the container's RAM budget.
    pending_txns: Vec<AppliedTxn>,
    /// Page-hash manifests for large objects (delta shipping). Built on
    /// activation or first large ship; dropped on evict.
    manifests: HashMap<String, Manifest>,
    /// Objects in the currently shipping boat (at most one boat in flight;
    /// the next launches the moment it lands, if anything is dirty).
    inflight: Option<HashSet<String>>,
    /// Parked long-polls per object, re-checked after every write txn
    /// (durable ones at boat launch, riding the waiter list to landing).
    polls: HashMap<String, Vec<PendingPoll>>,
    /// Set while draining for shutdown; answered when the last boat lands.
    closing: Option<oneshot::Sender<()>>,
    done: bool,
}

pub fn spawn(node: Node, id: usize, live_dir: PathBuf) -> anyhow::Result<mpsc::UnboundedSender<WorkerMsg>> {
    std::fs::create_dir_all(&live_dir)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    // Seed the explicit-owned set from routing exceptions loaded at boot.
    let owned: HashSet<String> = {
        let routing = node.routing.read().unwrap();
        routing
            .exceptions
            .iter()
            .filter(|&(_, w)| *w == id)
            .map(|(o, _)| o.clone())
            .collect()
    };
    let mut worker = Worker {
        node,
        id,
        live_dir,
        self_tx: tx.clone(),
        owned,
        objects: HashMap::new(),
        meta: HashMap::new(),
        queues: HashMap::new(),
        parked: HashMap::new(),
        next_txn: 0,
        txns_executed: 0,
        dirty: HashMap::new(),
        dirty_bytes: 0,
        pending_txns: Vec::new(),
        manifests: HashMap::new(),
        inflight: None,
        polls: HashMap::new(),
        closing: None,
        done: false,
    };
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            worker.handle(msg).await;
            if worker.done {
                break;
            }
        }
    });
    Ok(tx)
}

impl Worker {
    fn owns(&self, object: &str) -> bool {
        self.node.routing.read().unwrap().owner_of(object) == self.id
    }

    fn now(&self) -> u64 {
        self.node.clock.load(Ordering::Relaxed)
    }

    async fn handle(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::Submit {
                participants,
                ops,
                read_only,
                poll,
                cap,
                optimistic,
                resp,
            } => {
                let txn = self.next_txn;
                self.next_txn += 1;
                self.parked.insert(
                    txn,
                    Parked {
                        participants,
                        ops,
                        read_only,
                        optimistic,
                        poll,
                        cap,
                        resp,
                        acquired: 0,
                        taking: false,
                        activating: false,
                        crossed: false,
                    },
                );
                self.pump(vec![txn]).await;
            }
            WorkerMsg::Take {
                object,
                taker,
                resp,
            } => {
                if !self.owns(&object) {
                    let hint = {
                        let routing = self.node.routing.read().unwrap();
                        Some(routing.owner_of(&object)).filter(|w| *w != self.id)
                    };
                    let _ = resp.send(Err(TakeError::NotMine { hint }));
                    return;
                }
                let queue = self.queues.entry(object.clone()).or_default();
                queue.push_back(Entry::Take { taker, resp });
                if queue.len() == 1 {
                    let ready = self.service_front(&object).await;
                    self.pump(ready).await;
                }
            }
            WorkerMsg::Adopt { object, meta } => {
                self.admit(&object, meta, false).await;
                // A parked txn may have a take in flight for this object;
                // its retry loop will resolve back to us and short-circuit.
            }
            WorkerMsg::Shed => {
                let mut shed_any = false;
                let ids: Vec<String> = self.objects.keys().cloned().collect();
                for id in ids {
                    // Only idle, clean, unqueued objects may deactivate:
                    // their file equals durably shipped state.
                    if self.unshipped(&id) || self.queues.contains_key(&id) {
                        continue;
                    }
                    if let Some(LiveObject { conn, live_path }) = self.objects.remove(&id) {
                        drop(conn); // close before the ledger may unlink it
                        self.manifests.remove(&id);
                        self.node.disk.lock().unwrap().set_cache(live_path, self.id);
                        shed_any = true;
                    }
                }
                if shed_any {
                    self.node.enforce_disk();
                }
            }
            WorkerMsg::Taken {
                txn,
                object,
                from,
                result,
            } => self.on_taken(txn, object, from, result).await,
            WorkerMsg::ShipDone {
                objects,
                ok,
                unpromoted,
            } => self.on_ship_done(objects, ok, unpromoted).await,
            WorkerMsg::Activated {
                txn,
                object,
                result,
            } => self.on_activated(txn, object, result).await,
            WorkerMsg::CancelPoll {
                object,
                conn,
                frame,
            } => {
                if let Some(list) = self.polls.get_mut(&object) {
                    list.retain(|p| !(p.conn == conn && p.frame == frame));
                    if list.is_empty() {
                        self.polls.remove(&object);
                    }
                }
            }
            WorkerMsg::Shutdown { resp } => {
                self.closing = Some(resp);
                self.maybe_launch();
                self.maybe_finish_closing();
            }
            WorkerMsg::Stats { resp } => {
                let parked_polls = self.polls.values().map(Vec::len).sum();
                let _ = resp.send((self.txns_executed, self.owned.len(), parked_polls));
            }
        }
    }

    fn maybe_finish_closing(&mut self) {
        if self.inflight.is_none()
            && self.pending_txns.is_empty()
            && let Some(done) = self.closing.take()
        {
            let objects: Vec<String> = self.polls.keys().cloned().collect();
            for object in objects {
                self.fail_polls(&object, "worker shutting down; re-poll");
            }
            let _ = done.send(());
            self.done = true;
        }
    }

    /// Launch a boat if nothing is in flight and there's dirty state.
    /// No timer: ships leave as often as possible, so batch size adapts to
    /// load — one txn per boat when quiet, everything that accumulated
    /// during the last round trip when busy.
    ///
    /// Each dirty object ships either a full snapshot or a page delta
    /// against its manifest — snapshots for small objects and for
    /// compaction (long chains or big diffs), deltas otherwise.
    fn maybe_launch(&mut self) {
        if self.inflight.is_some() || self.pending_txns.is_empty() {
            return;
        }
        // Cut the boat along txn-connected components: a txn's participants
        // must ship under one commit record (atomic durability), but
        // independent components can wait for the next boat when this one
        // hits the byte cap — bounding one shipment's RAM to the budget.
        let pending = std::mem::take(&mut self.pending_txns);
        let mut selected: Vec<usize> = Vec::new(); // txn indices for this boat
        let mut boat_objects: HashSet<String> = HashSet::new();
        let mut boat_bytes = 0u64;
        let mut leftover: Vec<usize> = Vec::new();
        for component in txn_components(&pending) {
            let comp_objects: HashSet<&String> = component
                .iter()
                .flat_map(|&i| &pending[i].participants)
                .collect();
            let comp_bytes: u64 = comp_objects
                .iter()
                .map(|o| self.dirty.get(*o).copied().unwrap_or(0))
                .sum();
            // Always take at least one component, else nothing ever ships.
            if !selected.is_empty() && boat_bytes + comp_bytes > self.node.limits.max_boat_bytes {
                leftover.extend(component);
                continue;
            }
            boat_bytes += comp_bytes;
            boat_objects.extend(comp_objects.into_iter().cloned());
            selected.extend(component);
        }
        // Reassemble: leftover txns (order-preserved) stay pending; the
        // boat takes its objects out of dirty and its waiters along.
        let mut pending: Vec<Option<AppliedTxn>> = pending.into_iter().map(Some).collect();
        let mut waiters: Vec<Waiter> = Vec::new();
        for &i in &selected {
            if let Some(w) = pending[i].take().and_then(|t| t.waiter) {
                waiters.push(w);
            }
        }
        let mut keep: Vec<usize> = leftover;
        keep.sort_unstable();
        for i in keep {
            if let Some(t) = pending[i].take() {
                self.pending_txns.push(t);
            }
        }
        let objects: Vec<String> = boat_objects.into_iter().collect();
        for id in &objects {
            if let Some(bytes) = self.dirty.remove(id) {
                self.dirty_bytes = self.dirty_bytes.saturating_sub(bytes);
            }
        }
        let mut items = Vec::with_capacity(objects.len());
        let mut snap_err = false;
        for id in &objects {
            let live_path = &self.objects.get(id).expect("dirty object is live").live_path;
            let Ok(bytes) = std::fs::read(live_path) else {
                snap_err = true;
                break;
            };
            items.push(ShipItem {
                object: id.clone(),
                payload: self.ship_payload(id, bytes),
            });
        }
        if snap_err {
            // Local disk failure: revert to last durable, fail the waiters.
            for id in &objects {
                self.fail_polls(id, "state reverted; re-poll");
                purge(&mut self.objects, id);
                self.node
                    .disk
                    .lock()
                    .unwrap()
                    .remove(&self.live_dir.join(format!("{id}.db")));
                self.meta.remove(id);
                self.manifests.remove(id);
            }
            for (resp, _) in waiters {
                let _ = resp.send(Err(ApiError::internal("snapshot failed; state reverted")));
            }
            return;
        }
        // Durable polls are judged against exactly the state this boat
        // carries — checked here at launch, acked at the commit record by
        // joining the waiter list pessimistic txns already ride.
        for id in &objects {
            waiters.extend(self.take_fired_polls(id, true));
        }
        self.inflight = Some(objects.into_iter().collect());
        tokio::spawn(ship_task(
            self.node.clone(),
            self.self_tx.clone(),
            items,
            waiters,
        ));
    }

    /// How one dirty object rides the boat: small objects and compactions
    /// (long chain, or a diff too big to be worth it) ship whole snapshots;
    /// large objects with a healthy chain ship page deltas against their
    /// manifest.
    fn ship_payload(&mut self, id: &str, bytes: Vec<u8>) -> ShipPayload {
        if (bytes.len() as u64) < DELTA_MIN_BYTES {
            return ShipPayload::Snapshot {
                gc_deltas: self.manifests.contains_key(id),
                bytes,
            };
        }
        let Some(m) = self.manifests.get_mut(id) else {
            // First large ship: baseline with a snapshot.
            self.manifests.insert(id.to_string(), Manifest::of(&bytes, 0));
            return ShipPayload::Snapshot {
                gc_deltas: false,
                bytes,
            };
        };
        let d = delta::diff(m, &bytes);
        let delta_size: usize = d.pages.iter().map(|(_, p)| p.len()).sum();
        if m.chain_len + 1 > COMPACT_CHAIN
            || delta_size as u64 > bytes.len() as u64 / COMPACT_FRACTION_DENOM
        {
            m.chain_len = 0;
            ShipPayload::Snapshot {
                gc_deltas: true,
                bytes,
            }
        } else {
            m.chain_len += 1;
            ShipPayload::Delta {
                counter: d.counter,
                bytes: delta::encode(&d),
            }
        }
    }

    async fn on_ship_done(&mut self, objects: Vec<String>, ok: bool, unpromoted: Vec<String>) {
        self.inflight = None;
        if ok {
            self.node.stats.ships.fetch_add(1, Ordering::Relaxed);
            // Promotion failures leave the blob store behind acked local
            // state even though the commit record landed (recovery would
            // roll it forward at next boot — but a live take would activate
            // the stale base NOW). Self-heal: drop the manifest so the next
            // ship is a full snapshot baseline (a lost delta leaves a gap
            // no later delta bridges), and re-dirty the object so a repair
            // boat launches immediately. Dirty also means unshipped, which
            // keeps takes and returns waiting meanwhile.
            if !unpromoted.is_empty() {
                for id in &unpromoted {
                    self.manifests.remove(id);
                    if !self.dirty.contains_key(id) {
                        let bytes = self
                            .objects
                            .get(id)
                            .and_then(|o| std::fs::metadata(&o.live_path).ok())
                            .map(|m| m.len())
                            .unwrap_or(0);
                        self.dirty.insert(id.clone(), bytes);
                        self.dirty_bytes += bytes;
                    }
                }
                self.pending_txns.push(AppliedTxn {
                    participants: unpromoted,
                    waiter: None,
                });
            }
            // Durable polls registered mid-flight missed this boat's launch
            // check. If the object is clean right now, live state IS the
            // just-landed durable state — judge them against it.
            for id in &objects {
                if !self.dirty.contains_key(id) {
                    for (resp, fired) in self.take_fired_polls(id, true) {
                        let _ = resp.send(Ok(fired));
                    }
                }
            }
        } else {
            // The boat sank before its commit point: revert every affected
            // object to the last durable state. Optimistic acks inside the
            // boat are lost — that is the documented optimistic contract.
            eprintln!(
                "w{}: ship failed; reverting {} objects to last durable state",
                self.id,
                objects.len()
            );
            for id in &objects {
                // Parked polls judged optimistic state that just
                // un-happened; fail them loudly so clients re-poll against
                // one consistent history.
                self.fail_polls(id, "state reverted; re-poll");
                purge(&mut self.objects, id);
                self.meta.remove(id);
                self.manifests.remove(id);
                if let Some(bytes) = self.dirty.remove(id) {
                    self.dirty_bytes = self.dirty_bytes.saturating_sub(bytes);
                }
                self.node
                    .disk
                    .lock()
                    .unwrap()
                    .remove(&self.live_dir.join(format!("{id}.db")));
            }
            // Their unshipped txns died with the boat: these applied AFTER
            // the doomed boat launched, so their waiters never rode it.
            self.drop_pending_touching(&objects);
        }
        // Freshly shipped objects just became sheddable; give the disk
        // ledger a chance to reclaim if it's over budget.
        self.node.enforce_disk();
        // Next boat first, so takes/returns below see accurate dirty state.
        self.maybe_launch();
        let mut ready = Vec::new();
        for id in &objects {
            if self.queues.contains_key(id) {
                ready.extend(self.service_front(id).await);
            } else if !self.dirty.contains_key(id) {
                self.maybe_return_home(id).await;
            }
        }
        self.maybe_finish_closing();
        self.pump(ready).await;
    }

    /// Objects just reverted to durable state: pending txns touching them
    /// describe writes that no longer exist locally. Drop the entries and
    /// tell their waiters plainly rather than letting the reply slot die
    /// silently (or worse, letting a later boat snapshot a purged object).
    fn drop_pending_touching(&mut self, reverted: &[String]) {
        let reverted: HashSet<&String> = reverted.iter().collect();
        self.pending_txns.retain_mut(|t| {
            if t.participants.iter().any(|p| reverted.contains(p)) {
                if let Some((resp, _)) = t.waiter.take() {
                    let _ = resp.send(Err(ApiError::internal("state reverted; retry")));
                }
                false
            } else {
                true
            }
        });
    }

    /// Drive txns forward until everything is parked or done.
    async fn pump(&mut self, mut ready: Vec<u64>) {
        while let Some(txn) = ready.pop() {
            if self.advance(txn) {
                let unblocked = self.run_and_complete(txn).await;
                ready.extend(unblocked);
            }
        }
    }

    /// Acquire participants in sorted order. Returns true when the txn
    /// holds the head of every participant's queue, all of them live, and
    /// may run. Two async detours park the txn without blocking the loop:
    /// a Take (remote owner) and an Activation (owned but cold — the blob
    /// fetch runs in a spawned task so other objects keep being served).
    fn advance(&mut self, txn: u64) -> bool {
        loop {
            let Some(p) = self.parked.get_mut(&txn) else {
                return false;
            };
            if p.acquired == p.participants.len() {
                return true;
            }
            let object = p.participants[p.acquired].clone();
            let is_owner =
                self.node.routing.read().unwrap().owner_of(&object) == self.id;
            let p = self.parked.get_mut(&txn).unwrap();
            if is_owner {
                let queue = self.queues.entry(object.clone()).or_default();
                if !queue.iter().any(|e| matches!(e, Entry::Txn(t) if *t == txn)) {
                    queue.push_back(Entry::Txn(txn));
                }
                if !matches!(queue.front(), Some(Entry::Txn(t)) if *t == txn) {
                    return false; // waiting for the head; re-driven on pops
                }
                if !self.objects.contains_key(&object) {
                    // Head held, object cold: fetch off-loop. The held head
                    // keeps takes and later txns queued behind us.
                    if !p.activating {
                        p.activating = true;
                        let node = self.node.clone();
                        let reply = self.self_tx.clone();
                        let live_path = self.live_dir.join(format!("{object}.db"));
                        tokio::spawn(async move {
                            // Each in-flight fetch holds a full image in
                            // RAM; the permit caps how many at once.
                            let _permit =
                                node.activation_permits.clone().acquire_owned().await;
                            let result = fetch_image(&node.store, &object, &live_path)
                                .await
                                .map_err(|e| e.to_string());
                            let _ = reply.send(WorkerMsg::Activated {
                                txn,
                                object,
                                result,
                            });
                        });
                    }
                    return false; // waiting for Activated
                }
                p.acquired += 1;
                continue;
            }
            if !p.taking {
                p.taking = true;
                p.crossed = true;
                self.node.stats.takes.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(take_task(
                    self.node.clone(),
                    self.self_tx.clone(),
                    self.id,
                    txn,
                    object,
                ));
            }
            return false; // waiting for Taken
        }
    }

    async fn on_activated(
        &mut self,
        txn: u64,
        object: String,
        result: Result<(Vec<u8>, u32), String>,
    ) {
        if let Some(p) = self.parked.get_mut(&txn) {
            p.activating = false;
        }
        let outcome = result.and_then(|(image, chain_total)| {
            if self.owns(&object) && !self.objects.contains_key(&object) {
                materialize(&mut self.objects, &object, &self.live_dir, &image)
                    .map_err(|e| e.to_string())?;
                if image.len() as u64 >= DELTA_MIN_BYTES {
                    self.manifests
                        .insert(object.clone(), Manifest::of(&image, chain_total));
                }
                self.node.disk.lock().unwrap().set_live(
                    self.live_dir.join(format!("{object}.db")),
                    image.len() as u64,
                    self.id,
                );
                self.node.enforce_disk();
            }
            Ok(())
        });
        match outcome {
            Ok(()) => self.pump(vec![txn]).await,
            Err(e) => {
                let ready = self.fail_txn(txn, format!("activation failed: {e}")).await;
                self.pump(ready).await;
            }
        }
    }

    async fn on_taken(&mut self, txn: u64, object: String, from: usize, result: TakenResult) {
        let Some(p) = self.parked.get_mut(&txn) else {
            return;
        };
        p.taking = false;
        match result {
            TakenResult::Got(meta) => {
                self.admit(&object, meta, from != self.id).await;
                // The txn now holds the fresh object's queue head outright;
                // advance() will activate it (from commuter cache if this
                // object has lived here before).
                self.queues
                    .insert(object, VecDeque::from([Entry::Txn(txn)]));
                self.pump(vec![txn]).await;
            }
            TakenResult::AlreadyLocal => self.pump(vec![txn]).await,
            TakenResult::Failed(e) => {
                let ready = self.fail_txn(txn, e).await;
                self.pump(ready).await;
            }
        }
    }

    /// Apply hysteresis policy to a newly received object and record
    /// ownership (routing + checkpoint).
    async fn admit(&mut self, object: &str, tm: TransferMeta, _remote: bool) {
        let now = self.now();
        let mut meta = Meta {
            arrived_at: now,
            return_to: None,
            visit: None,
        };
        if tm.settled {
            let visits = match &tm.visit {
                Some(v) if v.worker == self.id && now >= v.last && now - v.last <= VISIT_WINDOW => {
                    v.count + 1
                }
                _ => 1,
            };
            // Repeat visits earn a move-in — but only if we're not already
            // crowded. Unchecked cohesion collapses everything onto one
            // mega-worker; a denied object stays a commuter instead.
            let crowded = self.node.routing.read().unwrap().crowded(self.id);
            if visits < REHOME_AFTER || crowded {
                meta.visit = Some(VisitInfo {
                    worker: self.id,
                    count: visits,
                    last: now,
                });
                meta.return_to = Some(tm.home);
            }
            // else: dragged here repeatedly and there's room — move in.
        } else {
            meta.visit = tm.visit; // returning home: keep the visit history
        }
        self.meta.insert(object.to_string(), meta);
        self.owned.insert(object.to_string());
        {
            let mut routing = self.node.routing.write().unwrap();
            if crate::cluster::default_worker(object, routing.logical) == self.id {
                routing.exceptions.remove(object);
            } else {
                routing.exceptions.insert(object.to_string(), self.id);
            }
        }
        self.checkpoint().await;
    }

    /// Re-check this object's parked polls (one durability class per pass).
    /// Fired polls are removed and returned with their computed reply — the
    /// caller either sends now (non-durable, or post-landing) or hands them
    /// to the departing boat's waiter list (durable, at launch). Polls whose
    /// client vanished are dropped here: lazy cleanup, no unsubscribe
    /// protocol needed.
    fn take_fired_polls(&mut self, object: &str, durable_pass: bool) -> Vec<Waiter> {
        let Some(list) = self.polls.remove(object) else {
            return Vec::new();
        };
        let Some(obj) = self.objects.get(object) else {
            // Not live (shed): polls keep waiting — the next write
            // reactivates the object and re-checks.
            self.polls.insert(object.to_string(), list);
            return Vec::new();
        };
        let mut fired = Vec::new();
        let mut keep = Vec::new();
        for (i, p) in list.into_iter().enumerate() {
            if p.resp.is_closed() {
                continue;
            }
            if p.durable != durable_pass {
                keep.push(p);
                continue;
            }
            match run_op(&obj.conn, &p.sql, &p.params) {
                Ok(result) => {
                    let results = vec![result];
                    let hash = poll_hash(&results);
                    if poll_ready(&p.baseline, &results, &hash) {
                        fired.push((
                            p.resp,
                            TxnResponse {
                                txn_id: format!("w{}-poll-{}", self.id, i),
                                results,
                                hash: Some(hash),
                            },
                        ));
                    } else {
                        keep.push(p);
                    }
                }
                // The schema changed under the query (e.g. DROP TABLE):
                // surface it rather than parking a poll that can never run.
                Err(e) => {
                    let _ = p.resp.send(Err(ApiError::bad_request(e)));
                }
            }
        }
        if !keep.is_empty() {
            self.polls.insert(object.to_string(), keep);
        }
        fired
    }

    /// This object's parked polls cannot be honored here anymore
    /// (migration, revert, shutdown): fail them all so clients re-poll.
    fn fail_polls(&mut self, object: &str, reason: &str) {
        if let Some(list) = self.polls.remove(object) {
            for p in list {
                let _ = p.resp.send(Err(ApiError::internal(reason.to_string())));
            }
        }
    }

    /// Unshipped local state? Then the blob store is stale for this object
    /// and ownership must not move until the boat lands.
    fn unshipped(&self, object: &str) -> bool {
        self.dirty.contains_key(object)
            || self
                .inflight
                .as_ref()
                .is_some_and(|boat| boat.contains(object))
    }

    /// Serve whatever is at the head of an object's queue. Returns txn ids
    /// ready to be pumped.
    async fn service_front(&mut self, object: &str) -> Vec<u64> {
        let Some(queue) = self.queues.get(object) else {
            return vec![];
        };
        match queue.front() {
            Some(Entry::Txn(t)) => vec![*t],
            // A take must wait for the object's state to be durable — the
            // receiver activates from the blob store. Boats ship as soon as
            // anything is dirty, so this resolves within one or two
            // ShipDones, which re-service this queue.
            Some(Entry::Take { .. }) if self.unshipped(object) => vec![],
            Some(Entry::Take { .. }) => self.grant_front(object).await,
            None => vec![],
        }
    }

    /// Grant the take at the head of this queue: quiesce, transfer, and
    /// bounce everyone left behind (they must re-resolve to the new owner).
    async fn grant_front(&mut self, object: &str) -> Vec<u64> {
        let mut queue = self.queues.remove(object).unwrap_or_default();
        let Some(Entry::Take { taker, resp }) = queue.pop_front() else {
            unreachable!("grant_front called on non-take head");
        };
        let tm = self.release(object, Some(taker)).await;
        let _ = resp.send(Ok(tm));

        let mut ready = Vec::new();
        for entry in queue {
            match entry {
                // Parked txns whose frontier was this object: re-drive; they
                // will discover the new owner and issue a take.
                Entry::Txn(t) => ready.push(t),
                Entry::Take { resp, .. } => {
                    let _ = resp.send(Err(TakeError::NotMine { hint: Some(taker) }));
                }
            }
        }
        ready
    }

    /// Drop local state and durably stop claiming this object. Remove side
    /// writes its checkpoint BEFORE the receiver adds, so no object is ever
    /// durably claimed twice.
    async fn release(&mut self, object: &str, new_owner: Option<usize>) -> TransferMeta {
        // Re-checks come from the worker that applies writes, and that is
        // about to be someone else: parked polls can't follow. Re-polling
        // is already the client's loop, so this is just an error.
        self.fail_polls(object, "object migrated; re-poll");
        evict(&mut self.objects, object);
        self.manifests.remove(object);
        // The kept file is now commuter cache: ledger may reclaim it.
        self.node
            .disk
            .lock()
            .unwrap()
            .set_cache(self.live_dir.join(format!("{object}.db")), self.id);
        let m = self.meta.remove(object);
        self.owned.remove(object);
        let now = self.now();
        let tm = match m {
            Some(m) => TransferMeta {
                settled: m.return_to.is_some()
                    || (self.node.hysteresis > 0
                        && now.saturating_sub(m.arrived_at) >= self.node.hysteresis),
                home: m.return_to.unwrap_or(self.id),
                visit: m.visit,
            },
            None => TransferMeta {
                settled: self.node.hysteresis > 0,
                home: self.id,
                visit: None,
            },
        };
        if let Some(new_owner) = new_owner {
            let mut routing = self.node.routing.write().unwrap();
            if crate::cluster::default_worker(object, routing.logical) == new_owner {
                routing.exceptions.remove(object);
            } else {
                routing.exceptions.insert(object.to_string(), new_owner);
            }
        }
        self.checkpoint().await;
        tm
    }

    // &mut self: a shared borrow held across the await would demand
    // Worker: Sync, which Connection forbids.
    async fn checkpoint(&mut self) {
        let mut owned: Vec<&String> = self.owned.iter().collect();
        owned.sort();
        let bytes = serde_json::to_vec(&json!({ "owned": owned })).expect("checkpoint serializes");
        if let Err(e) = self.node.store.put(&checkpoint_key(self.id), &bytes).await {
            eprintln!("w{}: checkpoint failed: {e}", self.id);
        }
    }

    async fn fail_txn(&mut self, txn: u64, msg: String) -> Vec<u64> {
        let Some(p) = self.parked.remove(&txn) else {
            return vec![];
        };
        let mut ready = Vec::new();
        // Release every queue entry this txn holds — acquired heads plus
        // the frontier entry it may be parked on (activation/head wait).
        for object in p.participants.iter() {
            if let Some(queue) = self.queues.get_mut(object) {
                queue.retain(|e| !matches!(e, Entry::Txn(t) if *t == txn));
                if queue.is_empty() {
                    self.queues.remove(object);
                } else {
                    ready.extend(self.service_front(object).await);
                }
            }
        }
        let _ = p
            .resp
            .send(Err(ApiError::internal(format!("acquisition failed: {msg}"))));
        ready
    }

    async fn run_and_complete(&mut self, txn: u64) -> Vec<u64> {
        let p = self.parked.remove(&txn).expect("parked txn exists");
        if p.crossed {
            self.node
                .stats
                .cross_worker_txns
                .fetch_add(1, Ordering::Relaxed);
        }
        self.txns_executed += 1;
        let now = self.now();
        for object in &p.participants {
            self.meta.entry(object.clone()).or_insert(Meta {
                arrived_at: now,
                return_to: None,
                visit: None,
            });
        }

        // Apply locally; durability is the boat's job.
        match self
            .apply(&p.participants, &p.ops, p.read_only, p.cap.as_ref())
            .await
        {
            Err(e) => {
                let _ = p.resp.send(Err(e));
            }
            Ok((results, changed)) => {
                let response = TxnResponse {
                    txn_id: format!("w{}-{}", self.id, txn),
                    results,
                    hash: None,
                };
                if p.read_only {
                    match p.poll {
                        // The poll's query just ran at the serialization
                        // point (queue head, object live): if it doesn't
                        // fire now, parking here is gapless — every later
                        // write re-checks it.
                        Some(po) => {
                            let object = p.participants[0].clone();
                            let hash = poll_hash(&response.results);
                            // A durable poll's condition may only be judged
                            // against durable state; with unshipped writes,
                            // discard this run and wait for the boat (its
                            // launch re-checks against exactly what ships).
                            if !(po.durable && self.unshipped(&object))
                                && poll_ready(&po.baseline, &response.results, &hash)
                            {
                                let mut response = response;
                                response.hash = Some(hash);
                                let _ = p.resp.send(Ok(response));
                            } else {
                                let op = &p.ops[0];
                                self.polls.entry(object).or_default().push(PendingPoll {
                                    conn: po.conn,
                                    frame: po.frame,
                                    sql: op.sql.clone(),
                                    params: op.params.clone(),
                                    durable: po.durable,
                                    baseline: po.baseline,
                                    resp: p.resp,
                                });
                            }
                        }
                        None => {
                            let _ = p.resp.send(Ok(response));
                        }
                    }
                } else if changed.is_empty() {
                    // TEMP-only (or no-op) write: durable state already
                    // matches, so there is nothing to ship — even a
                    // pessimistic txn acks right now. Ephemeral tables are
                    // signals, typing, cursors: same SQL, zero storage
                    // cost. Polls still fire below.
                    let _ = p.resp.send(Ok(response));
                    for object in &p.participants {
                        for (resp, fired) in self.take_fired_polls(object, false) {
                            let _ = resp.send(Ok(fired));
                        }
                    }
                } else {
                    for object in &changed {
                        if !self.dirty.contains_key(object) {
                            let bytes = self
                                .objects
                                .get(object)
                                .and_then(|o| std::fs::metadata(&o.live_path).ok())
                                .map(|m| m.len())
                                .unwrap_or(0);
                            self.dirty.insert(object.clone(), bytes);
                            self.dirty_bytes += bytes;
                            self.node
                                .disk
                                .lock()
                                .unwrap()
                                .touch(&self.live_dir.join(format!("{object}.db")), bytes);
                        }
                    }
                    // Backpressure only when genuinely needed: below the
                    // watermark, optimistic acks immediately. Above it,
                    // optimistic rides the boat like everyone else, which
                    // paces producers to ship speed until the backlog drains.
                    let waiter = if p.optimistic && self.dirty_bytes <= self.node.max_unshipped {
                        let _ = p.resp.send(Ok(response));
                        None
                    } else {
                        Some((p.resp, response))
                    };
                    // Boat grouping only binds the objects that must land
                    // together — the ones this txn durably changed.
                    self.pending_txns.push(AppliedTxn {
                        participants: changed,
                        waiter,
                    });
                    for object in &p.participants {
                        for (resp, fired) in self.take_fired_polls(object, false) {
                            let _ = resp.send(Ok(fired));
                        }
                    }
                }
            }
        }

        let mut ready = Vec::new();
        for object in &p.participants {
            let Some(queue) = self.queues.get_mut(object) else {
                continue;
            };
            let popped = queue.pop_front();
            debug_assert!(matches!(popped, Some(Entry::Txn(t)) if t == txn));
            if queue.is_empty() {
                self.queues.remove(object);
                self.maybe_return_home(object).await;
            } else {
                ready.extend(self.service_front(object).await);
            }
        }
        self.maybe_launch();
        ready
    }

    /// Hysteresis: a displaced object with an idle queue goes home before
    /// its clique next transacts. Deferred while unshipped (the ShipDone
    /// handler retries) — home would activate a stale blob otherwise.
    async fn maybe_return_home(&mut self, object: &str) {
        if self.unshipped(object) {
            return;
        }
        let Some(home) = self.meta.get(object).and_then(|m| m.return_to) else {
            return;
        };
        if home == self.id || !self.owns(object) {
            return;
        }
        self.node.stats.returns.fetch_add(1, Ordering::Relaxed);
        let mut tm = self.release(object, Some(home)).await;
        tm.settled = false; // it's going home, not visiting
        tm.home = home;
        let node = self.node.clone();
        let object = object.to_string();
        tokio::spawn(async move {
            send_adopt(&node, home, object, tm).await;
        });
    }

    // ------------------------------------------------------------ execution

    /// Apply a transaction to local state only. All-or-nothing across the
    /// participants (local SQLite txns), but durability is deferred to the
    /// boat: run_and_complete marks participants dirty and maybe_launch
    /// ships them.
    /// On writes, additionally returns the participants whose MAIN database
    /// file actually changed (header change counter moved). A txn that only
    /// touched TEMP tables — or changed nothing — commits locally, wakes
    /// polls, and ships nothing: ephemeral state is free.
    async fn apply(
        &mut self,
        participants: &[String],
        ops: &[Op],
        read_only: bool,
        cap: Option<&Arc<grants::Capability>>,
    ) -> Result<(Vec<OpResult>, Vec<String>), ApiError> {
        // Participants are guaranteed live: advance() activates cold
        // objects (off-loop) before a txn is allowed to run.
        fn conn_of<'a>(
            objects: &'a HashMap<String, LiveObject>,
            id: &str,
        ) -> &'a rusqlite::Connection {
            &objects.get(id).expect("participant activated").conn
        }

        if read_only {
            let mut results = Vec::with_capacity(ops.len());
            for op in ops {
                let conn = conn_of(&self.objects, &op.object);
                let stmt = conn
                    .prepare(&op.sql)
                    .map_err(|e| ApiError::bad_request(e.to_string()))?;
                if !stmt.readonly() {
                    return Err(ApiError::bad_request(
                        "query must be read-only; use /txn or /objects/{id}/exec for writes",
                    ));
                }
                drop(stmt);
                results.push(run_op(conn, &op.sql, &op.params).map_err(ApiError::bad_request)?);
            }
            return Ok((results, Vec::new()));
        }

        let counters_before: Vec<Option<u32>> = participants
            .iter()
            .map(|id| file_change_counter(&self.objects[id].live_path))
            .collect();
        // Capability holders run under SQLite's own authorizer: it fires
        // at prepare time for every action — through CTEs and trigger
        // cascades — so an insert-only token cannot smuggle an UPDATE in
        // anywhere. Installed per participant (a cross-object txn may
        // carry different verbs per object), removed before COMMIT.
        if let Some(cap) = cap {
            for id in participants {
                let object = id.clone();
                let cap = cap.clone();
                conn_of(&self.objects, id).authorizer(Some(
                    move |ctx: rusqlite::hooks::AuthContext<'_>| cap_gate(&cap, &object, &ctx),
                ));
            }
        }
        let clear_authorizers = |objects: &HashMap<String, LiveObject>| {
            if cap.is_some() {
                for id in participants {
                    conn_of(objects, id).authorizer(
                        None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
                    );
                }
            }
        };
        for id in participants {
            if let Err(e) = conn_of(&self.objects, id).execute_batch("BEGIN") {
                clear_authorizers(&self.objects);
                return Err(ApiError::internal(e.to_string()));
            }
        }

        let mut results = Vec::with_capacity(ops.len());
        let mut failed = None;
        for op in ops {
            match run_op(conn_of(&self.objects, &op.object), &op.sql, &op.params) {
                Ok(r) => results.push(r),
                Err(msg) => {
                    failed = Some(msg);
                    break;
                }
            }
        }
        if let Some(msg) = failed {
            for id in participants {
                let _ = conn_of(&self.objects, id).execute_batch("ROLLBACK");
            }
            clear_authorizers(&self.objects);
            return Err(ApiError::bad_request(format!(
                "op failed, transaction rolled back: {msg}"
            )));
        }
        clear_authorizers(&self.objects);

        let mut commit_err = None;
        for (i, id) in participants.iter().enumerate() {
            if let Err(e) = conn_of(&self.objects, id).execute_batch("COMMIT") {
                commit_err = Some((i, e));
                break;
            }
        }
        if let Some((failed_at, e)) = commit_err {
            // COMMIT can itself fail (a DEFERRED constraint, checked only
            // now). The failing participant and everyone after it still
            // hold an OPEN transaction: ROLLBACK erases exactly this txn
            // and preserves everything before it, acked-unshipped writes
            // included.
            for id in &participants[failed_at..] {
                let _ = conn_of(&self.objects, id).execute_batch("ROLLBACK");
            }
            // Participants before it already committed and cannot be
            // uncommitted: erase the txn atomically by reverting them to
            // durable state. Their unshipped writes revert too — the
            // sunk-boat contract, applied locally.
            for id in &participants[..failed_at] {
                self.fail_polls(id, "state reverted; re-poll");
                purge(&mut self.objects, id);
                self.manifests.remove(id);
                if let Some(bytes) = self.dirty.remove(id) {
                    self.dirty_bytes = self.dirty_bytes.saturating_sub(bytes);
                }
                self.node
                    .disk
                    .lock()
                    .unwrap()
                    .remove(&self.live_dir.join(format!("{id}.db")));
            }
            self.drop_pending_touching(&participants[..failed_at]);
            return Err(ApiError::internal(format!("local commit failed: {e}")));
        }

        // SQLite bumps the main file's change counter iff the commit wrote
        // it. Unchanged counter = TEMP-only (or no-op) writes: durable
        // state is already correct, so the boat has nothing to carry.
        let changed = participants
            .iter()
            .zip(&counters_before)
            .filter(|(id, before)| {
                let after = file_change_counter(&self.objects[id.as_str()].live_path);
                after != **before || after.is_none()
            })
            .map(|(id, _)| id.clone())
            .collect();
        Ok((results, changed))
    }
}

/// SQLite authorizer gate for capability holders. Maps engine actions to
/// grant verbs; reads inside a write txn are allowed for anyone holding
/// any verb on the object (you cannot meaningfully UPDATE without reading
/// the WHERE columns), while top-level reads go through the "read" verb at
/// the API layer. PRAGMA and ATTACH are flatly denied to capabilities.
fn cap_gate(
    cap: &grants::Capability,
    object: &str,
    ctx: &rusqlite::hooks::AuthContext<'_>,
) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};
    let verb = match ctx.action {
        AuthAction::Insert { .. } => "insert",
        AuthAction::Update { .. } => "update",
        AuthAction::Delete { .. } => "delete",
        AuthAction::Read { .. } => {
            let any = ["read", "insert", "update", "delete", "ddl"]
                .iter()
                .any(|v| grants::allows(cap, object, v));
            return if any {
                Authorization::Allow
            } else {
                Authorization::Deny
            };
        }
        AuthAction::Select
        | AuthAction::Function { .. }
        | AuthAction::Transaction { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Recursive => return Authorization::Allow,
        AuthAction::Pragma { .. } | AuthAction::Attach { .. } | AuthAction::Detach { .. } => {
            return Authorization::Deny;
        }
        // Everything else is schema-shaped: CREATE/DROP/ALTER/REINDEX/...,
        // temp variants included. Unknown actions land here too — deny by
        // default is the right failure mode for a security gate.
        _ => "ddl",
    };
    if grants::allows(cap, object, verb) {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

/// The 4-byte big-endian change counter in the SQLite header — the same
/// counter delta shipping versions by. None if unreadable (treated as
/// changed: when in doubt, ship).
fn file_change_counter(path: &std::path::Path) -> Option<u32> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).ok()?;
    f.seek(SeekFrom::Start(delta::HEADER_CHANGE_COUNTER as u64))
        .ok()?;
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf).ok()?;
    Some(u32::from_be_bytes(buf))
}

/// Group pending txns into txn-connected components (union-find): txns
/// sharing a participant must land under one commit record, but disjoint
/// groups may ship on different boats. Components come back in
/// first-appearance order, each as indices into `pending`.
fn txn_components(pending: &[AppliedTxn]) -> Vec<Vec<usize>> {
    fn find(parent: &mut [usize], i: usize) -> usize {
        if parent[i] != i {
            parent[i] = find(parent, parent[i]);
        }
        parent[i]
    }
    let mut parent: Vec<usize> = (0..pending.len()).collect();
    let mut first_txn_with: HashMap<&str, usize> = HashMap::new();
    for (i, txn) in pending.iter().enumerate() {
        for object in &txn.participants {
            match first_txn_with.get(object.as_str()) {
                Some(&j) => {
                    let (a, b) = (find(&mut parent, i), find(&mut parent, j));
                    parent[a] = b;
                }
                None => {
                    first_txn_with.insert(object, i);
                }
            }
        }
    }
    let mut components: Vec<Vec<usize>> = Vec::new();
    let mut slot_of_root: HashMap<usize, usize> = HashMap::new();
    for i in 0..pending.len() {
        let root = find(&mut parent, i);
        let slot = *slot_of_root.entry(root).or_insert_with(|| {
            components.push(Vec::new());
            components.len() - 1
        });
        components[slot].push(i);
    }
    components
}

pub enum ShipPayload {
    Snapshot {
        bytes: Vec<u8>,
        /// True when this snapshot compacts an existing delta chain: after
        /// promotion, deltas at or below its change counter get deleted.
        /// (Skipping the GC is always safe — activation ignores them.)
        gc_deltas: bool,
    },
    Delta {
        counter: u32,
        bytes: Vec<u8>,
    },
}

pub struct ShipItem {
    pub object: String,
    pub payload: ShipPayload,
}

impl ShipItem {
    fn staging_key(&self, staging_id: &str) -> String {
        match &self.payload {
            ShipPayload::Snapshot { .. } => format!("staging/{staging_id}/{}.snap", self.object),
            ShipPayload::Delta { counter, .. } => {
                format!("staging/{staging_id}/{}.delta.{counter:010}", self.object)
            }
        }
    }

    fn bytes(&self) -> &[u8] {
        match &self.payload {
            ShipPayload::Snapshot { bytes, .. } => bytes,
            ShipPayload::Delta { bytes, .. } => bytes,
        }
    }
}

/// Ship one boat: stage every item (snapshots for small/compacting objects,
/// page deltas for large ones), write ONE commit record covering the whole
/// batch — the commit point — then promote. Recovery replays promotion from
/// the staged blobs, so a crash mid-promote is rolled forward.
async fn ship_task(
    node: Node,
    reply: mpsc::UnboundedSender<WorkerMsg>,
    items: Vec<ShipItem>,
    waiters: Vec<(oneshot::Sender<Result<TxnResponse, ApiError>>, TxnResponse)>,
) {
    let staging_id = uuid::Uuid::new_v4().to_string();
    let objects: Vec<String> = items.iter().map(|i| i.object.clone()).collect();
    let payload_bytes: u64 = items.iter().map(|i| i.bytes().len() as u64).sum();
    let mut err: Option<String> = None;

    // Small boats inline their payload into the commit record itself: the
    // entire commit becomes one blob write. Big boats stage first.
    let inline = payload_bytes <= INLINE_MAX_BYTES;
    if !inline {
        // Stage in parallel: boats with many objects pay one RTT, not one each.
        let staging_keys: Vec<String> =
            items.iter().map(|i| i.staging_key(&staging_id)).collect();
        let staged = futures::future::join_all(
            items
                .iter()
                .zip(&staging_keys)
                .map(|(item, key)| node.store.put(key, item.bytes())),
        )
        .await;
        if let Some(e) = staged.into_iter().find_map(|r| r.err()) {
            err = Some(e.to_string());
        }
    }

    // Fencing gate. Three layers, cheapest first:
    // 1. wait out earliest_write — a takeover of a possibly-paused
    //    predecessor must let its recency TTL expire before we write;
    // 2. recency: if our lease hasn't been verified within the TTL (we may
    //    BE that paused predecessor, just woken), verify inline right now;
    // 3. the fenced flag, set by any failed verification.
    if err.is_none() {
        let deadline = *node.earliest_write.lock().unwrap();
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        if crate::cluster::lease_stale(&node) && !crate::cluster::verify_leases(&node).await {
            err = Some("lease superseded; commit refused".into());
        }
    }
    if err.is_none() && node.fenced.load(Ordering::SeqCst) {
        err = Some("node is fenced; commit refused".into());
    }

    if err.is_none() {
        let inline_items = if inline {
            items
                .iter()
                .map(|item| {
                    // Same names as staging-key suffixes; recovery promotes
                    // through the same path.
                    let name = item
                        .staging_key(&staging_id)
                        .rsplit('/')
                        .next()
                        .unwrap()
                        .to_string();
                    (name, hex_encode(item.bytes()))
                })
                .collect()
        } else {
            Vec::new()
        };
        let record = serde_json::to_vec(&TxnRecord {
            txn_id: staging_id.clone(),
            objects: objects.clone(),
            inline: inline_items,
        })
        .expect("record serializes");
        if let Err(e) = node.store.put(&txn_key(&staging_id), &record).await {
            err = Some(e.to_string());
        }
    }

    match err {
        None => {
            // COMMITTED — the record landed, so ack the waiters NOW.
            // Promotion and cleanup are pure roll-forward that recovery
            // would redo anyway; making clients wait for them was measured
            // at ~2.3s of the pessimistic ack against real R2. The internal
            // ShipDone stays at the END: it unlocks takes and the next
            // boat, both of which must see promoted state.
            node.stats
                .bytes_shipped
                .fetch_add(payload_bytes, Ordering::Relaxed);
            for (resp, response) in waiters {
                let _ = resp.send(Ok(response));
            }

            let mut unpromoted: Vec<String> = Vec::new();
            for item in &items {
                let (key, bytes) = match &item.payload {
                    ShipPayload::Snapshot { bytes, .. } => (object_key(&item.object), bytes),
                    ShipPayload::Delta { counter, bytes } => {
                        (delta::delta_key(&item.object, *counter), bytes)
                    }
                };
                if node.store.put(&key, bytes).await.is_err() {
                    unpromoted.push(item.object.clone());
                }
            }
            if unpromoted.is_empty() {
                if !inline {
                    for item in &items {
                        let _ = node.store.delete(&item.staging_key(&staging_id)).await;
                    }
                }
                let _ = node.store.delete(&txn_key(&staging_id)).await;
                // Compaction GC: superseded deltas are already ignored by
                // activation (counter <= base); deleting them is hygiene.
                for item in &items {
                    if let ShipPayload::Snapshot {
                        bytes,
                        gc_deltas: true,
                    } = &item.payload
                    {
                        let base_counter = delta::change_counter(bytes);
                        if let Ok(keys) = node.store.list(&delta::delta_prefix(&item.object)).await
                        {
                            for key in keys {
                                if delta::parse_delta_counter(&key, &item.object)
                                    .is_some_and(|c| c <= base_counter)
                                {
                                    let _ = node.store.delete(&key).await;
                                }
                            }
                        }
                    }
                }
            }
            let _ = reply.send(WorkerMsg::ShipDone {
                objects,
                ok: true,
                unpromoted,
            });
        }
        Some(e) => {
            if !inline {
                for item in &items {
                    let _ = node.store.delete(&item.staging_key(&staging_id)).await;
                }
            }
            for (resp, _) in waiters {
                let _ = resp.send(Err(ApiError::internal(format!("commit failed: {e}"))));
            }
            let _ = reply.send(WorkerMsg::ShipDone {
                objects,
                ok: false,
                unpromoted: Vec::new(),
            });
        }
    }
}

/// Resolve the current owner and take the object, chasing NotMine hints.
async fn take_task(
    node: Node,
    reply: mpsc::UnboundedSender<WorkerMsg>,
    my_worker: usize,
    txn: u64,
    object: String,
) {
    let mut last_err = String::from("no attempts");
    for attempt in 0..TAKE_RETRIES {
        let owner = node.routing.read().unwrap().owner_of(&object);
        if owner == my_worker {
            let _ = reply.send(WorkerMsg::Taken {
                txn,
                object,
                from: my_worker,
                result: TakenResult::AlreadyLocal,
            });
            return;
        }
        let outcome = {
            let local_tx = crate::cluster::local_sender(&node, owner);
            if let Some(tx) = local_tx {
                let (rtx, rrx) = oneshot::channel();
                if tx
                    .send(WorkerMsg::Take {
                        object: object.clone(),
                        taker: my_worker,
                        resp: rtx,
                    })
                    .is_err()
                {
                    Err(anyhow::anyhow!("local worker gone"))
                } else {
                    rrx.await.map_err(|_| anyhow::anyhow!("take dropped"))
                }
            } else {
                let cached = node.routing.read().unwrap().addr_of_worker(owner);
                let addr = match cached {
                    Some(addr) => Some(addr),
                    None => crate::cluster::resolve_addr(&node, owner).await,
                };
                match addr {
                    Some(addr) => {
                        match crate::rpc::take(&node, &addr, owner, &object, my_worker).await {
                            // Stale address (lease moved): refresh and retry.
                            Err(e) => {
                                crate::cluster::resolve_addr(&node, owner).await;
                                Err(e)
                            }
                            ok => ok,
                        }
                    }
                    None => Err(anyhow::anyhow!("no live node for worker {owner}")),
                }
            }
        };
        match outcome {
            Ok(Ok(meta)) => {
                let _ = reply.send(WorkerMsg::Taken {
                    txn,
                    object,
                    from: owner,
                    result: TakenResult::Got(meta),
                });
                return;
            }
            Ok(Err(TakeError::NotMine { hint })) => {
                if let Some(h) = hint {
                    node.routing
                        .write()
                        .unwrap()
                        .exceptions
                        .insert(object.clone(), h);
                }
                last_err = format!("owner moved (hint {hint:?})");
            }
            Ok(Err(TakeError::Failed(e))) => last_err = e,
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(std::time::Duration::from_millis(10 * (attempt as u64 + 1))).await;
    }
    let _ = reply.send(WorkerMsg::Taken {
        txn,
        object,
        from: my_worker,
        result: TakenResult::Failed(last_err),
    });
}

async fn send_adopt(node: &Node, home: usize, object: String, meta: TransferMeta) {
    let local_tx = crate::cluster::local_sender(node, home);
    if let Some(tx) = local_tx {
        let _ = tx.send(WorkerMsg::Adopt { object, meta });
        return;
    }
    let addr = node.routing.read().unwrap().addr_of_worker(home);
    if let Some(addr) = addr
        && let Err(e) = crate::rpc::adopt(node, &addr, home, object.clone(), meta).await
    {
        // Failed return: the object is orphaned (no checkpoint claims it)
        // and falls back to its hash-default worker. Data is safe.
        eprintln!("return of {object} to w{home} failed: {e}");
    }
}

// ---------------------------------------------------------------- recovery

#[derive(Serialize, Deserialize)]
struct TxnRecord {
    txn_id: String,
    objects: Vec<String>,
    /// Small boats skip staging entirely: payloads ride INSIDE the commit
    /// record as (staged-style name, hex bytes) pairs, making the whole
    /// commit ONE blob write. Entries use the same `<obj>.snap` /
    /// `<obj>.delta.<counter>` names as staging keys, so recovery promotes
    /// them through the identical code path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    inline: Vec<(String, String)>,
}

/// Boats whose total payload fits here commit with a single R2 op.
const INLINE_MAX_BYTES: u64 = 96 * 1024;

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

fn txn_key(staging_id: &str) -> String {
    format!("txns/{staging_id}.json")
}

/// Promote one staged blob to its final home, keyed by its suffix:
/// `<object>.snap` -> the base snapshot, `<object>.delta.<counter>` -> a
/// chain entry.
async fn promote_staged(store: &dyn BlobStore, staged_key: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let name = staged_key.rsplit('/').next().unwrap_or_default();
    if let Some(object) = name.strip_suffix(".snap") {
        // Monotone in the change counter: a stale record (its promotion
        // failed once, then a later boat shipped newer state) must not roll
        // the base backwards. Stale deltas need no guard — activation
        // already ignores counters at or below the base.
        let current = store
            .get_range(&object_key(object), delta::HEADER_CHANGE_COUNTER as u64, 4)
            .await?
            .filter(|b| b.len() == 4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]));
        if current.is_some_and(|c| c >= delta::change_counter(bytes)) {
            return Ok(());
        }
        store.put(&object_key(object), bytes).await?;
    } else if let Some((object, counter)) = name.rsplit_once(".delta.") {
        store
            .put(&format!("objects/{object}.d.{counter}"), bytes)
            .await?;
    }
    Ok(())
}

/// Startup recovery. Roll forward any transaction whose commit record exists
/// (it committed; promotion just never finished), then discard staging left
/// by transactions that never reached their commit point. Idempotent, so
/// concurrent booting processes may both run it.
pub async fn recover(store: &dyn BlobStore) -> anyhow::Result<()> {
    for key in store.list("txns/").await? {
        let Some(bytes) = store.get(&key).await? else {
            continue;
        };
        let Ok(record) = serde_json::from_slice::<TxnRecord>(&bytes) else {
            store.delete(&key).await?;
            continue;
        };
        if record.inline.is_empty() {
            for staged in store.list(&format!("staging/{}/", record.txn_id)).await? {
                if let Some(blob) = store.get(&staged).await? {
                    promote_staged(store, &staged, &blob).await?;
                }
                store.delete(&staged).await?;
            }
        } else {
            // Inline boat: the payload lives in the record itself.
            for (name, hexed) in &record.inline {
                if let Some(blob) = hex_decode(hexed) {
                    promote_staged(store, name, &blob).await?;
                }
            }
        }
        store.delete(&key).await?;
        println!("recovered committed txn {}", record.txn_id);
    }
    for key in store.list("staging/").await? {
        store.delete(&key).await?;
        println!("discarded uncommitted staging blob {key}");
    }
    Ok(())
}

// ------------------------------------------------------------- op helpers

fn run_op(conn: &rusqlite::Connection, sql: &str, params: &[Value]) -> Result<OpResult, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let params = json_params(params)?;
    if stmt.column_count() > 0 {
        let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut obj = serde_json::Map::new();
            for (i, name) in names.iter().enumerate() {
                let v = row.get_ref(i).map_err(|e| e.to_string())?;
                obj.insert(name.clone(), value_to_json(v));
            }
            out.push(Value::Object(obj));
        }
        Ok(OpResult::Rows { rows: out })
    } else {
        let n = stmt
            .execute(rusqlite::params_from_iter(params))
            .map_err(|e| e.to_string())?;
        Ok(OpResult::Affected { rows_affected: n })
    }
}

/// FNV-1a over the serialized results: the change-detection fingerprint a
/// client feeds back as `baseline`. Compared only against hashes we minted,
/// so the exact function is an implementation detail — but keep it stable
/// across processes (no per-process seeding) so a re-poll after failover
/// still short-circuits when nothing changed.
fn poll_hash(results: &[OpResult]) -> String {
    let bytes = serde_json::to_vec(results).unwrap_or_default();
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

/// Does a poll reply now? `baseline` present: when the result changed.
/// Absent: when the result is non-empty (condition-variable semantics).
fn poll_ready(baseline: &Option<String>, results: &[OpResult], hash: &str) -> bool {
    match baseline {
        Some(b) => b != hash,
        None => results
            .first()
            .is_some_and(|r| matches!(r, OpResult::Rows { rows } if !rows.is_empty())),
    }
}

fn json_params(params: &[Value]) -> Result<Vec<rusqlite::types::Value>, String> {
    use rusqlite::types::Value as Sql;
    params
        .iter()
        .map(|v| match v {
            Value::Null => Ok(Sql::Null),
            Value::Bool(b) => Ok(Sql::Integer(*b as i64)),
            Value::Number(n) => n
                .as_i64()
                .map(Sql::Integer)
                .or_else(|| n.as_f64().map(Sql::Real))
                .ok_or_else(|| format!("unsupported number: {n}")),
            Value::String(s) => Ok(Sql::Text(s.clone())),
            other => Err(format!("unsupported param type: {other}")),
        })
        .collect()
}

fn value_to_json(v: rusqlite::types::ValueRef<'_>) -> Value {
    use rusqlite::types::ValueRef::*;
    match v {
        Null => Value::Null,
        Integer(i) => json!(i),
        Real(f) => json!(f),
        Text(t) => Value::String(String::from_utf8_lossy(t).into_owned()),
        Blob(b) => Value::String(b.iter().map(|byte| format!("{byte:02x}")).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FsBlobStore;

    fn applied(participants: &[&str]) -> AppliedTxn {
        AppliedTxn {
            participants: participants.iter().map(|s| s.to_string()).collect(),
            waiter: None,
        }
    }

    #[test]
    fn txn_components_group_by_shared_participants() {
        // {a}, {b}, {a,b} all connect; {c} stands alone.
        let pending = vec![applied(&["a"]), applied(&["b"]), applied(&["a", "b"]), applied(&["c"])];
        assert_eq!(txn_components(&pending), vec![vec![0, 1, 2], vec![3]]);

        // Transitive: {a,b} + {b,c} + {c,d} is one chain.
        let chain = vec![applied(&["a", "b"]), applied(&["b", "c"]), applied(&["c", "d"])];
        assert_eq!(txn_components(&chain), vec![vec![0, 1, 2]]);

        // Fully disjoint txns keep their arrival order.
        let disjoint = vec![applied(&["x"]), applied(&["y"]), applied(&["z"])];
        assert_eq!(txn_components(&disjoint), vec![vec![0], vec![1], vec![2]]);
        assert!(txn_components(&[]).is_empty());
    }

    #[test]
    fn poll_hash_is_stable_across_processes() {
        // Clients feed hashes back as baselines after reconnects and
        // failovers — the function must never drift or gain a seed.
        assert_eq!(poll_hash(&[OpResult::Rows { rows: vec![] }]), "4bf9765328d07878");
        assert_eq!(
            poll_hash(&[OpResult::Rows { rows: vec![serde_json::json!({"n": 1})] }]),
            "94228ea8dcfd592b"
        );
    }

    #[test]
    fn poll_readiness_rules() {
        let empty = [OpResult::Rows { rows: vec![] }];
        let full = [OpResult::Rows { rows: vec![serde_json::json!({"n": 1})] }];
        let empty_hash = poll_hash(&empty);
        let full_hash = poll_hash(&full);

        // Condition-variable mode: fire on any non-empty result.
        assert!(!poll_ready(&None, &empty, &empty_hash));
        assert!(poll_ready(&None, &full, &full_hash));

        // Change-detection mode: fire when the hash moved — including to
        // EMPTY (deletes are visible), which mode one would miss.
        assert!(poll_ready(&Some("".into()), &empty, &empty_hash), "\"\" bootstraps");
        assert!(!poll_ready(&Some(full_hash.clone()), &full, &full_hash));
        assert!(poll_ready(&Some(full_hash), &empty, &empty_hash));
    }

    #[test]
    fn json_params_map_onto_sqlite_types() {
        use rusqlite::types::Value as Sql;
        let params = json_params(&[
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(42),
            serde_json::json!(1.5),
            serde_json::json!("hi"),
        ])
        .unwrap();
        assert_eq!(
            params,
            vec![
                Sql::Null,
                Sql::Integer(1),
                Sql::Integer(42),
                Sql::Real(1.5),
                Sql::Text("hi".into())
            ]
        );
        // Numbers past i64 degrade to Real rather than erroring...
        assert_eq!(json_params(&[serde_json::json!(u64::MAX)]).unwrap(), vec![Sql::Real(u64::MAX as f64)]);
        // ...but structured params are a caller bug, said plainly.
        assert!(json_params(&[serde_json::json!([1, 2])]).is_err());
        assert!(json_params(&[serde_json::json!({"k": 1})]).is_err());
    }

    #[test]
    fn sqlite_values_round_into_json() {
        use rusqlite::types::ValueRef;
        assert_eq!(value_to_json(ValueRef::Null), Value::Null);
        assert_eq!(value_to_json(ValueRef::Integer(-3)), json!(-3));
        assert_eq!(value_to_json(ValueRef::Real(2.5)), json!(2.5));
        assert_eq!(value_to_json(ValueRef::Text(b"ok")), json!("ok"));
        assert_eq!(value_to_json(ValueRef::Blob(&[0xde, 0xad])), json!("dead"));
    }

    #[test]
    fn hex_roundtrip_and_rejection() {
        assert_eq!(hex_encode(&[0x00, 0xff, 0x10]), "00ff10");
        assert_eq!(hex_decode("00ff10").unwrap(), vec![0x00, 0xff, 0x10]);
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
        assert!(hex_decode("abc").is_none(), "odd length");
        assert!(hex_decode("zz").is_none(), "not hex");
    }

    #[tokio::test]
    async fn recovery_rolls_forward_committed_txns() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> =
            Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());

        // A committed boat that never promoted: one snapshot, one delta.
        store.put("staging/t1/alice.snap", b"NEW").await.unwrap();
        store
            .put("staging/t1/carol.delta.0000000007", b"DELTA7")
            .await
            .unwrap();
        let record = serde_json::to_vec(&TxnRecord {
            txn_id: "t1".into(),
            objects: vec!["alice".into(), "carol".into()],
            inline: vec![],
        })
        .unwrap();
        store.put("txns/t1.json", &record).await.unwrap();
        // An INLINE boat that reached its commit point but never promoted:
        // the payload lives in the record itself.
        let record = serde_json::to_vec(&TxnRecord {
            txn_id: "t3".into(),
            objects: vec!["dave".into()],
            inline: vec![("dave.snap".into(), hex_encode(b"INLINE"))],
        })
        .unwrap();
        store.put("txns/t3.json", &record).await.unwrap();
        // ...and staging from a boat that never reached its commit point,
        // plus a record too corrupt to parse (dropped, not fatal).
        store.put("staging/t2/bob.snap", b"JUNK").await.unwrap();
        store.put("txns/garbage.json", b"not json").await.unwrap();

        recover(store.as_ref()).await.unwrap();

        assert!(store.get("txns/garbage.json").await.unwrap().is_none());

        assert_eq!(store.get("objects/alice.db").await.unwrap().unwrap(), b"NEW");
        assert_eq!(store.get("objects/dave.db").await.unwrap().unwrap(), b"INLINE");
        assert!(store.get("txns/t3.json").await.unwrap().is_none());
        assert_eq!(
            store.get("objects/carol.d.0000000007").await.unwrap().unwrap(),
            b"DELTA7"
        );
        assert!(store.get("txns/t1.json").await.unwrap().is_none());
        assert!(store.get("staging/t1/alice.snap").await.unwrap().is_none());
        assert!(store.get("staging/t2/bob.snap").await.unwrap().is_none());
        assert!(store.get("objects/bob.db").await.unwrap().is_none());
    }

    /// A store that lists keys it can no longer produce — the shape of a
    /// concurrent recoverer (or GC) winning a race between list and get.
    struct Ghostly {
        inner: FsBlobStore,
        ghosts: Vec<String>,
    }

    #[async_trait::async_trait]
    impl BlobStore for Ghostly {
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            if self.ghosts.iter().any(|g| g == key) {
                return Ok(None);
            }
            self.inner.get(key).await
        }
        async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            self.inner.put(key, bytes).await
        }
        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.inner.delete(key).await
        }
        async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            let mut keys = self.inner.list(prefix).await?;
            keys.extend(self.ghosts.iter().filter(|g| g.starts_with(prefix)).cloned());
            keys.sort();
            Ok(keys)
        }
        async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
            self.inner.create(key, bytes).await
        }
    }

    #[tokio::test]
    async fn recovery_tolerates_ghost_keys_and_bad_inline_hex() {
        let dir = tempfile::tempdir().unwrap();
        let inner = FsBlobStore::new(dir.path().join("blobs")).unwrap();
        // A real record whose staged blob is a ghost, and a record with
        // inline payload that doesn't decode: both are skipped politely.
        let record = |id: &str, inline: Vec<(String, String)>| {
            serde_json::to_vec(&TxnRecord {
                txn_id: id.into(),
                objects: vec!["erin".into()],
                inline,
            })
            .unwrap()
        };
        inner.put("txns/tg.json", &record("tg", vec![])).await.unwrap();
        inner
            .put("txns/tbad.json", &record("tbad", vec![("erin.snap".into(), "zz".into())]))
            .await
            .unwrap();
        let store = Ghostly {
            inner,
            ghosts: vec!["txns/ghost.json".into(), "staging/tg/erin.snap".into()],
        };

        recover(&store).await.unwrap();

        assert!(store.get("txns/tg.json").await.unwrap().is_none());
        assert!(store.get("txns/tbad.json").await.unwrap().is_none());
        assert!(
            store.get("objects/erin.db").await.unwrap().is_none(),
            "neither a ghost nor bad hex may fabricate object state"
        );
    }

    #[tokio::test]
    async fn recovery_never_rolls_a_base_backwards() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> =
            Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());
        let snap = |counter: u32| {
            let mut f = vec![0u8; 4096];
            f[delta::HEADER_CHANGE_COUNTER..delta::HEADER_CHANGE_COUNTER + 4]
                .copy_from_slice(&counter.to_be_bytes());
            f
        };
        // The live base is at counter 9; a stale record — from a boat whose
        // promotion failed and was later superseded by a repair boat —
        // still stages counter 3.
        store.put("objects/erin.db", &snap(9)).await.unwrap();
        store.put("staging/t9/erin.snap", &snap(3)).await.unwrap();
        let record = serde_json::to_vec(&TxnRecord {
            txn_id: "t9".into(),
            objects: vec!["erin".into()],
            inline: vec![],
        })
        .unwrap();
        store.put("txns/t9.json", &record).await.unwrap();

        recover(store.as_ref()).await.unwrap();

        let base = store.get("objects/erin.db").await.unwrap().unwrap();
        assert_eq!(
            delta::change_counter(&base),
            9,
            "a stale staged snapshot must not clobber a newer base"
        );
        assert!(
            store.get("txns/t9.json").await.unwrap().is_none(),
            "the stale record still gets cleaned up"
        );
    }
}
