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
//! Crash after 4 → rolled forward by `recover` at boot. Before 4 → staging
//! garbage, swept at boot. Failure after local commit → evict participants
//! so memory never outruns the blob store.

use crate::api::ApiError;
use crate::cluster::{
    Node, Op, OpResult, TakeError, TransferMeta, TxnResponse, VisitInfo, checkpoint_key,
};
use crate::object::{LiveObject, activate, evict, object_key};
use crate::store::BlobStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
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
    /// Internal: a spawned take task finished.
    Taken {
        txn: u64,
        object: String,
        from: usize,
        result: TakenResult,
    },
    Stats {
        resp: oneshot::Sender<(u64, usize)>,
    },
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
    resp: oneshot::Sender<Result<TxnResponse, ApiError>>,
    /// How many participants (in sorted order) we hold queue heads for.
    acquired: usize,
    taking: bool,
    crossed: bool,
}

struct Meta {
    arrived_at: u64,
    return_to: Option<usize>,
    visit: Option<VisitInfo>,
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
    };
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            worker.handle(msg).await;
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
                        resp,
                        acquired: 0,
                        taking: false,
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
            WorkerMsg::Taken {
                txn,
                object,
                from,
                result,
            } => self.on_taken(txn, object, from, result).await,
            WorkerMsg::Stats { resp } => {
                let _ = resp.send((self.txns_executed, self.owned.len()));
            }
        }
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
    /// holds the head of every participant's queue and may run.
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
                let queue = self.queues.entry(object).or_default();
                if !queue.iter().any(|e| matches!(e, Entry::Txn(t) if *t == txn)) {
                    queue.push_back(Entry::Txn(txn));
                }
                if matches!(queue.front(), Some(Entry::Txn(t)) if *t == txn) {
                    p.acquired += 1;
                    continue;
                }
                return false; // waiting for the head; re-driven on pops
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

    async fn on_taken(&mut self, txn: u64, object: String, from: usize, result: TakenResult) {
        let Some(p) = self.parked.get_mut(&txn) else {
            return;
        };
        p.taking = false;
        match result {
            TakenResult::Got(meta) => {
                self.admit(&object, meta, from != self.id).await;
                // The txn now holds the fresh object outright.
                self.queues
                    .insert(object, VecDeque::from([Entry::Txn(txn)]));
                let p = self.parked.get_mut(&txn).unwrap();
                p.acquired += 1;
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

    /// Serve whatever is at the head of an object's queue. Returns txn ids
    /// ready to be pumped.
    async fn service_front(&mut self, object: &str) -> Vec<u64> {
        let Some(queue) = self.queues.get(object) else {
            return vec![];
        };
        match queue.front() {
            Some(Entry::Txn(t)) => vec![*t],
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
        evict(&mut self.objects, object);
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
        // Release held queue heads (the first `acquired` participants).
        for object in p.participants.iter().take(p.acquired) {
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

        let result = self
            .execute(&p.participants, &p.ops, p.read_only)
            .await
            .map(|results| TxnResponse {
                txn_id: format!("w{}-{}", self.id, txn),
                results,
            });
        let _ = p.resp.send(result);

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
        ready
    }

    /// Hysteresis: a displaced object with an idle queue goes home before
    /// its clique next transacts.
    async fn maybe_return_home(&mut self, object: &str) {
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

    async fn execute(
        &mut self,
        participants: &[String],
        ops: &[Op],
        read_only: bool,
    ) -> Result<Vec<OpResult>, ApiError> {
        for id in participants {
            activate(&mut self.objects, id, &self.node.store, &self.live_dir).await?;
        }
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
            return Ok(results);
        }

        for id in participants {
            conn_of(&self.objects, id)
                .execute_batch("BEGIN")
                .map_err(|e| ApiError::internal(e.to_string()))?;
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
            return Err(ApiError::bad_request(format!(
                "op failed, transaction rolled back: {msg}"
            )));
        }

        let mut commit_err = None;
        for id in participants {
            if let Err(e) = conn_of(&self.objects, id).execute_batch("COMMIT") {
                commit_err = Some(e);
                break;
            }
        }
        if let Some(e) = commit_err {
            for id in participants {
                evict(&mut self.objects, id);
            }
            return Err(ApiError::internal(format!("local commit failed: {e}")));
        }

        // Snapshot synchronously first: Connection is Send but not Sync, so
        // no borrow of a connection may live across an await.
        let staging_id = uuid::Uuid::new_v4().to_string();
        let mut snapshots: Vec<Vec<u8>> = Vec::with_capacity(participants.len());
        let mut stage_err: Option<anyhow::Error> = None;
        for id in participants {
            match std::fs::read(&self.objects.get(id).unwrap().live_path) {
                Ok(bytes) => snapshots.push(bytes),
                Err(e) => {
                    stage_err = Some(e.into());
                    break;
                }
            }
        }
        if stage_err.is_none() {
            for (id, bytes) in participants.iter().zip(&snapshots) {
                if let Err(e) = self.node.store.put(&staging_key(&staging_id, id), bytes).await {
                    stage_err = Some(e);
                    break;
                }
            }
        }

        // Fencing, last line of defense: if the lease guard has flagged us
        // as superseded, refuse to pass the commit point. (The guard
        // fail-stops the process; this catches the in-flight stragglers.)
        if self.node.fenced.load(Ordering::SeqCst) {
            for id in participants {
                evict(&mut self.objects, id);
            }
            return Err(ApiError::internal("node is fenced; commit refused"));
        }

        // The commit point: one write of one blob.
        if stage_err.is_none() {
            let record = serde_json::to_vec(&TxnRecord {
                txn_id: staging_id.clone(),
                objects: participants.to_vec(),
            })
            .expect("record serializes");
            if let Err(e) = self.node.store.put(&txn_key(&staging_id), &record).await {
                stage_err = Some(e);
            }
        }
        if let Some(e) = stage_err {
            for id in participants {
                evict(&mut self.objects, id);
            }
            for id in participants {
                let _ = self.node.store.delete(&staging_key(&staging_id, id)).await;
            }
            return Err(ApiError::internal(format!("commit failed: {e}")));
        }

        // Committed. Promotion is pure roll-forward: on failure the record
        // and staging stay behind and recover() finishes at next boot.
        let mut promoted = true;
        for (id, bytes) in participants.iter().zip(&snapshots) {
            if self.node.store.put(&object_key(id), bytes).await.is_err() {
                promoted = false;
            }
        }
        if promoted {
            for id in participants {
                let _ = self.node.store.delete(&staging_key(&staging_id, id)).await;
            }
            let _ = self.node.store.delete(&txn_key(&staging_id)).await;
        }

        Ok(results)
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
            let local_tx = node.local.read().unwrap().get(&owner).cloned();
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
                let cached = node.routing.read().unwrap().addrs.get(&owner).cloned();
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
    let local_tx = node.local.read().unwrap().get(&home).cloned();
    if let Some(tx) = local_tx {
        let _ = tx.send(WorkerMsg::Adopt { object, meta });
        return;
    }
    let addr = node.routing.read().unwrap().addrs.get(&home).cloned();
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
}

fn staging_key(staging_id: &str, object_id: &str) -> String {
    format!("staging/{staging_id}/{object_id}.db")
}

fn txn_key(staging_id: &str) -> String {
    format!("txns/{staging_id}.json")
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
        for id in &record.objects {
            let staged = staging_key(&record.txn_id, id);
            if let Some(snapshot) = store.get(&staged).await? {
                store.put(&object_key(id), &snapshot).await?;
            }
            store.delete(&staged).await?;
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
    use std::sync::Arc;

    #[tokio::test]
    async fn recovery_rolls_forward_committed_txns() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> =
            Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());

        store.put("staging/t1/alice.db", b"NEW").await.unwrap();
        let record = serde_json::to_vec(&TxnRecord {
            txn_id: "t1".into(),
            objects: vec!["alice".into()],
        })
        .unwrap();
        store.put("txns/t1.json", &record).await.unwrap();
        store.put("staging/t2/bob.db", b"JUNK").await.unwrap();

        recover(store.as_ref()).await.unwrap();

        assert_eq!(store.get("objects/alice.db").await.unwrap().unwrap(), b"NEW");
        assert!(store.get("txns/t1.json").await.unwrap().is_none());
        assert!(store.get("staging/t1/alice.db").await.unwrap().is_none());
        assert!(store.get("staging/t2/bob.db").await.unwrap().is_none());
        assert!(store.get("objects/bob.db").await.unwrap().is_none());
    }
}
