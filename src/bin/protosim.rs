//! Standalone model of the log-structured commit protocol — a de-risking
//! spike BEFORE integrating into fafo. It answers: does a per-object
//! sequenced log, fenced by create-if-absent, with a SINGLE outcome key as
//! the atomic commit/abort decision, keep cross-object transfers atomic when
//! many writers fork on the same accounts, AND when writers pause mid-commit,
//! get resolved (aborted) by a peer, and then resume?
//!
//! The whole point is the pause adversary's sharp edge: the resolver-vs-
//! zombie race. Commit is `create(outcome/<tid>, Committed)`; abort is
//! `create(outcome/<tid>, Aborted)` — the SAME key, so exactly one wins and
//! a resumed zombie can never disagree with the peer that resolved it.
//!
//! Everything runs single-thread under a seeded scheduler; interleaving is at
//! store-op granularity, so "paused then resumed" is just one schedule.
//!
//! run: cargo run --release --bin protosim -- [seeds]

use std::collections::BTreeMap;

// ---------- abstract store: exactly R2's primitives ----------

#[derive(Clone)]
enum Cell {
    Base { seq: u64, val: i64 },
    Log { tid: u64, delta: i64, born: u64 },
    Outcome { committed: bool },
}

#[derive(Default)]
struct Store {
    m: BTreeMap<String, Cell>,
}
impl Store {
    fn get(&self, k: &str) -> Option<Cell> {
        self.m.get(k).cloned()
    }
    fn put(&mut self, k: String, c: Cell) {
        self.m.insert(k, c);
    }
    /// create-if-absent — the ONLY consensus primitive (R2 If-None-Match).
    fn create(&mut self, k: String, c: Cell) -> bool {
        use std::collections::btree_map::Entry;
        match self.m.entry(k) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(c);
                true
            }
        }
    }
    fn delete(&mut self, k: &str) {
        self.m.remove(k);
    }
}

fn base_key(o: &str) -> String {
    format!("a/{o}/base")
}
fn log_key(o: &str, s: u64) -> String {
    format!("a/{o}/log/{s:020}")
}
fn outcome_key(tid: u64) -> String {
    format!("o/{tid}")
}

/// Is txn `tid` committed? The single outcome key is the whole truth.
fn committed(s: &Store, tid: u64) -> bool {
    matches!(s.get(&outcome_key(tid)), Some(Cell::Outcome { committed: true }))
}
fn resolved(s: &Store, tid: u64) -> Option<bool> {
    match s.get(&outcome_key(tid)) {
        Some(Cell::Outcome { committed }) => Some(committed),
        _ => None,
    }
}

/// Committed state = base + contiguous log entries whose txn is committed.
fn read_committed(s: &Store, o: &str) -> (u64, i64) {
    let (mut seq, mut val) = match s.get(&base_key(o)) {
        Some(Cell::Base { seq, val }) => (seq, val),
        _ => (0, 0),
    };
    loop {
        match s.get(&log_key(o, seq + 1)) {
            Some(Cell::Log { tid, delta, .. }) if committed(s, tid) => {
                val += delta;
                seq += 1;
            }
            _ => break,
        }
    }
    (seq, val)
}

const TTL: u64 = 40; // ticks a prewrite may sit before a peer may resolve it

/// Try to clear a log entry blocking `seq` on `o`. Returns true if the slot
/// is now free (its txn aborted+rolled back, or never existed). This is the
/// Percolator lock-resolution: defer to the single outcome key.
fn resolve_slot(s: &mut Store, o: &str, seq: u64, now: u64) -> bool {
    let k = log_key(o, seq);
    match s.get(&k) {
        None => true,
        Some(Cell::Log { tid, born, .. }) => match resolved(s, tid) {
            Some(true) => false,          // committed: slot legitimately taken
            Some(false) => {
                s.delete(&k); // aborted: roll the lock back, free the slot
                true
            }
            None => {
                // In-flight. Only resolve once it has aged past the TTL — a
                // fresh lock is a live peer about to commit, not an orphan.
                if now.saturating_sub(born) <= TTL {
                    return false;
                }
                // Abort it via the single outcome key; if the owner committed
                // in the meantime our create loses and we must NOT roll back.
                let won = s.create(outcome_key(tid), Cell::Outcome { committed: false });
                if won || matches!(resolved(s, tid), Some(false)) {
                    s.delete(&k);
                    true
                } else {
                    false // owner committed first
                }
            }
        },
        Some(_) => false,
    }
}

// ---------- the transfer as a store-op-granular state machine ----------

#[derive(Clone, Copy, PartialEq)]
enum Step {
    Read,
    PreLow,
    PreHigh,
    Commit,
    Done,
    Aborted,
    Crashed, // died holding prewritten locks (never resumes); terminal
}

struct Xfer {
    tid: u64,
    from: String,
    amt: i64,
    low: String,
    high: String,
    low_seq: u64,
    high_seq: u64,
    low_delta: i64,
    high_delta: i64,
    made_low: bool,
    made_high: bool,
    step: Step,
    retries: u32,
    // pause behaviour (the adversary): a worker may go dark after prewriting
    resume_at: u64, // scheduler tick to resume at (0 = running)
    pause_budget: u32,
    crash: bool,      // never resumes: a permanent orphan
    acked_commit: bool,
}

impl Xfer {
    fn new(tid: u64, from: &str, to: &str, amt: i64, pauses: u32, crash: bool) -> Self {
        let (low, high) = if from <= to {
            (from.to_string(), to.to_string())
        } else {
            (to.to_string(), from.to_string())
        };
        let (low_delta, high_delta) = if low == *from { (-amt, amt) } else { (amt, -amt) };
        Xfer {
            tid,
            from: from.into(),
            amt,
            low,
            high,
            low_seq: 0,
            high_seq: 0,
            low_delta,
            high_delta,
            made_low: false,
            made_high: false,
            step: Step::Read,
            retries: 0,
            resume_at: 0,
            pause_budget: pauses,
            crash,
            acked_commit: false,
        }
    }
    fn active(&self) -> bool {
        !matches!(self.step, Step::Done | Step::Aborted | Step::Crashed)
    }
    fn runnable(&self, now: u64) -> bool {
        self.active() && now >= self.resume_at
    }

    /// One store interaction. `now` is the scheduler tick; `rng` lets a
    /// worker choose to pause. Between any two calls, any peer may run.
    fn step(&mut self, s: &mut Store, now: u64, rng: &mut Rng) {
        match self.step {
            Step::Read => {
                let (lo, lv) = read_committed(s, &self.low);
                let (hi, _hv) = read_committed(s, &self.high);
                let from_val = if self.low == self.from { lv } else { read_committed(s, &self.from).1 };
                if from_val < self.amt {
                    self.step = Step::Aborted;
                    return;
                }
                self.low_seq = lo;
                self.high_seq = hi;
                self.step = Step::PreLow;
            }
            Step::PreLow => {
                let k = log_key(&self.low, self.low_seq + 1);
                if s.create(k, Cell::Log { tid: self.tid, delta: self.low_delta, born: now }) {
                    self.made_low = true;
                    self.maybe_pause(now, rng);
                    self.step = Step::PreHigh;
                } else if !resolve_slot(s, &self.low, self.low_seq + 1, now) {
                    self.reset_or_giveup(s);
                } // else: slot freed, retry PreLow next tick (step stays)
            }
            Step::PreHigh => {
                let k = log_key(&self.high, self.high_seq + 1);
                if s.create(k, Cell::Log { tid: self.tid, delta: self.high_delta, born: now }) {
                    self.made_high = true;
                    // Fully prewritten but not yet committed — the nastiest
                    // orphan. A crashing worker dies right here, locks held.
                    if self.crash {
                        self.step = Step::Crashed;
                        return;
                    }
                    self.maybe_pause(now, rng);
                    self.step = Step::Commit;
                } else if !resolve_slot(s, &self.high, self.high_seq + 1, now) {
                    self.rollback(s);
                    self.reset_or_giveup(s);
                }
            }
            Step::Commit => {
                // THE commit point: race the single outcome key against any
                // resolver that aborted us while we were paused.
                let won = s.create(outcome_key(self.tid), Cell::Outcome { committed: true });
                if won {
                    self.acked_commit = true;
                    self.step = Step::Done;
                } else {
                    // A resolver beat us to it — we are aborted. Our prewrites
                    // are already (or will be) rolled back by whoever resolved.
                    self.rollback(s);
                    self.reset_or_giveup(s);
                }
            }
            Step::Done | Step::Aborted | Step::Crashed => {}
        }
    }

    fn maybe_pause(&mut self, now: u64, rng: &mut Rng) {
        if self.pause_budget > 0 && rng.below(2) == 0 {
            self.pause_budget -= 1;
            // pause for a spell straddling the TTL, so resume sometimes races
            // a resolver exactly at the boundary.
            let d = 10 + rng.below(TTL + 30);
            self.resume_at = now + d;
        }
    }
    fn rollback(&mut self, s: &mut Store) {
        // Delete ONLY entries that are still ours. While we were paused a
        // resolver may have aborted+cleared our lock and another txn may have
        // reclaimed that very slot and committed there — a blind delete-by-key
        // would erase the innocent txn's leg and tear it (money created).
        if self.made_high {
            safe_delete(s, &log_key(&self.high, self.high_seq + 1), self.tid);
        }
        if self.made_low {
            safe_delete(s, &log_key(&self.low, self.low_seq + 1), self.tid);
        }
        self.made_low = false;
        self.made_high = false;
    }
    fn reset_or_giveup(&mut self, s: &mut Store) {
        self.rollback(s);
        self.retries += 1;
        self.resume_at = 0;
        if self.retries > 120 {
            self.step = Step::Aborted;
        } else {
            self.step = Step::Read;
        }
    }
}

// ---------- CONTROL: naive last-writer-wins (today's base overwrite) ----------

struct Naive {
    from: String,
    to: String,
    amt: i64,
    vf: i64,
    vt: i64,
    step: Step,
}
impl Naive {
    fn new(from: &str, to: &str, amt: i64) -> Self {
        Naive { from: from.into(), to: to.into(), amt, vf: 0, vt: 0, step: Step::Read }
    }
    fn active(&self) -> bool {
        !matches!(self.step, Step::Done | Step::Aborted)
    }
    fn step(&mut self, s: &mut Store) {
        match self.step {
            Step::Read => {
                self.vf = base_val(s, &self.from);
                self.vt = base_val(s, &self.to);
                self.step = if self.vf < self.amt { Step::Aborted } else { Step::PreLow };
            }
            Step::PreLow => {
                s.put(base_key(&self.from), Cell::Base { seq: 0, val: self.vf - self.amt });
                self.step = Step::PreHigh;
            }
            Step::PreHigh => {
                s.put(base_key(&self.to), Cell::Base { seq: 0, val: self.vt + self.amt });
                self.step = Step::Done;
            }
            _ => {}
        }
    }
}
fn base_val(s: &Store, o: &str) -> i64 {
    match s.get(&base_key(o)) {
        Some(Cell::Base { val, .. }) => val,
        _ => 0,
    }
}

/// Delete a log entry only if it is still the given txn's — the tid-safe
/// rollback that keeps a resumed loser from erasing a reclaimer's committed
/// leg.
fn safe_delete(s: &mut Store, k: &str, tid: u64) {
    if let Some(Cell::Log { tid: t, .. }) = s.get(k) {
        if t == tid {
            s.delete(k);
        }
    }
}

// ---------- scheduler + oracle ----------

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

enum Worker {
    Log(Xfer),
    Naive(Naive),
}
impl Worker {
    fn active(&self) -> bool {
        match self {
            Worker::Log(x) => x.active(),
            Worker::Naive(n) => n.active(),
        }
    }
}

struct Report {
    committed: u64,
    given_up: u64,
    paused: u64,
    conserved: bool,
    ack_consistent: bool,
    total_end: i64,
    total_start: i64,
}

fn run(seed: u64, broken: bool) -> Report {
    let mut rng = Rng(seed ^ 0xDEADBEEF);
    let accounts = 3 + rng.below(4) as usize;
    let workers = 2 + rng.below(4) as usize;
    let n = 20 + rng.below(40) as usize;
    let init: i64 = 100;

    let mut store = Store::default();
    let names: Vec<String> = (0..accounts).map(|i| format!("acct{i}")).collect();
    for a in &names {
        store.put(base_key(a), Cell::Base { seq: 0, val: init });
    }
    let total_start = init * accounts as i64;

    let mut active: Vec<Worker> = Vec::new();
    let mut tid = 1u64;
    let mut paused_total = 0u64;
    for _ in 0..workers {
        for _ in 0..(n / workers + 1) {
            let from = names[rng.below(accounts as u64) as usize].clone();
            let mut to = names[rng.below(accounts as u64) as usize].clone();
            while to == from {
                to = names[rng.below(accounts as u64) as usize].clone();
            }
            let amt = 1 + rng.below(20) as i64;
            active.push(if broken {
                Worker::Naive(Naive::new(&from, &to, amt))
            } else {
                let pauses = rng.below(3) as u32; // some workers pause mid-commit
                let crash = rng.below(20) == 0; // ~5% never resume (orphans)
                if pauses > 0 {
                    paused_total += 1;
                }
                Worker::Log(Xfer::new(tid, &from, &to, amt, pauses, crash))
            });
            tid += 1;
        }
    }

    // Interleave at store-op granularity.
    let mut now = 0u64;
    let mut guard = 0u64;
    loop {
        let idxs: Vec<usize> = active
            .iter()
            .enumerate()
            .filter(|(_, w)| match w {
                Worker::Log(x) => x.runnable(now),
                Worker::Naive(nn) => nn.active(),
            })
            .map(|(i, _)| i)
            .collect();
        if idxs.is_empty() {
            // Maybe only paused workers remain; advance the clock to let their
            // resume (or a resolver's TTL) fire.
            let any_paused = active.iter().any(|w| w.active());
            if !any_paused {
                break;
            }
            now += 5;
            guard += 1;
            if guard > 2_000_000 {
                break;
            }
            continue;
        }
        let pick = idxs[rng.below(idxs.len() as u64) as usize];
        match &mut active[pick] {
            Worker::Log(x) => x.step(&mut store, now, &mut rng),
            Worker::Naive(nn) => nn.step(&mut store),
        }
        now += 1;
        guard += 1;
        if guard > 2_000_000 {
            break;
        }
    }

    // Final resolution sweep: any orphan lock (crashed worker) gets aborted so
    // its slot is reclaimable — models a resolver eventually running.
    if !broken {
        let keys: Vec<(String, u64)> = store
            .m
            .iter()
            .filter_map(|(k, c)| match c {
                Cell::Log { tid, .. } => Some((k.clone(), *tid)),
                _ => None,
            })
            .collect();
        for (k, t) in keys {
            if resolved(&store, t).is_none() {
                let _ = store.create(outcome_key(t), Cell::Outcome { committed: false });
            }
            if matches!(resolved(&store, t), Some(false)) {
                store.delete(&k);
            }
        }
    }

    let total_end: i64 = names.iter().map(|a| read_committed(&store, a).1).sum();
    let conserved = total_end == total_start;

    // Ack consistency: every worker that believes it committed must have a
    // Committed outcome durably (no zombie acked then rolled back).
    let ack_consistent = active.iter().all(|w| match w {
        Worker::Log(x) => !x.acked_commit || committed(&store, x.tid),
        _ => true,
    });

    let committed_count = active
        .iter()
        .filter(|w| matches!(w, Worker::Log(x) if x.step == Step::Done && x.acked_commit))
        .count() as u64;
    let given_up = active
        .iter()
        .filter(|w| matches!(w, Worker::Log(x) if x.step == Step::Aborted))
        .count() as u64;

    Report {
        committed: committed_count,
        given_up,
        paused: paused_total,
        conserved,
        ack_consistent,
        total_end,
        total_start,
    }
}

struct Sweep {
    viol: u64,
    cons_fail: u64,
    ack_fail: u64,
    committed: u64,
    given_up: u64,
    paused: u64,
    first_bad: Option<(u64, i64, i64)>, // seed, end, start
}
fn sweep(seeds: u64, broken: bool) -> Sweep {
    let mut s = Sweep {
        viol: 0,
        cons_fail: 0,
        ack_fail: 0,
        committed: 0,
        given_up: 0,
        paused: 0,
        first_bad: None,
    };
    for seed in 0..seeds {
        let r = run(seed, broken);
        s.committed += r.committed;
        s.given_up += r.given_up;
        s.paused += r.paused;
        if !r.conserved {
            s.cons_fail += 1;
        }
        if !r.ack_consistent {
            s.ack_fail += 1;
        }
        if !r.conserved || !r.ack_consistent {
            s.viol += 1;
            if s.first_bad.is_none() {
                s.first_bad = Some((seed, r.total_end, r.total_start));
            }
        }
    }
    s
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let seeds: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(20_000);

    let b = sweep(seeds, true);
    println!(
        "CONTROL (naive base overwrite): {seeds} seeds | VIOLATIONS: {} (conservation)",
        b.viol
    );

    let r = sweep(seeds, false);
    println!(
        "LOG-STRUCTURED + pause/resume/resolve: {seeds} seeds, {} committed, {} given-up, {} paused-workers",
        r.committed, r.given_up, r.paused
    );
    println!(
        "  VIOLATIONS: {} total  (conservation: {}, ack-consistency: {}){}",
        r.viol,
        r.cons_fail,
        r.ack_fail,
        r.first_bad
            .map(|(s, e, st)| format!("  | first: seed {s} (total {e} != {st})"))
            .unwrap_or_default()
    );

    println!();
    if b.viol == 0 {
        println!("⚠  model has NO TEETH — the naive control didn't tear. Meaningless.");
        std::process::exit(2);
    } else if r.viol == 0 {
        println!(
            "✓ VERDICT: control tears ({}); the log-structured protocol holds conservation AND\n  ack-consistency on every one of {seeds} seeds — INCLUDING workers that pause mid-commit,\n  get resolved by a peer, and resume. The resolver-vs-zombie race is safe. The pause\n  adversary greens at the protocol level; the rest is integration.",
            b.viol
        );
    } else {
        println!("✗ VERDICT: the protocol tears under pause/resolve. Design not ready — debug the seed above.");
        std::process::exit(1);
    }
}
