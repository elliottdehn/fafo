# DST: deterministic simulation testing for fafo

The pitch: `cargo run --release --bin dst -- mine` presses every core,
each core running simulated fafo clusters under seeded fault storms.
A crash is a bug. A bug is $100. Every crash replays exactly from
`dst run --seed N`.

The canary bug it must catch on day one: a client arms a last-will on a
node, the NODE dies (not just the socket), and the will never fires —
today's design runs wills at the node holding the socket, so a dead node
takes its promises with it.

## Why this is buildable now

Every fault already has a seam or nearly does:

- **Storage** — `BlobStore` is a trait; every fault test in the suite is
  already a ~20-line wrapper store. The sim uses an in-memory store with
  seeded latency and failures.
- **Inter-node RPC** — everything rides `rpc::call`/`rpc::health`. One
  `Transport` trait turns the network into a routing table the sim can
  delay, drop, and partition.
- **Time** — fencing, TTLs, and hysteresis all sleep through tokio. A
  `current_thread` runtime with `start_paused` gives virtual time: hours
  of cluster life per real second, and deadlocks caught instantly by
  virtual-time watchdogs.
- **Sessions** — wills and poll cleanup live in the WebSocket teardown.
  Extracting a socket-free `Session` (frames in, frames out, `close()`)
  lets the sim open real connections against real code and then kill
  them — or kill the node under them.

## Determinism contract

Same binary + same config + same seed ⇒ same run, bit for bit. What has
to be true, and how we make it true:

| Nondeterminism source        | Fix |
|------------------------------|-----|
| OS thread scheduling         | sim runs on a single-threaded runtime |
| Wall/monotonic clocks        | `start_paused` virtual time; fencing moves to `tokio::time::Instant` |
| Real sockets                 | none in sim: `Transport` routes in-process; HTTP server not started |
| `HashMap` iteration order    | crate-wide `Map`/`Set` aliases over a fixed-key hasher (`BuildHasherDefault<DefaultHasher>`) |
| `uuid::new_v4` staging ids   | per-node counter + node tag (better in prod too) |
| Auto-claim hash offset       | already FNV over the advertise string; sim sets explicit `sim://n{i}` advertises |
| Ephemeral ports              | sim never binds or reads them |
| RNG                          | one SplitMix64 stream from the config seed; consumed in scheduler order (single thread ⇒ deterministic) |
| SQLite                       | already deterministic given identical op sequences (MEMORY journal, no `random()`/`CURRENT_TIMESTAMP` in workload SQL) |

`dst check --seed N` runs the same seed twice and compares a running
hash of every client-visible event — the self-test that the contract
holds. Mining refuses to report a crash it cannot reproduce.

## Phase 1 — seams in production code (behavior-preserving)

1. `NodeConfig::new(store, live_dir)` with defaults; call sites shrink
   to overrides. New knobs: `transport` (None ⇒ HTTP), `serve_http`
   (false in sim), `exit_on_fence` (sim: crash the node instead of
   `process::exit`, which would kill the whole simulation).
2. `rpc::Transport` trait { `call`, `health` } + `Http` impl holding the
   reqwest client; `NodeInner.http` becomes `NodeInner.transport`.
3. Factor the RPC dispatch out of the axum handler into
   `api::handle_rpc(node, req) -> Response`; axum and the sim transport
   both call it.
4. Extract `api::Session` (auth + conn id + outstanding polls + will)
   with `frame(text) -> Option<String>` and `close()`; `ws_conn` becomes
   a thin socket pump around it. The sim opens Sessions directly.
5. Track every spawned task (server, guard, lazily-spawned workers) in
   `NodeInner.tasks`; add `NodeInner::crash()` = abort them all, no
   flush, no tombstones — a faithful kill -9.
6. Determinism sweep: `Map`/`Set` aliases everywhere iteration order can
   reach I/O or replies; staging-id counter; `tokio::time::Instant` for
   fencing stamps.

Suite must stay green after each step.

## Phase 2 — the simulator (`src/sim.rs`)

- **SimStore**: `MemBlobStore` (new, in `store.rs` — generally useful)
  wrapped with seeded latency (0–x ms virtual) and failure injection
  (fail windows, per-op probabilities). One instance per cluster: it IS
  the durable truth.
- **SimTransport**: name → Node routing table with seeded delay, drop
  probability, directional partitions, and "dead node" refusal.
- **SimWorld**: boots N nodes (`ClaimSpec::Auto`), runs the fault
  scheduler and the workload, owns the model.
- **Workload** (all through real public entry points):
  - bank transfers across K account objects, optimistic + pessimistic,
    idempotency-key rows for durability auditing
  - a pub/sub channel with a cursor-loop subscriber (poll semantics)
  - presence sessions arming wills, dying politely and impolitely
  - node crashes and restarts, partitions that heal, storage fault
    windows, lease takeovers under fire
- **Oracles** (violation ⇒ panic with seed + event trace):
  1. **Conservation**: at quiescent audit points and after the final
     stop-the-world restart, money sums exactly.
  2. **Durability**: every acked pessimistic row exists after recovery;
     lost optimistic rows only ever form a suffix (prefix consistency).
  3. **Wills**: a dead connection's armed will takes effect within
     bounded virtual time. `wills_survive_node_crash` in the config
     controls whether node-death orphaning is fatal — `true` demonstrates
     the canary bug; default `false` until the fix ships, so mining
     isn't drowned by a known issue.
  4. **Subscriber**: cursor loop sees channel ids strictly increasing,
     gapless at quiescence.
  5. **Liveness**: every submitted op resolves within a virtual-time
     bound; a hang is a deadlock and crashes with a state dump.
- **Config** (`DstConfig`, JSON, all defaulted): seed, nodes, logical
  workers, accounts, ops, fault rates/windows, fence TTL, oracle knobs.

## Phase 3 — the binary (`src/bin/dst.rs`)

- `dst run --seed N [--config f.json]` — one simulation, exit 0 or die.
- `dst check --seed N` — determinism self-test (two runs, trace hashes
  must match).
- `dst mine [--jobs N] [--seconds S]` — parent loops seeds derived from
  a base seed, one **subprocess per seed** (crash isolation catches
  aborts too, and each child is single-threaded ⇒ deterministic), all
  cores busy. Nonzero exit ⇒ reproduce once to confirm ⇒ write
  `crashes/seed-N.log` and keep mining.

## Phase 4 — sage asserts

`fafo_assert!` / `fafo_assert_eq!`: checked when a global paranoia flag
is set (sim always sets it; `FAFO_PARANOIA=1` for staging). Production
default: off — the goal in prod is to never crash; the goal in sim is to
crash the instant anything is off. First battery, worker loop:

- dirty_bytes == Σ dirty values; pending waiters ⊆ pending txns
- a running txn holds the head of every participant queue
- boat components never straddle two commit records
- inflight ∩ (shed candidates) = ∅; release only when clean
- routing exception for an owned object points at this worker
- poll re-checks only on live objects; acquired ≤ participants

## Phase 5 — proof and docs

- `dst check` passes on a spread of seeds.
- A pinned seed + `wills_survive_node_crash: true` demonstrably catches
  the canary will bug and replays it.
- Mining session (all cores) runs clean against the default config for a
  respectable stretch.
- AGENTS.md gets the "mine bugs for money" section: how to run, how to
  claim, what counts (any panic/abort from `dst run` at a reproducible
  seed).

## Out of scope, on the horizon

- The will FIX (durable wills: replicate armed wills into an object so a
  successor node fires them) — the DST exists to land that change safely.
- SIGSTOP-style pause faults and clock-rate skew.
- Schedule perturbation (seeded yield injection) to widen interleaving
  coverage beyond what latency jitter reaches.
- CI: `dst mine --seconds 300` as a merge gate.
