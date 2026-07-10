# fafo

Fuck around and find out: a miniature Durable-Objects-shaped database that
learns its own sharding and doesn't hinge on any single process or thread.

One small SQLite database per object. W logical workers (the stable
coordinate system) claimed by any number of processes via epoch leases.
Cross-object transactions with participants declared up-front. Blob storage
as the sole durable truth — ephemeral local disk everywhere else. Stop the
world, restart in any shape, pick up where it left off.

## Use it as infrastructure

```sh
./fafo up      # start in background, wait until healthy — then forget it
./fafo down    # graceful stop; ./fafo nuke wipes state
```

Then program against `http://127.0.0.1:8787` — **see [AGENTS.md](AGENTS.md)
for the API contract, patterns, and zero-dependency Python/TypeScript
clients in `clients/`.**

## Develop it

```sh
cargo run                        # foreground, one process claiming all workers
./demo.sh                        # accounts, atomic transfer, rejected overdraft
./cluster.sh                     # 4 processes -> kill -9 -> resume as 2 auto-claimers
cargo run --release --bin anneal # placement learning + stop-the-world proof
cargo test                       # atomicity, serializability, cross-node, recovery, SigV4
```

## Configuration (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `HOST` / `PORT` | `127.0.0.1` / `8787` | listen address (containers set `0.0.0.0:8080`) |
| `ADVERTISE` | `http://<local addr>` | base URL peers use to reach this node |
| `DATA_DIR` | `./data` | live working copies + fs blobs |
| `BLOB_STORE` | `fs` | `fs` or `r2` |
| `R2_ACCOUNT_ID` or `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | — | R2 credentials (S3 API, SigV4) |
| `LOGICAL_WORKERS` | `64` | fixed at cluster creation (create-once meta) |
| `CLAIM` | `all` | `all`, `7`, `0-15`, or `auto:<k>` (claim k free workers) |
| `CLUSTER_SECRET` | dev default + warning | shared secret for `/internal/rpc` |
| `API_TOKEN` | unset (open) | bearer token required on the public API |
| `HYST` | `200` | hysteresis tenure; `0` disables |

## Deploying to Cloudflare Containers

Containers have no direct container-to-container networking, so inter-node
RPC rides HTTP through the Worker: each instance advertises
`$PUBLIC_URL/internal/instance/<name>` and the Worker routes that prefix to
the named instance. Instances are identical — each auto-claims its share of
logical workers from the leases in R2 (`CLAIM=auto:16`), so scaling the
fleet is a config change, not a topology change. Container disk is
ephemeral, which is exactly fafo's assumption: R2 is the only truth.

```sh
wrangler r2 bucket create fafo-state
cd deploy
wrangler secret put CLUSTER_SECRET   # and API_TOKEN, R2_ACCOUNT_ID,
                                     # R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
wrangler deploy                      # builds ../Dockerfile, deploys Worker + containers
```

`deploy/wrangler.jsonc` + `deploy/src/index.ts` are the scaffold: a
`FafoNode` Container class (DO-managed, `sleepAfter` keeps it warm) and a
Worker that routes `/internal/instance/*` by name and spreads public traffic
across the fleet. SIGTERM (rolling deploys, scale-down) triggers graceful
shutdown: leases are tombstoned so replacements claim instantly. The
scaffold is written against the documented Containers API but has not been
deployed from this machine — treat the first deploy as a shakedown.

## API

```
POST /txn                   cross-object transaction; declare participants in `objects`
POST /objects/{id}/exec     single-object transaction (sugar over /txn)
POST /objects/{id}/query    read-only single statement
GET  /objects               list object ids
GET  /stats                 this process's workers, txns, takes, returns
```

Any process answers any request; transactions route (or proxy over RPC) to
the process holding the target worker's lease.

## Architecture

**No global anything in the hot path.** Each logical worker is a serial
event loop that is both admission authority (per-object FIFO queues) and
executor for the objects it owns. A transaction whose participants co-habit
— the common case once placement anneals — touches one worker and the blob
store. Period.

- **Admission** is decentralized deterministic locking: participants are
  acquired in sorted object order (local participant = queue position,
  remote = a Take, itself queued FIFO at the owner). Ordered acquisition =
  deadlock-free; FIFO queues = fair.
- **Commit**: transactions apply to local SQLite immediately and durability
  ships in **boats** — everything dirty coalesces into one staged snapshot
  set plus **one blob write of `txns/<id>.json`, the commit point for the
  whole batch**. Boats sail continuously (no timer): batch size adapts to
  load. Pessimistic txns (default) ack when their boat lands and act as
  barriers; `optimistic: true` acks on local apply and risks only the
  current boat — a crash rewinds the world to the last shipped boat,
  consistently (prefix consistency). Measured with 25ms simulated blob
  latency: 297 txn/s pessimistic vs 71,619 txn/s optimistic (~240x), boats
  of ~330 txns, every acked write durable. Crash after the record: rolled
  forward at boot; before: swept. `recover()` is idempotent.
- **Ownership moves only when clean**: takes and hysteresis returns wait
  for the object's boat to land, so a new owner never activates a stale
  snapshot.
- **Topology**: processes claim logical workers by creating
  `_lease/w<i>/e<epoch>.json` — `BlobStore::create` (create-if-absent,
  `If-None-Match: *` on R2) is the system's only consensus primitive. Dead
  holder → bump the epoch. 4 procs x 4 workers, 1 proc x 64: deployment detail.
- **Placement** persists as per-worker checkpoints (`_worker/<i>.json`),
  written remove-side-first on every transfer so no object is durably
  claimed twice; a crash between writes orphans the object, which falls back
  to its hash-default worker. Placement is a hint; the data is safe either way.
- **Learning**: cross-worker txns migrate strays to the plurality owner
  (cohesion), long-tenured objects displaced by one-offs snap back home,
  and 3 drags to the same worker within a window means move in (hysteresis).
  Ties break toward less-loaded workers and rehoming is denied to crowded
  ones (pressure) — without that cap the system observably collapses onto
  one mega-worker (87/96 objects on w0), quietly rebuilding the global
  sequencer we deleted. Two forces, held in tension.

Numbers from `anneal` (64 logical workers, 32 cliques of 3, 10% cross-clique
noise): cold start opens at ~30% cross-worker, anneals to ~13% (the noise
floor plus commuters), busiest worker carries ~7% of traffic across 32
active workers. After stop-the-world, phase 2 opens at ~15% — the learning
came back from the checkpoints, not from scratch.

## Security & fencing model

- Public API: optional bearer token (`API_TOKEN`). Inter-node RPC: cluster
  secret header, rejected with 401 otherwise. `/healthz` is deliberately
  open (lease claiming and the platform probe it).
- Every node runs a **lease guard**: if any claimed worker's epoch is
  superseded, the node fail-stops (`process::exit`), and the commit path
  independently refuses to pass the commit point once fenced. The residual
  window is the guard interval (5s) plus one write RTT — a process paused
  longer than that which wakes mid-commit can still race its replacement.
  Closing it fully needs storage-side conditional commits per epoch.
- Lease takeover needs a failed health check OR a tombstone (graceful
  shutdown writes them), so clean rolls are instant and crashed nodes are
  claimed after one probe. Health-check liveness is right for stopped
  worlds, too eager for network partitions (needs TTLs + clocks).

## Honest limitations

- Cross-node take retries are bounded, not starvation-free; pathological
  contention on one object pair can thrash. It also anneals away, which is
  the point.
- Snapshot-per-commit is O(db size) — right for small objects; big ones
  want WAL-delta shipping + compaction (Litestream's design).
- The R2 store's list parser assumes fafo's restricted key charset.
- `LOGICAL_WORKERS` is fixed at cluster creation; resharding is a manual
  migration.
