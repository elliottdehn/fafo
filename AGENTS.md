# fafo — agent & developer guide

fafo is a transactional object store you run locally and program against
over HTTP. One small SQLite database per object; atomic transactions across
objects; placement that shards itself. You do not need to understand the
internals to use it — this file is the contract.

## Run it, then forget it

```sh
./fafo up -d     # builds if needed, starts in background, waits until healthy
./fafo status    # liveness + placement stats
./fafo down      # graceful stop (state persists in ./data)
./fafo logs 100  # tail the log
./fafo nuke      # stop and wipe ALL local state
```

`./fafo up -d` is idempotent — safe to call at the top of any script (plain
`./fafo up` runs in the foreground for humans trying it out). Default
port 8787 (`FAFO_PORT` to change), state in `./data` (`FAFO_DATA`). State
survives stop/start/kill -9; `nuke` is the only thing that deletes data.

## Programming model

- An **object** is a named SQLite database (its own tables, indexes,
  constraints). Objects spring into existence on first write. Think "one
  object per entity": a user, an account, a game room, a device.
- Object ids: `[A-Za-z0-9_-]`, max 64 chars, must not start with `_`.
- A **transaction** declares every object it touches up-front, then runs
  SQL statements against them. All-or-nothing across all participants: if
  any statement fails (including constraint violations), nothing anywhere
  is applied.
- **One SQL statement per op.** Text after the first statement is silently
  ignored — send several ops instead.
- Params are JSON (`string | number | bool | null`), bound as `?1`, `?2`, …
- Serializable isolation. Reads through `/query` are consistent and
  rejected if the SQL would write.
- Transactions touching one object (or objects that transact together
  often) are fast; the system learns co-access patterns and migrates
  objects to minimize coordination. You don't manage placement.

## WebSocket: the production connection

Treat the WebSocket as your database connection; HTTP is for debugging,
curl, and one-off scripts. The difference is not cosmetic: each HTTP
request pays the full per-request platform path, while frames on an
established socket pay raw network RTT (measured in production: 41ms
frames vs ~100ms+ per HTTP request, and vs seconds when routing is cold).

```
GET /ws?for=<object>
Sec-WebSocket-Protocol: fafo, fafo-token.<API_TOKEN>
```

Auth rides the subprotocol header (the one header browsers CAN set on a
WebSocket) or a normal `Authorization: Bearer` — NEVER the URL; query
strings end up in access logs. The server selects `fafo` back.
`for` pins the socket to that object's owner instance — set it to the
object (or tenant) this connection will mostly touch, or frames for it pay
an inter-instance hairpin. One socket, many transactions, pipelined:

```jsonc
// -> frame            (id is yours, echoed back; objects may be omitted —
//                      they're inferred from the ops)
{ "id": 1, "ops": [{ "object": "alice", "sql": "UPDATE ...", "params": [1] }],
  "optimistic": true, "read_only": false }
// <- reply
{ "id": 1, "result": { "txn_id": "w17-4", "results": [ ... ] } }
// <- or
{ "id": 1, "error": "op failed, transaction rolled back: ...", "status": 400 }
```

Replies can arrive out of order (frames execute concurrently) — correlate
by `id`. `clients/fafo.ts` ships `FafoSocket` doing exactly this:

```ts
const conn = await FafoSocket.open(url, token, "my-hot-object");
await conn.txn([{ object: "alice", sql: "..." }]);
```

### Long-poll frames: waiting on the database

A poll is a read-only query whose reply is held until its condition holds.
Two flavors, one field apart:

```jsonc
// Condition variable (no baseline): replies when the result is non-empty.
// SQL is the condition language — WHERE, NOT EXISTS, aggregates, thresholds.
{ "id": 2, "poll": { "object": "chan",
    "sql": "SELECT * FROM msgs WHERE id > ?1 ORDER BY id LIMIT 100",
    "params": [4021] } }

// Change detection (baseline = hash from the previous reply): replies when
// the result differs. "" bootstraps with an immediate snapshot + hash.
{ "id": 3, "poll": { "object": "presence",
    "sql": "SELECT u FROM p ORDER BY u", "baseline": "9f2c81aa03d1e644" } }

// <- reply, whenever the condition holds (may be immediate)
{ "id": 2, "result": { "txn_id": "w17-poll-0", "results": [{ "rows": [...] }],
                       "hash": "b1d0..." } }

// Abandon an outstanding poll (also automatic on socket close)
{ "id": 2, "cancel": true }
```

The subscription is your loop: poll, process, move your cursor (it lives in
your own query params) or feed the hash back, poll again. Nothing is ever
missed: the initial check runs at the object's serialization point, so a
write cannot slip between "empty" and "parked". Add `"durable": true` to
have the condition judged only against durably-shipped state.

If a poll fails with a "re-poll" error (the object migrated, optimistic
state reverted, or the node is shutting down), just poll again — that IS
the recovery protocol. Polls are answered by the object's owner: pin the
socket with `?for=` (HTTP `/objects/{id}/poll` routes correctly already).

### Last-will transactions: what happens when you die

A connection can arm one will — an ordinary atomic transaction that runs
when the socket dies, whether it said goodbye or not (MQTT-style):

```jsonc
{ "id": 4, "will": { "ops": [{ "object": "room",
    "sql": "DELETE FROM presence WHERE session = ?1", "params": ["s-1"] }] } }
// <- { "id": 4, "result": { "will": "armed" } }
// re-arming replaces the will; empty ops disarm:
{ "id": 5, "will": { "ops": [] } }   // <- { "will": "disarmed" }
```

Wills are validated at arm time (a bad will is rejected while you can
still hear about it) and run with full txn semantics — so a will can
release locks, publish "user went offline" to a channel, and delete
presence rows in one atomic step.

Wills survive the death of the node holding your socket. Arming persists
the will durably with a deadline the holding node keeps refreshing; if
that node dies, the deadline lapses and any surviving node fires the will
(under the grants frozen at arm time). Two consequences worth designing
for: firing is **at-least-once** — a sweeper can die mid-fire and another
retry — so keep will ops idempotent (an `INSERT OR IGNORE`, a delete, an
UPSERT — the same idempotency money-like writes already want); and a
crash-orphaned will fires within a bounded delay (its refresh TTL), not
instantly, so for sub-second presence pair the will with an `expires_at`
column refreshed on a heartbeat and filter it in the view query — the
will is the durable backstop, the expiry is the fast path.

### Capability tokens: connecting untrusted devices

The root `API_TOKEN` is your backend's credential. To let end-user devices
connect directly, mint capability tokens — per-object, per-verb grants,
verified statelessly by every node (HMAC over the cluster secret; nothing
to look up, nothing to revoke: keep TTLs short):

```sh
curl -sX POST $F/grant -H "authorization: Bearer $ROOT" \
  -H 'content-type: application/json' -d '{
    "grants": [
      { "objects": "room-42",   "verbs": ["read", "poll", "insert"] },
      { "objects": "user-77-*", "verbs": ["read", "write", "poll"] }
    ], "ttl_secs": 900, "sub": "user-77" }'
# -> { "token": "fafo1.…", "exp": 1783… }
```

The token goes wherever the root token would (bearer header, or the
`fafo-token.` subprotocol on WebSocket). Verbs: `read`, `insert`,
`update`, `delete`, `ddl`, `poll` — plus `write` as shorthand for the
four write verbs. Inserts, updates, and deletes are deliberately separate
powers: publish-to-a-channel is not rewrite-history. Enforcement is
SQLite's own authorizer at statement-prepare time, so CTEs and trigger
cascades are classified by the engine — an insert-only token cannot
smuggle a DELETE through a trigger. `PRAGMA` and `ATTACH` are always
denied to capability holders; `/stats`, `/objects`, and `/grant` need
root. Wills run under the grants frozen at arm time, so an expired token
still keeps its already-authorized promise. Objects match exactly or by
prefix glob (`user-77-*`) — design your object ids so tenancy is a
prefix.

## HTTP API (debugging & scripts)

Same transactions, one request each. Base URL: `http://127.0.0.1:8787`.
All bodies are JSON (`content-type: application/json`). Errors:
`{"error": "..."}` with 4xx/5xx. If the server was started with
`API_TOKEN`, send `Authorization: Bearer <token>`.

### POST /txn — atomic cross-object transaction

```sh
curl -s localhost:8787/txn -H 'content-type: application/json' -d '{
  "objects": ["alice", "bob"],
  "ops": [
    {"object": "alice", "sql": "UPDATE account SET balance = balance - ?1", "params": [60]},
    {"object": "bob",   "sql": "UPDATE account SET balance = balance + ?1", "params": [60]}
  ]}'
# -> {"txn_id":"w17-4","results":[{"rows_affected":1},{"rows_affected":1}]}
```

Every op's `object` must appear in `objects`, or the request is rejected.
Results are per-op, in request order: `{"rows": [...]}` for statements that
return rows (SELECT, RETURNING), `{"rows_affected": n}` otherwise.

Optional: `"optimistic": true`. Optimistic transactions are acked as soon
as they apply locally and become durable with the next "boat" (writes
coalesce into one storage commit; boats ship continuously, so the window is
one storage round trip). The contract: a crash inside that window loses the
transaction — together with everything after it, consistently (the world
rewinds to the last boat; invariants always hold). Default `false` = acked
only when durable — and a pessimistic transaction doubles as a barrier: its
ack means everything before it on those objects is durable too. Rule of
thumb: telemetry, counters, caches → optimistic; money → pessimistic (or
optimistic writes followed by one pessimistic barrier).

### POST /objects/{id}/exec — single-object transaction

```sh
# one statement
curl -s localhost:8787/objects/alice/exec -H 'content-type: application/json' \
  -d '{"sql": "INSERT INTO account (balance) VALUES (?1)", "params": [100]}'

# several statements, all-or-nothing
curl -s localhost:8787/objects/alice/exec -H 'content-type: application/json' -d '{
  "ops": [
    {"sql": "CREATE TABLE IF NOT EXISTS account (balance INTEGER NOT NULL CHECK (balance >= 0))"},
    {"sql": "INSERT INTO account (balance) VALUES (?1)", "params": [100]}
  ]}'
```

### POST /objects/{id}/poll — long-poll

Same body as `query` plus optional `durable` and `baseline`; the response
hangs until the condition holds (see the WebSocket section for semantics).

```sh
curl -sX POST $F/objects/chan/poll -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM msgs WHERE id > ?1 ORDER BY id","params":[0]}'
# -> blocks until a message exists, then {"rows":[...],"hash":"..."}
```

### POST /objects/{id}/query — read-only

```sh
curl -s localhost:8787/objects/alice/query -H 'content-type: application/json' \
  -d '{"sql": "SELECT balance FROM account"}'
# -> {"rows":[{"balance":40}]}
```

Write SQL here returns 400 — use `/exec` or `/txn`.

### GET /objects, GET /stats, GET /healthz

Object listing, placement/traffic stats, liveness (always unauthenticated).

## Client libraries (zero dependencies)

- `clients/fafo.py` — stdlib-only Python; copy the file into your project.
- `clients/fafo.ts` — fetch-only TypeScript; works in Node 18+, Bun, Deno,
  browsers, Workers.

```python
from fafo import Fafo
db = Fafo()
db.txn(["alice", "bob"], [
    ("alice", "UPDATE account SET balance = balance - ?1", [60]),
    ("bob",   "UPDATE account SET balance = balance + ?1", [60]),
])
```

## Patterns

- **Create-if-missing schema**: start each object's first transaction with
  `CREATE TABLE IF NOT EXISTS ...` ops. There is no separate DDL step.
- **Invariants live in SQL**: `CHECK` constraints + cross-object txns give
  bank-grade invariants (see the overdraft demo in `demo.sh` — a failed
  CHECK on one object rolls back the other object's already-applied ops).
- **Idempotency**: a txn is atomic but the HTTP call is at-most-once from
  your side; on timeout you don't know if it committed. For money-like
  writes, add a client-supplied id column with a UNIQUE constraint and
  treat the constraint violation on retry as success.
- **Objects can be entity-sized or tenant-sized.** Small objects ship whole
  snapshots; past 64 KB they ship page deltas, so a multi-MB
  database-per-tenant object pays for what changed, not what it weighs.
  Cold objects activate off-loop (no head-of-line blocking of other
  tenants), and repeat visits to a worker reuse a local cache plus the
  delta chain — only a tenant's FIRST arrival at a worker pays full size.
- **High write throughput**: send `optimistic: true` and let boats coalesce
  (measured: ~240x at object-storage latency). Barrier with one pessimistic
  txn when you need a durability checkpoint.
- **Pub/sub**: a channel is an object with a `msgs` table
  (`id INTEGER PRIMARY KEY AUTOINCREMENT`). Publishers INSERT (optimistic
  for firehoses); consumers run the poll cursor loop —
  `WHERE id > ?cursor ORDER BY id LIMIT 100`, bump the cursor per reply.
  At-least-once falls out of the loop; the cursor makes it exactly-once.
  Retention is a `DELETE` on your schedule; replay is a smaller cursor.
- **Transactional outbox**: publish and state-change in ONE cross-object
  txn (`{objects: ["orders", "orders-events"], ops: [...]}`) — subscribers
  can never observe the message without the state change or vice versa.
- **Live views** (presence lists, scoreboards): change-detection poll on
  the view query itself. Bootstrap with `baseline: ""`, then feed each
  reply's hash back. Writes that don't change the result don't wake you;
  deletes do (the hash shrinks with the result).
- **Condition wait**: poll `SELECT 1 WHERE NOT EXISTS (...)` or any
  aggregate — "wake me when the queue drains" is
  `SELECT 1 WHERE (SELECT COUNT(*) FROM jobs WHERE done = 0) = 0`.
- **Ephemeral state is a table property, not a second API**: anything in a
  `CREATE TEMP TABLE` gets full SQL and wakes polls like durable state, but
  never rides a boat and never costs a storage op — typing indicators,
  60Hz cursor positions, occupancy counters, all free. The contract:
  temp state is per-activation; eviction, migration, and restart clear it
  (queries then error "no such table" — recreate on reconnect). Bonus from
  the same mechanism: writes that change nothing (UPDATE matching zero
  rows) also ship nothing.
- **Presence**: INSERT your row on connect, arm a will that DELETEs it,
  and let everyone else hold a change-detection poll on the roster query.
  Joins and leaves arrive as fresh snapshots; no heartbeat protocol to
  build (add an `expires_at` refresh as the node-death backstop).

## Deploying to Cloudflare

Prereqs: `wrangler` (npx is fine) authenticated, Docker daemon RUNNING
(wrangler builds the image locally), a Workers Paid plan (Containers).

```sh
npx wrangler r2 bucket create fafo-state
cd deploy && npm install

# Secrets. Generate the first two; the R2 pair comes from the dashboard:
# dash.cloudflare.com -> R2 -> Manage API tokens -> Object Read & Write,
# scoped to the bucket. wrangler cannot mint R2 S3 credentials.
openssl rand -hex 24 | npx wrangler secret put CLUSTER_SECRET
openssl rand -hex 24 | npx wrangler secret put API_TOKEN     # save these!
printf '<ACCOUNT_ID>'  | npx wrangler secret put R2_ACCOUNT_ID
printf 'fafo-state'    | npx wrangler secret put R2_BUCKET
printf '<KEY_ID>'      | npx wrangler secret put R2_ACCESS_KEY_ID
printf '<SECRET_KEY>'  | npx wrangler secret put R2_SECRET_ACCESS_KEY

npx wrangler deploy          # builds ../Dockerfile, pushes, creates the app
# then set vars.PUBLIC_URL in wrangler.jsonc to the printed URL + redeploy
```

Non-negotiables learned in production:

- **Do not switch commit engines under existing data.** The default is now the
  log-structured engine (`objects/<id>.L.<seq>` / `.B.<seq>` / `txns/<id>.O`);
  the legacy engine (`objects/<id>` / `.d.<counter>` / `txns/<id>.json`) is a
  different durable layout with no auto-migration between them. A fresh deploy
  is fine. But a deployment that already holds **legacy-format** state must
  either migrate it, wipe it, or stay pinned to legacy by setting
  `FAFO_LEGACY_COMMIT=1` on the app — otherwise the new engine boots, finds no
  keys it recognizes, and the data is invisible (not deleted, just unread).

- **Pin the container region** (`containers[].constraints.regions`) to the
  same region as your R2 bucket and your users. The beta scheduler
  otherwise places instances anywhere on Earth — we measured half a fleet
  in London and one container in Bangalore serving Ohio traffic at 0.9s a
  request. Geography is the latency budget; everything else is noise.
- **Keep both Dockerfile stages on the same Debian release.** `rust:*-slim`
  silently tracks new Debian releases; a newer-glibc build stage on an
  older-glibc runtime crashes before `main()` with `GLIBC_x.yz not found`.
- Rolling deploys replace instances gradually. Instances claim
  deterministic worker ranges (fafo-N owns [N*W/I,(N+1)*W/I)), so
  mid-rollout you may see mixed old/new behavior and transient
  "no live node holds logical worker" — it converges; don't panic-redeploy.

## Debugging production

```sh
npx wrangler tail fafo                      # live Worker exceptions + logs
npx wrangler containers list                # app status (provisioning/active)

# Per-instance introspection through the router (N = 0..instances-1):
curl $BASE/internal/instance/fafo-N/healthz # location, region, worker count
curl $BASE/internal/instance/fafo-N/stats \
  -H "authorization: Bearer $API_TOKEN"     # claim ranges, txns, ships

# Inspect R2 state directly (curl speaks SigV4):
B=https://<ACCOUNT_ID>.r2.cloudflarestorage.com/fafo-state
curl -s --aws-sigv4 "aws:amz:auto:s3" --user "$KEY_ID:$SECRET" \
  "$B?list-type=2&prefix=_lease/" | grep -o "<Key>[^<]*</Key>"
```

Reading the signs:

- **`error code: 1101`** = the Worker threw. `wrangler tail` while
  reproducing. "Container is not running / crashed checking ports" means
  the BINARY died at boot — reproduce locally with the exact image:
  `docker run --rm -e BLOB_STORE=r2 -e R2_... fafo-fafonode:<tag>`.
- **healthz shows `"workers":0`**: the node is mid-boot (the HTTP server
  deliberately starts before the claim loop) or lost its claims — check
  again in seconds, then check `_lease/` in the bucket for who holds what.
- **Slow requests**: check each instance's healthz `location` first;
  a misplaced container dominates everything. Then remember the tiers:
  reads/optimistic ≈ instance baseline + ~30ms; pessimistic adds one R2
  round trip; a request whose object lives on ANOTHER instance adds a
  ~0.5-1s hairpin (avoided for hash-default objects by the Worker's FNV
  routing; WebSocket clients should pin with `/ws?for=<object>`).
- **Wiping state**: deleting `_lease/*` and `_worker/*` from the bucket is
  safe (placement is a hint) but running nodes keep in-memory claims until
  restarted — wipe, then redeploy, then WAIT for all instances to report
  their deterministic ranges. Deleting `objects/*` deletes the data.
- Epoch chains growing under `_lease/b*/` are normal (each boot bumps);
  a node fail-stopping with `FENCED` lost its lease to a successor —
  also normal during takeovers. Ambient churn only matters if it loops.

## Developing fafo itself

```sh
cargo test                       # full suite (146 tests): atomicity,
                                 # serializability, cross-node RPC, crash
                                 # recovery, SigV4
cargo llvm-cov --lib             # line coverage (held at ~98.5%; the rest
                                 # is defensive arms and benign races)
./cluster.sh                     # 4 processes -> kill -9 -> resume as 2
cargo run --release --bin anneal # watch placement learning converge
```

The real correctness gate is not `cargo test` — it's the bug mine below.
`cargo test` proves the happy paths and the specific regressions; the mine is
what proves the system holds under crashes, partitions, clock skew, failing
storage, and zombie-writer takeovers, all at once, across thousands of seeds.
Any change to the commit path, ownership, leases, or recovery is not "done"
until the mine is green on it.

Layout: `src/worker.rs` (serial executor + commit protocol + admission),
`src/cluster.rs` (topology, leases, routing, checkpoints), `src/store.rs` +
`src/r2.rs` (blob backends; the trait is the persistence boundary),
`src/api.rs` (HTTP), `src/object.rs` (SQLite activation). Architecture and
guarantees: README.md. Deployment to Cloudflare Containers: `deploy/`.

Rules of the codebase:
- The blob store is the only durable truth. Live SQLite files are
  disposable caches; anything else must be reconstructable from blobs.
- Never hold a `&Connection` (or any shared borrow of worker state) across
  an `.await` — Connection is Send but not Sync; the compiler enforces it.
- The commit point is exactly one blob write (`txns/<id>.json`). Anything
  before it must be undoable; anything after it must be pure roll-forward.
- Ordering invariant: transactions acquire participants in sorted object
  order, and all admission is per-object FIFO. Violating either reintroduces
  deadlocks.

## The bug mine: a crash is a bug, a bug is $100

fafo is verified by deterministic simulation testing (`src/bin/dst.rs`): the
whole cluster on one thread, on virtual time, with seeded fault injection and
oracles that check conservation of money, transfer atomicity, cross-object
liveness, subscriber delivery, and last-will firing after every phase. **A
crash is a bug. A bug is $100.** Every crash replays bit-for-bit from its
seed, so a find is a repro, not a rumor.

**Your job, if you are here to earn: make it crash.**

```sh
cargo build --release --bin dst

# ALWAYS check disk first. The mine writes scratch to $TMPDIR; a full disk
# makes subprocesses block, reported as "HUNG" — which is NOT a bug.
df -h "$TMPDIR"

# Mine forever, all cores, every fault on at once:
./target/release/dst mine --fuzz --pause
```

- **The log-structured commit engine is the default** — the one that is
  actually clean, and the one you are mining. The legacy per-object-base path
  survives only as a fallback and determinism baseline, reachable with
  `FAFO_LEGACY_COMMIT=1`; a crash reachable only that way does not count.
- **`--fuzz`** derives the whole cluster shape from each seed: node count,
  storage-failure rate (up to ~53%), per-node clock skew, fence TTLs.
- **`--pause`** forces two pause-adversary faults per run: isolate a live
  node, let a peer steal its lease, let it rejoin and try to keep writing —
  the zombie writer fencing exists to stop.
- **`--jobs N`** caps parallelism (default: core count). **`--seconds S`**
  bounds the run (omit for endless). **`--seed BASE`** fixes where the seed
  stream starts.

**When it finds one.** The crash is confirmed by a re-run, then logged to
`crashes/seed-N.log` with the exact repro command (fault flags included).
Reproduce it, bit-for-bit:

```sh
./target/release/dst run --seed <N> --fuzz --pause
```

`dst check --seed <N>` runs a seed twice and diffs the trace hash — use it to
prove a fix is deterministic, and to prove a suspected bug is a real fault and
not nondeterminism in the harness itself.

**What is NOT a bug (do not claim the bounty for these):**
- A `HUNG` report when `$TMPDIR` is full. That is disk, not fafo. Clear it and
  re-run: `find "$TMPDIR" -maxdepth 1 -name 'dst-*' -delete`.
- A crash that only reproduces under `FAFO_LEGACY_COMMIT=1`. The legacy
  engine is the fallback, not the shipped default.
- A run that is merely slow. A fork serialized safely is legitimately slower
  than one that clobbers; the harness uses a progress-relative deadline, so
  slow-but-progressing is fine. Only a genuine hang (killed at the wall-clock
  timeout) or an oracle panic counts.

**The rules of a claim.** Every bug ever found is written up in `bugs.md` — 51
numbered entries, each with what the oracle saw, the root cause, the fix, and
a nastiness score. As of this writing the mine is at **zero crashes across
both `--fuzz` and `--pause`, thousands of seeds each**, so the bounty is for
genuinely new ground: a fresh seed that breaks an invariant. A fix earns the
$100 only if it:

1. makes the failing seed clean, and survives a fresh mine sweep;
2. adds the bug to `bugs.md` in the house format (oracle → root → fix, ELI5,
   nastiness);
3. keeps all 146 `cargo test`s green;
4. keeps both engines deterministic and unshifted: `dst check --seed 1` must
   stay `640 events, trace 943bb99f205b474b` (the default log engine), and
   `FAFO_LEGACY_COMMIT=1 dst check --seed 1` must stay
   `640 events, trace e85a7163e4ebb313` (the untouched legacy baseline).

That last rule is the tripwire: many "fixes" that silence one seed do so by
perturbing timing or shifting the fault RNG, which just relocates the crash to
a different seed. A fix that moves either determinism trace has changed base
behavior and is guilty until proven innocent.
