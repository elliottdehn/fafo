# fafo — agent & developer guide

fafo is a transactional object store you run locally and program against
over HTTP. One small SQLite database per object; atomic transactions across
objects; placement that shards itself. You do not need to understand the
internals to use it — this file is the contract.

## Run it, then forget it

```sh
./fafo up        # builds if needed, starts in background, waits until healthy
./fafo status    # liveness + placement stats
./fafo down      # graceful stop (state persists in ./data)
./fafo logs 100  # tail the log
./fafo nuke      # stop and wipe ALL local state
```

`./fafo up` is idempotent — safe to call at the top of any script. Default
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
presence rows in one atomic step. Caveat: a will runs at the node holding
your socket; if that node itself dies, it can't. Pair presence rows with
an `expires_at` column refreshed on a heartbeat and filter it in the view
query — the will is the fast path, the expiry is the backstop.

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
cargo test                       # full suite: atomicity, serializability,
                                 # cross-node RPC, crash recovery, SigV4
./cluster.sh                     # 4 processes -> kill -9 -> resume as 2
cargo run --release --bin anneal # watch placement learning converge
```

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
