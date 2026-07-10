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

## HTTP API

Base URL: `http://127.0.0.1:8787`. All bodies are JSON
(`content-type: application/json`). Errors: `{"error": "..."}` with 4xx/5xx.
If the server was started with `API_TOKEN`, send `Authorization: Bearer <token>`.

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
- **Keep objects small** (roughly: one entity's rows). Durability is
  snapshot-per-commit — a 100 MB object pays 100 MB per write txn.

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
