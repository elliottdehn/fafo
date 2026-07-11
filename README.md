# fafo

If you need a little database that you can just hammer hard and never goes
down and costs nothing, fafo is it.

Every **object** is its own SQLite database. Objects get ACID transactions
**across each other**, live queries (long-polls), presence, and per-object
auth — while the cluster shards itself, survives `kill -9`, and keeps its
only durable truth in object storage (local disk or S3/R2). Fuck around
and find out.

## Try it (one line)

```sh
git clone https://github.com/elliottdehn/fafo && cd fafo && ./fafo up
```

That builds, starts, and holds your terminal (Ctrl-C stops it cleanly;
`./fafo up -d` daemonizes). Then, from another terminal:

```sh
# every object is a database; this one is called "hello"
curl -XPOST localhost:8787/objects/hello/exec -H 'content-type: application/json' \
  -d '{"ops":[{"sql":"CREATE TABLE IF NOT EXISTS notes (body TEXT)"},{"sql":"INSERT INTO notes VALUES (?1)","params":["hello fafo"]}]}'

curl -XPOST localhost:8787/objects/hello/query -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM notes"}'
```

Cross-object atomic transaction — declare what you touch, commit to all of
it or none of it:

```sh
curl -XPOST localhost:8787/txn -H 'content-type: application/json' -d '{
  "objects": ["alice", "bob"],
  "ops": [
    {"object":"alice","sql":"UPDATE account SET balance = balance - 60"},
    {"object":"bob",  "sql":"UPDATE account SET balance = balance + 60"}
  ]}'
```

Long-poll any query — the reply comes back when the condition holds
(pub/sub, live views, and "wake me when the queue drains" are all just
queries):

```sh
curl -XPOST localhost:8787/objects/hello/poll -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM notes WHERE rowid > 1"}'   # blocks until true
```

Production traffic rides one WebSocket per client (transactions, polls,
and last-wills as frames) — see [AGENTS.md](AGENTS.md) for the full
contract, patterns, and the zero-dependency TypeScript/Python clients in
`clients/`.

## What you get

- **A database per object, for free.** Spinning one up is an API call and
  microseconds; an idle one costs the bytes it occupies. Object = user,
  tenant, document, channel — whatever your natural shard is.
- **ACID transactions ACROSS objects.** Declare participants up front,
  commit atomically to all of them. The thing durable-object platforms
  don't sell.
- **It shards itself.** Objects migrate toward the workers that transact
  on them together; hysteresis stops the ping-pong; a pressure force stops
  pile-ups. You never write a sharding function.
- **Kill -9 is a supported operation.** The commit point is one blob
  write; compute is disposable. Stop the world, restart in any shape, pick
  up where it left off.
- **Realtime built in.** Long-polls (condition + change-detection),
  last-will transactions (MQTT-style presence), ephemeral TEMP tables that
  wake watchers but never touch storage, and capability tokens so browsers
  connect directly with per-object, per-verb grants.
- **Fast where it counts.** ~80k optimistic writes/sec per object stream,
  group commit coalescing ~240x at object-storage latency, 26ms reads over
  a pinned WebSocket in production, ~$4 per billion writes back-of-envelope.

## How it works, in one paragraph

One worker owns an object at a time, so writes are serial and there are no
locks to take. Durable truth lives in object storage; the local file is a
disposable working copy. A write applies locally in microseconds and rides
the next "boat" — everything dirty coalesces into one commit record whose
single blob write is the atomic commit point for the whole batch, across
every database in the transaction. Cold object? Activate it from the blob.
Disk pressure? Evict it — it was never precious. Ownership moves by
learning from the transaction graph, fenced by epoch leases so a paused
zombie can't write over its successor. The long version:
[architecture.md](architecture.md).

## How it's tested

Mostly by dice. `dst` is a deterministic simulator: the whole cluster on
one thread in virtual time, with seeded crashes, partitions, and storage
faults, running five concurrent workloads chosen so every ACID guarantee
has an invariant pointed at it (conservation with a moving money supply,
racing escrow settlements, idempotent counters, watched feeds consumed
three ways). Any violation panics with a seed that replays it bit for bit.

```sh
cargo run --release --bin dst -- run --seed 7    # one cluster life
cargo run --release --bin dst -- mine --fuzz     # all cores, random shapes, forever
```

It has found and closed [27 bugs and counting](bugs.md) — the ledger, with
an ELI5 and a nastiness score for each, is honestly the best reading in
this repo.

## Deploying

Runs anywhere the binary runs (durable truth on local disk), and is built
for Cloudflare Containers + R2 (four instances of it run this repo's
production cluster). `deploy/` has the Worker + container scaffold;
[AGENTS.md](AGENTS.md) has the playbook. Details:
[architecture.md](architecture.md).

## Developing

```sh
cargo test                        # 140 tests: engine, api, grants, recovery
./demo.sh                         # accounts, atomic transfer, rejected overdraft
./cluster.sh                      # 4 procs -> kill -9 -> resume as 2
cargo run --release --bin anneal  # watch placement learn, then survive a restart
```

## Honest fine print

- Optimistic writes can lose the last in-flight boat on a crash — that's
  the contract; a pessimistic transaction is your durability barrier.
- One white-hot object is bounded by one core: spread load across objects
  (that's the whole idea).
- No global secondary indexes — build views with an atomic outbox, where
  the cost is printed in the participant list.
- A node that dies and is NEVER replaced strands its objects (production
  platforms always reschedule; safe orphan-reclaim is
  [documented future work](bugs.md)).
- More in [architecture.md](architecture.md#honest-limitations).

MIT. Built by fucking around; correctness by [finding out](bugs.md).
