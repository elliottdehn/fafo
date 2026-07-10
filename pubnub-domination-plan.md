# The PubNub Domination Plan

**Thesis: PubNub sells you a pipe and rents your history back to you. We are a
database whose queries are the channels.**

Every PubNub SKU is a workaround for not having state. They built an ephemeral
message pipe, then spent a decade bolting persistence (Message Persistence),
identity (Presence, App Context), access control (Access Manager), and logic
(Functions) back onto it — each one metered separately. We come from the other
side: every channel is a SQLite database with cross-object ACID transactions
and a durable blob-store spine. The plan is not to clone their feature list.
It is to add a small number of *maximally generic* primitives from which their
entire menu falls out as usage patterns.

Out of scope for this document: million-client fanout (the relay tier of
hibernating DOs is designed separately). Everything here assumes fanout within
one node's socket capacity.

---

## What they sell

| PubNub SKU | What it actually is |
|---|---|
| Pub/Sub | ephemeral message pipe, per-message metering |
| Message Persistence | a replay window (retention capped, billed extra) |
| Presence | who's connected, join/leave events, heartbeat timeouts |
| Signals | pub/sub again, but cheaper because undurable and tiny |
| App Context ("Objects") | key-value metadata about users/channels |
| Message Actions | reactions/receipts attached to messages |
| Access Manager | per-channel grant tokens with TTL |
| Functions | JS transforms on messages in flight |

Note the shape: six of eight are "we didn't have a database, so here's a
metered approximation of one."

## What we get for free once long-poll exists

- **History & replay** — it's a table. Retention policy is a `DELETE`.
  Unlimited history is our *default*; theirs is an upsell.
- **Per-subscriber filtering** — their filter-expression mini-language is our
  `WHERE` clause. Every subscriber brings its own query: joins, aggregates,
  windows. "Wake me when my unread count changes" is one line and structurally
  impossible for them.
- **Transactional publish** — the outbox pattern: state change + publish in
  one atomic cross-object txn. Their publish can never be atomic with
  anything.
- **App Context / Message Actions** — rows. Next.
- **Reliability** — the client cursor loop (below) beats a replay window:
  consumers converge on correct state by construction instead of replaying
  a queue and hoping.

---

## Phase 0 — Long-poll queries (the foundation)

A watch is a long-poll: submit a read-only query, and the reply arrives as
soon as the condition it expresses holds. That is the entire protocol.
Everything later in this plan is either a usage pattern of this or a small
primitive standing next to it.

Two flavors, one field apart:

- **Condition variable** (no `baseline`): reply as soon as the result is
  non-empty. SQL expresses arbitrary conditions — `NOT EXISTS`, aggregates,
  thresholds — so "wake me when the balance goes negative" is a poll.
- **Change detection** (`baseline` = hash from the previous reply): reply as
  soon as `hash(rows) != baseline`. Every reply carries the new hash; a
  first call with `baseline: ""` returns immediately (bootstrap snapshot).
  This is Consul's blocking-query index, with 8 bytes of server state per
  parked poll instead of a retained result set. Deletes are visible — a row
  vanishing from a non-empty result changes the hash.

### Protocol

```jsonc
// WebSocket frame (HTTP: POST /objects/{id}/poll with the same body)
{ "id": 7, "poll": { "object": "room-42",
                     "sql": "SELECT * FROM msgs WHERE id > ?1 ORDER BY id LIMIT 100",
                     "params": [4021],
                     "baseline": "",       // optional: change-detection mode
                     "durable": false } }  // optional: fire only on durable state

// reply, whenever the condition holds (may be immediate)
{ "id": 7, "result": { "results": [{ "rows": [ ... ] }], "hash": "9f2c..." } }

// cancel an outstanding poll
{ "id": 7, "cancel": true }
```

The client loop IS the subscription: poll, process, move your cursor (it
lives in your own query params) or carry the hash forward, poll again.
At-least-once delivery falls out of the loop; exactly-once is your cursor.

### Semantics: the client drives, the server parks

- Registration rides the object's txn queue like any read — the initial
  check happens at the serialization point, so there is no gap between
  "checked: empty" and "parked: will be re-checked on every later write."
- A parked poll is re-run after each write txn on the object. Non-empty (or
  hash-differs) → reply, done. A parked poll is just SQL text + a reply
  slot; there are no retained result sets and no diff engine.
- **No slow-consumer problem exists.** The server never pushes unbidden; a
  slow client polls slower. Backpressure is inherent in the shape.
- You observe **states, not events**: a row inserted and deleted between two
  polls is never seen. Anyone needing a durable event log writes one (it is
  a database) and polls *that* with a cursor.
- Migration, eviction-revert, and shutdown fail parked polls with a plain
  error ("re-poll") — no lifecycle protocol, because re-polling is already
  the client's loop. Polls survive plain shedding (the re-check reactivates
  the object on the next write).
- `durable: true`: the initial check waits for quiescence; parked checks run
  at **boat launch** (exactly the state being shipped) and fired polls join
  the boat's existing waiter list — acked at the commit record, the same
  machinery pessimistic txns already use. Zero new landing logic.
- Polls are node-local: pin the socket with `/ws?for=<object>` (the edge
  Worker already routes `/objects/{id}/*` to the owning instance).

### Userland patterns (document, don't build)

- **Feed / pub-sub**: `WHERE id > ?cursor ORDER BY id LIMIT 100`, bump the
  cursor each reply. Kafka-consumer shape, cursor owned by the client.
- **Live view** (presence list, scoreboard): change-detection poll on the
  view query itself; each reply is the fresh result.
- **Condition wait**: `SELECT 1 WHERE NOT EXISTS (...)`, aggregates, joins.

### Test battery (build = "test the fuck out of it")

immediate return when non-empty; parks-then-fires on insert; no lost wakeup
under concurrent write hammering (cursor loop consumes exactly 1..=N, no
gaps, no dupes); change-detection bootstrap + gapless hash loop + fires on
DELETE; durable poll silent before landing (delayed store), fires after;
durable registered while clean fires immediately; parked polls fail on
migration and succeed re-polled at the new owner; fail on revert (sinking
boat); survive shed; read-only enforcement; dead-client lazy cleanup (no
leak, no panic); two polls with different queries on one object; poll on a
cold object activates it; transactional-outbox end-to-end; all existing
tests stay green.

## Phase 1 — Last-will transactions (kills: Presence)

MQTT got this right decades ago. A connection registers arbitrary ops that
run when the socket dies:

```jsonc
{ "id": 9, "will": { "objects": ["room-42"],
                     "ops": [{ "object": "room-42",
                               "sql": "DELETE FROM presence WHERE session = ?1",
                               "params": ["s-abc"] }] } }
```

Presence is then just a pattern: `INSERT INTO presence ...` on connect, the
will deletes it, and everyone holding a change-detection poll on `SELECT * FROM
presence` sees joins and leaves. Because the will is a *full transaction*, it is also
lock release, "user went offline" events, session handoff — things PubNub
cannot express at all.

Design notes:
- Wills run server-side on socket close (any close: client drop, error,
  server shutdown). They are ordinary txns through `submit` — no new engine
  machinery.
- **Node crash strands wills.** The generic fix is the same one PubNub uses:
  presence rows carry an `expires_at` refreshed by heartbeat (WS ping/pong is
  already there), and a sweeper txn (or simply the watch query:
  `WHERE expires_at > strftime('%s','now')`) hides the stale rows. Document
  the pattern; don't pretend the gap doesn't exist.
- Wills should be replaceable and cancelable per connection.

## Phase 2 — Ephemeral tables (kills: Signals, typing, cursors, occupancy)

SQLite already has the primitive: `CREATE TEMP TABLE`. Temp tables live
outside the main database file, so writes to them **never dirty the object,
never ride a boat, never cost an R2 op** — but they still re-check parked polls,
and poll queries can join them against durable tables. Durable vs ephemeral
becomes a *table property*, not a second API:

```sql
CREATE TEMP TABLE typing (user TEXT PRIMARY KEY, at INTEGER);
-- 60Hz cursor updates, zero storage cost, evaporates on eviction
```

Mechanics: SQLite's update hook reports whether a write hit `main` or `temp` —
exactly the dirty-or-not signal the boat needs. A txn touching only temp
tables commits locally, fires polls, and ships nothing. Semantics to
document loudly: temp state is per-object-activation — eviction, migration,
and restart clear it (failed polls already signal all three).

This one feature subsumes PubNub Signals entirely, and it's what makes
presence/typing *cheap* rather than merely possible.

## Phase 3 — Capability tokens (kills: Access Manager; unlocks the market)

The unglamorous one that matters most commercially. PubNub's customers hand
connections directly to untrusted end-user devices; our single API token
cannot do that. So: HMAC-signed grants, minted by the customer's backend,
verified statelessly by any node with the cluster secret:

```jsonc
{ "grants": [ { "objects": "room-42",        // exact id or prefix glob
                "verbs": ["read", "watch"] },
              { "objects": "user-77-*",
                "verbs": ["read", "write", "watch", "will"] } ],
  "exp": 1760000000, "sub": "user-77" }
```

- Rides the existing subprotocol auth (`fafo-token.<jwt-ish>`), never a URL.
- Enforced at `submit`/poll registration: every participant object must be
  covered by a granted verb.
- Stateless verification (HMAC-SHA256 with the cluster secret; we already
  hand-roll SigV4, this is easier). Revocation = short TTLs, same as PAM.
- The full API token remains the root credential; grants are the tenant
  credential. This is the line between "your backend's database" and "the
  thing your users' browsers connect to" — which is the actual breakfast.

## Phase 4 — Reactive transactions (kills: Functions) — *deliberately last*

A poll whose reply is another transaction instead of a socket frame: "when
this condition holds, run these ops." PubNub Functions, except ACID and in
the same protocol. Ranked last on purpose:

- SQLite triggers already cover the single-object case, inside the txn, today.
- The cross-object case is genuinely useful (materialized rollups, routing,
  moderation queues) but is the one item here with real scope-creep risk: at
  the end of that road you're a reactive-dataflow engine. Build it only after
  0–3 are earning, and keep it to "watch fires a txn," nothing more.

---

## Non-goals (things we refuse to chase)

- **Million-client fanout** — separate design (hibernating-DO relay tier).
- **PoP-count marketing** — Cloudflare's edge is our answer; we route to the
  data, not to the nearest brochure.
- **Mobile push (APNs/FCM)** — a delivery sidecar someone can build on
  polls; not core.
- **Channel wildcards / channel groups** — a poll spanning objects breaks
  object-is-the-shard. "Open N polls on one socket" is the honest answer;
  fan-in views are Phase 4's job if they're anyone's.
- **Their pricing model** — per-message metering is the tell that the pipe is
  the product. We price like a database, because it is one.

## Feature parity scorecard

| PubNub | Here | Phase |
|---|---|---|
| Pub/Sub | long-polled query on a channel object | 0 |
| Message Persistence | it's a table; retention is a `DELETE` | 0 |
| Message filters | the poll query | 0 |
| Message Actions / App Context | rows | 0 |
| Guaranteed delivery | client cursor loop; `durable: true` | 0 |
| Signals | temp tables | 2 |
| Presence | last-will txn + change-detection poll (+ heartbeat expiry) | 1–2 |
| Access Manager | capability tokens | 3 |
| Functions | SQL triggers today; reactive txns later | 4 |
| Atomic publish-with-state-change | outbox txn — **no PubNub equivalent** | shipped |
| SQL over your channel | — **no PubNub equivalent** | shipped |

## Open questions

- Re-run cost on hot objects with many *distinct* parked polls: dedupe by
  (sql, params) is possible; do we also want per-object poll caps?
- Result-size cap on poll replies, and whether caps are grant-scoped.
- Temp-table writes and `read_only`: a txn that only writes temp tables is
  philosophically a write; keep it on the write path (correct serialization)
  but skip the boat.
- Grant format: hand-rolled HMAC envelope vs actual JWT (leaning hand-rolled;
  fewer dependencies, we already do SigV4).

## Sequencing

**Status: Phases 0–3 shipped** (45 Rust tests + live WS/HTTP verification
for each). Notes from the build, where reality improved on the plan:

- Phase 0 (long-poll): as designed; the change-detection `baseline` hash
  covers live views without any diff engine.
- Phase 1 (wills): as designed, plus wills execute under the grants frozen
  at arm time — an expired token still keeps its already-authorized
  promise.
- Phase 2 (temp tables): detection ended up cleaner than the update-hook
  idea — the SQLite header change counter (which delta shipping already
  trusts) says whether a commit touched the main file. Bonus: no-op writes
  ship nothing either.
- Phase 3 (capabilities): verbs became granular — read / insert / update /
  delete / ddl / poll ("write" = shorthand) — because append-to-a-log and
  rewrite-history are different powers. Enforcement is SQLite's authorizer
  at prepare time, so trigger cascades and CTEs can't smuggle verbs.

Phase 4 (reactive transactions) waits until something real demands it.
