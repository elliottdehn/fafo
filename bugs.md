# bugs.md — the ledger of found bugs

Every entry below was found by `dst` — deterministic simulation testing
(`cargo run --bin dst -- run --seed N`): a whole cluster in one thread on
virtual time, with seeded fault injection (crashes, partitions, delayed and
lost RPC, failing storage) and oracles that check conservation of money,
transfer atomicity, and liveness after every phase. A crash IS a repro: the
seed replays it bit for bit.

Baseline before the campaign: **18 of 50 seeds failing.** After: **0 of 50,
three consecutive sweeps**, 137/137 example tests green. All of these
survived the ~100-test example suite; none survived the dice.

Format: what the oracle saw → what was actually wrong → the fix.

---

## The fork family: ownership resurrection

The theme of the campaign. Ownership moves by handoff (take/grant/adopt),
and every bug here is some way for one handoff to produce **two live
writers** — after which both ship boats, histories interleave in the blob
store, and the conservation oracle reports a transfer torn in half.

**1. The forged deed.** The Take handler decided "is this mine to grant?"
from the in-memory routing map — a hint cache. A stale hint let a worker
grant an object it had durably renounced long ago; the remove-side
checkpoint no-oped (the object wasn't in `owned`), so the giver's old claim
survived to be resurrected at the next boot. Fix: grants consult the
durable ownership ledger (checkpoints), never hints.

**2. Hint poisoning.** `NotMine { hint }` replies were written into the
global routing map. A stale hint recorded there convinced a giver mid-adopt
that the object was "already local" — resurrecting ownership it had just
released. Fix: hints steer the requesting chase privately; only real
handshakes (grant, adopt, release) move routing.

**3. The self-lie.** A routing exception claiming *we* own an object the
durable ledger says we don't: serving it is bug 1, chasing it is an
AlreadyLocal livelock. Fix: `advance()` drops the lie on sight.

**4. Renounce-then-fail.** `checkpoint()` logged write failures and carried
on — so a transfer could "durably renounce" without durability. A restart
resurrected the stale claim: two writers, forked object (seed 5). Fix: a
release that cannot write its remove-side checkpoint **aborts the
transfer**; the takers get a retryable refusal and the object stays put.
Corollary: takes are refused during storage outages, by design.

**5. The invisible gap.** Between the giver's renounce and the receiver's
add-side checkpoint, the object looked durably unclaimed — and the
hash-default fallback would manufacture a second owner. Fix: **transit
markers** — the giver's checkpoint durably records `object -> destination`,
so mid-handoff is a visible state.

**6. The twice-spent deed** — the boss bug, and the reason generations
exist. One renunciation (a transit marker) could be consumed twice: once by
the deed traveling back to the taker, once by the Take handler's
Transit(me) healing reading the same marker. The second admit landed after
the first had already granted the object onward — resurrection, fork, torn
transfers across 15 of 50 seeds. Late Adopts did the same dance through a
different door. Two rounds of guards failed before the structural fix:

> **Per-object handoff generations.** Every release mints `gen+1` into the
> deed and the giver's transit marker; checkpoints store owned/transit with
> generations; `admit` refuses any deed not strictly newer than what the
> worker already knows; boot dual-claims and the durable-claim scan pick
> the highest generation. Staleness everywhere becomes an integer compare.
> One renunciation, one spend.

**7. Arbitrary dual-claim resolution.** Boot found an object in two
checkpoints and kept... the lower worker id. Deterministic, and wrong half
the time — resurrecting the stale claim with full confidence. Fix: highest
generation wins (it is definitionally the later handoff).

**8. Blind reclaim on failed returns.** A hysteresis return whose RPC
failed was reclaimed at the giver — but "failed" includes "the ACK was
lost": the home may have adopted, and reclaiming forked. (The reclaim was
itself the fix for an earlier livelock, which is how these go.) Fix under
generations: no blind reclaim; the object rests in transit and heals at the
home on next access. Correctness never leaves; liveness resumes with the
next touch.

**9. Upgrade amnesia.** The generation change altered the checkpoint wire
format — and old-format checkpoints (bare `owned` arrays, which production
R2 holds right now) silently failed to parse, i.e. every pre-upgrade claim
would evaporate at boot. Fix: a wire shim deserializes legacy checkpoints
at generation 1, outbid by any post-upgrade handoff.

## The durability pipeline

**10. Partial revert tearing.** When a boat sank, only the boat's objects
reverted to durable state — but a cross-object txn applied *while the boat
was in flight* could straddle a reverted object and a healthy one. Half the
txn un-happened; the other half shipped on the next boat. Fix:
`revert_closure` — reverts take the transitive txn-connected blast radius,
and dropped pessimistic waiters are failed loudly.

**11. COMMIT can fail too.** A deferred constraint fails at COMMIT time,
after earlier participants already committed. The old handler purged *all*
participants — destroying unrelated acked writes on some, leaving torn
state on others. Fix: ROLLBACK the still-open participants (erasing exactly
this txn), revert-closure the already-committed prefix.

**12. Death by purged participant.** A boat sinking between a txn's
acquisition and its execution purged an object out from under it;
`.expect("participant activated")` then killed the whole worker task, and
every later submission to it hung forever (seed 1's liveness fire). Fix:
detect and fail retryably.

**13. Committed but unpromoted.** The commit record landed; the final
object PUT didn't. Boot recovery would roll it forward eventually — but a
live take activates the stale base *now*. Fix: the worker self-heals
immediately (drop the manifest, re-dirty, sail a repair boat), and —

**14. Activation-time roll-forward.** Before any cold activation, committed
records touching the object are rolled forward, whole-record (atomicity),
so a taker can never activate state older than a durable commit. The
conservation oracle had caught this as money off by exactly one unit.

**15. Promotion going backwards.** A stale commit record retried its
promotion after newer state had shipped, rolling the base snapshot
backwards. Fix: promotion is monotone in the SQLite change counter.

## Liveness

**16. The amnesiac owner.** A fault-skipped boot read left a worker
ignorant of its own durable claims. Every routing map pointed at the hash
home; the home's ledger check correctly answered "w0 owns it" — to w0 —
whose chase logic *discarded hints naming itself* (they'd once caused a
livelock, bug 2's cousin). The one correction that could ever arrive was
being thrown away, forever (seed 34). Fix: a self-naming hint triggers a
ledger check; if the ledger confirms us, the worker heals its own memory.

**17. The tenure wipe.** The Take handler's home-adoption (formalizing
implicit hash-default ownership) stamped `arrived_at = now` — so every
object's first grant read as "unsettled," no displacement ever bounced
home, and the entire hysteresis mechanism was quietly dead. Only a
placement test noticed; every correctness oracle was happy. Fix:
formalizing what you already owned is not an arrival; genesis tenure.

**18. The eternal checkpoint.** `admit` retried its add-side checkpoint
forever, wedging the whole worker behind a storage outage. Under
generations the add side is safely best-effort — the giver's transit marker
already names us as the only valid owner — so: try once, log, move on.

**19. The clobbered queue.** `on_taken` installed a fresh one-entry queue
for the arrived object unconditionally — flattening any queue that already
existed and dropping its parked txns and takes on the floor (seed 32's
queue-pop assertion). Fix: only when no queue exists.

## Bugs in the mirror (the harness finding itself)

**20. The wall clock in a virtual world.** The fencing gate double-stamps
monotonic + wall time; the wall check exists to catch real system suspends.
Inside the simulator, virtual hours pass in wall milliseconds — unless the
host is busy, when a run's wall time could exceed FENCE_TTL and fence a
perfectly healthy node. Symptom: exactly one failure per parallel sweep, a
different seed each time, none reproducible alone. Fix: `wall_fence:
false` under simulation; unchanged in production.

**21. Entropy leaks** (fixed while building the harness, listed for
completeness): std HashMap's per-process seed randomized iteration order —
and therefore boat composition — across runs; a UUID in the ship path; raw
`std::time::Instant` in the ship gate. Determinism is the simulator's first
product; every one of these had to die before seed-replay meant anything.

---

## Lessons, paid for

- **Guards lose to structure.** The twice-spent deed was "fixed" twice
  with increasingly clever staleness guards; the first broke all 50 seeds,
  the second stranded objects behind stale markers that were never
  cleaned. Versioning the resource (generations) deleted both guards and
  the bug. If arbitration needs a heuristic, the state model is missing a
  number.
- **Every fix ships with a new failure mode.** The reclaim that fixed the
  orphan livelock caused lost-ack forks. The hint-discard that fixed the
  chase livelock caused the amnesiac strand. The checkpoint retry that
  fixed silent renounce wedged workers. The simulator caught each
  successor because it doesn't care that the code is newer now.
- **Correctness oracles miss liveness-of-purpose.** The tenure wipe (17)
  broke no invariant — money conserved, nothing torn — it just made the
  placement learner permanently inert. Some things only a behavioral test
  (or a benchmark regression) will ever catch.
- **The oracle beats the example suite ~19-0.** All of these passed 100+
  hand-written tests. The examples check the paths we imagined; the dice
  check the paths we didn't.
- **One seed = one repro** is the entire debugging experience. Every entry
  above went: failing seed → `FAFO_DST_LOG=1` replay → read the object's
  lifecycle → the bug names itself. No bug took longer to *find* than to
  fix.
