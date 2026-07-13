# bugs.md — the ledger of found bugs

Every *numbered* entry below was found by `dst` — deterministic simulation
testing (`cargo run --bin dst -- run --seed N`): a whole cluster in one
thread on virtual time, with seeded fault injection (crashes, partitions,
delayed and lost RPC, failing storage) and oracles that check conservation
of money, transfer atomicity, and liveness after every phase. A crash IS a
repro: the seed replays it bit for bit. (The lettered T-series near the end
is a separate lineage: bugs the plain example suite caught before the dice
campaign began — kept here because a ledger of found bugs should own the
ones that didn't need the fancy tool.)

Baseline before the campaign: **18 of 50 seeds failing.** After: **0 of 50,
three consecutive sweeps**, 140/140 example tests green — including with the
will oracle live (bug 22), the specific bug the harness was built to catch.
The endless miner (`dst mine`) then found bug 23 in ~1,500 fresh seeds, and
bug 24 the moment a **multi-workload** battery (ERC20 supply, escrow
settlement races, idempotent counters, three-way watched feeds — all racing
at once) cranked cross-object contention past what plain transfers reach.
Two more faults — per-node clock skew and a **pause adversary** (a live node
loses its lease to a takeover and rejoins) — then flushed out the fencing
trio, bugs 25–27. All of these survived the example suite; none survived the
dice.

Bugs 25–27 also exposed a limit the single-key fencing primitives could only
approximate, documented below as *"the bug that isn't fixed yet."* That
prediction was then paid off: a **log-structured commit** (behind
`FAFO_LOG_PRIMARY`) turned the pause adversary from a documented boundary into
a solvable problem, and mining under it — `dst mine --fuzz --pause`, every
crash a bug — drove the whole system from ~13% of seeds crashing to **zero in
both modes.** That campaign is bugs **28–50**, in its own section after the
"not fixed yet" note (kept intact, because the honest record of the boundary
is the reason the fix exists).

Format: what the oracle saw → what was actually wrong → the fix — plus an
ELI5 and a nastiness score (severity × subtlety × blast radius; 10 means
"silently forks customer data and no test on earth was going to catch it").

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

> ELI5: The doorman was handing out apartment keys based on an old sticky note instead of checking the deed registry. **Nastiness: 8/10.**

**2. Hint poisoning.** `NotMine { hint }` replies were written into the
global routing map. A stale hint recorded there convinced a giver mid-adopt
that the object was "already local" — resurrecting ownership it had just
released. Fix: hints steer the requesting chase privately; only real
handshakes (grant, adopt, release) move routing.

> ELI5: Someone wrote overheard gossip straight into the official phone book, and everyone dialed it. **Nastiness: 7/10.**

**3. The self-lie.** A routing exception claiming *we* own an object the
durable ledger says we don't: serving it is bug 1, chasing it is an
AlreadyLocal livelock. Fix: `advance()` drops the lie on sight.

> ELI5: Your own address book says you live in a house you sold; acting on it either squats the house or mails letters to yourself forever. **Nastiness: 5/10.**

**4. Renounce-then-fail.** `checkpoint()` logged write failures and carried
on — so a transfer could "durably renounce" without durability. A restart
resurrected the stale claim: two writers, forked object (seed 5). Fix: a
release that cannot write its remove-side checkpoint **aborts the
transfer**; the takers get a retryable refusal and the object stays put.
Corollary: takes are refused during storage outages, by design.

> ELI5: You "sold" your car but the DMV paperwork bounced — and months later both of you were driving it. **Nastiness: 8/10.**

**5. The invisible gap.** Between the giver's renounce and the receiver's
add-side checkpoint, the object looked durably unclaimed — and the
hash-default fallback would manufacture a second owner. Fix: **transit
markers** — the giver's checkpoint durably records `object -> destination`,
so mid-handoff is a visible state.

> ELI5: While a package is between houses, nobody officially owns it — and the city, seeing an ownerless address, helpfully builds a second package. **Nastiness: 7/10.**

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

> ELI5: One "I'm giving up the house" note got photocopied into two keys, and both keyholders redecorated. Generations put a serial number on the note. **Nastiness: 10/10.**

**7. Arbitrary dual-claim resolution.** Boot found an object in two
checkpoints and kept... the lower worker id. Deterministic, and wrong half
the time — resurrecting the stale claim with full confidence. Fix: highest
generation wins (it is definitionally the later handoff).

> ELI5: Two people claim one house; the judge rules for whoever's name sorts first alphabetically. Confidently. Every time. **Nastiness: 6/10.**

**8. Blind reclaim on failed returns.** A hysteresis return whose RPC
failed was reclaimed at the giver — but "failed" includes "the ACK was
lost": the home may have adopted, and reclaiming forked. (The reclaim was
itself the fix for an earlier livelock, which is how these go.) Fix under
generations: no blind reclaim; the object rests in transit and heals at the
home on next access. Correctness never leaves; liveness resumes with the
next touch.

> ELI5: Your thank-you card didn't arrive, so you took the gift back. The card had arrived. Now there are two gifts and one is counterfeit. **Nastiness: 8/10.**

**9. Upgrade amnesia.** The generation change altered the checkpoint wire
format — and old-format checkpoints (bare `owned` arrays, which production
R2 holds right now) silently failed to parse, i.e. every pre-upgrade claim
would evaporate at boot. Fix: a wire shim deserializes legacy checkpoints
at generation 1, outbid by any post-upgrade handoff.

> ELI5: The office's new filing system couldn't read old folders — so it concluded those customers never existed. This one was headed for production. **Nastiness: 9/10.**

## The durability pipeline

**10. Partial revert tearing.** When a boat sank, only the boat's objects
reverted to durable state — but a cross-object txn applied *while the boat
was in flight* could straddle a reverted object and a healthy one. Half the
txn un-happened; the other half shipped on the next boat. Fix:
`revert_closure` — reverts take the transitive txn-connected blast radius,
and dropped pessimistic waiters are failed loudly.

> ELI5: The bank undid your transfer on one side only: the money left your account AND never arrived — or arrived AND never left. An eraser that only erases half a line. **Nastiness: 9/10.**

**11. COMMIT can fail too.** A deferred constraint fails at COMMIT time,
after earlier participants already committed. The old handler purged *all*
participants — destroying unrelated acked writes on some, leaving torn
state on others. Fix: ROLLBACK the still-open participants (erasing exactly
this txn), revert-closure the already-committed prefix.

> ELI5: On signing day the third signer's pen exploded, so the office shredded everyone's entire folders — including documents from other deals. **Nastiness: 7/10.**

**12. Death by purged participant.** A boat sinking between a txn's
acquisition and its execution purged an object out from under it;
`.expect("participant activated")` then killed the whole worker task, and
every later submission to it hung forever (seed 1's liveness fire). Fix:
detect and fail retryably.

> ELI5: A cashier found their register missing and fainted — and the whole store stopped serving everyone, forever, over one fainted cashier. **Nastiness: 8/10.**

**13. Committed but unpromoted.** The commit record landed; the final
object PUT didn't. Boot recovery would roll it forward eventually — but a
live take activates the stale base *now*. Fix: the worker self-heals
immediately (drop the manifest, re-dirty, sail a repair boat), and —

> ELI5: The receipt printed but the shelf never got restocked; the sale is official and the shelf is lying about it. **Nastiness: 6/10.**

**14. Activation-time roll-forward.** Before any cold activation, committed
records touching the object are rolled forward, whole-record (atomicity),
so a taker can never activate state older than a durable commit. The
conservation oracle had caught this as money off by exactly one unit.

> ELI5: You moved into a house while the courthouse's approved-but-unfiled renovation order sat in its inbox — so you furnished last year's floor plan. **Nastiness: 8/10.**

**15. Promotion going backwards.** A stale commit record retried its
promotion after newer state had shipped, rolling the base snapshot
backwards. Fix: promotion is monotone in the SQLite change counter.

> ELI5: An old sticky note got re-applied on top of a newer one, and everyone trusted the top of the stack. **Nastiness: 6/10.**

## Liveness

**16. The amnesiac owner.** A fault-skipped boot read left a worker
ignorant of its own durable claims. Every routing map pointed at the hash
home; the home's ledger check correctly answered "w0 owns it" — to w0 —
whose chase logic *discarded hints naming itself* (they'd once caused a
livelock, bug 2's cousin). The one correction that could ever arrive was
being thrown away, forever (seed 34). Fix: a self-naming hint triggers a
ledger check; if the ledger confirms us, the worker heals its own memory.

> ELI5: The city clerk keeps mailing you "this house is yours" and you keep replying "impossible, ask him" — about yourself — and shredding the letter. **Nastiness: 7/10.**

**17. The tenure wipe.** The Take handler's home-adoption (formalizing
implicit hash-default ownership) stamped `arrived_at = now` — so every
object's first grant read as "unsettled," no displacement ever bounced
home, and the entire hysteresis mechanism was quietly dead. Only a
placement test noticed; every correctness oracle was happy. Fix:
formalizing what you already owned is not an arrival; genesis tenure.

> ELI5: The residency clock deciding "does this object live here?" got reset every time somebody knocked on the door — so nothing ever qualified as home, and the whole learn-your-placement feature quietly played dead. **Nastiness: 7/10.**

**18. The eternal checkpoint.** `admit` retried its add-side checkpoint
forever, wedging the whole worker behind a storage outage. Under
generations the add side is safely best-effort — the giver's transit marker
already names us as the only valid owner — so: try once, log, move on.

> ELI5: The clerk refused to serve anyone until one form was filed — and the filing office was closed indefinitely. The line was the entire town. **Nastiness: 7/10.**

**19. The clobbered queue.** `on_taken` installed a fresh one-entry queue
for the arrived object unconditionally — flattening any queue that already
existed and dropping its parked txns and takes on the floor (seed 32's
queue-pop assertion). Fix: only when no queue exists.

> ELI5: Installing the new customer's ticket dispenser by throwing away the existing line of people. **Nastiness: 6/10.**

## Bugs in the mirror (the harness finding itself)

**20. The wall clock in a virtual world.** The fencing gate double-stamps
monotonic + wall time; the wall check exists to catch real system suspends.
Inside the simulator, virtual hours pass in wall milliseconds — unless the
host is busy, when a run's wall time could exceed FENCE_TTL and fence a
perfectly healthy node. Symptom: exactly one failure per parallel sweep, a
different seed each time, none reproducible alone. Fix: `wall_fence:
false` under simulation; unchanged in production.

> ELI5: On a film set where a year passes per minute, the security guard fired the actors for "being late" — by checking his real wristwatch. Once per sweep, different actor each time, never on the replay. **Nastiness: 8/10.**

**21. Entropy leaks** (fixed while building the harness, listed for
completeness): std HashMap's per-process seed randomized iteration order —
and therefore boat composition — across runs; a UUID in the ship path; raw
`std::time::Instant` in the ship gate. Determinism is the simulator's first
product; every one of these had to die before seed-replay meant anything.

> ELI5: You can't study the game tape if the dice secretly roll differently every time you rewatch it. **Nastiness: 5/10.**

## The bug the harness was built for

**22. The will that died with its node.** A last-will is a transaction a
client arms to run when its connection dies — release my locks, mark me
offline, delete my presence row. fafo ran it from the node holding the
socket, on socket close. That is correct when the socket closes and the
node lives. It is silently wrong when the NODE dies: the socket is just as
dead, the client is just as gone, but the promise was process memory and
the process is ash. The will oracle names it exactly — *"connection for X
is dead (node crashed) but its will never fired"* — reproducible at any
seed with a crash-doomed will via `dst run --no-durable-wills`.

> **Durable wills.** Arming now also writes the will (ops + frozen grants)
> into a `_wills` system object with a `deadline`. While the connection
> lives, its node pushes the deadline forward (the refresher). When the
> node dies, the refreshing stops, the deadline lapses, and any surviving
> node's sweeper claims the lapsed will and fires it — under the frozen
> capability, so the authorizer still gates every action. A clean close
> deletes the durable copy so it never double-fires; the claim keeps the
> common case single-fire; and because a sweeper can die mid-fire and
> another reclaim, firing is at-least-once and the will ops must be
> idempotent — the same contract the HTTP path already documents.

The deadline clock is the one seam production and simulation genuinely
differ on: wall time across nodes in production (the fencing model's
bounded-clock-rate assumption already relies on it), a shared virtual
instant under the simulator so deadlines stay comparable AND the run
replays bit-for-bit. Default now: **0 of 50 seeds fail with the will
oracle live**, and the fix is proven both directions — persistence off
reproduces the original loss, persistence on satisfies the oracle.

> ELI5: You left a note with the doorman — "if I don't come back, water my plants." Then the whole building burned down, doorman and note with it, and your plants died. Now the note is filed at city hall with a timer; when your check-ins stop, the next building over waters the plants. **Nastiness: 9/10.**

---

**23. The orphaned queue** *(found by `dst mine`: 3 crashes in ~1,500
endless seeds, one root cause — living inside fix #10).* Takes wait at an
object's queue head while its state is unshipped, and queues get
re-serviced when the boat lands. But `revert_closure` can make an object
clean by *reverting* — beyond the sinking boat's own object list — and
nothing re-serviced those queues: the take slept forever, with a growing
line of transactions starving behind it. Fix: reverts flag their objects'
queues and `pump()` — the funnel every mutation path exits through —
drains the flags. "Every fix ships with a new failure mode," shipping
inside the fix that taught it.

> ELI5: The "now serving" bell rings when a shipment arrives — but when a shipment gets cancelled the counter also frees up, and nobody ever rang the bell. **Nastiness: 7/10.**

## Config-fuzzing: the same system, ten thousand shapes

Seed-fuzzing explores *schedules* on one cluster shape. Config-fuzzing
(`dst mine --fuzz`) derives the whole shape from the seed too — node count,
latencies, fault rates, fence TTL, worker-space size, workload sizes — so a
crash is still `(seed)` and replayable, but the search now covers the
parameter space, not just one point in it. It found bug 24 on the first
minute.

**24. Routing and truth, diverged.** A worker holds two views of who owns
an object: `owned` (its private, durable-backed truth) and the shared
routing map (a *hint*). An earlier fix taught `advance()` to distrust the
hint when it lied *in our favor* ("routing says we own it, but we never
admitted it"). It never handled the mirror: routing naming *someone else*
for an object we actually own. Then our own transactions take-chase the
wrong peer, who bounces us with "NotMine, try yourself" — and the self-heal
Adopt is at the generation we already hold, so `admit` correctly refuses it
as stale, so nothing ever corrects the map. The take loops forever; every
transaction behind it starves. Cross-object contention from the
multi-workload battery surfaced it instantly (seed 8614294886503471524).
Fix: `owned` is authoritative — if the object is in it, we ARE the owner,
and `advance()` heals routing toward that truth, both directions.

> ELI5: The county says you own the house and you agree — but your own GPS insists it's across town, so you keep driving to a stranger's door, who sends you home, where your GPS sends you back. Forever. **Nastiness: 8/10.**

## The pause adversary: fencing under takeover

Three forks the DST only reached once it grew two new faults: **per-node
clock-rate skew** (each node's clock ticks at a slightly different rate) and
the **pause adversary** — a live node is isolated from its peers while a
rebooted peer takes over its lease, then it rejoins and tries to keep
writing. That is the textbook zombie-writer scenario fencing exists for, and
it flushed out three separate ways a refused write survived anyway. Together
they cut pause-fault safety forks from ~33% of seeds to under 1%.

**25. The fenced write that committed anyway.** The commit gate verified the
lease only when its own recency stamp had already gone stale (a
`lease_stale &&` fast-path). But a node can be superseded while its stamp is
perfectly fresh: it keeps refreshing its lease through the *store* even while
RPC-partitioned from everyone, so "my stamp is recent" and "I still hold the
lease" are not the same claim. A boat whose block had already been taken over
sailed straight through the fast path and wrote its commit record on top of
the new owner's — two writers, one object. Fix: verify the lease
**unconditionally** at every commit point. One `_lease/` list per boat is the
price of storage-verified fencing, and it is not optional.

> ELI5: The bouncer only re-checked your wristband if it *looked* faded. But a wristband can be revoked while it still looks new — so people who'd already been kicked out walked right back in. Now everyone gets re-checked at the door, every time. **Nastiness: 9/10 (silent cross-node fork).**

**26. Resurrection from the commuter cache.** Activation has a fast path: if a
local database file survives from a previous stay, skip the download and just
apply the deltas newer than it. But a fenced writer's *refused* write also
lives in that local file — at a change counter **ahead of** anything durable.
On the next activation the fast path trusted the cache, saw its high counter,
and resurrected the write the commit gate had just correctly refused. Fix:
trust the cache only within `[base, max_durable]` — a counter beyond every
durable delta is an un-committed local scribble, not a shortcut. Discard it
and download the base.

> ELI5: Your laptop had a newer draft than the server, so it "helpfully" kept yours — except that draft was the exact edit the server had rejected. Trusting local-is-newer un-rejected it. Now "newer than the server" only counts if the server has actually seen it. **Nastiness: 8/10.**

**27. Shipping an object you no longer own.** A worker that had gone dirty on
an object, then handed that object off in a migration, would still carry it
into its next boat and ship it — a fork minted at the source, two workers
writing one object's history. The fork tripwire caught it downstream, but the
write had already sailed. Fix: before launching a boat, `maybe_launch` reverts
any dirty object the worker no longer owns (and the whole txn-closure around
it) instead of shipping it. Never ship what you don't own.

> ELI5: You packed a box that belongs to the new tenant and almost mailed it under your name. Now the check happens before the truck leaves: anything that isn't yours comes back off the truck. **Nastiness: 8/10.**

## The bug that isn't fixed yet (honesty)

Config-fuzzing also found that a node which **permanently dies with no
restart** strands its lease blocks: nothing adopts them, and its objects go
unreadable forever. ~14% of *unconstrained* configs hit it.

This one is documented, not fixed — deliberately. It sits at the edge of
the operating envelope: the deployment target (Cloudflare Containers)
always reschedules a crashed instance, so "fewer nodes, forever" is not a
state production reaches. To close it you would let a *peer* speculatively
reclaim a presumed-dead node's blocks — and **nine** attempts at that
reclaim have now been reverted for making things worse, the exact "every fix
ships a new failure mode" trap. The lineage, shortest to deepest:

1. Reclaim on an RPC **health check** forked immediately: health-check rides
   RPC, so a live-but-*partitioned* holder reads as dead, gets its block
   stolen, and both write.
2. Reclaim on **lease deadlines** (a holder refreshes through the store;
   reclaim only a lapsed lease) fixed the partition race — a partitioned
   holder still refreshes — but still forked, because a block takeover does
   not reconcile with per-*object* ownership.
3. A **generation-keyed** durable layout (each owner writes to its own
   generation's key, so a superseded writer is invisible) regressed 7×: it
   is fundamentally incompatible with page-delta shipping, because a delta is
   relative to a base and cannot cross a generation boundary.
4. **Compare-and-swap on the base** (the store's new `put_cas` / `version`,
   which model R2's `If-Match`) fenced single writers correctly but proved
   **net-neutral**: single-key CAS cannot make a *cross-object* transfer
   atomic — once one half's base is durably promoted you cannot un-promote
   it, so fencing the other half just *relocates* the tear.
5. A **commit-time claim** (`create`-if-absent on a per-object-version key,
   the right idea for fencing the fork *before* any write) regressed too,
   because the fence has to cover **every** promotion path — the recovery
   roll-forward still promoted with a plain overwrite and clobbered anyway.

The definitive root, paid for in full: safe speculative reclaim needs
**cross-object atomic commit under a fork**, which single-key primitives
can only approximate. The genuine fix is a log-structured commit — every
promotion path (ship, recovery, compaction) funnelled through one fenced,
ordered, idempotent apply, so a committed record lands exactly once and no
path can clobber. That is a protocol, not a patch.

What the campaign *did* settle is where the line actually is. Holding every
realistic fault on — crashes, restarts, clock skew, a reliable store — and
turning the speculative-reclaim pause **off**, the system is **974 / 974
seeds clean, zero safety and zero liveness violations**. The entire residual
(~0.6%) lives in that one adversary: a peer stealing a partitioned-but-live
node's blocks — a takeover the platform never asks for, since it reschedules
genuinely-dead instances instead. So the honest status is not "broken": it
is a system provably clean in the model it runs in, whose fuzzer is
stress-testing a deliberately-unbuilt feature. The disciplined call remains
to keep the bounded availability limit rather than ship the data corruption
that "fixing" it keeps introducing — and to reach for the log-structured
commit only if speculative reclaim ever becomes a feature worth its cost.

> ELI5: If a band member is stranded off-stage and never replaced, their songs stop — but our roadies keep trying to have someone *else* grab that member's guitar mid-song, and it keeps ending with two people playing the same part out of sync, which is worse. We've now tried nine kinds of roadie. So we wrote down "wait for the venue to send a real replacement" (which it always does) and proved the band is flawless when nobody freelances the swap. **Nastiness: 5/10 (bounded availability, no data risk in the supported model).**

## The log-structured commit: the genuine fix — mined to zero

The section above ended with a prediction: the only thing that safely closes
speculative reclaim is a **log-structured commit** — every promotion path
(ship, recovery, compaction) funnelled through one fenced, ordered, idempotent
apply, so a committed record lands exactly once and no path can clobber. That
protocol got built (behind `FAFO_LOG_PRIMARY`, de-risked first in a standalone
model — `src/bin/protosim.rs` — that held conservation and ack-consistency over
20k seeds of permanent fork + crash where a naive base-overwrite control tears
99% of the time). Durable state became a per-object append-only log of
committed snapshots: `objects/<id>.L.<seq>` entries, a compacted base
`objects/<id>.B.<seq>`, and a single create-if-absent outcome key
`txns/<id>.O` as the atomic commit switch. Activation *folds the committed
prefix*; the fold is the truth.

That unlocked the adversary the old model could only document. With clobbering
replaced by fenced serialization, `dst mine --fuzz --pause` — **two forced
pause faults, per-node clock skew, and up to ~53% failing-storage, every crash
a bug** — became an honest bug-finder. It went from ~13% of seeds crashing to
**zero, across both `--fuzz` and `--pause`, thousands of seeds each.** The
bugs it took to get there fall in five acts. (Two of them, 28 and 50, are
"bugs in the mirror" — the mine lying to itself — and are the reason the rest
could be trusted.)

### Act I — sharpening the mine (a crash must *mean* a bug)

**28. The deadline that hid the crimes.** The liveness oracle killed a run on
an **absolute** 600 s-virtual deadline. But a fork *serialized safely* by the
log is legitimately slow (many `acked=false` retries), and those slow-but-fine
seeds hit the deadline **before the pre-shutdown safety oracles ran** — so the
harness was crashing on liveness and never checking conservation. An early
"safety 0" reading was measured through that blind spot. Fix: a
**progress-relative** deadline (no cap while producers make progress; a bounded
drain after) plus a per-seed wall-clock timeout so a true hang is still caught.
The moment it landed, real forks the deadline had been masking became visible.

> ELI5: The exam had a time limit, and slow-but-correct students got thrown out before their papers were graded — so we "never saw a wrong answer." We stopped the clock while a pencil is still moving. Suddenly we could see the mistakes. **Nastiness: 7/10 (a test bug that manufactures false confidence — the worst kind).**

**29. The will that fired but couldn't be seen.** A subscriber oracle demanded
`want == channel_msgs` — every publish ACKed. But a pause-fault **lost ack**
(the write committed durably; the client only saw the error) leaves
`want < channel_msgs` forever even though every message is present. Fix: drain
to the durable tail and accept give-ups, exactly as the transfer oracles do.

> ELI5: You mailed all the letters, but one delivery receipt got lost, so the ledger insisted a letter was missing that had actually arrived. Count what's in the mailbox, not the receipts. **Nastiness: 6/10 (false liveness failure).**

**30. The subscriber that read ghosts.** A non-durable poll saw rows from a
transaction that then **rolled back**, and SQLite **reused the autoincrement
id** the ghost had taken — so the next real message got an id the subscriber
had already marked seen, and was skipped. Fix: subscribers poll **durable**
(committed) state; exactly-once delivery cannot be built on reads that can
un-happen.

> ELI5: You crossed a name off the guest list because you saw them walk in — but they turned around and left, and the next guest got handed the same numbered badge, so you thought they'd already come. Only cross off people who actually stayed. **Nastiness: 7/10 (silently dropped a committed message).**

### Act II — integrating a log that *serializes* forks instead of clobbering

**31. The live file that lied about the log.** The deepest integration bug: a
worker's live SQLite file can drift from the committed log. Under a fork it
would ship a full snapshot at `seq S+1` built from a live file that had **never
seen a rival's committed `seq S`** — silently overwriting it at a fresh seq.
(`protosim` never hit this: its workers re-read committed state on every write;
real SQLite carries a warm file.) Fix: **rebase-on-divergence** — the worker
tracks `log_seq` (the committed seq its live file reflects), a ship carries its
`expected_seq`, and the prewrite aborts when the durable `committed_seq` has
moved past it, reverting and re-folding off the fresh prefix. Cut the clobber
from 6 seeds to 1.

> ELI5: You typed your edit onto a printout, not the shared doc — and someone else's saved change never made it onto your paper, so when you finally uploaded, you erased them. Now you re-download before saving, and if the shared copy moved, you retype on top of *theirs*. **Nastiness: 9/10 (silent cross-node fork).**

**32. The poll served from a stale desk.** A feed watcher's poll ran on a
worker whose live conn was behind the log — a *peer* had committed with no
local write to trigger the write-only rebase, so the poll never fired though
every write was durable. Fix: read-side rebase — re-fold the committed prefix
**in place** for a clean object when a durable poll won't fire, plus a periodic
recheck of parked polls for objects a peer advanced. (A first, revert-based
attempt regressed safety through churn; folding in place does not.)

> ELI5: The clerk kept answering "no mail yet" from yesterday's pile because nobody told them a new bag had arrived. Now they glance at the current bag before answering. **Nastiness: 6/10 (feed liveness).**

**33. The leapfrog that never converged.** Two nodes ping-pong committed seqs
on the same hot accounts (rapid migration under the pause fork). The log makes
them serialize *safely* — no clobber — but ~2× slower, and ownership never
settles, so the transfer phase can outrun the feed watchers' deadline. Fix:
**relinquish on rebase** — a worker that loses a prewrite `durable_claim`s the
object and, if a ≥-generation peer owns it, drops it and routes future txns to
the winner. Rebases per seed: 191 → 2.

> ELI5: Two people kept grabbing the same doorknob and letting go so the other could try, forever. Now the one who loses the grab just steps aside and lets the winner through. **Nastiness: 6/10 (liveness under contention).**

### Act III — boot, orphans, and redistribution

**34. The orphan dead-end.** After crash + reboot, a block's durable lease
still named the node's **own address** (a crash skips the graceful release),
but the fresh incarnation holds nothing — so peers saw the holder "alive" and
never reclaimed, and the node didn't host it either. Every take there
dead-ended `NotMine{None}`; the object was unreadable for the rest of the run.
The fix was the *opposite* of reclaiming (six reclaim variants all forked):
**boot with nothing** — on boot, release every unreleased lease still naming
us. They become claimable and redistribute through the proven-safe claim path;
we never resurrect stale ownership.

> ELI5: A shop's sign still said "OPEN, run by Alice" after Alice quit and a new Alice arrived with no keys — so customers waited at a locked door forever. New Alice now takes the old sign down on day one, and the block gets properly re-let. **Nastiness: 7/10 (permanent orphan; objects unreadable).**

**35. The reused outcome key.** The commit switch is keyed by `staging_id =
node-tag + ship-seq`, and the tag was `addr-hash + boot-epoch` — which
**repeated across reboots**. Two genuinely different transactions from two
incarnations could hash to the same outcome key: one's commit flipped the
other's entries to "committed," tearing it. Fix: a unique **boot nonce** minted
from the durable store's `version()` of a per-node boot key.

> ELI5: Two different contracts got filed under the same case number because the numbering reset when the office rebooted — so signing one "approved" the other. Now every boot draws a fresh, never-repeated number. **Nastiness: 9/10 (silent torn transaction).**

**36. The quota-starved orphan.** Boot-claim caps how many blocks a node grabs
(load balancing). But that quota was capping **orphaned** blocks too — blocks
with a dead holder's lease that *must* be absorbed for their objects to be
reachable. A block neither survivor's quota reached stayed orphaned, and a
multi-object txn gathering an account there hung forever. Fix: orphaned
(existing-lease) blocks **bypass** the quota; only the fresh pool is balanced.

> ELI5: Movers had a "max 10 boxes each" rule and applied it even to boxes left behind by the tenant who moved out — so some boxes sat in the empty apartment forever. The rule now only limits *new* boxes; abandoned ones always get carried out. **Nastiness: 7/10 (gather deadlock).**

**37. The fence-free reclaim.** Reclaiming a **released** lease skipped the
fencing-TTL wait, on the theory that a graceful release means the predecessor
is done. But boot-release (34) tombstones a *crashed* predecessor, whose
in-flight boat may still be settling — reclaiming it fence-free raced that
straggler into a two-writer fork. Fix: wait out the fence TTL before the first
write on **every** reclaim, released or not.

> ELI5: You assumed a "checked-out" hotel room was empty and walked in — but the last guest left in a hurry and a bag was still on its way down. Now you wait the full checkout window before touching anything, even for rooms marked vacant. **Nastiness: 8/10 (cross-writer fork).**

### Act IV — compaction and the fold

**38. The superseded compactor.** Compaction rewrites an object's log into a
fresh base. A worker that had *already been fenced* could still run a
compaction and rewrite the log out from under the true owner. Fix: gate
compaction behind the same lease verification as a commit — a superseded owner
compacts nothing.

> ELI5: A fired editor still had the keys and kept "tidying" the master manuscript. Now tidying requires a badge that gets revoked when you're let go. **Nastiness: 8/10.**

**39. The base-vs-log read race.** `fold_committed` / `committed_seq` read the
base, then listed the log entries above it. A compaction landing **between**
those two reads shows a stale (low) base with entries starting *above* it — a
false gap — so the fold stops early and returns too low a seq. A node that
trusts it prewrites at an already-subsumed seq, and its write is silently
ignored by every later fold: a two-writer fork, invisible until the deadline
fix (28) let runs finish. Fix: re-read the base *after* folding; if it moved
past where we stopped, our view straddled a compaction — fold again.

> ELI5: You checked the table of contents, then the chapters, but the book was re-indexed in between — so you concluded chapter 5 was missing and wrote a *new* chapter 5 on top of the real one. Now you re-check the contents after reading; if it changed, you start over. **Nastiness: 9/10 (silent dropped write).**

**40. The destructive trim.** Compaction originally deleted the log tail it had
folded into the base. A concurrent fold that straddled the delete lost the
committed writes in the deleted range. Fix: compaction is **additive** — it
publishes a new immutable base with create-if-absent and **never trims**.
Nothing committed can vanish; the base only *shortcuts* the fold. (Reclaiming
the now-redundant tail is a separate at-rest GC problem, not a hot-path one.)

> ELI5: The archivist made a summary and then shredded the originals — and anyone mid-read lost the pages being shredded. Now the summary is added alongside; the originals are never destroyed on the live path. **Nastiness: 8/10.**

**41. The abandoned self-lease.** A reboot claims a *fresh* quota slice and can
abandon a block whose durable lease still names us at a held epoch — peers see
us alive and never reclaim, we don't host it, orphan. Fix: `ensure_local_sender`
re-adopts a block whose lease is **durably ours** at its held epoch on demand
(no new lease, no epoch bump, no reclaim — provably safe), curing the amnesia.

> ELI5: You still legally owned a storage unit but forgot you had it, and the front desk wouldn't re-rent it because your name was on the lease. Now, when someone asks for it, you check the lease, see it's yours, and just open it. **Nastiness: 6/10 (orphan liveness).**

### Act V — the two-writer stale-snapshot forks, and the last mile to zero

These are the residual the earlier notes called "the last ~1%": a stale full
snapshot committing at a fresh seq **over** a committed write. Each is a
different way the rebase guard (31) got bypassed.

**42. The committed-rival step-over.** When our target seq was held by a rival
that had actually **committed** under us — its lock looked *pending* because a
store-fault-timed outcome read missed the commit — the prewrite stepped to the
next seq and shipped our stale snapshot on top, dropping the rival's write. Fix:
a committed rival (whether the outcome reads committed *or* our abort-race loses
to it) means **rebase**, never step over.

> ELI5: The seat looked empty because the ticket scanner glitched, so you sat in the next one and ignored the person already there. Now, if anyone might be seated, you re-check and move to a truly free row. **Nastiness: 9/10 (silent fork).**

**43. The stale re-acquire cache.** Re-acquiring a migrated object seeded
`log_seq` to the fresh `committed_seq` but kept a **stale cached live file**
from a prior tenure — so the next ship built a snapshot on old bytes yet
believed it was current. Fix: on re-acquire, purge the cache and re-baseline
**only for a clean object** (its next access re-folds); a *dirty* object is
left alone so its unshipped writes rebase instead of shipping a lie.

> ELI5: You got the apartment back and updated the address label, but the fridge still had last tenant's food — and you served it as fresh. Now, on move-in, you empty the fridge (if you brought nothing of your own) and restock from the store. **Nastiness: 9/10 (silent fork).**

**44. The stale-owner read.** A worker owned an account at a `log_seq` behind
the durable prefix — an orphan had committed past it — and, with no local write
to trigger a rebase, **served a read behind a committed write** at quiescence.
The durable truth was correct the whole time; only the read was stale. Fix:
before serving a read, re-fold a clean participant whose `log_seq` trails the
committed prefix (generalizing the durable-poll refresh of 32 to plain reads).

> ELI5: The teller quoted your balance from an old ledger page because no new deposit had crossed *their* desk — even though the deposit was safely in the vault. Now they flip to the current page before reading it out. **Nastiness: 8/10 (stale read of committed data).**

**45. The reconstruction erasure.** The genuine "base reconstruction" fork the
old notes feared. A write on a **dirty** object — or a **single-node
revert/fold-race** under heavy fault — captured a live file missing a committed
write, and shipped a full snapshot that erased it at a fresh seq. The seq-based
rebase guard couldn't see it: `log_seq` read *current* while the conn lagged.
Two-part fix, because `log_seq` is exactly what lies. (a) Before a write lands
on a clean participant, rebuild its base from the durable fold
**unconditionally** — not gated on `log_seq` (scoped to data objects; folding
before every write to the hot `_wills` singleton multiplied its fault surface).
(b) A **content fence at ship**: before prewriting a snapshot, compare its
`writes` ledger to the durable committed fold; if it's missing a key the log
has already committed, it would erase that write — rebase instead. Content-level,
so it catches every variant the seq guard misses.

> ELI5: You copied the ledger to a fresh page but skipped a committed line, then filed your page as the new master — deleting that line. Now, before filing, you diff your page against the vault copy; if the vault has a line you don't, you copy it down first. **Nastiness: 10/10 (silently forks customer money; the deepest fork in the ledger).**

**46. The phantom-claim deadlock.** A worker's own multi-object gather wanted
an object whose durable claim was `Transit`/`Owned` to **itself** — but `admit`
refused the self-adopt because `self.transit` recorded a *higher* generation
from a release whose checkpoint had **never durably landed** (a ghost). The take
chased to us, admit refused every pass, and the gather wedged forever. Fix: when
durable authoritatively names us at *exactly* this generation and we hold the
object in neither `owned` nor a real durable claim, that higher gen is a
phantom — drop it and adopt. Guarded hard so a genuinely-spent renunciation
(durable moved on to a real owner) is still refused.

> ELI5: You refused to pick up your own package because your notebook said "you already forwarded this to yourself at a later date" — a note you'd scribbled but never actually mailed. When the post office confirms it's yours *right now*, you tear up the phantom note and take it. **Nastiness: 7/10 (gather deadlock).**

**47. The self-orphaned lock.** A boat that durably prewrote its entries then
**failed before its commit switch** (a store fault on the outcome create) left
them **pending**. Its own next ship collided with those pending locks at the
same seq — a fresh lock says "don't steal" — and rebased, reverted, retried
into the same wall until they aged out. Under churn + fault the reclaim window
often never came, so a hot object (`_wills`) livelocked. Fix: on boat failure,
roll back the boat's **own** prewritten entries — guarded two ways so it can
never erase a real commit (delete only an entry still ours, and only after
checking the outcome didn't actually land).

> ELI5: You reserved a table, the payment machine died, and you left the "reserved" card on it — then couldn't sit down because *your own* card said the table was taken. Now, if the payment fails, you pick your card back up. **Nastiness: 7/10 (hot-object livelock).**

**48. The will that couldn't claim.** The last-will sweep gated *firing* an
orphaned will on a claim UPDATE to the single hot `_wills` object landing
durably. Under crash/reboot churn + faults that claim thrashed and never
landed — so a genuinely dead-node will **never fired at all**, defeating the
very guarantee the harness was built for (bug 22). But the claim is only an
optimization to cut *redundant* fires: fires are idempotent by contract (the
DST arms `INSERT OR IGNORE`). Fix: skip a will only when the claim **provably**
went to another sweeper (`rows_affected == 0`); if it merely couldn't land, fire
anyway. At-least-once, which is the real guarantee.

> ELI5: The lifeguards agreed only one of them blows the whistle, and to decide who, they had to sign a shared clipboard first — but the clipboard was underwater, so *nobody* ever blew the whistle while someone drowned. Now, if you can't reach the clipboard, you blow the whistle anyway; two whistles never hurt. **Nastiness: 8/10 (the flagship feature silently failing under load).**

**49. The read that hung on a dead owner.** A read of a committed object stuck
in `Transit` to a worker on a **crashed, un-reclaimed** node routed to the dead
owner and retried forever. But under the log, committed state is *always*
available. Fix: `submit_routed` serves a **read-only** txn straight from the
durable fold (`durable_read`) when the routed owner can't — no live node, or any
owner error. Linearizable (the fold is exactly the committed prefix; a
pessimistic write's log entry precedes its outcome, so it's included), and never
in-flight state. Writes are never rerouted this way: they need their one owner.

> ELI5: You asked the one clerk who owned a file, but they'd gone home sick and nobody took their desk — so you stood at an empty counter forever. Since the file is also in the public archive, we now just read it from the archive when the clerk's out. (But you still can't *change* it without a clerk.) **Nastiness: 6/10 (read availability).**

**50. The scratch file that collided.** The content fence (45b) wrote each
comparison into an SQLite scratch file named per-object — and the mine runs
**parallel subprocesses sharing `/tmp`**, plus two nodes in one process can
ship the same forked object at once. They clobbered each other's scratch bytes,
making the fence's (correctness-affecting) rebase decision **non-deterministic**
— surfacing as a false-positive crash on a seed that was clean on every direct
run. Fix: key the scratch on process id + `staging_id` + object, so no two
comparisons ever share a file.

> ELI5: Two chefs checking recipes were both scribbling on the same shared notepad, so each read the other's notes and cooked the wrong dish at random. Give every chef their own notepad. **Nastiness: 7/10 (a tooling bug that fakes a product bug — a mirror crack, like 28).**

### Where the line moved

The earlier section drew the honest line at *"clean in the supported model;
the residual lives in one deliberately-unbuilt feature."* The log-structured
commit **erased that line.** With the fence protecting every write, the
orphaned-block liveness that had looked like a protocol boundary turned out to
be two ordinary operations — a will fire and a committed read — that simply
**didn't degrade gracefully** when their routing hint pointed at a dead node.
Neither needed ownership to *converge*; each had a safe, idempotent fallback
(fire the idempotent will anyway; read the committed value from the log).
Making the operation robust beat trying to converge ownership — and two
tempting "convergence" fixes were built and **rejected** on the way (pinning
`_wills` routing, which didn't converge the lease split; and fencing phantom
lease holds in the guard, which false-fired on a store-list blip and broke a
test). Final state: `dst mine --fuzz` and `dst mine --pause` at **zero
crashes**, every historically-failing seed clean, safety zero in both modes,
the flag-off world byte-identical, and the flag-on world deterministic. The
system is no longer clean *in its model* — it is clean *under the adversary
the model was built to break it with*.

## Before the dice: what the example suite caught

Not everything needed the simulator. These four surfaced earlier, during
the coverage push that took the hand-written suite from ~47 tests to 140 —
each one flushed out by *writing a test and watching it disagree with the
code*. Lettered, not numbered, because they predate the campaign: the
counter above is the dice's kill list, and these are the ones the plain
tests got to first.

**T1. Off-by-a-block at the seams.** `block_of` (worker → lease block) and
`block_range` (block → worker range) are inverses that must agree on every
boundary. When the block count doesn't divide the worker space evenly —
1,000,000 workers over 256 blocks is 3906.25 each — they disagreed at the
edges: `block_of(3906)` said block 0 while the old floor-division
`block_range` filed 3906 under block 1. Lease coverage and routing could
then disagree about who owns a boundary worker. The shipped default
(4096 / 256) divides evenly, so it never bit in practice — but the
million-worker config, which the README advertises, would. Found by an
exhaustive tiling test: every worker must land in exactly one block's
range, checked across five W/B combos including 1M / 256. Fix:
`block_range` uses the ceiling form that inverts `block_of` exactly.

> ELI5: The floor plan and the room directory disagreed about which room the closet belonged to — but only in buildings whose floors don't divide evenly, which nobody had built yet. **Nastiness: 6/10.**

**T2. The decode allocation bomb.** `delta::decode` read a 4-byte page
count out of a blob and handed it straight to `Vec::with_capacity`. A
corrupt or hostile delta claiming four billion pages aborts the process on
the allocation, before a single bounds check runs — a one-blob denial of
service, and the R2 store's contents are not all self-authored. Found by a
decode-fuzzing test throwing truncated and absurd headers at it. Fix: the
page count is checked against what the buffer could physically hold before
anything is allocated.

> ELI5: A form declared "enclosed: 4 billion pages," so the clerk began clearing four billion desks to lay them out — and only *then* would have noticed the envelope was empty. **Nastiness: 6/10.**

**T3. Invariants that weren't.** "Invariants live in SQL" is the product's
pitch: `CHECK` constraints and foreign keys enforce your business rules
inside the engine, atomically, across a cross-object transaction. But
SQLite defaults foreign-key enforcement *off*, and `PRAGMA
foreign_keys=ON` issued inside a transaction — the only place API ops ever
run — is a silent no-op. So no client could actually turn it on: every
foreign key was decoration. Found by writing a deferred-constraint test
and watching it pass when it should have failed. Fix: `materialize` sets
`foreign_keys=ON` per connection at activation, the one place the pragma
takes.

> ELI5: The bank advertised "we enforce your rules," but the rule-checker shipped switched off, and the only switch was inside a room that's locked whenever anyone's working. **Nastiness: 7/10.**

**T4. COMMIT can refuse — caught here first.** A deferred constraint is
checked only at COMMIT, so a transaction whose every op succeeded can still
fail *at commit*, after earlier participants already committed. The
original handler `purge`d every participant on any commit error —
destroying unrelated already-acked writes on some, leaving torn state on
others — and a boat sinking between a txn's acquisition and its execution
could `.expect()`-panic the whole worker. A coverage test that forced a
deferred FK violation surfaced it; the first fix (ROLLBACK the still-open
participants, revert the committed prefix) later hardened under the dice
into bugs **11** and **12** with `revert_closure`. The example suite found
the hole; the simulator found how deep it went.

> ELI5: On signing day the last signer's pen died — and the first version of the office shredded everyone's folder. The tests caught the shredding; the dice caught the pen dying mid-signature across three different desks. **Nastiness: 7/10.**

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
- **The oracle beats the example suite, but the suite isn't zero.** Every
  numbered bug above passed 100+ hand-written tests before the dice found
  it — the examples check the paths we imagined, the dice check the ones we
  didn't. But the suite earns its keep on the paths we *can* imagine and
  simply hadn't exercised: T1–T4 fell to nothing more than writing the
  missing test and reading its disagreement. Fuzz the decoder, tile the
  whole worker space, force the constraint that fires late — cheap tests,
  real bugs, no simulator required. Write those first; spend the dice on
  what's left.
- **One seed = one repro** is the entire debugging experience. Every entry
  above went: failing seed → `FAFO_DST_LOG=1` replay → read the object's
  lifecycle → the bug names itself. No bug took longer to *find* than to
  fix.

---

## Appendix: what the multi-workload simulator guards

`dst` runs these workloads **concurrently in one world**, both phases,
under the same crashes / partitions / storage faults. Each was chosen so
that a specific ACID guarantee has an invariant pointed straight at it — a
forked object, a torn transaction, or a lost durable write trips at least
one of them.

| Workload | Guards | Invariant the oracle checks |
|---|---|---|
| Transfers (A↔B) | **A**tomicity, **C**onsistency | money conserved; every transfer's debit and credit exist together or not at all |
| ERC20 (mint / burn / transfer + supply) | Atomicity with a **moving** total | Σ balances = supply = supply ledger; every multi-leg op is all-legs-or-none |
| Escrow sagas (open → settle, half racing both ways) | **I**solation, exactly-once | a saga settles once or never — never both directions; capital = parties + funds still held |
| Idempotent counters (retry-safe increments) | **D**urability under lost-ack | `n` == COUNT(distinct inc keys): a retry that double-applied would overshoot |
| Watched feeds — cursor poll | watch completeness | every durable message reaches a consumer; ids strictly increase; no key twice |
| Watched feeds — **durable** cursor poll | durable delivery contract | a row a durable poll delivered still exists after every later crash |
| Watched feeds — change-detection (hash) | poll semantics | every fire presents a genuinely different result hash |
| Last-wills (durable, sweeper-fired) | liveness of promises | a will armed on a node that *crashes* still fires |

Plus the always-on oracles: **liveness** (any single op hung past the
virtual timeout is a deadlock) and **recovery** (every audit re-runs on a
cold node booted from the blob store alone).

The optimistic contract is honored throughout: an optimistically-acked
write may vanish if its boat sinks in a crash — so the oracles audit
against *surviving durable state*, never against the ack flag. That
distinction is itself load-bearing; getting it wrong makes an oracle cry
wolf at correct behavior (it did, twice, while these were being written).
