# God prompt: build a deterministic simulation test, and test this codebase.

Some prompts are worth more than others. The most valuable instruction I gave while building a distributed database was one sentence: build a deterministic simulation test, and test this codebase. It did more work per word than anything else I typed, and I think it should be a reflex for anyone writing stateful software right now.

Context first. I built a small distributed database. Every object is its own SQLite file, ownership migrates between stateless nodes, and a blob store with no transactions is the only durable truth. That last part is the whole problem. It is a machine practically designed to produce split-brain forks and half-committed transfers. Killing a few nodes and checking that money adds up catches the easy bugs and lies to you about the hard ones: the one in ten thousand, timing-dependent, never-reproduces kind.

So I did not test it that way. I asked for a deterministic simulation test, and it became the hero of the whole project.

Here is what that one sentence actually buys you.

**The entire cluster runs in one thread on virtual time.** No real sockets, no real clocks, no real disk. Every node, every RPC, every storage call is a function call. Time is a number you advance.

**Every fault comes from a seed.** Crashes, partitions, dropped and delayed RPCs, half the storage writes failing, per-node clock skew, and a pause adversary: isolate a live node, let a peer steal its lease, let it rejoin and try to keep writing. The textbook zombie writer. All of it derived deterministically from a single number.

**Oracles check the invariants after every phase.** Money is conserved. Transfers are atomic, both legs or neither. Committed reads never vanish. A dead client's last will always fires.

**A crash is a bug, and a crash replays bit for bit.** This is the whole trick. Seed 5242420418152406189 fails the same way every time. There is no "couldn't reproduce." The seed is the repro. So I priced it (a crash is a bug, a bug is $100) and let it mine on all cores, forever.

FoundationDB and TigerBeetle have preached this for years. I am a recent convert with all the zeal that implies.

**What the prompt found.** I kept a ledger. It is at 51 numbered bugs, each with what the oracle saw, what was actually wrong, the fix, and a nastiness score. Two of the 10 out of 10s:

The twice-spent deed. One "I am giving up this object" note could get consumed twice during a handoff, resurrecting ownership after it had already been granted away. Two live writers, one object, histories interleaved in the store. The fix puts a serial number on every handoff so staleness becomes an integer compare. Three rounds of failed guards before that clicked. No unit test I would ever think to write catches it.

The reconstruction erasure. A worker ships a full snapshot built from a local file that never saw a peer's committed write, silently erasing it. It corrupts money, it is timing-dependent, and it is invisible to any test that does not model concurrent forked writers under failing storage. The simulator found it in seconds and handed me the seed.

**The best part is when the harness catches itself.** Three of the 51 are the test lying to itself, which is the most dangerous kind, because it manufactures false confidence. A liveness timeout was killing slow-but-correct runs before the safety checks ran, so I had been reading "zero violations" through a blind spot. And bug 51, found on the literal victory lap: I turned the miner on to run forever, and an hour in it reported a bug. The bug was StorageFull. It had filled my disk. My own earlier fix wrote a tiny scratch file per operation and never deleted them, and across millions of operations that adds up. The zero-crash miner's first real discovery was a defect in its own tooling. Correct. Humbling.

**The honest fine print, because a good prompt still needs a definition of done.** The residual eventually shrank to a class I thought was a fundamental protocol boundary, and I wrote it up as "not fixed yet." It was not a boundary. It was two ordinary operations that simply did not degrade gracefully when they pointed at a dead node, each with a safe fallback. I also built two clever fixes and threw both away when the simulator showed they made things worse, which is the entire point of a test you cannot sweet-talk. So the system is clean under exactly the adversary the model encodes: crashes, reboots, skew, half the writes failing, forced zombie takeovers, across thousands of fresh seeds, deterministically. It is not yet proven against real object storage's weirder eventual-consistency corners, and the new commit protocol is not the default yet. That is the next mile.

But the thing that was actually rotten, silent forks corrupting money under fault, is gone. And I have a machine that will scream, with a replayable seed, the instant it comes back. That is the confidence. It is not a feeling. It is a standing invariant: a crash is a bug, I cannot stop finding them, and right now I cannot find any.

Why I call it a god prompt. Writing code has never been cheaper. Trusting code has never been more expensive. "Build a deterministic simulation test, and test this codebase" is one sentence that closes that gap, because it does not ask a person or a model to be careful. It manufactures the adversary, runs it a million times, and refuses to be talked out of a failure. You get a bug with a seed attached, or you get silence you can actually believe.

The database is about 7,000 lines of Rust, MIT, running in production for an audience of one, bug ledger and all: https://github.com/elliottdehn/fafo

If you build anything stateful, make this your reflex. One thread, virtual time, a seed, one invariant you actually care about. It will find something in the first hour. It found something in mine, and it has not stopped since. Including, apparently, my disk.

## If you've read this far through my AI-generated post, you deserve a prize.

So here is a game, and the prize is real money.

An AI wrote this post. An AI built the simulation test. An AI mined the bugs and drove the crash count to zero across every fault at once. The obvious question is whether it missed one. I will pay you to find out.

Point your own coding agent at it. Paste this:

> Clone https://github.com/elliottdehn/fafo, read AGENTS.md, and follow the section titled "The bug mine." Build the dst binary, check the disk, then run the deterministic simulation mine under --fuzz --pause. If it reports a crash, give me the seed and the crash log.

Or drive it yourself:

```sh
git clone https://github.com/elliottdehn/fafo && cd fafo
cargo build --release --bin dst
df -h "$TMPDIR"                       # make sure you actually have disk
./target/release/dst mine --fuzz --pause
```

Let it run. Every crash it finds is confirmed by a re-run and written to crashes/seed-N.log with the exact command to replay it. Read the "what is NOT a bug" list in AGENTS.md before you get excited: a HUNG on a full disk is disk, not fafo, and it will not pay.

The deal: **post a failing seed as a comment.** I reproduce it on my machine, bit for bit, because that is the entire point of this technique. If it breaks an invariant, it is a bug, and you get **$100 in USDC.** Just post your address. Send a fix that follows the rules in AGENTS.md and you will have done something a small number of people on Earth can do, which is worth more than the hundred bucks.

Fair warning: the thing has survived thousands of seeds under crashes, partitions, clock skew, half the writes failing, and forced zombie takeovers. I think it is clean. I have thought that before, several times, and the simulator has corrected me every time. That is exactly why I am asking you to try.
