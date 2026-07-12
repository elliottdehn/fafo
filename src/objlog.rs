//! The log-structured commit layout — integration step 1: ADDITIVE and INERT.
//!
//! An object's durable history becomes a base plus an ordered log of per-txn
//! entries keyed by a per-object commit SEQUENCE (not the SQLite change
//! counter, which collides across a fork — see bugs.md). Reading folds the
//! base and the contiguous committed log; the base is just a compaction
//! cache. A cross-object txn ties its participants' log entries together
//! through a single outcome key, so the commit is one atomic decision and a
//! fork can't tear it (validated in `bin/protosim.rs`).
//!
//! This module defines the KEYS, the entry wire format, and the FOLD. The
//! write side is dual-written alongside today's base/delta promotion (step 1)
//! and nothing reads it yet; the read path is switched over in step 2. Keep
//! it inert until then.

use crate::delta;
use crate::store::BlobStore;

/// One committed log entry for `id` at sequence `seq`. Zero-padded so a
/// lexical LIST returns entries in seq order.
pub fn log_key(id: &str, seq: u64) -> String {
    format!("objects/{id}.L.{seq:020}")
}

/// The object's current durable sequence (the highest committed log seq).
pub fn seq_key(id: &str) -> String {
    format!("objects/{id}.SEQ")
}

/// Parse the seq out of a log key `objects/<id>.L.<seq>`.
pub fn parse_log_seq(key: &str, id: &str) -> Option<u64> {
    key.strip_prefix(&format!("objects/{id}.L."))?.parse().ok()
}

/// A compacted base holds the folded image AT a sequence: everything at or
/// below it is subsumed, so the reader only folds the log entries above it.
pub fn base_seq_key(id: &str, seq: u64) -> String {
    format!("objects/{id}.B.{seq:020}")
}
pub fn parse_base_seq(key: &str, id: &str) -> Option<u64> {
    key.strip_prefix(&format!("objects/{id}.B."))?.parse().ok()
}

/// The highest compacted base (image, seq), or (empty, 0) if none — the
/// starting point for a fold.
pub async fn read_base(store: &dyn BlobStore, id: &str) -> anyhow::Result<(Vec<u8>, u64)> {
    let mut best: Option<u64> = None;
    for k in store.list(&format!("objects/{id}.B.")).await? {
        if let Some(s) = parse_base_seq(&k, id) {
            best = Some(best.map_or(s, |b| b.max(s)));
        }
    }
    match best {
        Some(seq) => {
            let img = store.get(&base_seq_key(id, seq)).await?.unwrap_or_default();
            Ok((img, seq))
        }
        None => Ok((Vec::new(), 0)),
    }
}

/// The single atomic commit/abort decision for a txn. Commit and abort both
/// race to `create` it (create-if-absent), so EXACTLY ONE wins — a resumed
/// zombie can never disagree with the peer that resolved it. Value: [1] =
/// committed, [0] = aborted. (protosim proved this closes the fork.)
pub fn outcome_key(txn: &str) -> String {
    format!("txns/{txn}.O")
}

/// Commit `txn`: create its outcome as committed. Returns false if a resolver
/// already aborted it (we lost the race) — the caller must NOT ack.
pub async fn commit_txn(store: &dyn BlobStore, txn: &str) -> anyhow::Result<bool> {
    let won = store.create(&outcome_key(txn), &[1]).await?;
    let ok = won || committed(store, txn).await;
    if std::env::var_os("FAFO_DST_LOG").is_some() {
        eprintln!("commit {txn} -> {ok} (won={won})");
    }
    Ok(ok)
}

/// Abort `txn`: create its outcome as aborted. Returns true if we won (or it
/// was already aborted), false if it committed first (leave it alone).
pub async fn abort_txn(store: &dyn BlobStore, txn: &str) -> anyhow::Result<bool> {
    if store.create(&outcome_key(txn), &[0]).await? {
        return Ok(true);
    }
    Ok(matches!(outcome(store, txn).await?, Some(false)))
}

pub async fn outcome(store: &dyn BlobStore, txn: &str) -> anyhow::Result<Option<bool>> {
    Ok(store
        .get(&outcome_key(txn))
        .await?
        .map(|b| b.first() == Some(&1)))
}

/// A log entry's txn is committed iff its outcome is Committed.
pub async fn committed(store: &dyn BlobStore, txn: &str) -> bool {
    matches!(outcome(store, txn).await, Ok(Some(true)))
}

/// A log entry's payload: a full snapshot (self-contained) or a page-delta
/// against the previous seq's image.
#[derive(Debug, Clone, PartialEq)]
pub enum LogPayload {
    Snapshot(Vec<u8>),
    Delta(Vec<u8>), // exactly `delta::encode(&d)` bytes
}

/// One log entry: the txn that wrote it (so a reader can check its outcome),
/// the node-clock time it was prewritten (so a resolver only reclaims a lock
/// aged past the TTL, never a live rival mid-commit), and its payload.
/// Wire: tag(1) | born(8 BE) | txn_len(2 BE) | txn | payload.
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub txn: String,
    pub born: u64,
    pub payload: LogPayload,
}

const TAG_SNAPSHOT: u8 = 0;
const TAG_DELTA: u8 = 1;

pub fn encode_entry(txn: &str, born: u64, p: &LogPayload) -> Vec<u8> {
    let (tag, body): (u8, &[u8]) = match p {
        LogPayload::Snapshot(b) => (TAG_SNAPSHOT, b),
        LogPayload::Delta(b) => (TAG_DELTA, b),
    };
    let t = txn.as_bytes();
    let mut out = Vec::with_capacity(11 + t.len() + body.len());
    out.push(tag);
    out.extend_from_slice(&born.to_be_bytes());
    out.extend_from_slice(&(t.len() as u16).to_be_bytes());
    out.extend_from_slice(t);
    out.extend_from_slice(body);
    out
}

pub fn decode_entry(bytes: &[u8]) -> Option<LogEntry> {
    let (&tag, rest) = bytes.split_first()?;
    let born = u64::from_be_bytes(rest.get(..8)?.try_into().ok()?);
    let rest = rest.get(8..)?;
    let tlen = u16::from_be_bytes([*rest.first()?, *rest.get(1)?]) as usize;
    let rest = rest.get(2..)?;
    let txn = String::from_utf8(rest.get(..tlen)?.to_vec()).ok()?;
    let body = rest.get(tlen..)?.to_vec();
    let payload = match tag {
        TAG_SNAPSHOT => LogPayload::Snapshot(body),
        TAG_DELTA => LogPayload::Delta(body),
        _ => return None,
    };
    Some(LogEntry { txn, born, payload })
}

/// Encode a log entry from the exact bytes a boat produced (snapshot image or
/// encoded delta), stamped with the committing txn and its birth time.
pub fn entry_from_ship(txn: &str, born: u64, is_snapshot: bool, bytes: &[u8]) -> Vec<u8> {
    let p = if is_snapshot {
        LogPayload::Snapshot(bytes.to_vec())
    } else {
        LogPayload::Delta(bytes.to_vec())
    };
    encode_entry(txn, born, &p)
}

/// Fold a base image and an ordered run of log entries into the current
/// image. Entries MUST be in seq order: a snapshot resets the image, a delta
/// applies against it. This is the read path step 2 will adopt.
pub fn fold(base: Vec<u8>, entries: &[LogPayload]) -> anyhow::Result<Vec<u8>> {
    let mut image = base;
    for e in entries {
        match e {
            LogPayload::Snapshot(b) => image = b.clone(),
            LogPayload::Delta(b) => {
                let d = delta::decode(b)?;
                delta::apply(&mut image, &d);
            }
        }
    }
    Ok(image)
}

/// The highest committed seq of `id` — the compacted base seq plus the
/// contiguous committed run of log entries above it — without folding
/// payloads. The next write goes at this + 1.
pub async fn committed_seq(store: &dyn BlobStore, id: &str) -> anyhow::Result<u64> {
    let (_, base_seq) = read_base(store, id).await?;
    let mut keys = store.list(&format!("objects/{id}.L.")).await?;
    keys.sort();
    let mut seq = base_seq;
    for k in &keys {
        let Some(this_seq) = parse_log_seq(k, id) else { continue };
        if this_seq <= base_seq {
            continue; // subsumed by the base
        }
        if this_seq != seq + 1 {
            break; // a gap: the committed prefix ends here
        }
        let Some(b) = store.get(k).await? else { break };
        let Some(entry) = decode_entry(&b) else { break };
        if !committed(store, &entry.txn).await {
            break;
        }
        seq = this_seq;
    }
    Ok(seq)
}

/// Prewrite (or idempotently re-confirm) `txn`'s log entry for `id` at the
/// next committed seq, fencing forks with create-if-absent. Returns the seq
/// on success, or None if a live rival holds the slot (the caller aborts).
/// In the supported single-writer model an occupied slot is always our own
/// retry, a committed rival (advance), or a crashed orphan (clear + retake).
pub async fn prewrite(
    store: &dyn BlobStore,
    id: &str,
    txn: &str,
    entry: &[u8],
    expected: u64,
    now_ms: u64,
    ttl_ms: u64,
) -> anyhow::Result<Option<u64>> {
    // Rebase guard: our snapshot builds on `expected`. If the durable
    // committed prefix has moved past it, a fork peer committed a change our
    // live file never saw — shipping would clobber it. Abort so the worker
    // reverts and re-activates off the fresh committed fold.
    if committed_seq(store, id).await? != expected {
        return Ok(None);
    }
    let mut seq = expected + 1;
    for _ in 0..64 {
        let key = log_key(id, seq);
        if store.create(&key, entry).await? {
            if std::env::var_os("FAFO_DST_LOG").is_some() {
                eprintln!("prewrite {txn} {id}@{seq}");
            }
            return Ok(Some(seq));
        }
        // Occupied — decide by the holder.
        let Some(existing) = store.get(&key).await? else {
            continue; // vanished (a resolver cleared it): retry same seq
        };
        let Some(held) = decode_entry(&existing) else {
            return Ok(None);
        };
        if held.txn == txn {
            return Ok(Some(seq)); // already ours (idempotent retry)
        }
        match outcome(store, &held.txn).await? {
            Some(true) => {
                // A COMMITTED rival owns this seq: our view is stale (it
                // committed a change we never saw), so our payload — computed
                // against an older state — would clobber its txn if shipped at
                // a higher seq. Abort; the worker reverts, re-activates on the
                // fresh committed prefix, and re-runs the txn (rebase). This
                // is what protosim did implicitly by re-reading on every retry.
                return Ok(None);
            }
            Some(false) => {
                // Aborted: roll the lock back and retake this seq.
                delete_if_txn(store, &key, &held.txn).await?;
            }
            None => {
                // Pending. A FRESH lock is a live peer about to commit — do
                // NOT steal it; abort our own attempt (the caller retries
                // later, by which point the holder has committed or aged).
                // Only an AGED lock (past the TTL) is a dead orphan we may
                // reclaim — and we reclaim it through the single outcome key
                // (abort_txn), so a resumed holder's commit loses the race
                // and never disagrees with us.
                if now_ms.saturating_sub(held.born) <= ttl_ms {
                    return Ok(None);
                }
                if abort_txn(store, &held.txn).await? {
                    delete_if_txn(store, &key, &held.txn).await?;
                } else {
                    seq += 1; // it committed under us; step over it
                }
            }
        }
    }
    Ok(None)
}

/// Delete a log entry only if it still belongs to `txn` — never erase a
/// reclaimer's entry that took the freed slot (the tid-safe rollback
/// protosim needed).
async fn delete_if_txn(store: &dyn BlobStore, key: &str, txn: &str) -> anyhow::Result<()> {
    if let Some(b) = store.get(key).await?
        && decode_entry(&b).is_some_and(|e| e.txn == txn)
    {
        store.delete(key).await?;
    }
    Ok(())
}

/// The object's current durable sequence marker (0 if none). An 8-byte
/// big-endian counter under `seq_key` (used only by the step-1 dual-write).
pub async fn read_seq(store: &dyn BlobStore, id: &str) -> u64 {
    store
        .get(&seq_key(id))
        .await
        .ok()
        .flatten()
        .filter(|b| b.len() == 8)
        .map(|b| u64::from_be_bytes(b[..8].try_into().unwrap()))
        .unwrap_or(0)
}

/// Read `id`'s durable log and fold the CONTIGUOUS COMMITTED PREFIX over
/// `base` — the read path. Entries are listed in seq order; an entry counts
/// only if its txn's commit record is present, and the fold stops at the
/// first gap or uncommitted/pending entry (a page-delta at seq N is only
/// valid applied on the image at seq N-1, so the prefix must be contiguous).
/// Returns (image, highest committed seq).
pub async fn fold_committed(store: &dyn BlobStore, id: &str) -> anyhow::Result<(Vec<u8>, u64)> {
    let (base, base_seq) = read_base(store, id).await?;
    let mut keys = store.list(&format!("objects/{id}.L.")).await?;
    keys.sort();
    let mut payloads: Vec<LogPayload> = Vec::new();
    let mut seq = base_seq;
    for k in &keys {
        let Some(this_seq) = parse_log_seq(k, id) else { continue };
        if this_seq <= base_seq {
            continue; // subsumed by the compacted base
        }
        if this_seq != seq + 1 {
            break; // a gap: the committed prefix ends here
        }
        let Some(b) = store.get(k).await? else {
            // Deleted under us by a concurrent compaction: fail so the caller
            // re-reads the fresh base rather than folding across a hole.
            anyhow::bail!("log entry {k} vanished mid-fold (compaction race); retry");
        };
        let Some(entry) = decode_entry(&b) else { break };
        if !committed(store, &entry.txn).await {
            break; // pending/aborted: the committed prefix ends here
        }
        payloads.push(entry.payload);
        seq = this_seq;
    }
    Ok((fold(base, &payloads)?, seq))
}

/// Compaction: fold the committed prefix into a fresh base at its seq, then
/// trim the log entries and older bases it subsumes. Bounds fold cost to the
/// entries ABOVE the base. Write-new-base-BEFORE-delete so a concurrent
/// reader always sees a consistent (base, tail): it either reads the old base
/// + full log, or the new base + trimmed log. Only worth doing once the tail
/// exceeds `keep`.
pub async fn compact(store: &dyn BlobStore, id: &str, keep: u64) -> anyhow::Result<()> {
    let (image, seq) = fold_committed(store, id).await?;
    let (_, base_seq) = read_base(store, id).await?;
    if seq < base_seq + keep {
        return Ok(()); // tail too short to bother
    }
    // 1. Publish the new base at the committed seq.
    store.put(&base_seq_key(id, seq), &image).await?;
    // 2. Trim: log entries at or below it, and any older base.
    for k in store.list(&format!("objects/{id}.L.")).await? {
        if parse_log_seq(&k, id).is_some_and(|s| s <= seq) {
            store.delete(&k).await?;
        }
    }
    for k in store.list(&format!("objects/{id}.B.")).await? {
        if parse_base_seq(&k, id).is_some_and(|s| s < seq) {
            store.delete(&k).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_db(pages: usize, ps: usize, counter: u32, fill: u8) -> Vec<u8> {
        let mut f = vec![fill; pages * ps];
        let encoded_ps: u16 = if ps == 65536 { 1 } else { ps as u16 };
        f[16..18].copy_from_slice(&encoded_ps.to_be_bytes());
        f[24..28].copy_from_slice(&counter.to_be_bytes());
        f
    }

    #[test]
    fn keys_sort_in_seq_order() {
        // Lexical order must equal numeric order so a prefix LIST folds right.
        assert!(log_key("acct-1", 2) < log_key("acct-1", 10));
        assert!(log_key("acct-1", 9) < log_key("acct-1", 100));
        assert!(log_key("acct-1", 0) < log_key("acct-1", 1));
    }

    #[test]
    fn entry_roundtrips_both_kinds() {
        let snap = LogPayload::Snapshot(vec![1, 2, 3]);
        assert_eq!(
            decode_entry(&encode_entry("b655-7", 4200, &snap)),
            Some(LogEntry { txn: "b655-7".into(), born: 4200, payload: snap })
        );
        let d = LogPayload::Delta(vec![9, 8, 7]);
        assert_eq!(
            decode_entry(&encode_entry("t2-c1-9", 0, &d)),
            Some(LogEntry { txn: "t2-c1-9".into(), born: 0, payload: d })
        );
        assert_eq!(decode_entry(&[]), None);
    }

    #[test]
    fn fold_snapshot_then_deltas_reconstructs_exactly() {
        // seq1: full snapshot; seq2/seq3: page-deltas against the prior image.
        let v1 = fake_db(8, 4096, 5, 0xaa);

        let mut manifest = delta::Manifest::of(&v1, 0);
        let mut v2 = v1.clone();
        v2[24..28].copy_from_slice(&6u32.to_be_bytes());
        v2[3 * 4096 + 17] = 0x77;
        let d2 = delta::encode(&delta::diff(&mut manifest, &v2));

        let mut v3 = v2.clone();
        v3[24..28].copy_from_slice(&7u32.to_be_bytes());
        v3[5 * 4096 + 11] = 0x33;
        let d3 = delta::encode(&delta::diff(&mut manifest, &v3));

        let entries = vec![
            LogPayload::Snapshot(v1.clone()),
            LogPayload::Delta(d2),
            LogPayload::Delta(d3),
        ];
        // Fold from an EMPTY base: the first entry is a full snapshot.
        let folded = fold(Vec::new(), &entries).unwrap();
        assert_eq!(folded, v3, "fold must byte-exactly reconstruct seq 3");

        // Folding a prefix reconstructs the intermediate state.
        let folded2 = fold(Vec::new(), &entries[..2]).unwrap();
        assert_eq!(folded2, v2, "fold of seq 1..2 == state at seq 2");
    }

    #[test]
    fn fold_over_a_compacted_base_applies_only_the_tail() {
        // Simulate compaction: base already holds seq 2, tail is one delta.
        let v2 = fake_db(8, 4096, 6, 0xcd);
        let mut manifest = delta::Manifest::of(&v2, 0);
        let mut v3 = v2.clone();
        v3[24..28].copy_from_slice(&7u32.to_be_bytes());
        v3[2 * 4096 + 9] = 0x55;
        let d3 = delta::encode(&delta::diff(&mut manifest, &v3));
        let folded = fold(v2, &[LogPayload::Delta(d3)]).unwrap();
        assert_eq!(folded, v3);
    }
}
