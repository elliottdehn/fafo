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

/// The single atomic commit/abort decision for a txn — its presence-with-
/// value is the whole truth (step 3 wires the fence; defined here so the
/// key scheme lives in one place).
pub fn outcome_key(txn: &str) -> String {
    format!("txns/{txn}.O")
}

/// A log entry's payload: a full snapshot (self-contained) or a page-delta
/// against the previous seq's image. Wire format: one tag byte then bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum LogPayload {
    Snapshot(Vec<u8>),
    Delta(Vec<u8>), // exactly `delta::encode(&d)` bytes
}

const TAG_SNAPSHOT: u8 = 0;
const TAG_DELTA: u8 = 1;

pub fn encode_entry(p: &LogPayload) -> Vec<u8> {
    let (tag, body): (u8, &[u8]) = match p {
        LogPayload::Snapshot(b) => (TAG_SNAPSHOT, b),
        LogPayload::Delta(b) => (TAG_DELTA, b),
    };
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(tag);
    out.extend_from_slice(body);
    out
}

pub fn decode_entry(bytes: &[u8]) -> Option<LogPayload> {
    match bytes.split_first() {
        Some((&TAG_SNAPSHOT, rest)) => Some(LogPayload::Snapshot(rest.to_vec())),
        Some((&TAG_DELTA, rest)) => Some(LogPayload::Delta(rest.to_vec())),
        _ => None,
    }
}

/// Encode the payload a boat just promoted as a log entry, reusing the exact
/// bytes it already produced (snapshot image or encoded delta).
pub fn entry_from_ship(is_snapshot: bool, bytes: &[u8]) -> Vec<u8> {
    let p = if is_snapshot {
        LogPayload::Snapshot(bytes.to_vec())
    } else {
        LogPayload::Delta(bytes.to_vec())
    };
    encode_entry(&p)
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

/// The object's current durable sequence (0 if none). An 8-byte big-endian
/// counter under `seq_key`.
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

/// Read the whole durable log for `id` and fold it over `base`, in seq order.
/// This is the core of step 2's read path; used in step 1 only as an inert
/// consistency cross-check (it ignores the commit-outcome gate that step 3
/// adds, so it assumes every listed entry is committed).
pub async fn fold_log(
    store: &dyn BlobStore,
    id: &str,
    base: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let mut keys = store.list(&format!("objects/{id}.L.")).await?;
    keys.sort();
    let mut entries = Vec::with_capacity(keys.len());
    for k in &keys {
        if let Some(b) = store.get(k).await?
            && let Some(p) = decode_entry(&b)
        {
            entries.push(p);
        }
    }
    fold(base, &entries)
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
        assert_eq!(decode_entry(&encode_entry(&snap)), Some(snap));
        let d = LogPayload::Delta(vec![9, 8, 7]);
        assert_eq!(decode_entry(&encode_entry(&d)), Some(d));
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
