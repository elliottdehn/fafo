//! Page-level delta shipping for large objects.
//!
//! A boat normally ships an object's whole snapshot — O(db size) per commit,
//! which is wrong for db-per-tenant objects. Instead, the worker keeps a
//! page-hash manifest per live object and ships only the pages that changed
//! since the last boat, as one delta blob.
//!
//! Versioning rides on SQLite itself: the file header's change counter
//! (bytes 24..28, big-endian, incremented on every commit in rollback-journal
//! modes) names each state. A delta produces the state with its counter;
//! the base snapshot embeds its own. Activation applies, in order, exactly
//! the deltas newer than the base — which makes compaction crash-safe with
//! no manifest: put the new base, then delete old deltas at leisure; any
//! stragglers are ≤ the base's counter and get ignored, then GC'd later.
//!
//! Deltas are PHYSICAL (pages, not SQL). Logical replay would be smaller
//! still, but imports a determinism requirement (CURRENT_TIMESTAMP,
//! random()) that silently forks replicas. Pages don't care what the SQL
//! meant.

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

/// Objects smaller than this always ship as full snapshots.
pub const DELTA_MIN_BYTES: u64 = 64 * 1024;
/// Compact (ship a snapshot, reset the chain) when the chain grows past
/// this many deltas...
pub const COMPACT_CHAIN: u32 = 16;
/// ...or when a single delta would carry more than this fraction of the file.
pub const COMPACT_FRACTION_DENOM: u64 = 3;

const HEADER_PAGE_SIZE: usize = 16;
pub const HEADER_CHANGE_COUNTER: usize = 24;

pub fn page_size(file: &[u8]) -> usize {
    if file.len() < 100 {
        return 4096;
    }
    let raw = u16::from_be_bytes([file[HEADER_PAGE_SIZE], file[HEADER_PAGE_SIZE + 1]]);
    match raw {
        1 => 65536, // SQLite encodes 64 KiB as 1
        0 => 4096,
        n => n as usize,
    }
}

/// The SQLite file change counter: a monotonic version for the whole file,
/// maintained by SQLite itself on every committing write.
pub fn change_counter(file: &[u8]) -> u32 {
    if file.len() < 100 {
        return 0;
    }
    u32::from_be_bytes([
        file[HEADER_CHANGE_COUNTER],
        file[HEADER_CHANGE_COUNTER + 1],
        file[HEADER_CHANGE_COUNTER + 2],
        file[HEADER_CHANGE_COUNTER + 3],
    ])
}

fn hash_page(page: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    h.write(page);
    h.finish()
}

/// Per-object memory of the last-shipped state: one u64 per page (~0.2% of
/// the object's size). Rebuilt on activation, dropped on evict.
pub struct Manifest {
    pub page_size: usize,
    pub hashes: Vec<u64>,
    pub chain_len: u32,
}

impl Manifest {
    pub fn of(file: &[u8], chain_len: u32) -> Self {
        let page_size = page_size(file);
        let hashes = file.chunks(page_size).map(hash_page).collect();
        Self {
            page_size,
            hashes,
            chain_len,
        }
    }
}

/// One boat's changes to one object.
pub struct Delta {
    /// The state this delta produces (the file's change counter after it).
    pub counter: u32,
    pub file_len: u64,
    pub page_size: u32,
    /// (page index, page bytes) — the last page may be short.
    pub pages: Vec<(u32, Vec<u8>)>,
}

/// Diff `file` against the manifest, updating the manifest in place to
/// describe `file`. Returns the changed pages.
pub fn diff(manifest: &mut Manifest, file: &[u8]) -> Delta {
    let ps = manifest.page_size;
    let mut pages = Vec::new();
    let mut new_hashes = Vec::with_capacity(file.len().div_ceil(ps));
    for (i, chunk) in file.chunks(ps).enumerate() {
        let h = hash_page(chunk);
        new_hashes.push(h);
        if manifest.hashes.get(i) != Some(&h) {
            pages.push((i as u32, chunk.to_vec()));
        }
    }
    manifest.hashes = new_hashes;
    Delta {
        counter: change_counter(file),
        file_len: file.len() as u64,
        page_size: ps as u32,
        pages,
    }
}

/// Apply a delta to a file image: overwrite pages, then fix the length
/// (handles both growth and VACUUM-style shrink).
pub fn apply(file: &mut Vec<u8>, delta: &Delta) {
    let ps = delta.page_size as usize;
    file.resize(delta.file_len as usize, 0);
    for (page_no, bytes) in &delta.pages {
        let start = *page_no as usize * ps;
        let end = (start + bytes.len()).min(file.len());
        if start < file.len() {
            file[start..end].copy_from_slice(&bytes[..end - start]);
        }
    }
}

/// Wire format: fixed little-endian header, then pages.
///   [counter u32][file_len u64][page_size u32][n_pages u32]
///   n × ([page_no u32][len u32][bytes])
pub fn encode(delta: &Delta) -> Vec<u8> {
    let body: usize = delta.pages.iter().map(|(_, b)| 8 + b.len()).sum();
    let mut out = Vec::with_capacity(20 + body);
    out.extend_from_slice(&delta.counter.to_le_bytes());
    out.extend_from_slice(&delta.file_len.to_le_bytes());
    out.extend_from_slice(&delta.page_size.to_le_bytes());
    out.extend_from_slice(&(delta.pages.len() as u32).to_le_bytes());
    for (page_no, bytes) in &delta.pages {
        out.extend_from_slice(&page_no.to_le_bytes());
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

pub fn decode(bytes: &[u8]) -> anyhow::Result<Delta> {
    anyhow::ensure!(bytes.len() >= 20, "delta too short");
    let counter = u32::from_le_bytes(bytes[0..4].try_into()?);
    let file_len = u64::from_le_bytes(bytes[4..12].try_into()?);
    let page_size = u32::from_le_bytes(bytes[12..16].try_into()?);
    let n = u32::from_le_bytes(bytes[16..20].try_into()?) as usize;
    // Every page costs at least its 8-byte header, so a count the buffer
    // cannot possibly hold is corruption — reject it before trusting it
    // with an allocation.
    anyhow::ensure!(n <= (bytes.len() - 20) / 8, "delta page count exceeds payload");
    let mut pages = Vec::with_capacity(n);
    let mut at = 20;
    for _ in 0..n {
        anyhow::ensure!(bytes.len() >= at + 8, "truncated delta page header");
        let page_no = u32::from_le_bytes(bytes[at..at + 4].try_into()?);
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into()?) as usize;
        at += 8;
        anyhow::ensure!(bytes.len() >= at + len, "truncated delta page");
        pages.push((page_no, bytes[at..at + len].to_vec()));
        at += len;
    }
    Ok(Delta {
        counter,
        file_len,
        page_size,
        pages,
    })
}

/// Blob keys: deltas live beside the base, zero-padded so lexicographic
/// list order is numeric apply order.
pub fn delta_key(object: &str, counter: u32) -> String {
    format!("objects/{object}.d.{counter:010}")
}

pub fn delta_prefix(object: &str) -> String {
    format!("objects/{object}.d.")
}

pub fn parse_delta_counter(key: &str, object: &str) -> Option<u32> {
    key.strip_prefix(&delta_prefix(object))?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_db(pages: usize, ps: usize, counter: u32, fill: u8) -> Vec<u8> {
        let mut f = vec![fill; pages * ps];
        // minimal SQLite-ish header fields we rely on
        let encoded_ps: u16 = if ps == 65536 { 1 } else { ps as u16 };
        f[16..18].copy_from_slice(&encoded_ps.to_be_bytes());
        f[24..28].copy_from_slice(&counter.to_be_bytes());
        f
    }

    #[test]
    fn diff_apply_roundtrip_reconstructs_exactly() {
        let old = fake_db(8, 4096, 5, 0xaa);
        let mut manifest = Manifest::of(&old, 0);

        let mut new = old.clone();
        new[24..28].copy_from_slice(&6u32.to_be_bytes()); // counter bump (page 0)
        new[3 * 4096 + 17] = 0x77; // mutate page 3
        new.extend(vec![0xbb; 4096]); // grow by a page

        let delta = diff(&mut manifest, &new);
        assert_eq!(delta.counter, 6);
        // pages 0 (header), 3 (mutation), 8 (growth) — and nothing else
        let changed: Vec<u32> = delta.pages.iter().map(|(n, _)| *n).collect();
        assert_eq!(changed, vec![0, 3, 8]);

        let encoded = encode(&delta);
        let decoded = decode(&encoded).unwrap();
        let mut rebuilt = old.clone();
        apply(&mut rebuilt, &decoded);
        assert_eq!(rebuilt, new, "byte-exact reconstruction");
    }

    #[test]
    fn apply_handles_shrink() {
        let old = fake_db(8, 4096, 5, 0xaa);
        let mut manifest = Manifest::of(&old, 0);
        let mut new = old[..4 * 4096].to_vec(); // VACUUM-style shrink
        new[24..28].copy_from_slice(&6u32.to_be_bytes());
        let delta = diff(&mut manifest, &new);
        let mut rebuilt = old.clone();
        apply(&mut rebuilt, &decode(&encode(&delta)).unwrap());
        assert_eq!(rebuilt, new);
    }

    #[test]
    fn unchanged_file_diffs_to_nothing() {
        let db = fake_db(8, 4096, 5, 0xaa);
        let mut manifest = Manifest::of(&db, 0);
        let delta = diff(&mut manifest, &db);
        assert!(delta.pages.is_empty(), "identical bytes ship zero pages");
        assert_eq!(delta.counter, 5);
        assert_eq!(delta.file_len, db.len() as u64);
    }

    #[test]
    fn page_size_reads_sqlite_encodings() {
        assert_eq!(page_size(&fake_db(2, 512, 1, 0)), 512);
        assert_eq!(page_size(&fake_db(1, 65536, 1, 0)), 65536, "64 KiB is encoded as 1");
        assert_eq!(page_size(b"short"), 4096, "headerless files get the default");
        assert_eq!(page_size(&[0u8; 100]), 4096, "a zeroed header gets the default too");
    }

    #[test]
    fn change_counter_of_a_headerless_file_is_zero() {
        assert_eq!(change_counter(&[]), 0);
        assert_eq!(change_counter(&[0u8; 99]), 0);
        assert_eq!(change_counter(&fake_db(1, 512, 42, 0)), 42);
    }

    #[test]
    fn decode_rejects_corruption_without_panicking() {
        assert!(decode(&[]).is_err(), "empty");
        assert!(decode(&[0u8; 19]).is_err(), "shorter than the header");

        // A header claiming u32::MAX pages in a 28-byte buffer must be
        // rejected up front, not fed to an allocator.
        let mut evil = vec![0u8; 28];
        evil[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&evil).is_err(), "absurd page count");

        // One page whose declared length runs past the buffer.
        let mut truncated = encode(&Delta {
            counter: 1,
            file_len: 4096,
            page_size: 4096,
            pages: vec![(0, vec![0xaa; 4096])],
        });
        truncated.truncate(100);
        assert!(decode(&truncated).is_err(), "truncated page body");
    }

    #[test]
    fn delta_keys_list_in_apply_order() {
        // Zero-padding is what makes lexicographic list order numeric.
        let mut keys = [delta_key("t", 100), delta_key("t", 2), delta_key("t", 30)];
        keys.sort();
        let counters: Vec<u32> = keys
            .iter()
            .map(|k| parse_delta_counter(k, "t").unwrap())
            .collect();
        assert_eq!(counters, vec![2, 30, 100]);
        assert_eq!(parse_delta_counter("objects/t.d.junk", "t"), None);
        assert_eq!(parse_delta_counter(&delta_key("t", 7), "other"), None);
    }

    /// The whole scheme leans on SQLite bumping the header change counter
    /// on every committing write in rollback-journal modes. Verify against
    /// the real SQLite, exactly as the worker configures it.
    #[test]
    fn sqlite_change_counter_increments_per_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        let _: String = conn
            .query_row("PRAGMA journal_mode=MEMORY", [], |r| r.get(0))
            .unwrap();
        conn.execute_batch("PRAGMA synchronous=OFF").unwrap();
        conn.execute_batch("CREATE TABLE t (n INTEGER)").unwrap();
        let c1 = change_counter(&std::fs::read(&path).unwrap());
        conn.execute("INSERT INTO t VALUES (1)", []).unwrap();
        let c2 = change_counter(&std::fs::read(&path).unwrap());
        conn.execute("INSERT INTO t VALUES (2)", []).unwrap();
        let c3 = change_counter(&std::fs::read(&path).unwrap());
        assert!(c1 < c2 && c2 < c3, "counter must increment per commit: {c1} {c2} {c3}");
    }
}
