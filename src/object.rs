//! One small SQLite database per object.
//!
//! The blob store holds the durable snapshot (`objects/<id>.db`); the file in
//! a worker's live dir is a disposable working copy rebuilt from the blob on
//! activation. Ownership of an object belongs to exactly one worker at a
//! time (the coordinator's routing map decides which), and within a worker
//! everything runs serially — that is the single-writer guarantee, no locks
//! required.

use crate::Map;
use crate::store::BlobStore;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct LiveObject {
    pub conn: Connection,
    pub live_path: PathBuf,
}

pub fn object_key(id: &str) -> String {
    format!("objects/{id}.db")
}

pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && !id.starts_with('_') // reserved for system state (_worker/, _lease/, _meta/)
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Fetch an object's durable state, delta-aware and cache-aware. Pure blob
/// I/O — no Connection is touched, so this can run in a spawned task while
/// the worker keeps serving other objects.
///
/// The commuter fast path: if a local file survives from a previous stay
/// (evictions for ownership transfer keep it — the transfer was flushed, so
/// the file equals a durably shipped state), peek the base's change counter
/// with a 4-byte ranged read. No compaction while we were away means the
/// delta chain still connects to our cached state: apply only the deltas
/// newer than the cache and skip the base download entirely.
///
/// Returns (file image, total delta chain length in the store).
pub async fn fetch_image(
    store: &Arc<dyn BlobStore>,
    id: &str,
    live_path: &Path,
) -> anyhow::Result<(Vec<u8>, u32)> {
    // STEP 2 (FAFO_LOG_PRIMARY): the log is the source of truth — fold its
    // committed prefix. The base/delta path below becomes a compaction cache
    // only (step 4). The u32 (delta chain length) is meaningless here, so 0.
    if std::env::var_os("FAFO_LOG_PRIMARY").is_some() {
        let (image, _seq) = crate::objlog::fold_committed(store.as_ref(), id, Vec::new()).await?;
        if std::env::var_os("FAFO_DST_LOG").is_some() {
            eprintln!(
                "activate {id}: log-fold {} bytes @seq {_seq}",
                image.len()
            );
        }
        return Ok((image, 0));
    }
    let base_counter = store
        .get_range(&object_key(id), crate::delta::HEADER_CHANGE_COUNTER as u64, 4)
        .await?
        .filter(|b| b.len() == 4)
        .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]));
    let Some(base_counter) = base_counter else {
        return Ok((Vec::new(), 0)); // no base blob: fresh object
    };

    // The durable delta chain, listed up front: it defines how far durable
    // state actually reaches. The commuter cache is only trustworthy up to
    // that point — a cache counter BEYOND every durable delta is an
    // uncommitted local write (a boat the owner applied but that fencing
    // then refused, so its commit record never landed). Trusting such a
    // cache resurrects a fenced write and forks history (found by the DST
    // pause fault: a live node loses its lease, its refused boat's write
    // survives in the cache, a later activation replays it).
    let delta_keys = store.list(&crate::delta::delta_prefix(id)).await?;
    let mut max_durable = base_counter;
    for key in &delta_keys {
        if let Some(counter) = crate::delta::parse_delta_counter(key, id)
            && counter > base_counter
        {
            max_durable = max_durable.max(counter);
        }
    }

    let cached = std::fs::read(live_path).ok().filter(|b| b.len() >= 100);
    let cache_counter = cached.as_deref().map(crate::delta::change_counter);
    let mut image = match cached {
        // Cache within [base, max_durable]: it is a valid checkpoint on the
        // durable chain, so the base download is skipped. A cache BELOW the
        // base (stale) or ABOVE max_durable (uncommitted, ahead of durable)
        // is discarded — download the base and rebuild from durable deltas.
        Some(cache)
            if (base_counter..=max_durable).contains(&crate::delta::change_counter(&cache)) =>
        {
            cache
        }
        _ => store.get(&object_key(id)).await?.unwrap_or_default(),
    };
    if std::env::var_os("FAFO_DST_LOG").is_some() {
        eprintln!(
            "activate {id}: base@{base_counter} cache@{cache_counter:?} max_durable@{max_durable} using@{}",
            crate::delta::change_counter(&image)
        );
    }

    let have = crate::delta::change_counter(&image);
    let mut chain_total = 0u32;
    for key in delta_keys {
        let Some(counter) = crate::delta::parse_delta_counter(&key, id) else {
            continue;
        };
        if counter <= base_counter {
            continue; // superseded by a compaction; GC'd lazily
        }
        chain_total += 1;
        if counter <= have {
            continue; // already reflected in the cached image
        }
        let Some(bytes) = store.get(&key).await? else {
            // Listed but gone: a compaction GC'd it under us mid-fetch.
            // Applying the rest over the gap would silently build a stale
            // image; fail the activation instead — a retry re-reads the
            // fresh base and succeeds.
            anyhow::bail!("delta {key} vanished mid-activation (compaction race); retry");
        };
        crate::delta::apply(&mut image, &crate::delta::decode(&bytes)?);
    }
    Ok((image, chain_total))
}

/// Make a fetched image live: write the working copy and open the
/// connection. Synchronous and cheap — safe inside the worker loop.
pub fn materialize(
    objects: &mut Map<String, LiveObject>,
    id: &str,
    live_dir: &Path,
    image: &[u8],
) -> anyhow::Result<()> {
    let live_path = live_dir.join(format!("{id}.db"));
    if image.is_empty() {
        // Fresh object: opening the connection creates an empty database.
        let _ = std::fs::remove_file(&live_path);
    } else {
        std::fs::write(&live_path, image)?;
    }
    let conn = Connection::open(&live_path)?;
    // The live file is scratch — the blob store is the source of truth — so
    // trade its crash-safety away for speed. cache_size is capped tight
    // (256 KB) because a node may hold hundreds of live connections and
    // SQLite's 2 MB default would quietly eat the container's RAM.
    // foreign_keys must be set HERE: "invariants live in SQL" is the
    // contract, SQLite defaults enforcement off, and a PRAGMA inside a
    // transaction (the only place API ops run) is a silent no-op.
    let _mode: String = conn.query_row("PRAGMA journal_mode=MEMORY", [], |r| r.get(0))?;
    conn.execute_batch("PRAGMA synchronous=OFF; PRAGMA cache_size=-256; PRAGMA foreign_keys=ON")?;
    objects.insert(id.to_string(), LiveObject { conn, live_path });
    Ok(())
}

/// Drop the live connection but KEEP the file as a commuter cache. Only
/// valid when the file matches durably shipped state — i.e. after a flush,
/// which every ownership transfer is gated on.
pub fn evict(objects: &mut Map<String, LiveObject>, id: &str) {
    objects.remove(id);
}

/// Drop the live connection AND the file. For poisoned state: failed ships,
/// failed local commits — anything where the file may exceed what the blob
/// store confirmed.
pub fn purge(objects: &mut Map<String, LiveObject>, id: &str) {
    if let Some(LiveObject { conn, live_path }) = objects.remove(id) {
        drop(conn); // close before unlinking
        let _ = std::fs::remove_file(live_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::{Delta, HEADER_CHANGE_COUNTER, delta_key, encode};
    use crate::store::FsBlobStore;

    #[test]
    fn valid_ids_are_the_documented_charset() {
        for good in ["a", "room-42", "user_77", "A-Z_0-9", &"x".repeat(64)] {
            assert!(valid_id(good), "{good:?} should be valid");
        }
        for bad in [
            "",                  // empty
            &"x".repeat(65),     // too long
            "_meta",             // leading underscore is system space
            "a/b",               // path separator
            "a.db",              // '.' would collide with blob suffixes
            "café",              // ascii only
            "a b",               // whitespace
        ] {
            assert!(!valid_id(bad), "{bad:?} should be rejected");
        }
    }

    fn fake_db(counter: u32, fill: u8) -> Vec<u8> {
        let mut f = vec![fill; 4096];
        f[16..18].copy_from_slice(&4096u16.to_be_bytes());
        f[HEADER_CHANGE_COUNTER..HEADER_CHANGE_COUNTER + 4]
            .copy_from_slice(&counter.to_be_bytes());
        f
    }

    fn page_with_counter(counter: u32, fill: u8) -> Vec<u8> {
        fake_db(counter, fill)
    }

    async fn harness() -> (tempfile::TempDir, Arc<dyn BlobStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> =
            Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());
        (dir, store)
    }

    #[tokio::test]
    async fn fetch_image_of_a_fresh_object_is_empty() {
        let (dir, store) = harness().await;
        let (image, chain) = fetch_image(&store, "new", &dir.path().join("new.db"))
            .await
            .unwrap();
        assert!(image.is_empty());
        assert_eq!(chain, 0);
    }

    #[tokio::test]
    async fn fetch_image_applies_only_deltas_newer_than_the_base() {
        let (dir, store) = harness().await;
        store.put(&object_key("t"), &fake_db(5, 0xaa)).await.unwrap();
        // Counter 4 is superseded by the base; 6 must apply on top of it.
        store
            .put(
                &delta_key("t", 4),
                &encode(&Delta {
                    counter: 4,
                    file_len: 4096,
                    page_size: 4096,
                    pages: vec![(0, page_with_counter(4, 0x11))],
                }),
            )
            .await
            .unwrap();
        store
            .put(
                &delta_key("t", 6),
                &encode(&Delta {
                    counter: 6,
                    file_len: 4096,
                    page_size: 4096,
                    pages: vec![(0, page_with_counter(6, 0xbb))],
                }),
            )
            .await
            .unwrap();

        // A key in delta position that isn't a delta is skipped, not fatal.
        store.put("objects/t.d.not-a-counter", b"junk").await.unwrap();

        let (image, chain) = fetch_image(&store, "t", &dir.path().join("t.db"))
            .await
            .unwrap();
        assert_eq!(image, page_with_counter(6, 0xbb));
        assert_eq!(chain, 1, "superseded deltas don't count toward the chain");
    }

    #[tokio::test]
    async fn commuter_cache_at_or_past_the_base_skips_the_download() {
        // A store whose full GET of the base blob fails: the only way
        // fetch_image can succeed is by trusting the local cache.
        struct NoBaseDownload(FsBlobStore);
        #[async_trait::async_trait]
        impl BlobStore for NoBaseDownload {
            async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                anyhow::ensure!(!key.ends_with(".db"), "base download not allowed: {key}");
                self.0.get(key).await
            }
            async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.0.put(key, bytes).await
            }
            async fn delete(&self, key: &str) -> anyhow::Result<()> {
                self.0.delete(key).await
            }
            async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
                self.0.list(prefix).await
            }
            async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
                self.0.create(key, bytes).await
            }
            async fn get_range(
                &self,
                key: &str,
                o: u64,
                l: u64,
            ) -> anyhow::Result<Option<Vec<u8>>> {
                self.0.get_range(key, o, l).await
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let inner = FsBlobStore::new(dir.path().join("blobs")).unwrap();
        inner.put(&object_key("t"), &fake_db(5, 0xaa)).await.unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(NoBaseDownload(inner));

        // Cache PAST the base counter: good enough, no download — and a
        // delta the cache already reflects (counter 6 ≤ cached 7) is
        // skipped rather than re-applied.
        store
            .put(
                &delta_key("t", 6),
                &encode(&Delta {
                    counter: 6,
                    file_len: 4096,
                    page_size: 4096,
                    pages: vec![(0, fake_db(6, 0x66))],
                }),
            )
            .await
            .unwrap();
        // Cache AT the durable frontier (counter 6 = base 5 + delta 6):
        // a valid checkpoint, served untouched with no base download, and
        // the delta it already reflects (6 <= 6) isn't re-applied.
        let live = dir.path().join("t.db");
        std::fs::write(&live, fake_db(6, 0xcc)).unwrap();
        let (image, _) = fetch_image(&store, "t", &live).await.unwrap();
        assert_eq!(image, fake_db(6, 0xcc), "cached bytes served untouched");

        // A cache AHEAD of durable (counter 7 > max durable 6) holds an
        // uncommitted local write — a boat fencing refused, so no delta 7
        // ever landed. Trusting it would resurrect the fenced write, so it
        // is discarded and the base is downloaded (blocked here -> error).
        std::fs::write(&live, fake_db(7, 0xcc)).unwrap();
        let err = fetch_image(&store, "t", &live).await.unwrap_err();
        assert!(err.to_string().contains("base download"), "cache ahead of durable forces a download");

        // A stale cache (counter behind the base) must NOT be trusted.
        std::fs::write(&live, fake_db(3, 0xdd)).unwrap();
        let err = fetch_image(&store, "t", &live).await.unwrap_err();
        assert!(err.to_string().contains("base download"), "stale cache forces a download");

        // The wrapper's pass-throughs exist only to satisfy the trait.
        store.put("scratch", b"1").await.unwrap();
        assert!(store.create("scratch2", b"1").await.unwrap());
        store.delete("scratch").await.unwrap();
    }

    #[tokio::test]
    async fn a_vanished_mid_chain_delta_fails_activation_loudly() {
        // list() advertises a delta that get() cannot produce (GC race):
        // activation must error rather than build a gapped image.
        struct PhantomDelta(FsBlobStore);
        #[async_trait::async_trait]
        impl BlobStore for PhantomDelta {
            async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                self.0.get(key).await
            }
            async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.0.put(key, bytes).await
            }
            async fn delete(&self, key: &str) -> anyhow::Result<()> {
                self.0.delete(key).await
            }
            async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
                let mut keys = self.0.list(prefix).await?;
                keys.push(delta_key("t", 6));
                keys.sort();
                Ok(keys)
            }
            async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
                self.0.create(key, bytes).await
            }
            async fn get_range(
                &self,
                key: &str,
                o: u64,
                l: u64,
            ) -> anyhow::Result<Option<Vec<u8>>> {
                self.0.get_range(key, o, l).await
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let inner = FsBlobStore::new(dir.path().join("blobs")).unwrap();
        inner.put(&object_key("t"), &fake_db(5, 0xaa)).await.unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(PhantomDelta(inner));
        let err = fetch_image(&store, "t", &dir.path().join("t.db"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("vanished"), "got: {err}");

        // The wrapper's pass-throughs exist only to satisfy the trait.
        store.put("scratch", b"1").await.unwrap();
        assert!(store.create("scratch2", b"1").await.unwrap());
        store.delete("scratch").await.unwrap();
        assert_eq!(store.get_range("scratch2", 0, 1).await.unwrap().unwrap(), b"1");
    }

    #[tokio::test]
    async fn materialize_evict_purge_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut objects = Map::default();

        // Fresh object: an empty image opens as an empty database.
        materialize(&mut objects, "x", dir.path(), &[]).unwrap();
        let obj = objects.get("x").unwrap();
        obj.conn.execute_batch("CREATE TABLE t (n)").unwrap();
        let live_path = obj.live_path.clone();

        // Evict keeps the file (commuter cache); purge deletes it.
        evict(&mut objects, "x");
        assert!(live_path.exists(), "evict keeps the working copy");
        let image = std::fs::read(&live_path).unwrap();
        materialize(&mut objects, "x", dir.path(), &image).unwrap();
        purge(&mut objects, "x");
        assert!(!live_path.exists(), "purge removes the working copy");
    }
}
