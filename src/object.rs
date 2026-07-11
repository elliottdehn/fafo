//! One small SQLite database per object.
//!
//! The blob store holds the durable snapshot (`objects/<id>.db`); the file in
//! a worker's live dir is a disposable working copy rebuilt from the blob on
//! activation. Ownership of an object belongs to exactly one worker at a
//! time (the coordinator's routing map decides which), and within a worker
//! everything runs serially — that is the single-writer guarantee, no locks
//! required.

use crate::store::BlobStore;
use rusqlite::Connection;
use std::collections::HashMap;
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
    let base_counter = store
        .get_range(&object_key(id), crate::delta::HEADER_CHANGE_COUNTER as u64, 4)
        .await?
        .filter(|b| b.len() == 4)
        .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]));
    let Some(base_counter) = base_counter else {
        return Ok((Vec::new(), 0)); // no base blob: fresh object
    };

    let cached = std::fs::read(live_path).ok().filter(|b| b.len() >= 100);
    let mut image = match cached {
        // Cache at or past the base: the delta chain bridges the gap and
        // the base download is skipped entirely.
        Some(cache) if crate::delta::change_counter(&cache) >= base_counter => cache,
        _ => store.get(&object_key(id)).await?.unwrap_or_default(),
    };

    let have = crate::delta::change_counter(&image);
    let mut chain_total = 0u32;
    for key in store.list(&crate::delta::delta_prefix(id)).await? {
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
    objects: &mut HashMap<String, LiveObject>,
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
    let _mode: String = conn.query_row("PRAGMA journal_mode=MEMORY", [], |r| r.get(0))?;
    conn.execute_batch("PRAGMA synchronous=OFF; PRAGMA cache_size=-256")?;
    objects.insert(id.to_string(), LiveObject { conn, live_path });
    Ok(())
}

/// Drop the live connection but KEEP the file as a commuter cache. Only
/// valid when the file matches durably shipped state — i.e. after a flush,
/// which every ownership transfer is gated on.
pub fn evict(objects: &mut HashMap<String, LiveObject>, id: &str) {
    objects.remove(id);
}

/// Drop the live connection AND the file. For poisoned state: failed ships,
/// failed local commits — anything where the file may exceed what the blob
/// store confirmed.
pub fn purge(objects: &mut HashMap<String, LiveObject>, id: &str) {
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

        // Cache exactly at the base counter: good enough, no download.
        let live = dir.path().join("t.db");
        std::fs::write(&live, fake_db(5, 0xcc)).unwrap();
        let (image, _) = fetch_image(&store, "t", &live).await.unwrap();
        assert_eq!(image, fake_db(5, 0xcc));

        // A stale cache (counter behind the base) must NOT be trusted.
        std::fs::write(&live, fake_db(3, 0xdd)).unwrap();
        let err = fetch_image(&store, "t", &live).await.unwrap_err();
        assert!(err.to_string().contains("base download"), "stale cache forces a download");
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
    }

    #[tokio::test]
    async fn materialize_evict_purge_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut objects = HashMap::new();

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
