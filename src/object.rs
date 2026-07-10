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
            continue;
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
    if let Some(obj) = objects.remove(id) {
        let path = obj.live_path.clone();
        drop(obj);
        let _ = std::fs::remove_file(path);
    }
}
