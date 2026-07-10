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

/// Ensure `id` is live in this worker's cache, loading the durable snapshot
/// if needed. Careful: no `&Connection` may be held across an await
/// (Connection is Send but not Sync), so the blob fetch happens before any
/// connection is touched.
pub async fn activate(
    objects: &mut HashMap<String, LiveObject>,
    id: &str,
    store: &Arc<dyn BlobStore>,
    live_dir: &Path,
) -> anyhow::Result<()> {
    if objects.contains_key(id) {
        return Ok(());
    }
    let blob = store.get(&object_key(id)).await?;
    let live_path = live_dir.join(format!("{id}.db"));
    match blob {
        Some(bytes) => std::fs::write(&live_path, bytes)?,
        // Fresh object: opening the connection creates an empty database.
        None => {
            let _ = std::fs::remove_file(&live_path);
        }
    }
    let conn = Connection::open(&live_path)?;
    // The live file is scratch — the blob store is the source of truth — so
    // trade its crash-safety away for speed.
    let _mode: String = conn.query_row("PRAGMA journal_mode=MEMORY", [], |r| r.get(0))?;
    conn.execute_batch("PRAGMA synchronous=OFF")?;
    objects.insert(id.to_string(), LiveObject { conn, live_path });
    Ok(())
}

/// Drop cached state so the next activation reloads from the durable blob.
/// Used on migration (ownership moved to another worker) and whenever local
/// state may have outrun what the blob store confirmed.
pub fn evict(objects: &mut HashMap<String, LiveObject>, id: &str) {
    if let Some(obj) = objects.remove(id) {
        let path = obj.live_path.clone();
        drop(obj);
        let _ = std::fs::remove_file(path);
    }
}
