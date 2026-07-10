//! Resource governor: mold the node to its container.
//!
//! Cloudflare Containers instances range from lite (1/16 vCPU, 256 MiB RAM,
//! 2 GB disk) to standard-4 (4 vCPU, 12 GiB, 20 GB). Budgets are derived
//! from the detected memory limit (cgroups, which the platform sets) and
//! scale with the instance — no per-instance-type configuration:
//!
//!   disk budget      what the ledger lets local files occupy (live + cache)
//!   max boat bytes   RAM ceiling for one shipment; bigger backlogs split
//!                    into consecutive boats along txn-group boundaries
//!   activation permits  concurrent cold-object fetches held in RAM
//!
//! The disk ledger tracks every local working file. Over budget it deletes
//! commuter-cache files LRU-first (always safe — they're an optimization),
//! then asks the heaviest worker to shed idle live objects, which turn into
//! cache and become deletable.

use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub memory_bytes: u64,
    pub disk_budget: u64,
    pub max_boat_bytes: u64,
    pub activation_permits: usize,
}

impl Limits {
    /// Detect from the environment: MEMORY_MB / DISK_MB env vars win, else
    /// the cgroup memory limit (v2 then v1), else a 4 GiB dev default.
    /// Disk defaults to the Cloudflare ratio (~1.6x memory) unless given.
    pub fn detect() -> Self {
        let memory_bytes = env_bytes("MEMORY_MB")
            .or_else(cgroup_memory_limit)
            .unwrap_or(4 * 1024 * 1024 * 1024);
        let disk_total = env_bytes("DISK_MB").unwrap_or(memory_bytes + (memory_bytes * 2) / 3);
        Self::derive(memory_bytes, disk_total)
    }

    pub fn derive(memory_bytes: u64, disk_total: u64) -> Self {
        Self {
            memory_bytes,
            // Leave headroom for the image, logs, and temp files.
            disk_budget: (disk_total * 6) / 10,
            // One boat may hold this much payload in RAM. Clamp so lite
            // still ships multi-MB objects and standard-4 doesn't hoard.
            max_boat_bytes: (memory_bytes / 8).clamp(16 * 1024 * 1024, 1024 * 1024 * 1024),
            activation_permits: if memory_bytes >= 4 * 1024 * 1024 * 1024 { 4 } else { 2 },
        }
    }

    /// Default optimistic-backpressure watermark when MAX_UNSHIPPED_MB is
    /// not set explicitly: same scale as a boat.
    pub fn default_max_unshipped(&self) -> u64 {
        self.max_boat_bytes
    }
}

fn env_bytes(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()?
        .parse::<u64>()
        .ok()
        .map(|mb| mb * 1024 * 1024)
}

fn cgroup_memory_limit() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",                    // cgroup v2
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",  // cgroup v1
    ] {
        if let Ok(raw) = std::fs::read_to_string(path) {
            let raw = raw.trim();
            if raw == "max" {
                continue;
            }
            if let Ok(v) = raw.parse::<u64>() {
                // Absurd v1 sentinel values mean "unlimited".
                if v < 1 << 46 {
                    return Some(v);
                }
            }
        }
    }
    None
}

// ------------------------------------------------------------- disk ledger

#[derive(Clone, Copy, PartialEq)]
pub enum FileState {
    /// Has an open connection; only its worker may release it (via Shed).
    Live,
    /// Commuter cache: safe for the ledger to delete at any moment.
    Cache,
}

struct Entry {
    bytes: u64,
    state: FileState,
    worker: usize,
    touched: u64,
}

pub struct DiskLedger {
    budget: u64,
    used: u64,
    seq: u64,
    files: HashMap<PathBuf, Entry>,
}

/// What enforcement decided; the caller (who can reach worker channels)
/// carries out the shed request.
pub struct Enforcement {
    pub deleted_cache_bytes: u64,
    /// Still over budget: ask this worker to shed idle live objects.
    pub shed_from_worker: Option<usize>,
}

impl DiskLedger {
    pub fn new(budget: u64) -> Self {
        Self {
            budget,
            used: 0,
            seq: 0,
            files: HashMap::new(),
        }
    }

    pub fn used(&self) -> u64 {
        self.used
    }

    fn upsert(&mut self, path: PathBuf, bytes: u64, state: FileState, worker: usize) {
        self.seq += 1;
        let touched = self.seq;
        if let Some(prev) = self.files.insert(
            path,
            Entry {
                bytes,
                state,
                worker,
                touched,
            },
        ) {
            self.used -= prev.bytes;
        }
        self.used += bytes;
    }

    pub fn set_live(&mut self, path: PathBuf, bytes: u64, worker: usize) {
        self.upsert(path, bytes, FileState::Live, worker);
    }

    pub fn set_cache(&mut self, path: PathBuf, worker: usize) {
        match std::fs::metadata(&path) {
            Ok(m) => self.upsert(path, m.len(), FileState::Cache, worker),
            // File never materialized (or already gone): drop the entry.
            Err(_) => self.remove(&path),
        }
    }

    pub fn touch(&mut self, path: &PathBuf, bytes: u64) {
        self.seq += 1;
        if let Some(e) = self.files.get_mut(path) {
            self.used = self.used - e.bytes + bytes;
            e.bytes = bytes;
            e.touched = self.seq;
        }
    }

    pub fn remove(&mut self, path: &PathBuf) {
        if let Some(e) = self.files.remove(path) {
            self.used -= e.bytes;
        }
    }

    /// Delete LRU cache files until under budget; if caches alone don't
    /// cover it, name the worker with the most live bytes for shedding.
    pub fn enforce(&mut self) -> Enforcement {
        let mut deleted = 0u64;
        while self.used > self.budget {
            let victim = self
                .files
                .iter()
                .filter(|(_, e)| e.state == FileState::Cache)
                .min_by_key(|(_, e)| e.touched)
                .map(|(p, _)| p.clone());
            let Some(path) = victim else { break };
            let _ = std::fs::remove_file(&path);
            if let Some(e) = self.files.remove(&path) {
                self.used -= e.bytes;
                deleted += e.bytes;
            }
        }
        let shed_from_worker = if self.used > self.budget {
            let mut live_by_worker: HashMap<usize, u64> = HashMap::new();
            for e in self.files.values() {
                if e.state == FileState::Live {
                    *live_by_worker.entry(e.worker).or_default() += e.bytes;
                }
            }
            live_by_worker.into_iter().max_by_key(|(_, b)| *b).map(|(w, _)| w)
        } else {
            None
        };
        Enforcement {
            deleted_cache_bytes: deleted,
            shed_from_worker,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_deletes_lru_cache_first_then_asks_for_shed() {
        let dir = tempfile::tempdir().unwrap();
        let mk = |name: &str, len: usize| {
            let p = dir.path().join(name);
            std::fs::write(&p, vec![0u8; len]).unwrap();
            p
        };
        let mut ledger = DiskLedger::new(1000);
        let live = mk("live.db", 600);
        let old_cache = mk("old.db", 300);
        let new_cache = mk("new.db", 300);
        ledger.set_live(live.clone(), 600, 7);
        ledger.set_cache(old_cache.clone(), 7);
        ledger.set_cache(new_cache.clone(), 7);
        assert_eq!(ledger.used(), 1200);

        let e = ledger.enforce();
        // Oldest cache deleted, budget satisfied, no shed needed.
        assert_eq!(e.deleted_cache_bytes, 300);
        assert!(!old_cache.exists());
        assert!(new_cache.exists());
        assert!(e.shed_from_worker.is_none());

        // Live files alone exceed a tighter budget: shed request names w7.
        let mut tight = DiskLedger::new(100);
        tight.set_live(live, 600, 7);
        let e = tight.enforce();
        assert_eq!(e.shed_from_worker, Some(7));
    }

    #[test]
    fn limits_scale_with_instance() {
        let lite = Limits::derive(256 * 1024 * 1024, 2 * 1024 * 1024 * 1024);
        assert_eq!(lite.max_boat_bytes, 32 * 1024 * 1024); // mem/8, above the 16 MiB floor
        assert_eq!(lite.activation_permits, 2);
        let std4 = Limits::derive(12 * 1024 * 1024 * 1024, 20 * 1024 * 1024 * 1024);
        assert_eq!(std4.max_boat_bytes, 1024 * 1024 * 1024); // clamped ceiling
        assert_eq!(std4.activation_permits, 4);
        let std1 = Limits::derive(4 * 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        assert_eq!(std1.max_boat_bytes, 512 * 1024 * 1024); // mem/8, unclamped
        assert_eq!(std4.disk_budget, (20u64 * 1024 * 1024 * 1024) * 6 / 10);
    }
}
